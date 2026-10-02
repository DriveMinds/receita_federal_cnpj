"""Acesso ao servidor da RFB: listagem barata, assinatura do ZIP via Range e download com retomada.

Princípio: gastar o mínimo de requisições. Uma listagem (1-2 requisições) já diz o tamanho/etag/data de
cada ZIP; só se algo mudou olhamos o diretório central do ZIP (1 requisição Range de ~64 KB) e só se o
conteúdo realmente mudou baixamos o arquivo.
"""
import io
import logging
import re
import threading
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, unquote, urljoin
from xml.etree import ElementTree as ET

import requests

log = logging.getLogger(__name__)

VERSION_RE = re.compile(r"^\d{4}-\d{2}$")
RETRY_STATUS = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class RemoteFile:
    name: str
    version: str
    url: str
    size: int
    etag: str | None = None
    last_modified: str | None = None

    @property
    def fingerprint(self) -> str:
        return f"{self.size}|{self.etag or ''}|{self.last_modified or ''}"


class Http:
    """Sessão HTTP educada com o servidor: intervalo mínimo entre requisições, retry com backoff e Retry-After."""

    def __init__(self, interval=1.0, retries=6, timeout=60, auth=None, headers=None):
        self.session = requests.Session()
        self.session.auth = auth
        self.session.headers.update({"User-Agent": "rfb-cnpj-sync/1.0 (+dados abertos; uso moderado)"})
        self.session.headers.update(headers or {})
        self.interval, self.retries, self.timeout = interval, retries, timeout
        self._lock = threading.Lock()
        self._next = 0.0
        self.request_count = 0

    def _throttle(self):
        with self._lock:
            wait = self._next - time.monotonic()
            self._next = max(time.monotonic(), self._next) + self.interval
            self.request_count += 1
        if wait > 0:
            time.sleep(wait)

    def request(self, method, url, stream=False, **kw) -> requests.Response:
        for attempt in range(self.retries + 1):
            self._throttle()
            try:
                r = self.session.request(method, url, stream=stream, timeout=self.timeout, **kw)
            except (requests.ConnectionError, requests.Timeout) as exc:
                if attempt == self.retries:
                    raise
                delay = min(2 ** attempt * 2, 120)
                log.warning("%s %s falhou (%s); nova tentativa em %ss", method, url, exc, delay)
                time.sleep(delay)
                continue
            if r.status_code in RETRY_STATUS and attempt < self.retries:
                try:
                    delay = min(int(r.headers.get("Retry-After", "")), 300)
                except ValueError:
                    delay = min(2 ** attempt * 2, 120)
                log.warning("%s %s -> %s; nova tentativa em %ss", method, url, r.status_code, delay)
                r.close()
                time.sleep(delay)
                continue
            r.raise_for_status()
            return r
        raise RuntimeError("unreachable")


# ---------------------------------------------------------------- fontes

class WebDavSource:
    """Compartilhamento público Nextcloud da RFB. Um PROPFIND devolve tamanho, etag e data de todos os arquivos."""

    PROPFIND = ('<?xml version="1.0"?><d:propfind xmlns:d="DAV:"><d:prop><d:getlastmodified/>'
                '<d:getcontentlength/><d:getetag/><d:resourcetype/></d:prop></d:propfind>')

    def __init__(self, base_url, token, http: Http):
        self.root = f"{base_url}/public.php/webdav/"
        self.http = http
        http.session.auth = (token, "")
        http.session.headers["X-Requested-With"] = "XMLHttpRequest"

    def _propfind(self, path=""):
        r = self.http.request("PROPFIND", self.root + path, data=self.PROPFIND, headers={"Depth": "1"})
        ns = {"d": "DAV:"}
        out = []
        for resp in ET.fromstring(r.content).findall("d:response", ns):
            name = unquote(resp.findtext("d:href", "", ns).rstrip("/").rsplit("/", 1)[-1])
            prop = next((p.find("d:prop", ns) for p in resp.findall("d:propstat", ns)
                         if " 200 " in (p.findtext("d:status", "", ns) or "")), None)
            if prop is None:
                continue
            out.append({
                "name": name,
                "dir": prop.find("d:resourcetype/d:collection", ns) is not None,
                "size": int(prop.findtext("d:getcontentlength", "0", ns) or 0),
                "etag": (prop.findtext("d:getetag", None, ns) or "").strip('"') or None,
                "mtime": prop.findtext("d:getlastmodified", None, ns),
            })
        return out

    def versions(self):
        return sorted(e["name"] for e in self._propfind() if e["dir"] and VERSION_RE.match(e["name"]))

    def list_files(self, version):
        entries = self._propfind(quote(version) + "/" if version else "")
        return [RemoteFile(e["name"], version, self.root + (quote(version) + "/" if version else "") + quote(e["name"]),
                           e["size"], e["etag"], e["mtime"])
                for e in entries if not e["dir"] and e["name"].lower().endswith(".zip")]


class IndexSource:
    """Listagem HTTP (autoindex). Tamanhos do índice vêm arredondados, então fazemos um HEAD por ZIP."""

    def __init__(self, base_url, http: Http):
        self.root = base_url.rstrip("/") + "/"
        self.http = http

    def _links(self, url):
        html = self.http.request("GET", url).text
        return [unquote(h) for h in re.findall(r'href="([^"?#]+)"', html, re.I)
                if not h.startswith(("/", "http", ".."))]

    def versions(self):
        return sorted(h.rstrip("/") for h in self._links(self.root) if VERSION_RE.match(h.rstrip("/")) and h.endswith("/"))

    def list_files(self, version):
        base = self.root + (version + "/" if version else "")
        out = []
        for h in self._links(base):
            if not h.lower().endswith(".zip"):
                continue
            url = urljoin(base, quote(h))
            r = self.http.request("HEAD", url)
            out.append(RemoteFile(h, version, url, int(r.headers.get("Content-Length", 0)),
                                  (r.headers.get("ETag") or "").strip('"') or None, r.headers.get("Last-Modified")))
        return out


def make_source(cfg, http: Http):
    if cfg.source == "webdav":
        return WebDavSource(cfg.base_url, cfg.share_token, http)
    if cfg.source == "index":
        return IndexSource(cfg.base_url, http)
    raise ValueError(f"RFB_SOURCE inválido: {cfg.source}")


def latest_listing(source):
    versions = source.versions()
    version = versions[-1] if versions else ""
    return version, source.list_files(version)


# ---------------------------------------------------------------- assinatura do ZIP sem baixá-lo

def zip_signature(zf: zipfile.ZipFile) -> str:
    """CRC32 + tamanho descompactado de cada membro: identifica o conteúdo, independente de data/etag."""
    return ";".join(f"{i.filename}:{i.CRC:08x}:{i.file_size}" for i in sorted(zf.infolist(), key=lambda i: i.filename))


class _RangeFile(io.RawIOBase):
    """Arquivo somente-leitura sobre HTTP Range; o zipfile só lê o fim (EOCD + diretório central)."""

    def __init__(self, http, url, size, window=1 << 16):
        self.http, self.url, self.size, self.window = http, url, size, window
        self.pos, self._buf, self._start = 0, b"", 0

    def seekable(self): return True
    def readable(self): return True
    def tell(self): return self.pos

    def seek(self, off, whence=0):
        self.pos = {0: off, 1: self.pos + off, 2: self.size + off}[whence]
        return self.pos

    def _fetch(self, start, end):
        r = self.http.request("GET", self.url, headers={"Range": f"bytes={start}-{end - 1}"})
        if r.status_code != 206:
            raise RuntimeError("servidor não suporta Range")
        self._buf, self._start = r.content, start

    def read(self, n=-1):
        if n < 0:
            n = self.size - self.pos
        n = min(n, self.size - self.pos)
        if n <= 0:
            return b""
        if not (self._start <= self.pos and self.pos + n <= self._start + len(self._buf)):
            span = max(n, self.window)
            end = min(self.size, self.pos + span)
            start = self.pos if end < self.size else max(0, min(self.pos, self.size - span))
            self._fetch(start, end)
        o = self.pos - self._start
        self.pos += n
        return self._buf[o:o + n]

    def readinto(self, b):
        d = self.read(len(b))
        b[:len(d)] = d
        return len(d)


def remote_signature(http: Http, f: RemoteFile) -> str | None:
    """Assinatura do conteúdo lendo só o fim do ZIP remoto (≈1 requisição). None se Range não for suportado."""
    try:
        with zipfile.ZipFile(_RangeFile(http, f.url, f.size)) as zf:
            return zip_signature(zf)
    except (RuntimeError, zipfile.BadZipFile, requests.RequestException) as exc:
        log.info("assinatura remota indisponível para %s (%s); baixando", f.name, exc)
        return None


def local_signature(path: Path) -> str:
    with zipfile.ZipFile(path) as zf:
        return zip_signature(zf)


# ---------------------------------------------------------------- download

def download(http: Http, f: RemoteFile, dest: Path) -> Path:
    """Baixa para <dest>.part com retomada (Range/If-Range) e só renomeia quando o tamanho confere."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size == f.size:
        return dest
    part = dest.with_name(dest.name + ".part")
    for _ in range(3):
        have = part.stat().st_size if part.exists() else 0
        if have > f.size:
            part.unlink()
            have = 0
        if have < f.size:
            headers = {"Range": f"bytes={have}-"} if have else {}
            if have and f.etag:
                headers["If-Range"] = f'"{f.etag}"'
            r = http.request("GET", f.url, stream=True, headers=headers)
            mode = "ab" if r.status_code == 206 else "wb"  # 200 => servidor ignorou Range/arquivo mudou: recomeça
            with r, open(part, mode) as out:
                for chunk in r.iter_content(1 << 20):
                    out.write(chunk)
        if part.stat().st_size == f.size:
            part.replace(dest)
            return dest
        part.unlink(missing_ok=True)
    raise RuntimeError(f"{f.name}: tamanho baixado difere do anunciado ({f.size})")
