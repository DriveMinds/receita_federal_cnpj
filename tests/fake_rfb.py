"""Servidor falso da RFB (WebDAV + índice HTTP, com Range e contagem de requisições) para os testes."""
import io
import re
import threading
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote


def make_zip(member: str, lines: list, encoding="latin-1") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(member, ("\n".join(lines) + "\n").encode(encoding))
    return buf.getvalue()


class FakeRFB:
    def __init__(self):
        self.files = {}  # "2026-09/Empresas0.zip" -> (bytes, mtime)
        self.log = []    # (method, path, range)
        self.etag_n = 0
        outer = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a): pass

            def _send(self, code, body=b"", headers=None):
                self.send_response(code)
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)

            def _meta(self, key):
                data, mtime, etag = outer.files[key]
                return data, {"Last-Modified": mtime, "ETag": f'"{etag}"', "Accept-Ranges": "bytes"}

            def do_PROPFIND(self):
                n = int(self.headers.get("Content-Length") or 0)
                self.rfile.read(n)
                outer.log.append(("PROPFIND", self.path, None))
                sub = unquote(self.path.split("/webdav/", 1)[1]).strip("/")
                rows = []
                if sub == "":
                    for v in sorted({k.split("/")[0] for k in outer.files}):
                        rows.append(f'<d:response><d:href>/public.php/webdav/{v}/</d:href><d:propstat><d:prop>'
                                    f'<d:resourcetype><d:collection/></d:resourcetype></d:prop>'
                                    f'<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>')
                else:
                    for k in sorted(outer.files):
                        if k.startswith(sub + "/"):
                            data, h = self._meta(k)
                            rows.append(
                                f'<d:response><d:href>/public.php/webdav/{k}</d:href><d:propstat><d:prop>'
                                f'<d:getlastmodified>{h["Last-Modified"]}</d:getlastmodified>'
                                f'<d:getcontentlength>{len(data)}</d:getcontentlength>'
                                f'<d:getetag>{h["ETag"]}</d:getetag><d:resourcetype/></d:prop>'
                                f'<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>')
                body = ('<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">' + "".join(rows) + "</d:multistatus>").encode()
                self._send(207, body, {"Content-Type": "application/xml"})

            def do_GET(self):
                outer.log.append((self.command, self.path, self.headers.get("Range")))
                path = unquote(self.path)
                if path.startswith("/idx/") and not path.lower().endswith(".zip"):
                    sub = path[len("/idx/"):].strip("/")
                    if sub == "":
                        names = sorted({k.split("/")[0] + "/" for k in outer.files})
                    else:
                        names = [k.split("/", 1)[1] for k in sorted(outer.files) if k.startswith(sub + "/")]
                    html = "<html><body>" + "".join(f'<a href="{n}">{n}</a><br>' for n in names) + "</body></html>"
                    return self._send(200, html.encode(), {"Content-Type": "text/html"})
                key = re.sub(r"^/(public\.php/webdav|idx)/", "", path)
                if key not in outer.files:
                    return self._send(404)
                data, h = self._meta(key)
                rng = self.headers.get("Range")
                if rng:
                    m = re.match(r"bytes=(\d+)-(\d*)", rng)
                    a = int(m.group(1))
                    b = int(m.group(2)) if m.group(2) else len(data) - 1
                    if a >= len(data):
                        return self._send(416)
                    h["Content-Range"] = f"bytes {a}-{b}/{len(data)}"
                    return self._send(206, data[a:b + 1], h)
                self._send(200, data, h)

            do_HEAD = do_GET

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.httpd.server_port}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def put(self, key, data: bytes):
        self.etag_n += 1
        self.files[key] = (data, f"Mon, 0{1 + self.etag_n % 9} Sep 2026 10:00:00 GMT", f"e{self.etag_n}")

    def gets(self, prefix=""):
        return [e for e in self.log if e[0] == "GET" and e[1].find(".zip") > 0 and prefix in e[1]]

    def close(self):
        self.httpd.shutdown()
