"""Orquestração: listar -> comparar com o estado -> (assinatura) -> baixar -> aplicar diff -> registrar."""
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import psycopg2

from . import remote
from . import schema as sch
from .config import Config
from .loader import load_zip

log = logging.getLogger(__name__)
LOCK_KEY = 7_2024_01


@dataclass
class Plan:
    version: str
    unchanged: list = field(default_factory=list)   # (RemoteFile)
    identical: list = field(default_factory=list)   # fingerprint mudou, conteúdo não (RemoteFile, sig)
    to_load: list = field(default_factory=list)     # (RemoteFile, sig|None)
    unknown: list = field(default_factory=list)     # ZIPs que não sabemos carregar
    missing: list = field(default_factory=list)     # no estado, mas sumiram do servidor (nomes)
    requests: int = 0

    @property
    def pending(self):
        return bool(self.to_load or self.identical)


def init_db(cfg: Config):
    with psycopg2.connect(cfg.dsn) as conn, conn.cursor() as cur:
        for stmt in sch.ddl(cfg.schema):
            cur.execute(stmt)


def _state(cfg):
    with psycopg2.connect(cfg.dsn) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT name, fingerprint, content_sig FROM {sch.q(cfg.schema)}.sync_file")
        return {n: (fp, sig) for n, fp, sig in cur.fetchall()}


def build_plan(cfg: Config, http: remote.Http, force=False, only=None) -> Plan:
    source = remote.make_source(cfg, http)
    version, files = remote.latest_listing(source)
    plan = Plan(version)
    state = _state(cfg)
    names = set()
    for f in sorted(files, key=lambda f: f.name):
        if only and sch.classify(f.name) and sch.classify(f.name)[0].name not in only:
            continue
        names.add(f.name)
        if not sch.classify(f.name):
            plan.unknown.append(f)
            continue
        old = state.get(f.name)
        if old and old[0] == f.fingerprint and not force:
            plan.unchanged.append(f)
            continue
        sig = remote.remote_signature(http, f)  # ~1 requisição Range, evita baixar GBs se o conteúdo é o mesmo
        if old and sig and old[1] == sig and not force:
            plan.identical.append((f, sig))
        else:
            plan.to_load.append((f, sig))
    plan.missing = sorted(set(state) - names) if not only else []
    plan.requests = http.request_count
    return plan


def _record(cfg, f, tbl, src, sig):
    def record(cur, st):
        cur.execute(
            f"""INSERT INTO {sch.q(cfg.schema)}.sync_file
                (name, version, fingerprint, content_sig, tbl, src, rows_total, inserted, updated, deleted, loaded_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
                ON CONFLICT (name) DO UPDATE SET version=EXCLUDED.version, fingerprint=EXCLUDED.fingerprint,
                content_sig=EXCLUDED.content_sig, tbl=EXCLUDED.tbl, src=EXCLUDED.src, rows_total=EXCLUDED.rows_total,
                inserted=EXCLUDED.inserted, updated=EXCLUDED.updated, deleted=EXCLUDED.deleted, loaded_at=now()""",
            (f.name, f.version, f.fingerprint, sig, tbl.name, src, st.rows, st.inserted, st.updated, st.deleted))
    return record


def run(cfg: Config, force=False, only=None, prune=False) -> dict:
    http = remote.Http(cfg.request_interval)
    lock = psycopg2.connect(cfg.dsn)
    lock.autocommit = True
    with lock.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,))
        if not cur.fetchone()[0]:
            raise RuntimeError("outra sincronização já está em execução")
    try:
        init_db(cfg)
        with psycopg2.connect(cfg.dsn) as c, c.cursor() as cur:
            cur.execute(f"INSERT INTO {sch.q(cfg.schema)}.sync_run DEFAULT VALUES RETURNING id")
            run_id = cur.fetchone()[0]
        plan = build_plan(cfg, http, force, only)
        report = {"version": plan.version, "unchanged": len(plan.unchanged), "identical": len(plan.identical),
                  "loaded": {}, "unknown": [f.name for f in plan.unknown], "missing": plan.missing}
        status = "ok"
        try:
            _execute(cfg, http, plan, report)
            if plan.missing:
                if prune:
                    _prune(cfg, plan.missing, report)
                else:
                    log.warning("ZIPs carregados antes e ausentes agora (use --prune para remover seus dados): %s",
                                plan.missing)
        except Exception:
            status = "failed"
            raise
        finally:
            report["http_requests"] = http.request_count
            with psycopg2.connect(cfg.dsn) as c, c.cursor() as cur:
                cur.execute(f"UPDATE {sch.q(cfg.schema)}.sync_run SET finished_at=now(), version=%s, status=%s, detail=%s "
                            f"WHERE id=%s", (plan.version, status, json.dumps(report), run_id))
        return report
    finally:
        lock.close()


def _execute(cfg, http, plan: Plan, report):
    # ZIPs cujo conteúdo é idêntico: só atualiza a impressão digital, sem baixar nem recarregar.
    for f, sig in plan.identical:
        with psycopg2.connect(cfg.dsn) as c, c.cursor() as cur:
            cur.execute(f"UPDATE {sch.q(cfg.schema)}.sync_file SET fingerprint=%s, version=%s WHERE name=%s",
                        (f.fingerprint, f.version, f.name))
    jobs = sorted(plan.to_load, key=lambda x: (sch.LOAD_ORDER.index(sch.classify(x[0].name)[0].name), x[0].name))
    if not jobs:
        return
    dl_slots = threading.Semaphore(cfg.download_concurrency)

    def job(f, sig):
        tbl, src = sch.classify(f.name)
        dest = cfg.data_dir / (f.version or "_") / f.name
        with dl_slots:
            log.info("baixando %s (%.1f MB)", f.name, f.size / 1e6)
            remote.download(http, f, dest)
        sig = remote.local_signature(dest)
        stats = load_zip(cfg, tbl, src, dest, _record(cfg, f, tbl, src, sig))
        log.info("%s: %d linhas (+%d novas, ~%d alteradas, =%d iguais, -%d removidas)",
                 f.name, stats.rows, stats.inserted, stats.updated, stats.unchanged, stats.deleted)
        _cleanup_cache(cfg, f, dest)
        return f.name, tbl.name, stats

    touched = set()
    with ThreadPoolExecutor(max(1, cfg.workers)) as ex:
        futs = [ex.submit(job, f, sig) for f, sig in jobs]
        errors = []
        for fut in as_completed(futs):
            try:
                name, tname, st = fut.result()
                report["loaded"][name] = vars(st) | {"unchanged": st.unchanged}
                touched.add(tname)
            except Exception as exc:  # um ZIP ruim não impede os demais; ele será refeito na próxima execução
                log.error("falha: %s", exc)
                errors.append(exc)
    with psycopg2.connect(cfg.dsn) as c, c.cursor() as cur:
        for t in touched:
            cur.execute(f"ANALYZE {sch.q(cfg.schema)}.{sch.q(t)}")
    if errors:
        raise RuntimeError(f"{len(errors)} arquivo(s) falharam; primeiro erro: {errors[0]}") from errors[0]


def _cleanup_cache(cfg, f, dest):
    if not cfg.keep_zips:
        dest.unlink(missing_ok=True)
        return
    for other in cfg.data_dir.glob(f"*/{f.name}"):
        if other != dest:
            other.unlink(missing_ok=True)


def _prune(cfg, names, report):
    with psycopg2.connect(cfg.dsn) as c, c.cursor() as cur:
        for n in names:
            cur.execute(f"SELECT tbl, src FROM {sch.q(cfg.schema)}.sync_file WHERE name=%s", (n,))
            tbl, src = cur.fetchone()
            cur.execute(f"DELETE FROM {sch.q(cfg.schema)}.{sch.q(tbl)} WHERE src=%s", (src,))
            report.setdefault("pruned", {})[n] = cur.rowcount
            cur.execute(f"DELETE FROM {sch.q(cfg.schema)}.sync_file WHERE name=%s", (n,))


def verify(cfg: Config) -> list:
    """Compara count(*) de cada tabela com a soma de linhas registradas dos seus ZIPs. Lista as divergências."""
    problems = []
    with psycopg2.connect(cfg.dsn) as c, c.cursor() as cur:
        for t in sch.TABLES.values():
            cur.execute(f"SELECT coalesce(sum(rows_total),0), count(*) FROM {sch.q(cfg.schema)}.sync_file WHERE tbl=%s",
                        (t.name,))
            expected, nfiles = cur.fetchone()
            cur.execute(f"SELECT count(*) FROM {sch.q(cfg.schema)}.{sch.q(t.name)}")
            actual = cur.fetchone()[0]
            if nfiles and expected != actual:
                problems.append(f"{t.name}: esperado {expected}, banco tem {actual}")
            elif not nfiles:
                problems.append(f"{t.name}: nenhum ZIP carregado")
    return problems
