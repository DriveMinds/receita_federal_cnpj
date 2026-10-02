import argparse
import logging
import sys

from . import remote, sync
from .config import Config


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="rfb_cnpj", description=__doc__)
    ap.add_argument("--env-file")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init", help="cria schema, tabelas e tabelas de estado")
    c = sub.add_parser("check", help="só consulta o servidor (barato); sai com 10 se há mudanças pendentes")
    c.add_argument("--force", action="store_true")
    s = sub.add_parser("sync", help="baixa o que mudou e aplica o diff no PostgreSQL")
    s.add_argument("--force", action="store_true", help="ignora o estado e recarrega tudo (ainda aplica só o diff)")
    s.add_argument("--only", nargs="+", help="limita a tabelas (ex.: empresa socios)")
    s.add_argument("--prune", action="store_true", help="remove dados de ZIPs que sumiram do servidor")
    sub.add_parser("verify", help="confere contagens do banco contra o que foi carregado")
    sub.add_parser("status", help="mostra o estado dos arquivos carregados")
    a = ap.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    cfg = Config.from_env(a.env_file)

    if a.cmd == "init":
        sync.init_db(cfg)
        print("ok")
    elif a.cmd == "check":
        sync.init_db(cfg)
        plan = sync.build_plan(cfg, remote.Http(cfg.request_interval), force=a.force)
        print(f"versão remota: {plan.version or '(raiz)'} | requisições: {plan.requests}")
        print(f"sem mudança: {len(plan.unchanged)} | conteúdo idêntico: {len(plan.identical)} | a carregar: {len(plan.to_load)}")
        for f, _ in plan.to_load:
            print(f"  carregar {f.name} ({f.size / 1e6:.1f} MB)")
        for n in plan.missing:
            print(f"  ausente no servidor: {n}")
        return 10 if plan.pending else 0
    elif a.cmd == "sync":
        rep = sync.run(cfg, force=a.force, only=a.only, prune=a.prune)
        print(f"versão {rep['version']}: {len(rep['loaded'])} carregado(s), {rep['unchanged']} sem mudança, "
              f"{rep['identical']} idêntico(s); {rep['http_requests']} requisições ao servidor")
        for n, st in rep["loaded"].items():
            print(f"  {n}: +{st['inserted']} ~{st['updated']} ={st['unchanged']} -{st['deleted']}")
    elif a.cmd == "verify":
        problems = sync.verify(cfg)
        print("\n".join(problems) or "ok: contagens batem")
        return 1 if problems else 0
    elif a.cmd == "status":
        import psycopg2
        with psycopg2.connect(cfg.dsn) as conn, conn.cursor() as cur:
            cur.execute(f'SELECT name, version, rows_total, loaded_at FROM "{cfg.schema}".sync_file ORDER BY tbl, name')
            for r in cur.fetchall():
                print(*r)
    return 0


if __name__ == "__main__":
    sys.exit(main())
