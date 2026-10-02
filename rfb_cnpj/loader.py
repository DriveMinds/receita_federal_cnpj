"""Carga de um ZIP: COPY em staging -> upsert só do que mudou -> remoção do que sumiu, tudo em 1 transação."""
import logging
import zipfile
from dataclasses import dataclass

import psycopg2

from . import schema as sch

log = logging.getLogger(__name__)


@dataclass
class LoadStats:
    rows: int
    inserted: int
    updated: int
    deleted: int

    @property
    def unchanged(self):
        return self.rows - self.inserted - self.updated


class VerificationError(RuntimeError):
    pass


def load_zip(cfg, table: sch.Table, src: int, zip_path, record) -> LoadStats:
    """Aplica o conteúdo do ZIP à tabela. `record(cur, stats)` grava o estado na MESMA transação,
    então o banco nunca diz "arquivo carregado" sem os dados estarem lá (e vice-versa)."""
    conn = psycopg2.connect(cfg.dsn)
    try:
        with conn, conn.cursor() as cur:  # 'with conn' = uma transação (commit/rollback)
            cur.execute("SET LOCAL synchronous_commit = off")
            cur.execute("SET LOCAL work_mem = %s", (cfg.work_mem,))
            cur.execute(sch.raw_ddl(table))
            with zipfile.ZipFile(zip_path) as zf:
                for info in zf.infolist():
                    # zipfile confere o CRC32 ao chegar no fim do membro: ZIP corrompido => exceção => rollback
                    with zf.open(info) as fh:
                        cur.copy_expert(sch.copy_sql(table), fh, size=1 << 20)
            cur.execute(sch.staging_sql(table, cfg.schema))
            cur.execute(sch.dedupe_sql(table))
            cur.execute("ANALYZE s")
            cur.execute("SELECT count(*) FROM s")
            rows = cur.fetchone()[0]
            cur.execute(sch.upsert_sql(table, cfg.schema), {"src": src})
            inserted, updated = cur.fetchone()
            cur.execute(sch.delete_sql(table, cfg.schema), {"src": src})
            deleted = cur.rowcount
            # Garantia de completude: tudo que está no arquivo está no banco, e nada além disso para esta parte.
            cur.execute(f"SELECT count(*) FROM {sch.q(cfg.schema)}.{sch.q(table.name)} WHERE src = %s", (src,))
            in_db = cur.fetchone()[0]
            if in_db != rows:
                raise VerificationError(
                    f"{zip_path.name}: arquivo tem {rows} linhas distintas mas o banco tem {in_db} com src={src}")
            stats = LoadStats(rows, inserted, updated, deleted)
            record(cur, stats)
        return stats
    finally:
        conn.close()
