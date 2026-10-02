import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass
class Config:
    dsn: str
    schema: str = "cnpj"
    source: str = "webdav"  # webdav (Nextcloud da RFB) | index (listagem HTTP/Apache)
    base_url: str = "https://arquivos.receitafederal.gov.br"
    share_token: str = "YggdBLfdninEJX9"
    data_dir: Path = Path("data/zips")
    workers: int = 4  # cargas simultâneas no banco
    download_concurrency: int = 2  # conexões simultâneas com o servidor da RFB
    request_interval: float = 1.0  # segundos mínimos entre requisições ao servidor da RFB
    keep_zips: bool = True
    work_mem: str = "256MB"

    @classmethod
    def from_env(cls, env_file: str | None = None) -> "Config":
        load_dotenv(env_file)
        e = os.environ
        dsn = e.get("DATABASE_URL") or (
            f"host={e.get('DB_HOST', 'localhost')} port={e.get('DB_PORT', '5432')} "
            f"dbname={e.get('DB_NAME', 'Dados_RFB')} user={e.get('DB_USER', 'postgres')} "
            f"password={e.get('DB_PASSWORD', '')}"
        )
        d = cls(dsn=dsn)
        return cls(
            dsn=dsn,
            schema=e.get("DB_SCHEMA", d.schema),
            source=e.get("RFB_SOURCE", d.source),
            base_url=e.get("RFB_BASE_URL", d.base_url).rstrip("/"),
            share_token=e.get("RFB_SHARE_TOKEN", d.share_token),
            data_dir=Path(e.get("DATA_DIR") or e.get("OUTPUT_FILES_PATH") or d.data_dir),
            workers=int(e.get("RFB_WORKERS", d.workers)),
            download_concurrency=int(e.get("RFB_DOWNLOAD_CONCURRENCY", d.download_concurrency)),
            request_interval=float(e.get("RFB_REQUEST_INTERVAL", d.request_interval)),
            keep_zips=e.get("RFB_KEEP_ZIPS", "1") not in ("0", "false", "no"),
            work_mem=e.get("PG_WORK_MEM", d.work_mem),
        )
