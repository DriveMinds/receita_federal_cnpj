"""Especificação das tabelas e geração do SQL (DDL, staging, upsert, delete)."""
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Col:
    name: str
    kind: str  # text | int | date | num

    @property
    def pg_type(self) -> str:
        return {"text": "text", "int": "integer", "date": "date", "num": "numeric(18,2)"}[self.kind]


@dataclass(frozen=True)
class Table:
    name: str
    zip_pattern: str  # regex do nome do ZIP; grupo 1 = número da parte
    cols: tuple
    key: tuple  # colunas da chave natural; vazio => a chave é o hash da linha inteira
    indexes: tuple = ()

    @property
    def keyed_by_hash(self) -> bool:
        return not self.key


def _cols(spec: str) -> tuple:
    return tuple(Col(*p.split(":")) for p in spec.split())


def _lookup(name, zip_name, kind="int"):
    return Table(name, rf"^{zip_name}()\.zip$", _cols(f"codigo:{kind} descricao:text"), ("codigo",))


TABLES = {
    t.name: t
    for t in (
        Table(
            "empresa", r"^Empresas(\d*)\.zip$",
            _cols("cnpj_basico:text razao_social:text natureza_juridica:int qualificacao_responsavel:int "
                  "capital_social:num porte_empresa:int ente_federativo_responsavel:text"),
            ("cnpj_basico",),
        ),
        Table(
            "estabelecimento", r"^Estabelecimentos(\d*)\.zip$",
            _cols("cnpj_basico:text cnpj_ordem:text cnpj_dv:text identificador_matriz_filial:int nome_fantasia:text "
                  "situacao_cadastral:int data_situacao_cadastral:date motivo_situacao_cadastral:int "
                  "nome_cidade_exterior:text pais:int data_inicio_atividade:date cnae_fiscal_principal:text "
                  "cnae_fiscal_secundaria:text tipo_logradouro:text logradouro:text numero:text complemento:text "
                  "bairro:text cep:text uf:text municipio:int ddd_1:text telefone_1:text ddd_2:text telefone_2:text "
                  "ddd_fax:text fax:text correio_eletronico:text situacao_especial:text data_situacao_especial:date"),
            ("cnpj_basico", "cnpj_ordem", "cnpj_dv"),
        ),
        Table(
            "socios", r"^Socios(\d*)\.zip$",
            _cols("cnpj_basico:text identificador_socio:int nome_socio_razao_social:text cpf_cnpj_socio:text "
                  "qualificacao_socio:int data_entrada_sociedade:date pais:int representante_legal:text "
                  "nome_do_representante:text qualificacao_representante_legal:int faixa_etaria:int"),
            (),  # não há chave natural: a identidade da linha é o hash do conteúdo
            indexes=("cnpj_basico",),
        ),
        Table(
            "simples", r"^Simples()\.zip$",
            _cols("cnpj_basico:text opcao_pelo_simples:text data_opcao_simples:date data_exclusao_simples:date "
                  "opcao_mei:text data_opcao_mei:date data_exclusao_mei:date"),
            ("cnpj_basico",),
        ),
        _lookup("cnae", "Cnaes", "text"),
        _lookup("moti", "Motivos"),
        _lookup("munic", "Municipios"),
        _lookup("natju", "Naturezas"),
        _lookup("pais", "Paises"),
        _lookup("quals", "Qualificacoes"),
    )
}

# Tabelas grandes primeiro, para equilibrar o paralelismo.
LOAD_ORDER = ["estabelecimento", "empresa", "socios", "simples",
              "cnae", "moti", "munic", "natju", "pais", "quals"]


def classify(zip_name: str):
    """Retorna (tabela, número da parte) para o nome de um ZIP da RFB, ou None se não for conhecido."""
    for t in TABLES.values():
        m = re.match(t.zip_pattern, zip_name, re.I)
        if m:
            return t, int(m.group(1) or 0)
    return None


def q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def ddl(schema: str) -> list:
    s = q(schema)
    out = [
        f"CREATE SCHEMA IF NOT EXISTS {s}",
        # Datas da RFB: '0', '00000000' ou vazio = ausente; datas impossíveis (ex.: 30/02) viram NULL em vez de abortar a carga.
        f"""CREATE OR REPLACE FUNCTION {s}.parse_date(t text) RETURNS date LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
            SELECT CASE
              WHEN t ~ '^[12][0-9]{{3}}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])$'
               AND substr(t, 7, 2)::int <= extract(day FROM (make_date(substr(t,1,4)::int, substr(t,5,2)::int, 1)
                                                            + interval '1 month - 1 day'))::int
              THEN make_date(substr(t,1,4)::int, substr(t,5,2)::int, substr(t,7,2)::int)
            END $$""",
        f"""CREATE TABLE IF NOT EXISTS {s}.sync_file (
            name text PRIMARY KEY, version text NOT NULL, fingerprint text NOT NULL, content_sig text NOT NULL,
            tbl text NOT NULL, src smallint NOT NULL, rows_total bigint NOT NULL,
            inserted bigint NOT NULL DEFAULT 0, updated bigint NOT NULL DEFAULT 0, deleted bigint NOT NULL DEFAULT 0,
            loaded_at timestamptz NOT NULL DEFAULT now())""",
        f"""CREATE TABLE IF NOT EXISTS {s}.sync_run (
            id bigserial PRIMARY KEY, started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
            version text, status text NOT NULL DEFAULT 'running', detail jsonb)""",
    ]
    for t in TABLES.values():
        cols = [f"{q(c.name)} {c.pg_type}" for c in t.cols]
        pk = "row_hash" if t.keyed_by_hash else ", ".join(q(k) for k in t.key)
        out.append(
            f"CREATE TABLE IF NOT EXISTS {s}.{q(t.name)} ({', '.join(cols)}, row_hash uuid NOT NULL, "
            f"src smallint NOT NULL, PRIMARY KEY ({pk}))"
        )
        out.append(f"CREATE INDEX IF NOT EXISTS {q(t.name + '_src')} ON {s}.{q(t.name)} (src)")
        for ix in t.indexes:
            out.append(f"CREATE INDEX IF NOT EXISTS {q(t.name + '_' + ix)} ON {s}.{q(t.name)} ({q(ix)})")
    return out


def _typed(col: Col, raw: str) -> str:
    if col.kind == "text":
        return f"NULLIF({raw}, '')"
    if col.kind == "int":
        return f"NULLIF({raw}, '')::integer"
    if col.kind == "date":
        return f"__SCHEMA__.parse_date({raw})"
    if col.kind == "num":
        return f"NULLIF(replace({raw}, ',', '.'), '')::numeric(18,2)"
    raise ValueError(col.kind)


def copy_sql(t: Table) -> str:
    cols = ", ".join(f"c{i}" for i in range(len(t.cols)))
    return (f"COPY raw ({cols}) FROM STDIN WITH (FORMAT csv, DELIMITER ';', QUOTE '\"', ENCODING 'LATIN1')")


def raw_ddl(t: Table) -> str:
    return "CREATE TEMP TABLE raw (" + ", ".join(f"c{i} text" for i in range(len(t.cols))) + ") ON COMMIT DROP"


def staging_sql(t: Table, schema: str) -> str:
    """Materializa a carga já tipada, com o hash do conteúdo (sem ordenar linhas largas: é o passo mais caro)."""
    n = len(t.cols)
    hash_expr = "md5(concat_ws(E'\\x1f', " + ", ".join(f"coalesce(c{i}, '')" for i in range(n)) + "))::uuid"
    select = ", ".join(f"{_typed(c, f'r.c{i}')} AS {q(c.name)}" for i, c in enumerate(t.cols))
    sql = f"CREATE TEMP TABLE s ON COMMIT DROP AS SELECT {select}, {hash_expr} AS row_hash FROM raw r"
    return sql.replace("__SCHEMA__", q(schema))


def dedupe_sql(t: Table) -> str:
    """Mantém uma linha por chave (o upsert falha se a mesma chave aparece duas vezes). Ordena só chave + ctid."""
    keys = ["row_hash"] if t.keyed_by_hash else [q(k) for k in t.key]
    return ("DELETE FROM s WHERE ctid IN (SELECT ctid FROM (SELECT ctid, row_number() OVER "
            f"(PARTITION BY {', '.join(keys)} ORDER BY ctid) AS rn FROM s) d WHERE rn > 1)")


def upsert_sql(t: Table, schema: str) -> str:
    names = [q(c.name) for c in t.cols] + ["row_hash", "src"]
    sel = [q(c.name) for c in t.cols] + ["row_hash", "%(src)s"]
    target = f"{q(schema)}.{q(t.name)}"
    conflict = "row_hash" if t.keyed_by_hash else ", ".join(q(k) for k in t.key)
    sets = ", ".join(f"{n} = EXCLUDED.{n}" for n in names)
    return (f"WITH up AS (INSERT INTO {target} AS t ({', '.join(names)}) SELECT {', '.join(sel)} FROM s "
            f"ON CONFLICT ({conflict}) DO UPDATE SET {sets} "
            f"WHERE t.row_hash IS DISTINCT FROM EXCLUDED.row_hash OR t.src IS DISTINCT FROM EXCLUDED.src "
            f"RETURNING (xmax = 0) AS inserted) "
            f"SELECT count(*) FILTER (WHERE inserted), count(*) FILTER (WHERE NOT inserted) FROM up")


def delete_sql(t: Table, schema: str) -> str:
    keys = ["row_hash"] if t.keyed_by_hash else [q(k) for k in t.key]
    cond = " AND ".join(f"s.{k} = t.{k}" for k in keys)
    return (f"DELETE FROM {q(schema)}.{q(t.name)} t WHERE t.src = %(src)s "
            f"AND NOT EXISTS (SELECT 1 FROM s WHERE {cond})")
