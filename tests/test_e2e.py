import os

import psycopg2
import pytest

from rfb_cnpj import remote, sync
from rfb_cnpj.config import Config
from tests.fake_rfb import FakeRFB, make_zip

DSN = os.environ.get("TEST_DSN", "host=/var/tmp/pgtest port=5433 dbname=rfbtest user=postgres")
V = "2026-09"


def emp(basico, nome, capital="1000,50", nat="2062"):
    return f'"{basico}";"{nome}";"{nat}";"49";"{capital}";"01";""'


def estab(basico, ordem="0001", dv="91", sit="02", data="20200131", fantasia=""):
    f = [basico, ordem, dv, "1", fantasia, sit, data, "0", "", "", "20100505", "0111301", "0111302,4711301",
         "RUA", "DAS FLORES", "10", "", "CENTRO", "01001000", "SP", "7107", "11", "12345678", "", "", "", "",
         "A@B.COM", "", "0"]
    return ";".join(f'"{x}"' for x in f)


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("no_proxy", "127.0.0.1")


@pytest.fixture
def srv():
    s = FakeRFB()
    yield s
    s.close()


@pytest.fixture(params=["webdav", "index"])
def cfg(request, srv, tmp_path):
    with psycopg2.connect(DSN) as c, c.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS cnpj_test CASCADE")
    c = Config(dsn=DSN, schema="cnpj_test", source=request.param, data_dir=tmp_path / "zips",
               base_url=srv.url if request.param == "webdav" else srv.url + "/idx",
               share_token="tok", request_interval=0, workers=3)
    return c


def q(cfg, sql, *a):
    with psycopg2.connect(DSN) as c, c.cursor() as cur:
        cur.execute(sql.replace("T.", cfg.schema + "."), a)
        return cur.fetchall()


def publish(srv, empresas0, empresas1, estabs=None, socios=None):
    srv.put(f"{V}/Empresas0.zip", make_zip("K.EMPRECSV", empresas0))
    srv.put(f"{V}/Empresas1.zip", make_zip("K.EMPRECSV", empresas1))
    srv.put(f"{V}/Estabelecimentos0.zip", make_zip("K.ESTABELE", estabs or [estab("00000001")]))
    srv.put(f"{V}/Socios0.zip", make_zip("K.SOCIOCSV", socios or [
        '"00000001";"2";"FULANO";"***123456**";"49";"20200101";"";"***000000**";"";"00";"4"']))
    srv.put(f"{V}/Simples.zip", make_zip("K.SIMPLES", ['"00000001";"S";"20200101";"00000000";"N";"20200230";"0"']))
    srv.put(f"{V}/Cnaes.zip", make_zip("K.CNAECSV", ['"0111301";"Cultivo de arroz"']))
    srv.put(f"{V}/Paises.zip", make_zip("K.PAISCSV", ['"105";"BRASIL"']))
    srv.put("not-a-version/readme.txt", b"x")


def test_initial_load_then_noop_then_diff(srv, cfg):
    publish(srv, [emp("00000001", "ACME  LTDA"), emp("00000002", "JOSÉ & FILHOS", "1234,5")],
            [emp("00000003", 'COM ""ASPAS"" E ; PONTO-E-VÍRGULA')],
            estabs=[estab("00000001"), estab("00000002")])

    # --- carga inicial
    rep = sync.run(cfg)
    assert rep["loaded"]["Empresas0.zip"]["inserted"] == 2
    assert q(cfg, "SELECT count(*) FROM T.empresa")[0][0] == 3
    # tipos e normalização
    nome, cap = q(cfg, "SELECT razao_social, capital_social FROM T.empresa WHERE cnpj_basico='00000002'")[0]
    assert nome == "JOSÉ & FILHOS" and float(cap) == 1234.5
    assert q(cfg, 'SELECT razao_social FROM T.empresa WHERE cnpj_basico=\'00000003\'')[0][0] == 'COM "ASPAS" E ; PONTO-E-VÍRGULA'
    cnae, dt = q(cfg, "SELECT cnae_fiscal_principal, data_inicio_atividade FROM T.estabelecimento LIMIT 1")[0]
    assert cnae == "0111301" and str(dt) == "2010-05-05"  # zero à esquerda preservado
    simples = q(cfg, "SELECT data_exclusao_simples, data_opcao_mei FROM T.simples")[0]
    assert simples == (None, None)  # '00000000' e '20200230' (inválida) viram NULL
    assert all("nenhum ZIP" in p for p in sync.verify(cfg)), sync.verify(cfg)  # só faltam tabelas que o teste não publica

    # --- nada mudou: zero downloads de ZIP e poucas requisições
    srv.log.clear()
    rep = sync.run(cfg)
    assert rep["loaded"] == {} and rep["unchanged"] == 7
    assert srv.gets() == []
    assert rep["http_requests"] <= 2 + 7  # listagem (+1 HEAD por ZIP no modo index)

    # --- reempacotado com conteúdo idêntico (etag/data novos): só lê o diretório central, não baixa
    srv.put(f"{V}/Empresas1.zip", make_zip("K.EMPRECSV", [emp("00000003", 'COM ""ASPAS"" E ; PONTO-E-VÍRGULA')]))
    srv.log.clear()
    rep = sync.run(cfg)
    assert rep["identical"] == 1 and rep["loaded"] == {}
    assert all(r is not None and r.startswith("bytes=") for _, _, r in srv.gets()), "só Range pequeno"
    full = [e for e in srv.gets() if e[2] is None]
    assert full == []

    # --- mudança real: 1 linha alterada, 1 nova, 1 removida
    before = q(cfg, "SELECT xmin::text, cnpj_basico FROM T.empresa ORDER BY 2")
    srv.put(f"{V}/Empresas0.zip", make_zip("K.EMPRECSV", [emp("00000001", "ACME  LTDA"),  # igual
                                                           emp("00000004", "NOVA SA")]))     # 00000002 sumiu
    srv.put(f"{V}/Empresas1.zip", make_zip("K.EMPRECSV", [emp("00000003", "RENOMEADA")]))   # alterada
    srv.log.clear()
    rep = sync.run(cfg)
    e0, e1 = rep["loaded"]["Empresas0.zip"], rep["loaded"]["Empresas1.zip"]
    assert (e0["inserted"], e0["updated"], e0["unchanged"], e0["deleted"]) == (1, 0, 1, 1)
    assert (e1["inserted"], e1["updated"], e1["deleted"]) == (0, 1, 0)
    rows = dict(q(cfg, "SELECT cnpj_basico, razao_social FROM T.empresa"))
    assert rows == {"00000001": "ACME  LTDA", "00000003": "RENOMEADA", "00000004": "NOVA SA"}
    after = dict((b, x) for x, b in q(cfg, "SELECT xmin::text, cnpj_basico FROM T.empresa"))
    assert after["00000001"] == dict((b, x) for x, b in before)["00000001"], "linha igual não pode ser reescrita"
    assert {e[1].rsplit("/", 1)[-1] for e in srv.gets() if e[2] is None} == {"Empresas0.zip", "Empresas1.zip"}

    # --- linha migra de um ZIP para outro (não pode ser apagada pelo ZIP de origem)
    srv.put(f"{V}/Empresas0.zip", make_zip("K.EMPRECSV", [emp("00000001", "ACME  LTDA")]))
    srv.put(f"{V}/Empresas1.zip", make_zip("K.EMPRECSV", [emp("00000003", "RENOMEADA"), emp("00000004", "NOVA SA")]))
    sync.run(cfg)
    assert {r[0] for r in q(cfg, "SELECT cnpj_basico FROM T.empresa")} == {"00000001", "00000003", "00000004"}
    assert all("nenhum ZIP" in p for p in sync.verify(cfg)), sync.verify(cfg)  # só faltam tabelas que o teste não publica


def test_socios_without_key_and_duplicates(srv, cfg):
    s1 = '"00000001";"2";"FULANO";"***123456**";"49";"20200101";"";"***000000**";"";"00";"4"'
    s2 = '"00000001";"2";"BELTRANO";"***654321**";"49";"20200101";"";"***000000**";"";"00";"4"'
    publish(srv, [emp("00000001", "A")], [emp("00000002", "B")], socios=[s1, s1, s2])
    sync.run(cfg)
    assert q(cfg, "SELECT count(*) FROM T.socios")[0][0] == 2  # duplicata exata colapsada
    publish(srv, [emp("00000001", "A")], [emp("00000002", "B")], socios=[s2])
    rep = sync.run(cfg)
    assert rep["loaded"]["Socios0.zip"]["deleted"] == 1
    assert [r[0] for r in q(cfg, "SELECT nome_socio_razao_social FROM T.socios")] == ["BELTRANO"]


def test_corrupt_zip_rolls_back_and_retries(srv, cfg):
    publish(srv, [emp("00000001", "A")], [emp("00000002", "B")])
    sync.run(cfg)
    good, _, _ = srv.files[f"{V}/Empresas0.zip"]
    bad = bytearray(make_zip("K.EMPRECSV", [emp("00000001", "A"), emp("00000009", "LIXO")]))
    bad[40] ^= 0xFF  # corrompe o payload comprimido => CRC/inflate falha no meio do COPY
    srv.put(f"{V}/Empresas0.zip", bytes(bad))
    with pytest.raises(RuntimeError):
        sync.run(cfg)
    assert {r[0] for r in q(cfg, "SELECT cnpj_basico FROM T.empresa")} == {"00000001", "00000002"}
    # servidor corrige o arquivo: próxima execução converge
    srv.put(f"{V}/Empresas0.zip", make_zip("K.EMPRECSV", [emp("00000001", "A"), emp("00000009", "OK")]))
    sync.run(cfg)
    assert {r[0] for r in q(cfg, "SELECT cnpj_basico FROM T.empresa")} == {"00000001", "00000002", "00000009"}


def test_download_resume(srv, tmp_path):
    data = make_zip("K.EMPRECSV", [emp("%08d" % i, "X" * 50) for i in range(2000)])
    srv.put(f"{V}/Empresas0.zip", data)
    http = remote.Http(0)
    http.session.auth = ("tok", "")
    f = remote.WebDavSource(srv.url, "tok", http).list_files(V)[0]
    dest = tmp_path / "Empresas0.zip"
    (tmp_path / "Empresas0.zip.part").write_bytes(data[:100])
    srv.log.clear()
    remote.download(http, f, dest)
    assert dest.read_bytes() == data
    assert srv.gets()[0][2] == "bytes=100-"
