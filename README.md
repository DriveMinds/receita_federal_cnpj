# Dados Públicos CNPJ
- Fonte oficial da Receita Federal do Brasil, [aqui](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj).
- Layout dos arquivos, [aqui](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf).

A Receita Federal do Brasil disponibiliza bases com os dados públicos do cadastro nacional de pessoas jurídicas (CNPJ).

De forma geral, nelas constam as mesmas informações que conseguimos ver no cartão do CNPJ, quando fazemos uma consulta individual, acrescidas de outros dados de Simples Nacional, sócios e etc. Análises muito ricas podem sair desses dados, desde econômicas, mercadológicas até investigações.

Nesse repositório consta um processo de ETL para **i)** baixar os arquivos; **ii)** descompactar; **iii)** ler, tratar e **iv)** inserir num banco de dados relacional PostgreSQL.

---------------------

### Infraestrutura necessária:
- Python 3.11+
- PostgreSQL 14+ (testado em 16)

---------------------

### Como funciona a sincronização incremental

O processo antigo apagava as tabelas e recarregava tudo (horas). O `rfb_cnpj` faz o contrário: o banco é a
linha de base e só o que mudou é gravado. Em ordem do mais barato para o mais caro:

1. **Listagem (1–2 requisições).** Um `PROPFIND` WebDAV (ou a listagem HTTP + um `HEAD` por ZIP) traz tamanho, ETag e
   data de todos os arquivos da pasta mais recente (`AAAA-MM`). Se a impressão digital de todos bate com a guardada em
   `sync_file`, **nada é baixado** e o job termina em segundos. É isso que torna viável rodar todo dia.
2. **Assinatura do ZIP (1 requisição `Range` de ~64 KB por ZIP alterado).** Lê só o diretório central do ZIP remoto
   (CRC32 + tamanho de cada membro). Se o conteúdo é o mesmo (arquivo apenas republicado), não baixa nada.
3. **Download só do que mudou**, com retomada (`Range`/`If-Range`), `.part` + conferência de tamanho, no máximo 2
   conexões, intervalo mínimo entre requisições e backoff com `Retry-After` em 429/5xx.
4. **Diff direto no PostgreSQL.** O CSV é lido do ZIP em streaming (sem extrair para disco) para uma tabela temporária
   via `COPY`; um hash MD5 da linha é comparado com o `row_hash` gravado: `INSERT … ON CONFLICT DO UPDATE … WHERE hash
   diferente` só escreve linhas novas/alteradas (linhas iguais não geram versões mortas nem WAL), e um `DELETE` remove o
   que sumiu daquela parte. Cada ZIP é uma transação única, que também grava o estado: ou tudo entra, ou nada.
5. **Verificação.** Ao fim de cada ZIP, `count(*)` das linhas daquela parte no banco precisa ser igual ao número de
   linhas distintas do arquivo, senão a transação é revertida. `python -m rfb_cnpj verify` confere as tabelas inteiras.

> Os dados são publicados como um *snapshot* mensal. Na prática o diff diário quase sempre diz "nada mudou"; uma vez
> por mês quase todos os ZIPs mudam e é preciso baixá-los (o servidor não permite baixar “só a diferença” de um ZIP,
> pois a compressão deflate não tem acesso aleatório). Nesse dia o ganho está em não reescrever o banco inteiro.

### How to use:
```
pip install -r requirements.txt
cp .env.example .env                    # ajuste banco/fonte
python -m rfb_cnpj init                 # cria schema, tabelas e tabelas de estado
python -m rfb_cnpj check                # barato: sai com código 10 se há mudanças pendentes
python -m rfb_cnpj sync                 # baixa o que mudou e aplica o diff
python -m rfb_cnpj verify               # confere contagens
python -m rfb_cnpj status
```
Agendamento diário (cron): `15 6 * * * cd /app && python -m rfb_cnpj sync >> sync.log 2>&1`.
`--only empresa socios` limita as tabelas; `--force` ignora o estado (ainda aplica só o diff); `--prune` remove dados de
ZIPs que deixaram de existir no servidor. Testes: `pytest` (usa `TEST_DSN`, um PostgreSQL descartável).

Tabelas ficam no schema `cnpj` (configurável). Cada linha tem `row_hash` e `src` (índice da parte do ZIP de origem).
Datas viram `date` (`0`/`00000000`/inválidas → `NULL`), `capital_social` vira `numeric`, e códigos com zeros à esquerda
(CNAE, CEP, CNPJ) permanecem texto. `socios` não tem chave natural; sua identidade é o hash do conteúdo.

---------------------

### Tabelas geradas:
- Para maiores informações, consulte o [layout](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/arquivos/NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf).
  - `empresa`: dados cadastrais da empresa em nível de matriz
  - `estabelecimento`: dados analíticos da empresa por unidade / estabelecimento (telefones, endereço, filial, etc)
  - `socios`: dados cadastrais dos sócios das empresas
  - `simples`: dados de MEI e Simples Nacional
  - `cnae`: código e descrição dos CNAEs
  - `quals`: tabela de qualificação das pessoas físicas - sócios, responsável e representante legal.
  - `natju`: tabela de naturezas jurídicas - código e descrição.
  - `moti`: tabela de motivos da situação cadastral - código e descrição.
  - `pais`: tabela de países - código e descrição.
  - `munic`: tabela de municípios - código e descrição.


- Pelo volume de dados, as tabelas  `empresa`, `estabelecimento`, `socios` e `simples` possuem índices para a coluna `cnpj_basico`, que é a principal chave de ligação entre elas.

### Modelo de Entidade Relacionamento:
![alt text](https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ/blob/master/Dados_RFB_ERD.png)