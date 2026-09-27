# Pipeline ETL com mecanismos de autocorreção

Trabalho de Conclusão de Curso do **MBA em Engenharia de Software da USP/Esalq**, 2026.

- **Autor:** Diogenes dos Santos Oliveira
- **Orientador:** Anaximandro Anderson Pereira Melo De Souza

Este repositório contém o pipeline ETL e o experimento usados no TCC. O pipeline processa dados de
faturamento de energia elétrica e da Contribuição para Iluminação Pública (CIP) e tem quatro mecanismos
de autocorreção. O experimento compara o pipeline **sem** esses mecanismos (BASELINE) e **com** eles
(TRATAMENTO) sob falhas injetadas de forma controlada.

> **Sobre os dados:** todos os dados são fictícios. Veja [Aviso sobre os dados](#aviso-sobre-os-dados).

---

## Objetivo

Avaliar se mecanismos de autocorreção embutidos no pipeline aumentam a taxa de execuções concluídas com
sucesso diante de falhas comuns em ETL, e medir o custo desses mecanismos em tempo de recuperação e em
overhead quando nada falha.

## Arquitetura

```
                    ┌───────────────────────────── Airflow (DAG energia_pipeline) ─────────────────────────┐
 data/input/*.csv → │    check_input_file        → ingest_bronze      → load_silver    → load_gold         │
                    └──────────┬─────────────────────────┬──────────────────┬──────────────┬───────────────┘
                               │                         │                  │              │
                               ▼                         ▼                  ▼              ▼
PostgreSQL energia_db: public.tbl_controle_carga      bronze.*           silver.*       gold.vw_*
```

A arquitetura segue o padrão medalhão, com três camadas no PostgreSQL (`energia_db`):

| Camada | Conteúdo | Scripts |
|---|---|---|
| **bronze** | Cópia bruta do CSV (`bronze.tbl_carga_raw`), histórico de todas as cargas (`tbl_carga_raw_hist`) e quarentena de linhas rejeitadas (`tbl_carga_quarentena`) | `sql/bronze/`, `scripts/ingestao_bronze.py` |
| **silver** | Modelo normalizado: clientes, instalações, endereços, faturas, pagamentos, CIP e domínios (31 tabelas) | `sql/silver/` |
| **gold** | Views analíticas: consumo, arrecadação e perda de arrecadação da CIP, ticket médio, rankings | `sql/gold/` |

Cada carga é registrada em `public.tbl_controle_carga` com status, tipo de falha, mecanismo acionado,
tentativas e tempo de recuperação. As execuções do experimento ficam em `public.tbl_experimento`.

### Tasks da DAG (`docker/airflow/dags/pipeline_energia.py`)

| # | Task | O que faz |
|---|------|-----------|
| 1 | `check_input_file` | Verifica se o CSV de entrada existe. Se não existir, aciona o fallback de arquivo |
| 2 | `ingest_bronze` | Executa `scripts/ingestao_bronze.py`: conexão com retry, validação de schema, leitura, conversão de tipos e carga linha a linha com quarentena |
| 3 | `load_silver` | Executa, em ordem, os `.sql` de `sql/silver/` |
| 4 | `load_gold` | Executa os `.sql` de `sql/gold/` (views) |

## Mecanismos de autocorreção

Os quatro mecanismos são ligados e desligados juntos pela Airflow Variable `feature_autocorrecao`
(`ON` = TRATAMENTO, `OFF` = BASELINE). A detecção do problema acontece nos dois grupos. O que muda
é a reação automática.

| Mecanismo (`mecanismo_utilizado`) | Classe de falha tratada (`tipo_falha`) | Onde | Comportamento com ON | Com OFF |
|---|---|---|---|---|
| **Retry com backoff exponencial** (`RETRY_BACKOFF`) | `CONEXAO`: indisponibilidade transitória do PostgreSQL | `scripts/db_utils.py` (tenacity) | Até 3 tentativas, com esperas de 2 s e 4 s, só em `psycopg2.OperationalError` | 1 tentativa |
| **Quarentena** (`QUARENTENA`) | `DADOS_NULOS`: campos obrigatórios nulos em algumas linhas | `scripts/ingestao_bronze.py` | Isola a linha em `bronze.tbl_carga_quarentena` e segue com o lote | A primeira linha inválida aborta o lote inteiro |
| **Validação de schema** (`VALIDACAO_SCHEMA`) | `SCHEMA`: colunas renomeadas ou ausentes no CSV | `scripts/validador_schema.py` | Compara o cabeçalho com as 59 colunas esperadas e falha cedo (*fail fast*), com diagnóstico e sem retry. Detecta o problema, mas não o corrige | Validação pulada |
| **Fallback de arquivo** (`FALLBACK_ARQUIVO`) | `ARQUIVO_AUSENTE`: arquivo de entrada primário não encontrado | `scripts/verificar_arquivo.py` | Usa o `Amostra*.csv` mais recente do diretório | Falha imediata |

## Requisitos

- Docker e Docker Compose (Docker Desktop no Windows).
- Python 3.10+ **no host**, para o runner do experimento e os scripts de análise:
  ```bash
  pip install psycopg2-binary requests scipy
  ```
  O `requirements.txt` lista as dependências do pipeline, que já estão na imagem do Airflow. O `scipy`
  é usado só por `scripts/experimento/analise_tempo_ingestao.py`.
- Bash (Git Bash no Windows) para `scripts/experimento/rodar_tudo.sh`.
- Portas livres: 5432 (PostgreSQL), 8088 (Airflow) e 8080 (pgAdmin).

## Como subir o ambiente

```bash
cd docker
docker compose up -d
```

Serviços (projeto `projeto_tcc_energia`):

| Serviço | Acesso | Credenciais (somente ambiente local) |
|---|---|---|
| Airflow (webserver) | http://localhost:8088 | `admin` / `admin` |
| PostgreSQL `energia_db` | `localhost:5432` | `energia_user` / `energia_pass` |
| pgAdmin | http://localhost:8080 | `admin@energia.com` / `admin` |

Na primeira subida, o PostgreSQL executa os scripts de `sql/init`, `sql/bronze`, `sql/silver` e `sql/gold`
montados em `/docker-entrypoint-initdb.d/`. O `airflow-init` cria o usuário e a variável
`feature_autocorrecao=ON`. O `airflow-conn-init` despausa a DAG.

As credenciais estão fixas no `docker-compose.yml` e servem apenas ao ambiente local de
desenvolvimento. Não reutilize esse arquivo em nenhum ambiente exposto.

## Como executar o pipeline

Pela interface do Airflow (DAG `energia_pipeline` → *Trigger DAG*) ou pela linha de comando:

```bash
docker exec projeto_tcc_energia-airflow-scheduler-1 airflow dags trigger energia_pipeline
```

Para alternar entre os modos:

```bash
docker exec projeto_tcc_energia-airflow-scheduler-1 airflow variables set feature_autocorrecao OFF   # BASELINE
docker exec projeto_tcc_energia-airflow-scheduler-1 airflow variables set feature_autocorrecao ON    # TRATAMENTO (padrão)
```

## Como reproduzir o experimento

Todos os comandos rodam **no host**, a partir da raiz do repositório, com o ambiente de pé.

### Injeção de falhas (`scripts/injecao_falhas/`)

| Script | Falha | Uso |
|---|---|---|
| `injetar_conexao.py` | Para o contêiner `energia-postgres` por N segundos e o sobe de novo | `python scripts/injecao_falhas/injetar_conexao.py --duracao 10` |
| `injetar_schema.py` | Troca o trecho `VALOR_FATURA` por `VALOR_FAT` no cabeçalho do CSV, o que altera duas colunas: `VALOR_FATURA` → `VALOR_FAT` e `VALOR_FATURA_SUB` → `VALOR_FAT_SUB` | `... injetar_schema.py aplicar` / `restaurar` |
| `injetar_dados_nulos.py` | Esvazia `VALOR_FATURA` em 10 linhas e `MUNICIPIO` em 5 (seed = 42) | `... injetar_dados_nulos.py aplicar` / `restaurar` |
| `injetar_arquivo_ausente.py` | Move o CSV para `.bkp` e deixa uma cópia `Amostra_Backup_Fallback.csv` | `... injetar_arquivo_ausente.py aplicar` / `restaurar` |
| `restaurar_tudo.py` | Desfaz qualquer injeção de arquivo pendente (restaura o `.bkp`) | `python scripts/injecao_falhas/restaurar_tudo.py` |

Antes de qualquer rodada, confira se não há `.bkp` pendente em `data/input/`. Se houver, rode
`restaurar_tudo.py`.

### Runner (`scripts/experimento/runner.py`)

Para cada iteração, o runner registra a execução em `tbl_experimento`, injeta a falha, dispara a DAG pela API
REST, aguarda o fim, restaura o ambiente e associa a carga (`id_carga_afetada`):

```bash
python scripts/experimento/runner.py --grupo TRATAMENTO --tipo-falha CONEXAO --iteracoes 30
# --grupo: BASELINE | TRATAMENTO
# --tipo-falha: CONEXAO | SCHEMA | DADOS_NULOS | ARQUIVO_AUSENTE | NENHUMA
```

Matriz completa (2 grupos × 4 tipos de falha × 30 iterações; **não inclui** NENHUMA):

```bash
bash scripts/experimento/rodar_tudo.sh 30
```

Cenário de controle, sem falha, usado para medir o overhead:

```bash
python scripts/experimento/runner.py --grupo BASELINE   --tipo-falha NENHUMA --iteracoes 30
python scripts/experimento/runner.py --grupo TRATAMENTO --tipo-falha NENHUMA --iteracoes 30
```

> O runner espera no máximo 180 s por DAG run (`POLL_TIMEOUT_S`). Como a carga silver não é idempotente
> (ver [Limitações](#limitações-conhecidas)), a DAG fica mais lenta a cada execução bem-sucedida. Na rodada
> NENHUMA/TRATAMENTO de 2026-09-26, o timeout foi elevado para 900 s **em tempo de execução**, sem
> alterar o arquivo. Veja o comando exato em [docs/notas_tecnicas.md](docs/notas_tecnicas.md) §11.

### Exportação e análise

```bash
# Consultas de sql/consultas/02_analise_experimento.sql → CSVs + resultados_consolidado.md
python scripts/experimento/exportar_resultados.py

# Tempo real de ingestão por carga, extraído dos logs do scheduler (ver Limitações)
python scripts/experimento/analise_tempo_ingestao.py
# ou, para reproduzir a partir da cópia versionada do log:
python scripts/experimento/analise_tempo_ingestao.py --arquivo-log docs/evidencias/scheduler_20260926.log
```

## Resultados, notas técnicas e evidências

| Onde | O que contém |
|---|---|
| `scripts/experimento/resultados/` | CSVs gerados: taxa de sucesso (`01_`), MTTR (`02_`), % auto-resolvido (`03_`), overhead por `data_fim − data_inicio` (`04_`), consolidado (`05_consolidado_…`), tempo de ingestão pelos logs (`05_tempo_ingestao.csv` e `_resumo.csv`), além de `resultados_consolidado.md` |
| [docs/notas_tecnicas.md](docs/notas_tecnicas.md) | Origem e reprodução de cada número do TCC, decisões de medição, linha do tempo da falha de conexão, decomposição do MTTR e do overhead, testes estatísticos e um script que recalcula tudo a partir de `docs/evidencias/` |
| `docs/evidencias/` | Cópias dos logs do scheduler, do PostgreSQL e dos runners, extrato das 300 execuções (`tbl_experimento` × `tbl_controle_carga`) e **`SHA256SUMS.txt`** com o hash de cada arquivo |

### Tabela do TCC → arquivo(s) de origem

| Tabela / número do TCC | Arquivo(s) de origem | Detalhes |
|---|---|---|
| Tabela 5: taxa de sucesso | `resultados/01_taxa_sucesso_por_grupo_e_falha.csv`, `resultados/05_consolidado_baseline_vs_tratamento.csv` | [notas §2](docs/notas_tecnicas.md#2-tabela-5-taxa-de-sucesso) |
| Tabela 6: MTTR | `resultados/02_mttr_por_tipo_falha.csv` (**só a linha CONEXAO**) | [notas §3](docs/notas_tecnicas.md#3-tabela-6-mttr-e-sua-decomposição) |
| Decomposição do MTTR e linha do tempo da falha de conexão | `evidencias/scheduler_20260926.log`, `evidencias/energia-postgres_20260926.log` | [notas §3.3–3.5](docs/notas_tecnicas.md#33-linha-do-tempo-da-falha-de-conexão-iteração-1-tratamento-carga-91) |
| Tabela 7: tempo de ingestão na camada Bronze por grupo e cenário | `resultados/05_tempo_ingestao_resumo.csv` | [notas §4](docs/notas_tecnicas.md#4-tabela-7-tempo-de-ingestão-na-camada-bronze-por-grupo-e-cenário) |
| Tabela 8: mecanismos e auto-resoluções | `resultados/03_pct_auto_resolvido.csv` | [notas §5](docs/notas_tecnicas.md#5-tabela-8-mecanismos-acionados-e-auto-resoluções) |
| Tabela 9: quarentena | `evidencias/tbl_experimento_controle_carga_20260926.csv` (colunas `qtd_registros*`) | [notas §6](docs/notas_tecnicas.md#6-tabela-9-quarentena) |
| Detecção no cenário SCHEMA | `evidencias/scheduler_20260926.log`, `evidencias/tbl_experimento_controle_carga_20260926.csv` | [notas §7](docs/notas_tecnicas.md#7-detecção-no-cenário-schema) |
| Tempo de ingestão e custo da validação de schema | `resultados/05_tempo_ingestao.csv`, `resultados/05_tempo_ingestao_resumo.csv` | [notas §8](docs/notas_tecnicas.md#8-tempo-de-ingestão) |
| Overhead total (+16,1 ms) e atribuível (≈ 8,8 ms) | `resultados/05_tempo_ingestao_resumo.csv`; decomposição por etapa: `evidencias/scheduler_20260926.log` | [notas §9](docs/notas_tecnicas.md#9-overhead-total-e-atribuível) |
| Testes estatísticos (Mann-Whitney, t de Welch, Spearman) | `resultados/05_tempo_ingestao.csv` / `evidencias/scheduler_20260926.log` | [notas §10](docs/notas_tecnicas.md#10-testes-estatísticos) |

Os caminhos `resultados/` e `evidencias/` são relativos a `scripts/experimento/` e `docs/`, respectivamente.

> **Métricas do banco que não são as do TCC.** Dois arquivos de `resultados/` usam métricas gravadas no banco
> que não correspondem às usadas no TCC:
>
> - `02_mttr_por_tipo_falha.csv`: as linhas **DADOS_NULOS** e **ARQUIVO_AUSENTE** trazem `tempo_recuperacao_ms`,
>   que fora do cenário CONEXAO é só o tempo de uma conexão bem-sucedida (≈ 6 ms), e não o tempo da quarentena
>   ou do fallback. O TCC usa **só a linha CONEXAO**.
> - `04_overhead_execucao.csv`: usa `data_fim − data_inicio`, que termina no início do `TRUNCATE` (`NOW()` devolve
>   o início da transação) e deixa de fora os INSERTs. O TCC mede o tempo de ingestão **pelos logs**
>   (`05_tempo_ingestao.csv` e `05_tempo_ingestao_resumo.csv`).
>
> A explicação completa está em [docs/notas_tecnicas.md §1](docs/notas_tecnicas.md#1-decisões-de-medição).

Para conferir a integridade das evidências:

```bash
cd docs/evidencias && sha256sum -c SHA256SUMS.txt
```

Os arquivos de `docs/evidencias/` **não devem ser editados**, porque qualquer alteração invalida os hashes.

## Estrutura de pastas

```
.
├── data/input/                      CSV de entrada anonimizado (Amostra_Base_Dados.csv, 267 linhas)
├── docker/
│   ├── docker-compose.yml           PostgreSQL (energia + airflow), Airflow 2.9.3, pgAdmin
│   └── airflow/
│       ├── dags/pipeline_energia.py DAG energia_pipeline (4 tasks)
│       └── plugins/
├── docs/
│   ├── notas_tecnicas.md            origem e reprodução dos números do TCC
│   └── evidencias/                  logs e extratos com SHA-256
├── scripts/
│   ├── db_utils.py                  conexão com retry/backoff (tenacity)
│   ├── ingestao_bronze.py           ingestão bronze + quarentena
│   ├── validador_schema.py          validação de schema
│   ├── verificar_arquivo.py         fallback de arquivo
│   ├── aplicar_migracao.py
│   ├── anonimizar_dados.py          anonimização do CSV de entrada
│   ├── injecao_falhas/              scripts de injeção e restauração
│   └── experimento/
│       ├── runner.py                runner BASELINE × TRATAMENTO
│       ├── rodar_tudo.sh            matriz completa
│       ├── exportar_resultados.py
│       ├── analise_tempo_ingestao.py
│       └── resultados/
├── sql/
│   ├── init/                        esquemas, tbl_controle_carga, migração de autocorreção
│   ├── bronze/  silver/  gold/      DDL e cargas por camada
│   └── consultas/                   consultas de inspeção e de análise do experimento
└── requirements.txt
```

## Limitações conhecidas

1. **A carga silver não é idempotente.** Cada execução bem-sucedida acumula dados nas tabelas silver
   (`silver.tbl_cip` passou de 150 milhões de linhas depois de 150 execuções de sucesso), e o `load_silver`
   fica progressivamente mais lento (de < 2 s para > 230 s). Isso não afeta a ingestão bronze medida no
   experimento, mas pode estourar o timeout de 180 s do runner. Para uma nova rodada limpa, recrie o
   volume do banco (`docker compose down -v`, **o que apaga todos os dados e resultados gravados**).
2. **`data_fim` usa `NOW()`, que é o início da transação.** No caminho de sucesso, a transação do UPDATE
   final começa no `TRUNCATE`, antes dos INSERTs. Por isso `data_fim − data_inicio` (≈ 34 ms) não mede a
   ingestão completa (≈ 280 ms) e também não inclui o tempo de retry. O tempo de ingestão é medido
   **pelos logs** ("ID da carga criada" → "Carga concluída") com `analise_tempo_ingestao.py`. Da mesma
   forma, `tempo_recuperacao_ms` só mede recuperação de fato no cenário CONEXAO. Nos demais, registra
   apenas o tempo de uma conexão bem-sucedida. Detalhes em
   [docs/notas_tecnicas.md](docs/notas_tecnicas.md) §1.
3. **Os grupos foram executados em sequência.** As 30 iterações de BASELINE rodam antes das 30 de
   TRATAMENTO, sem intercalação. Diferenças ambientais, como o crescimento do banco, ficam confundidas
   com o grupo. Na medição de overhead, só ~8,8 ms dos +16,1 ms observados podem ser atribuídos
   diretamente a um mecanismo (validação de schema). Ver [docs/notas_tecnicas.md](docs/notas_tecnicas.md) §9–11.
4. A recuperação de CONEXAO depende do cronograma fixo do backoff (2 s + 4 s). Nas 30 execuções, a
   conexão voltou na **última** tentativa permitida, com folga de 2 a 4 s. Uma indisponibilidade um pouco
   mais longa esgotaria as tentativas.
5. Os logs de `ingestao_bronze.py` só existem no stdout do contêiner do scheduler, que se perde quando o
   contêiner é recriado. Por isso as cópias ficam em `docs/evidencias/`.

## Aviso sobre os dados

Os dados são fictícios, gerados pelo autor. Após a execução do experimento, a versão publicada do CSV teve endereços, bairros, CEPs, coordenadas e identificadores (conta, instalação, fatura) substituídos por valores genéricos, por precaução, já que os nomes de logradouros usados como referência geográfica correspondem a ruas existentes. A estrutura, a ordem das linhas, os campos obrigatórios e os demais valores foram preservados, de modo que a reprodução do experimento gera os mesmos comportamentos relatados no TCC. O script de anonimização está em `scripts/anonimizar_dados.py`.

Colunas substituídas: `ENDERECO`, `NUMERO`, `COMPLEMENTO`, `BAIRRO`, `CEP`, `LATITUDE`, `LONGITUDE`,
`CONTA_CONTRATO`, `INSTALACAO`, `FATURA` e `DOCUMENTO_IMPRESSAO`. `NOME`, `CPF` e `CNPJ` já continham
valores genéricos (`NOME DA PESSOA FISICA`, `EMPRESA LTDA` e um CPF/CNPJ fixo).

Os logs e extratos de `docs/evidencias/` foram gerados antes da anonimização e não foram alterados, para
preservar os hashes. Eles não contêm valores das colunas substituídas.
