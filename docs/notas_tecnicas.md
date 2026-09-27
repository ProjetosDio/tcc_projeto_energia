# Notas técnicas: origem e reprodução dos números do TCC

Este documento liga cada número apresentado no TCC ao arquivo de onde ele vem e à consulta ou ao script
que o reproduz. Também registra as decisões de medição que explicam por que algumas métricas vêm dos
logs e não do banco.

## Sumário

- [0. Fontes, convenções e como reproduzir](#0-fontes-convenções-e-como-reproduzir)
- [1. Decisões de medição](#1-decisões-de-medição)
- [2. Tabela 5: taxa de sucesso](#2-tabela-5-taxa-de-sucesso)
- [3. Tabela 6: MTTR e sua decomposição](#3-tabela-6-mttr-e-sua-decomposição)
- [4. Tabela 7: Tempo de ingestão na camada Bronze por grupo e cenário](#4-tabela-7-tempo-de-ingestão-na-camada-bronze-por-grupo-e-cenário)
- [5. Tabela 8: mecanismos acionados e auto-resoluções](#5-tabela-8-mecanismos-acionados-e-auto-resoluções)
- [6. Tabela 9: quarentena](#6-tabela-9-quarentena)
- [7. Detecção no cenário SCHEMA](#7-detecção-no-cenário-schema)
- [8. Tempo de ingestão](#8-tempo-de-ingestão)
- [9. Overhead total e atribuível](#9-overhead-total-e-atribuível)
- [10. Testes estatísticos](#10-testes-estatísticos)
- [11. Limitações que afetam a leitura dos números](#11-limitações-que-afetam-a-leitura-dos-números)
- [Anexo A. Script de reprodução a partir das evidências](#anexo-a-script-de-reprodução-a-partir-das-evidências)
- [Anexo B. Coleta das evidências](#anexo-b-coleta-das-evidências)

---

## 0. Fontes, convenções e como reproduzir

### Execuções

| Rodada | Data (UTC) | Execuções | `id_experimento` | `id_carga` |
|---|---|--:|---|---|
| Matriz com falha injetada: 2 grupos × 4 falhas × 30 iterações | 2026-09-20, 15:56 → 17:02 | 240 | 1–240 | 1–210 (CONEXAO/BASELINE não grava carga) |
| Cenário de controle NENHUMA: 2 grupos × 30 iterações | 2026-09-26, 17:42 → 19:52 | 60 | 241–300 | 211–270 |

### Arquivos de origem

| Arquivo | Conteúdo | Usado para |
|---|---|---|
| `docs/evidencias/tbl_experimento_controle_carga_20260926.csv` | `tbl_experimento` LEFT JOIN `tbl_controle_carga`, as 300 execuções | Tabelas 5, 6, 8 e 9; tempos gravados no banco |
| `docs/evidencias/scheduler_20260926.log` | stdout do contêiner do scheduler, que recebe os logs de `ingestao_bronze.py` e `db_utils.py` das 300 execuções | Tempo de ingestão, decomposição do overhead, linha do tempo da conexão, detecção no SCHEMA |
| `docs/evidencias/energia-postgres_20260926.log` | log do PostgreSQL (paradas e reinícios do contêiner) | Linha do tempo da falha de conexão |
| `docs/evidencias/runner_{baseline,tratamento}_nenhuma_20260926.log` | stdout do runner nas 60 execuções NENHUMA | Conferência da rodada NENHUMA |
| `docs/evidencias/SHA256SUMS.txt` | SHA-256 dos cinco arquivos acima | Integridade (`cd docs/evidencias && sha256sum -c SHA256SUMS.txt`) |
| `scripts/experimento/resultados/01_…` a `05_consolidado_…` | Saída de `exportar_resultados.py` (consultas de `sql/consultas/02_analise_experimento.sql` sobre o banco) | Tabelas 5, 6 (linha CONEXAO) e 8 |
| `scripts/experimento/resultados/05_tempo_ingestao.csv` e `05_tempo_ingestao_resumo.csv` | Saída de `analise_tempo_ingestao.py` (logs + banco) | Tempo de ingestão, validação de schema, overhead |

### Três formas de reproduzir

1. **Offline, só com as evidências versionadas**: o script do [Anexo A](#anexo-a-script-de-reprodução-a-partir-das-evidências)
   lê o CSV e os logs de `docs/evidencias/` e recalcula todos os números deste documento. Não precisa de
   banco nem de Docker, só de Python 3 com `scipy`.
2. **Com o banco `energia_db` populado**: as consultas SQL citadas em cada seção e
   `python scripts/experimento/exportar_resultados.py`.
3. **Tempo de ingestão pelo script do repositório** (lê o log salvo e associa as cargas pelo banco):
   ```bash
   PYTHONIOENCODING=utf-8 python scripts/experimento/analise_tempo_ingestao.py \
       --arquivo-log docs/evidencias/scheduler_20260926.log
   ```

### Convenções

- Desvio-padrão sempre **amostral** (`STDDEV_SAMP` no SQL, `statistics.stdev` no Python).
- Todos os horários estão em UTC: o banco (`Etc/UTC`), o Airflow e o PostgreSQL usam o mesmo fuso.
- Os timestamps do `logging` do Python têm resolução de 1 ms. Todo intervalo medido pelo log tem
  incerteza de ±1 ms.
- "Sucesso" de uma execução é o estado final da DAG run observado pelo runner
  (`tbl_experimento.observacoes = 'success'`), e não o `status` da carga.

---

## 1. Decisões de medição

### 1.1 `NOW()` é o início da transação: o tempo de ingestão vem dos logs

`public.tbl_controle_carga` tem `data_inicio` (DEFAULT `now()`) e `data_fim` (gravado com `SET data_fim = NOW()`).
No PostgreSQL, `NOW()` é `transaction_timestamp()`: devolve o **início da transação corrente**, e não o
instante em que o comando roda. Com o psycopg2 em `autocommit=False`, a transação começa no primeiro comando
depois do `commit()` anterior.

| Caminho | UPDATE de `data_fim` | Primeiro comando da transação | O que `data_fim` marca |
|---|---|---|---|
| Sucesso, sem quarentena | `scripts/ingestao_bronze.py:591` | `TRUNCATE` (`:324`) | o início do TRUNCATE, **antes** de todos os INSERTs |
| Sucesso, com quarentena | `scripts/ingestao_bronze.py:570` | `TRUNCATE` (`:324`) | o início do TRUNCATE |
| Schema inválido (TRATAMENTO) | `scripts/ingestao_bronze.py:237` | o próprio UPDATE | o fim real |
| Exceção no laço de linhas (BASELINE) | `scripts/ingestao_bronze.py:630` | o próprio UPDATE, depois de `conn.rollback()` | o fim real |
| Arquivo ausente (BASELINE) | `docker/airflow/dags/pipeline_energia.py:81-87` | o mesmo INSERT que preenche `data_inicio` | igual a `data_inicio`: **0 ms por construção** |

Exemplo, carga 91 (TRATAMENTO/CONEXAO, iteração 1):

| Evento | Horário (UTC) | Fonte |
|---|---|---|
| `data_inicio` | 16:20:14.681461 | CSV de evidências |
| "Limpando tabela bronze.tbl_carga_raw..." (logo antes do TRUNCATE) | 16:20:14.714 | `scheduler_20260926.log`, linha 4104 |
| `data_fim` | 16:20:14.714667 | CSV de evidências |
| "Carga concluída com sucesso. Registros inseridos: 267" | 16:20:14.963 | `scheduler_20260926.log`, linha 4108 |

`data_fim` coincide com o TRUNCATE. O `data_carga` das 267 linhas dessa carga em `bronze.tbl_carga_raw_hist`
também é 16:20:14.714667, o que confirma o mesmo início de transação:

```sql
SELECT id_carga, data_inicio, data_fim FROM public.tbl_controle_carga WHERE id_carga = 91;
SELECT MIN(data_carga), MAX(data_carga) FROM bronze.tbl_carga_raw_hist WHERE carga_id = 91;
```

**Consequência.** Nos caminhos de sucesso, `data_fim − data_inicio` (≈ 34 ms) cobre só validação de schema,
leitura do CSV e conversões. Deixa de fora os 267 INSERTs, a cópia para o histórico e o commit. Por isso o
**tempo de ingestão** usado no TCC é medido nos logs de `ingestao_bronze.py`, entre
"ID da carga criada: N" (`:225`, logo após o commit do INSERT da carga) e "Carga concluída com sucesso".
O resultado é ≈ 280 ms. O script é `scripts/experimento/analise_tempo_ingestao.py` (§8).

### 1.2 `tempo_recuperacao_ms` só é recuperação real no cenário CONEXAO

A coluna recebe `ResultadoConexao.tempo_total_ms`, cronometrado dentro de `conectar_com_retry()`:

- início: `scripts/db_utils.py:43` (`inicio = time.monotonic()`), antes da 1ª tentativa;
- fim: `:69` (sucesso) ou `:76` (esgotou as tentativas), truncado para `int` em ms.

Nenhum outro ponto do código grava essa coluna. Quarentena, fallback de arquivo e validação de schema não têm
cronômetro próprio. Portanto:

- em **CONEXAO/TRATAMENTO**, a coluna mede de fato o tempo até a conexão voltar, incluindo as tentativas que
  falharam e as esperas do backoff (≈ 11 066 ms);
- em **todos os outros cenários**, inclusive no BASELINE, ela contém o tempo de **uma conexão bem-sucedida
  na 1ª tentativa** (5–8 ms). Não é o tempo da quarentena nem do fallback.

Por isso o MTTR do TCC é **só a linha CONEXAO** de `02_mttr_por_tipo_falha.csv` (§3). As linhas DADOS_NULOS
(6,20 ms) e ARQUIVO_AUSENTE (6,00 ms) desse arquivo são tempo de conexão. O BASELINE registra valores
equivalentes (6,07 ms e 5,17 ms) sem acionar mecanismo nenhum.

`tentativas_recuperacao` conta o total de tentativas, incluindo a primeira: 3 significa "conectou na 3ª
tentativa", ou seja, depois de 2 retries.

`data_inicio` só é gravado depois que a conexão foi estabelecida, então o tempo de retry fica **antes** de
`data_inicio` e não entra em `data_fim − data_inicio`. Os dois intervalos são consecutivos e não se sobrepõem:

```
|<-------- tempo_recuperacao_ms ≈ 11 066 ms -------->|<- data_fim − data_inicio ≈ 34 ms ->|<- ≈ 250 ms ->|
 tentativa 1, espera 2 s, tentativa 2, espera 4 s,    INSERT da carga, validação de        267 INSERTs,
 tentativa 3 OK   (db_utils.py:43 → :69)              schema, leitura CSV, conversões      histórico, commit
                                                      → TRUNCATE
|<---------------------------------------- tempo de ingestão pelo log ≈ 280 ms ------------------------->|
                                                     ("ID da carga criada" → "Carga concluída")
```

### 1.3 Decomposição do overhead pelos marcos do log

`ingestao_bronze.py` escreve uma linha de log em cada etapa do caminho de sucesso. As diferenças entre marcos
consecutivos dão o tempo de cada etapa, com resolução de 1 ms:

| Marco no log | Linha do código | Etapa que termina nele |
|---|---|---|
| "ID da carga criada: N" | `ingestao_bronze.py:225` | (início) |
| "Schema validado com sucesso" (só TRATAMENTO) | `validador_schema.py` | validação de schema |
| "Lendo arquivo: …" | `ingestao_bronze.py:257` | tudo antes da leitura: no TRATAMENTO, `validar_schema_csv()` (`:230`); no BASELINE, só o `if` falso (`:229`) |
| "Limpando tabela bronze.tbl_carga_raw..." | antes de `:324` | leitura do CSV + conversão de tipos |
| "Tabela limpa com sucesso." | depois de `:324` | TRUNCATE |
| "Inserindo dados na tabela histórica…" | — | laço de 267 INSERTs (com savepoint por linha) |
| "Dados copiados para histórico: 267 registros" | — | `INSERT … SELECT` no histórico |
| "Carga concluída com sucesso. Registros inseridos: 267" | — | UPDATE de controle + commit |

Entre "ID da carga criada" e "Lendo arquivo" o único código que difere entre os grupos é a validação de schema.
Por isso esse intervalo mede diretamente o custo dela. As demais etapas executam o mesmo código nos dois grupos
quando não há falha (a checagem de campos nulos e os savepoints rodam nos dois, e a quarentena só age quando
há nulo). Diferenças nelas não podem ser atribuídas a um mecanismo (§9).

### 1.4 Retry com backoff: configuração que determina o MTTR

`scripts/db_utils.py:60-65` usa `tenacity 8.5.0` (versão instalada no contêiner do scheduler):

| Parâmetro | Valor |
|---|---|
| `stop` | `stop_after_attempt(max_tentativas)`, 3 na conexão principal (`ingestao_bronze.py:128`) |
| `wait` | `wait_exponential(multiplier=2)`: espera = 2 × 2^(n−1) s depois da tentativa n que falhou, sem jitter |
| `retry` | `retry_if_exception_type(psycopg2.OperationalError)` |
| `before_sleep` | não configurado: as esperas não aparecem no log, só "Tentativa n/3" |
| BASELINE | `FEATURE_AUTOCORRECAO=OFF` força 1 tentativa (`db_utils.py:39-40`) |

Esperas: **2 s** depois da 1ª tentativa e **4 s** depois da 2ª. A 3ª é a última. O retry do Airflow está
desligado (`"retries": 0` em `pipeline_energia.py:21`). O único retry que existe é o do tenacity.

---

## 2. Tabela 5: taxa de sucesso

| Tipo de falha | BASELINE | TRATAMENTO |
|---|--:|--:|
| CONEXAO | 0/30 (0 %) | 30/30 (100 %) |
| SCHEMA | 0/30 (0 %) | 0/30 (0 %) |
| DADOS_NULOS | 0/30 (0 %) | 30/30 (100 %) |
| ARQUIVO_AUSENTE | 0/30 (0 %) | 30/30 (100 %) |
| NENHUMA (controle) | 30/30 (100 %) | 30/30 (100 %) |

- **Origem:** `scripts/experimento/resultados/01_taxa_sucesso_por_grupo_e_falha.csv` e
  `05_consolidado_baseline_vs_tratamento.csv` (sem NENHUMA). Evidência:
  `docs/evidencias/tbl_experimento_controle_carga_20260926.csv`, coluna `observacoes`.
- **Reprodução:** consultas 1 e 5 de `sql/consultas/02_analise_experimento.sql`
  (via `exportar_resultados.py`), ou a seção "Tabela 5" do Anexo A.
- **Critério:** estado final da DAG run (`observacoes = 'success'`), com LEFT JOIN para não perder as
  execuções que não gravaram carga (CONEXAO/BASELINE).

Na mesma consulta, o `status` gravado em `tbl_controle_carga` é coerente: SUCESSO em todas as execuções
bem-sucedidas e ERRO em todas as outras que têm carga. As 30 execuções CONEXAO/BASELINE não têm carga (§3.4).

---

## 3. Tabela 6: MTTR e sua decomposição

### 3.1 Valor

| Tipo de falha | Execuções recuperadas | MTTR média | DP | Mín | Máx | Tentativas até conectar |
|---|--:|--:|--:|--:|--:|---|
| CONEXAO (TRATAMENTO) | 30 | **11 066 ms (11,07 s)** | 16,96 ms | 11 030 ms | 11 101 ms | 3ª tentativa em 30/30 |

- **Origem:** linha CONEXAO de `scripts/experimento/resultados/02_mttr_por_tipo_falha.csv`. Evidência: colunas
  `tempo_recuperacao_ms` e `tentativas_recuperacao` do CSV de evidências (cargas 91–120), e as 30 linhas
  "Conexão estabelecida após 3 tentativa(s) em N ms" de `scheduler_20260926.log`.
- **Reprodução:** consulta 2 de `sql/consultas/02_analise_experimento.sql`, ou a seção "Tabela 6" do Anexo A.
  O DP amostral vem de:
  ```sql
  SELECT ROUND(AVG(c.tempo_recuperacao_ms)::numeric,2), ROUND(STDDEV_SAMP(c.tempo_recuperacao_ms)::numeric,2),
         MIN(c.tempo_recuperacao_ms), MAX(c.tempo_recuperacao_ms),
         SUM((c.tentativas_recuperacao=3)::int) AS na_3a_tentativa
  FROM public.tbl_experimento e JOIN public.tbl_controle_carga c ON c.id_carga = e.id_carga_afetada
  WHERE e.grupo='TRATAMENTO' AND e.tipo_falha_injetada='CONEXAO';
  ```
- **Só a linha CONEXAO é MTTR** (ver §1.2).

### 3.2 Como a falha é injetada

`scripts/injecao_falhas/injetar_conexao.py` executa `docker stop energia-postgres`, espera `--duracao 10` s,
executa `docker start` e espera mais 5 s. O runner o chama **em segundo plano** (`subprocess.Popen`) e
dispara a DAG logo em seguida. Com o contêiner parado, o DNS interno do Docker deixa de resolver
`energia-postgres`, e cada tentativa falha por DNS, e não por "connection refused". As 60 falhas
registradas no BASELINE (2 por iteração: a conexão principal e a de registro da falha) são todas
`could not translate host name "energia-postgres" to address`, com duração média de **2 544 ms**
(DP 21; 2 515–2 628), extraída de `scheduler_20260926.log`.

### 3.3 Linha do tempo da falha de conexão: iteração 1 (TRATAMENTO, carga 91)

t = 0 é o "received fast shutdown request" do PostgreSQL.

| Horário (UTC) | t (s) | Evento | Fonte |
|---|--:|---|---|
| 16:20:00.553 | −0,10 | runner grava `timestamp_injecao` | CSV de evidências |
| **16:20:00.655** | **0,00** | PostgreSQL recebe o pedido de parada | `energia-postgres_20260926.log`, linha 630 |
| 16:20:00.676 | 0,02 | "database system is shut down" | idem, linha 636 |
| 16:20:01.597 | 0,94 | `check_input_file` começa: o arquivo existe, a task não acessa o banco | `scheduler_20260926.log`, linha 4084 |
| 16:20:02.665 | 2,01 | `ingest_bronze` começa | idem, linha 4096 |
| **16:20:03.622** | **2,97** | **Tentativa 1/3**: falha por DNS em ≈ 2,53 s | idem, linha 4097 |
| | | espera de 2 s (tenacity) | inferido: 4,532 s − 2 s de espera |
| **16:20:08.154** | **7,50** | **Tentativa 2/3**: falha por DNS em ≈ 2,52 s | idem, linha 4098 |
| | | espera de 4 s (tenacity) | inferido: 6,519 s − 4 s de espera |
| 16:20:11.146 | 10,49 | PostgreSQL inicia | `energia-postgres_20260926.log`, linha 640 |
| **16:20:11.160** | **10,51** | **"ready to accept connections"**: banco disponível | idem, linha 645 |
| **16:20:14.673** | **14,02** | **Tentativa 3/3** | `scheduler_20260926.log`, linha 4099 |
| **16:20:14.681** | **14,03** | "Conexão estabelecida após 3 tentativa(s) em 11058 ms"; `data_inicio` = 14.681461 | idem, linha 4100; CSV |
| 16:20:14.683 | 14,03 | "ID da carga criada: 91" | idem, linha 4101 |
| 16:20:14.715 | 14,06 | `data_fim` (= início do TRUNCATE) | CSV |
| 16:20:14.963 | 14,31 | "Carga concluída com sucesso. Registros inseridos: 267" | idem, linha 4108 |
| 16:20:15.673 | 15,02 | `load_silver` começa (conecta na 1ª tentativa) | idem, linha 4120 |

### 3.4 Estatística da linha do tempo nas 30 iterações

Cada sequência "Tentativa 1/3 → 2/3 → 3/3 → Conexão estabelecida" de `scheduler_20260926.log` é associada ao
"received fast shutdown request" imediatamente anterior e ao "ready to accept connections" seguinte em
`energia-postgres_20260926.log` (script no Anexo A, seção "Linha do tempo").

| Intervalo | Média | DP | Mín | Máx |
|---|--:|--:|--:|--:|
| parada → tentativa 1 | 2,662 s | 0,358 | 1,892 | 3,306 |
| tentativa 1 → tentativa 2 (falha DNS + 2 s) | 4,544 s | 0,013 | 4,524 | 4,573 |
| tentativa 2 → tentativa 3 (falha DNS + 4 s) | 6,513 s | 0,012 | 6,489 | 6,533 |
| tentativa 3 → conexão estabelecida | 0,008 s | 0,001 | 0,008 | 0,010 |
| **banco indisponível (parada → "ready")** | **10,545 s** | 0,023 | 10,505 | 10,585 |
| banco disponível → tentativa 3 (pipeline ainda no backoff) | 3,175 s | 0,355 | 2,353 | 3,799 |
| `tempo_recuperacao_ms` | 11,066 s | 0,017 | 11,030 | 11,101 |

### 3.5 Decomposição do MTTR

| Componente | Duração média | Origem |
|---|--:|---|
| Tentativa 1 (falha por DNS) | 2,544 s | (tentativa 1 → 2) − 2 s |
| Espera do backoff após a 1ª falha | 2,000 s | `wait_exponential(multiplier=2)`, n = 1 |
| Tentativa 2 (falha por DNS) | 2,513 s | (tentativa 2 → 3) − 4 s |
| Espera do backoff após a 2ª falha | 4,000 s | `wait_exponential(multiplier=2)`, n = 2 |
| Tentativa 3 (sucesso) | 0,008 s | tentativa 3 → conexão estabelecida |
| **Soma** | **11,065 s** | medido: 11,066 s |

Leitura:

- O MTTR é determinado pelo **cronograma do backoff (2 s + 4 s) somado a duas falhas de DNS de ≈ 2,5 s**. Ele
  não acompanha a duração real da indisponibilidade (10,5 s). Isso explica o DP de só 17 ms.
- Nas 30 iterações o banco voltou entre a 2ª e a 3ª tentativa, e a conexão foi restabelecida na **3ª e última
  tentativa permitida**. O banco ficou disponível de 2,35 a 3,80 s antes da 3ª tentativa, tempo em que o
  pipeline ainda aguardava o backoff.
- Uma indisponibilidade ≈ 2–4 s mais longa esgotaria as 3 tentativas e o TRATAMENTO falharia (§11).

### 3.6 CONEXAO no BASELINE: nenhuma carga gravada

Com a flag OFF, as duas conexões são forçadas a 1 tentativa: a principal (`ingestao_bronze.py:128`) e a que
serviria só para registrar a falha (`:140`). As duas falham por DNS. O script loga "Também não foi possível
conectar para registrar a falha em tbl_controle_carga. Falha não persistida." e termina com `RuntimeError`.
Por isso `id_carga_afetada` é NULL nas 30 execuções CONEXAO/BASELINE, e o sucesso é 0/30 pela DAG run.

---

## 4. Tabela 7: Tempo de ingestão na camada Bronze por grupo e cenário

Tempo de ingestão medido pelos logs ("ID da carga criada" → "Carga concluída com sucesso"), em ms:

| Cenário | Grupo | n | Média | DP | Mín | Máx | Mediana |
|---|---|--:|--:|--:|--:|--:|--:|
| NENHUMA | BASELINE | 30 | 271,13 | 11,12 | 237 | 297 | 269,5 |
| NENHUMA | TRATAMENTO | 30 | 287,27 | 8,71 | 271 | 306 | 287,5 |
| CONEXAO | TRATAMENTO | 30 | 279,93 | 7,18 | 254 | 301 | 280,0 |
| DADOS_NULOS | TRATAMENTO | 30 | 281,93 | 11,76 | 254 | 325 | 282,0 |
| ARQUIVO_AUSENTE | TRATAMENTO | 30 | 278,47 | 13,77 | 246 | 298 | 281,5 |

- **Origem:** linhas `tempo_ingestao_ms` de `scripts/experimento/resultados/05_tempo_ingestao_resumo.csv`
  (por carga: `05_tempo_ingestao.csv`). Evidência: `scheduler_20260926.log`.
- **Reprodução:** `analise_tempo_ingestao.py --arquivo-log docs/evidencias/scheduler_20260926.log`, ou a seção
  "Tempo de ingestão" do Anexo A.
- Em CONEXAO, o tempo de ingestão **não inclui** os ≈ 11 s de recuperação, que ocorrem antes de
  "ID da carga criada" (§1.2). O tempo total da carga nesse cenário é MTTR + ingestão ≈ 11,35 s.
- DADOS_NULOS insere 252 linhas e põe 15 em quarentena. O tempo fica próximo ao dos cenários com 267 INSERTs
  porque cada linha em quarentena também é um INSERT (em `bronze.tbl_carga_quarentena`).
- Os cenários em que a carga falha não têm tempo de ingestão (não chegam a "Carga concluída") e não entram
  nesta tabela. O tempo até a falha no cenário SCHEMA está no §7.

---

## 5. Tabela 8: mecanismos acionados e auto-resoluções

| Mecanismo (`mecanismo_utilizado`) | Cenário | Acionamentos (TRATAMENTO) | Auto-resolvidas | % auto-resolvido |
|---|---|--:|--:|--:|
| RETRY_BACKOFF | CONEXAO | 30 | 30 | 100 % |
| QUARENTENA | DADOS_NULOS | 30 | 30 | 100 % |
| FALLBACK_ARQUIVO | ARQUIVO_AUSENTE | 30 | 30 | 100 % |
| VALIDACAO_SCHEMA | SCHEMA | 30 | 0 | 0 % |

- **Origem:** `scripts/experimento/resultados/03_pct_auto_resolvido.csv`. Evidência: colunas
  `mecanismo_utilizado` e `resolvido_automaticamente` do CSV de evidências.
- **Reprodução:** consulta 3 de `sql/consultas/02_analise_experimento.sql`, e:
  ```sql
  SELECT c.mecanismo_utilizado, e.grupo, COUNT(*) acionamentos, SUM(c.resolvido_automaticamente::int) auto
  FROM public.tbl_experimento e JOIN public.tbl_controle_carga c ON c.id_carga = e.id_carga_afetada
  GROUP BY 1,2 ORDER BY 1,2;
  ```
- **Filtro obrigatório `grupo = 'TRATAMENTO'`.** No BASELINE, o INSERT de arquivo ausente
  (`pipeline_energia.py:81-87`) grava o rótulo `FALLBACK_ARQUIVO` mesmo com o fallback desligado (mensagem:
  "fallback desativado: FEATURE_AUTOCORRECAO=OFF"). Sem o filtro, `FALLBACK_ARQUIVO` aparece 60 vezes.
- A validação de schema **detecta** a falha, mas não a corrige: é *fail fast* por projeto. Por isso tem
  30 acionamentos e 0 auto-resoluções.

---

## 6. Tabela 9: quarentena

| Item | Valor |
|---|--:|
| Linhas por arquivo | 267 |
| Linhas processadas (30 cargas × 267) | 8 010 |
| Linhas válidas inseridas | 7 560 (252 por carga) |
| Linhas em quarentena | 450 (15 por carga) |
| — motivo `Campos obrigatórios nulos: ['VALOR_FATURA']` | 300 (10 por carga) |
| — motivo `Campos obrigatórios nulos: ['MUNICIPIO']` | 150 (5 por carga) |
| Aproveitamento | 7 560 / 8 010 = **94,38 %** |

- **Origem:** colunas `qtd_registros` e `qtd_registros_quarentena` do CSV de evidências (cargas 151–180).
  O detalhamento por motivo vem de `bronze.tbl_carga_quarentena` (só no banco).
- **Reprodução:**
  ```sql
  SELECT SUM(qtd_registros) ins, SUM(qtd_registros_quarentena) quar,
         ROUND(100.0*SUM(qtd_registros)/(SUM(qtd_registros)+SUM(qtd_registros_quarentena)),2) pct
  FROM public.tbl_controle_carga c JOIN public.tbl_experimento e ON e.id_carga_afetada = c.id_carga
  WHERE e.grupo='TRATAMENTO' AND e.tipo_falha_injetada='DADOS_NULOS';

  SELECT q.motivo_rejeicao, COUNT(*) total, COUNT(DISTINCT q.id_carga) cargas
  FROM bronze.tbl_carga_quarentena q
  JOIN public.tbl_experimento e ON e.id_carga_afetada = q.id_carga
  WHERE e.grupo='TRATAMENTO' AND e.tipo_falha_injetada='DADOS_NULOS'
  GROUP BY 1;
  ```
  Os totais também saem da seção "Tabela 9" do Anexo A.
- `injetar_dados_nulos.py` usa seed = 42, então as linhas afetadas são sempre as mesmas: 13, 16, 17, 45, 48,
  53, 58, 72, 112, 115, 120, 126, 141, 217 e 259.
- No BASELINE, a mesma linha 13 aborta a carga inteira: 0 linhas aproveitadas.

---

## 7. Detecção no cenário SCHEMA

`injetar_schema.py` faz um `replace` de substring no cabeçalho. Por isso **duas colunas** mudam:
`VALOR_FATURA → VALOR_FAT` e `VALOR_FATURA_SUB → VALOR_FAT_SUB`.

| | BASELINE | TRATAMENTO |
|---|---|---|
| Onde detecta | no laço de linhas, na **linha 1** (`ingestao_bronze.py:436`) | na validação do cabeçalho (`validar_schema_csv()`, `ingestao_bronze.py:230`), antes de ler os dados |
| Como classifica | `tipo_falha = 'DADOS_NULOS'`, mecanismo `NENHUM` | `tipo_falha = 'SCHEMA'`, mecanismo `VALIDACAO_SCHEMA` |
| Mensagem gravada | `Campos obrigatórios nulos: ['VALOR_FATURA']` | `Schema inválido: 2 colunas faltando, 2 colunas extras. Faltando: ['VALOR_FATURA', 'VALOR_FATURA_SUB']. Extras: ['VALOR_FAT', 'VALOR_FAT_SUB']` |
| O que roda antes da falha | leitura completa do CSV, conversões, TRUNCATE, INSERT da 1ª linha, rollback | leitura só do cabeçalho (`pd.read_csv(nrows=0)`) |
| Tempo até a falha (`data_fim − data_inicio`, ms) | **36,63** (DP 2,62; 34,49–48,15) | **10,77** (DP 0,46; 10,03–12,12) |
| Resultado | DAG failed, 30/30 | DAG failed, 30/30 (`sys.exit(2)` → `AirflowFailException`, sem retry) |

- **Métrica:** `data_fim − data_inicio` de `public.tbl_controle_carga`. Nos dois caminhos de falha o UPDATE
  de `data_fim` é o primeiro comando de uma transação nova (`ingestao_bronze.py:237` no TRATAMENTO, `:630`
  depois do `rollback()` no BASELINE), então `data_fim` marca o fim real (§1.1).
- **Origem:** colunas `data_inicio`, `data_fim`, `tipo_falha`, `mecanismo_utilizado` e `mensagem_erro` do CSV
  de evidências, cargas 1–30 (BASELINE) e 121–150 (TRATAMENTO).
- **Reprodução:**
  ```sql
  SELECT e.grupo, COUNT(c.id_carga) n,
         ROUND(AVG(EXTRACT(EPOCH FROM (c.data_fim-c.data_inicio))*1000)::numeric,2) media_ms,
         ROUND(STDDEV_SAMP(EXTRACT(EPOCH FROM (c.data_fim-c.data_inicio))*1000)::numeric,2) dp_ms,
         ROUND(MIN(EXTRACT(EPOCH FROM (c.data_fim-c.data_inicio))*1000)::numeric,2) min_ms,
         ROUND(MAX(EXTRACT(EPOCH FROM (c.data_fim-c.data_inicio))*1000)::numeric,2) max_ms
  FROM public.tbl_experimento e
  JOIN public.tbl_controle_carga c ON c.id_carga = e.id_carga_afetada
  WHERE e.tipo_falha_injetada = 'SCHEMA'
  GROUP BY 1 ORDER BY 1;
  ```
  Ou a seção "data_fim − data_inicio" do Anexo A (linhas SCHEMA).

No BASELINE, a validação de schema é pulada (`ingestao_bronze.py:229`). A coluna renomeada só se manifesta
quando `row.get("VALOR_FATURA")` devolve `None` na primeira linha, e a falha acaba registrada como dado nulo.
No TRATAMENTO, a falha é detectada ≈ 3,4× mais cedo, antes de tocar na bronze, e é classificada com a causa
correta.

---

## 8. Tempo de ingestão

Métricas extraídas por `scripts/experimento/analise_tempo_ingestao.py` (ver §1.3 para os marcos):

| Coluna em `05_tempo_ingestao*.csv` | Intervalo no log |
|---|---|
| `tempo_ingestao_ms` | "ID da carga criada: N" → "Carga concluída com sucesso" |
| `pre_leitura_ms` | "ID da carga criada: N" → "Lendo arquivo:" |
| `validacao_schema_ms` | "ID da carga criada: N" → "Schema validado com sucesso" (só TRATAMENTO) |
| `leitura_ate_concluida_ms` | "Lendo arquivo:" → "Carga concluída com sucesso" |

Os resultados de `tempo_ingestao_ms` estão no §4. O custo da validação de schema é estável entre cenários:

| `validacao_schema_ms` (TRATAMENTO) | NENHUMA | CONEXAO | DADOS_NULOS | ARQUIVO_AUSENTE |
|---|--:|--:|--:|--:|
| Média (DP), ms | 8,87 (0,63) | 9,20 (0,48) | 8,90 (0,61) | 8,83 (0,53) |

- **Origem:** `scripts/experimento/resultados/05_tempo_ingestao_resumo.csv`.
- `05_tempo_ingestao.csv` tem uma linha por carga com experimento associado (270). Cargas que não chegam a
  "Carga concluída" (as que falham) ficam com o tempo vazio.
- O tempo medido começa depois da conexão e termina no log "Carga concluída". Não inclui a inicialização do
  subprocesso Python nem o overhead do Airflow: a task `ingest_bronze` inteira leva ≈ 1,3 s nos dois grupos.

---

## 9. Overhead total e atribuível

### 9.1 Overhead total (cenário NENHUMA)

| | BASELINE (mecanismos OFF) | TRATAMENTO (mecanismos ON) | Diferença |
|---|--:|--:|--:|
| Média | 271,13 ms | 287,27 ms | **+16,13 ms (+5,95 %)** |
| Mediana | 269,5 ms | 287,5 ms | +18,0 ms (+6,68 %) |

- **Origem:** `05_tempo_ingestao_resumo.csv` (linhas `tempo_ingestao_ms`, cenário NENHUMA). Evidência:
  `scheduler_20260926.log`, cargas 211–270.
- **Reprodução:** `analise_tempo_ingestao.py` imprime o overhead e os testes no final da execução; ou a seção
  "Decomposição do overhead" do Anexo A.

### 9.2 Decomposição por etapa

Médias (DP) em ms, 30 execuções por grupo, cenário NENHUMA:

| Etapa (marcos do log) | BASELINE | TRATAMENTO | Δ | Mann-Whitney p |
|---|--:|--:|--:|--:|
| Validação de schema ("ID criada" → "Lendo arquivo") | 0,07 (0,25) | 8,90 (0,61) | **+8,83** | 9,0 × 10⁻¹³ |
| Leitura do CSV + conversões ("Lendo arquivo" → "Limpando tabela") | 23,60 (1,19) | 22,23 (0,90) | −1,37 | 4,9 × 10⁻⁷ |
| TRUNCATE ("Limpando tabela" → "Tabela limpa") | 10,03 (5,68) | 15,07 (5,28) | +5,03 | 1,6 × 10⁻⁵ |
| Laço de 267 INSERTs ("Tabela limpa" → "Inserindo dados na tabela histórica") | 230,97 (6,55) | 233,17 (4,89) | +2,20 | 0,31 (n.s.) |
| INSERT…SELECT no histórico ("Inserindo…" → "Dados copiados") | 2,60 (0,56) | 2,33 (0,48) | −0,27 | 0,062 (n.s.) |
| UPDATE de controle + commit ("Dados copiados" → "Carga concluída") | 3,87 (3,27) | 5,57 (3,39) | +1,70 | 0,0024 |
| **Total** | **271,13 (11,12)** | **287,27 (8,71)** | **+16,13** | 3,1 × 10⁻⁷ |

- **Origem:** `scheduler_20260926.log` (não há CSV de resultados por etapa).
- **Reprodução:** seção "Decomposição do overhead" do Anexo A.

### 9.3 Overhead atribuível

- **Validação de schema: +8,8 ms por carga.** É a única etapa em que o TRATAMENTO executa código que o BASELINE
  não executa (§1.3). Equivale a **3,3 %** do tempo de ingestão do BASELINE e a 55 % do overhead total.
- **Laço de INSERT (+2,2 ms, não significativo).** Sem falha, o código é idêntico nos dois grupos: a checagem
  de campos nulos (`ingestao_bronze.py:426-436`) e os savepoints por linha rodam nos dois.
- **TRUNCATE (+5,0 ms) e UPDATE/commit (+1,7 ms).** São operações de banco com código idêntico nos dois grupos
  e DP alto. Não podem ser atribuídas a um mecanismo. A hipótese mais provável é o estado do banco: o
  TRATAMENTO rodou depois do BASELINE, com o banco maior e uma carga silver mais pesada logo antes de cada
  ingestão (§11). Essa hipótese não foi testada.
- **Leitura do CSV (−1,4 ms).** É compatível com o arquivo já estar no cache do sistema operacional, depois de
  a validação ter lido o cabeçalho alguns milissegundos antes. Também é hipótese não testada.

**Resultado:** overhead atribuível ≈ **8,8 ms (3,3 %)**; overhead total medido **+16,1 ms (+6,0 %)**, que
funciona como limite superior.

### 9.4 Métrica do banco para o mesmo cenário

`04_overhead_execucao.csv` compara `data_fim − data_inicio`: BASELINE 27,19 ms e TRATAMENTO 35,65 ms
(Δ = +8,46 ms). Como essa métrica termina no TRUNCATE (§1.1), ela só alcança a validação de schema, e por isso
o Δ fica próximo dos 8,8 ms medidos diretamente. Não mede o overhead da ingestão completa e não é usada no TCC.

---

## 10. Testes estatísticos

Comparação TRATAMENTO × BASELINE no cenário NENHUMA, 30 × 30 cargas, testes bilaterais:

```python
scipy.stats.mannwhitneyu(trat, base, alternative="two-sided")
scipy.stats.ttest_ind(trat, base, equal_var=False)   # t de Welch
```

| Métrica | Δ média | Mann-Whitney U | p (MW) | t de Welch | gl | p (Welch) |
|---|--:|--:|--:|--:|--:|--:|
| `tempo_ingestao_ms` | +16,13 | 796,5 | 3,06 × 10⁻⁷ | 6,255 | 54,8 | 6,27 × 10⁻⁸ |
| `pre_leitura_ms` (validação de schema) | +8,83 | 900,0 | 8,96 × 10⁻¹³ | 73,50 | 38,8 | 2,76 × 10⁻⁴³ |
| `leitura_ate_concluida_ms` | +7,30 | 646,5 | 0,0037 | 2,827 | 54,8 | 0,0065 |

- **Origem:** `scheduler_20260926.log`, via `05_tempo_ingestao.csv`.
- **Reprodução:** `analise_tempo_ingestao.py` (stdout, bloco "overhead NENHUMA") ou o Anexo A. Versão usada:
  `scipy 1.18.1`.
- U = 900 em `pre_leitura_ms` é o máximo possível (30 × 30): todas as cargas do TRATAMENTO são mais lentas que
  todas as do BASELINE nessa etapa.

**Tendência dentro de cada grupo** (Spearman, iteração × tempo, NENHUMA), para avaliar deriva ao longo da rodada:

| Grupo | Métrica | ρ | p | Média das 10 primeiras | Média das 10 últimas |
|---|---|--:|--:|--:|--:|
| BASELINE | `tempo_ingestao_ms` | 0,488 | 0,006 | 265,1 | 279,4 |
| TRATAMENTO | `tempo_ingestao_ms` | −0,392 | 0,032 | 290,3 | 282,6 |
| BASELINE | laço de INSERT | 0,198 | 0,294 | 227,4 | 232,9 |
| TRATAMENTO | laço de INSERT | 0,064 | 0,737 | 232,7 | 232,4 |

As tendências do tempo total têm sinais opostos, então não há uma deriva monotônica única que explique a
diferença entre os grupos. O efeito do ambiente também não pode ser descartado (§11). No laço de INSERT não há
tendência em nenhum grupo.

---

## 11. Limitações que afetam a leitura dos números

1. **Grupos não intercalados.** As 30 execuções BASELINE rodam antes das 30 TRATAMENTO (desenho do runner).
   Diferenças de ambiente ficam confundidas com o grupo. Por isso o overhead é reportado como atribuível
   (8,8 ms) e total (16,1 ms). O desenho mais robusto seria alternar os grupos a cada iteração.
2. **A carga silver não é idempotente.** Cada execução bem-sucedida acumula linhas nas tabelas silver. Durante a
   rodada NENHUMA, `silver.tbl_cip` passou de 33,5 milhões para 150,9 milhões de linhas, o banco foi de 6,2 GB
   para 26 GB, e a task `load_silver` foi de 45 s para 233 s. A task `ingest_bronze` ficou em ≈ 1,3 s nos dois
   grupos, e não houve sobreposição entre DAG runs. Fonte: metadados do banco `airflow` (não versionados):
   ```sql
   -- banco airflow: nenhuma DAG run NENHUMA sobreposta a outra
   SELECT count(*) FROM dag_run a JOIN dag_run b
     ON a.run_id < b.run_id AND a.start_date < b.end_date AND b.start_date < a.end_date
   WHERE a.run_id LIKE 'exp_%nenhuma%' AND b.run_id LIKE 'exp_%nenhuma%';
   ```
3. **Timeout do runner na rodada NENHUMA/TRATAMENTO.** Por causa do item 2, a DAG passaria do
   `POLL_TIMEOUT_S = 180` do runner. Se isso acontecesse, a próxima iteração começaria com a silver anterior
   ainda rodando, e o TRUNCATE da bronze esperaria o lock. O TRATAMENTO rodou com o timeout elevado a 900 s
   **só em memória**, sem alterar `runner.py`. A DAG mais longa levou 237,5 s. Comando usado:
   ```bash
   PYTHONUNBUFFERED=1 python -c "
   import sys
   sys.path.insert(0, 'scripts/experimento')
   import runner
   runner.POLL_TIMEOUT_S = 900
   sys.argv = ['runner.py', '--grupo', 'TRATAMENTO', '--tipo-falha', 'NENHUMA', '--iteracoes', '30']
   runner.main()
   "
   ```
   O BASELINE rodou com o comando padrão
   (`python scripts/experimento/runner.py --grupo BASELINE --tipo-falha NENHUMA --iteracoes 30`).
   A saída dos dois está em `docs/evidencias/runner_*_nenhuma_20260926.log`.
4. **Margem do retry.** A recuperação de CONEXAO dependeu do cronograma fixo do backoff e ocorreu sempre na
   última tentativa, com 2,4–3,8 s de folga (§3.4).
5. **Resolução do log: 1 ms.** Os efeitos principais (+16 ms no total; 8,8 ms com DP 0,6 na validação) são bem
   maiores que isso.

---

## Anexo A. Script de reprodução a partir das evidências

Recalcula, só com os arquivos de `docs/evidencias/`, todos os números das seções 2 a 10 (exceto o detalhamento
da quarentena por motivo, que está só no banco). Rode a partir da raiz do repositório:

```bash
PYTHONIOENCODING=utf-8 python reproduzir.py   # salve o código abaixo como reproduzir.py
```

```python
import csv, re, statistics as st
from collections import Counter, defaultdict
from datetime import datetime as D
from scipy import stats

EV = "docs/evidencias/"
rows = list(csv.DictReader(open(EV + "tbl_experimento_controle_carga_20260926.csv", encoding="utf-8")))
g = defaultdict(list)
for r in rows:
    g[(r["grupo"], r["tipo_falha_injetada"])].append(r)
cargas = {int(r["id_carga"]): r for r in rows if r["id_carga"]}
dur = lambda r: (D.fromisoformat(r["data_fim"]) - D.fromisoformat(r["data_inicio"])).total_seconds() * 1000
resumo = lambda v: f"n={len(v)} média={st.mean(v):.2f} dp={st.stdev(v):.2f} mín={min(v)} máx={max(v)} mediana={st.median(v)}"

print("== Tabela 5: sucessos (observacoes='success')")
for k in sorted(g):
    print(k, sum(r["observacoes"] == "success" for r in g[k]), "/", len(g[k]))

print("\n== Tabela 6 (tempo_recuperacao_ms, linha CONEXAO/TRATAMENTO) e data_fim − data_inicio (linhas SCHEMA: §7)")
for k in sorted(g):
    v = [r for r in g[k] if r["id_carga"]]
    if not v:
        print(k, "sem carga gravada"); continue
    print(k, "data_fim−data_inicio:", resumo([round(dur(r), 2) for r in v]))
    print("   tempo_recuperacao_ms:", resumo([int(r["tempo_recuperacao_ms"]) for r in v]),
          "| tentativas:", dict(Counter(r["tentativas_recuperacao"] for r in v)))

print("\n== Tabela 8: acionamentos e auto-resoluções")
for grp in ("BASELINE", "TRATAMENTO"):
    c = Counter((r["mecanismo_utilizado"], r["resolvido_automaticamente"]) for r in cargas.values() if r["grupo"] == grp)
    print(grp, dict(c))

print("\n== Tabela 9: quarentena (TRATAMENTO/DADOS_NULOS)")
v = g[("TRATAMENTO", "DADOS_NULOS")]
ins = sum(int(r["qtd_registros"]) for r in v); quar = sum(int(r["qtd_registros_quarentena"]) for r in v)
print(f"inseridas={ins} quarentena={quar} aproveitamento={100 * ins / (ins + quar):.2f}%")

# --- marcos do log de ingestao_bronze.py, por id_carga
RE = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}) - (\S+) - (\w+) - (.*)$")
MARCOS = [("schema", "Schema validado com sucesso"), ("lendo", "Lendo arquivo"), ("limpando", "Limpando tabela"),
          ("limpa", "Tabela limpa"), ("hist", "Inserindo dados na tabela hist"), ("copiados", "Dados copiados"),
          ("ok", "Carga concluída")]
log, cur, sched = {}, None, []
for l in open(EV + "scheduler_20260926.log", encoding="utf-8", errors="replace"):
    m = RE.match(l.rstrip("\r\n"))
    if not m:
        continue
    t = D.strptime(m.group(1), "%Y-%m-%d %H:%M:%S,%f"); msg = m.group(4)
    sched.append((t, m.group(2), msg))
    mi = re.match(r"ID da carga criada: (\d+)$", msg)
    if mi:
        cur = {"id": t}; log[int(mi.group(1))] = cur; continue
    if cur is not None:
        for k, p in MARCOS:
            if msg.startswith(p):
                cur.setdefault(k, t)

def ms(k, a, b):
    d = log.get(k, {})
    return (d[b] - d[a]).total_seconds() * 1000 if a in d and b in d else None

def amostra(grp, cen, a, b):
    return [x for k, r in cargas.items() if r["grupo"] == grp and r["tipo_falha_injetada"] == cen
            for x in [ms(k, a, b)] if x is not None]

print("\n== Tempo de ingestão (Tabela 7, §4) e validação de schema (§8)")
for grp, cen in [("BASELINE", "NENHUMA"), ("TRATAMENTO", "NENHUMA"), ("TRATAMENTO", "CONEXAO"),
                 ("TRATAMENTO", "DADOS_NULOS"), ("TRATAMENTO", "ARQUIVO_AUSENTE")]:
    print(grp, cen, "ingestão:", resumo(amostra(grp, cen, "id", "ok")))
    if grp == "TRATAMENTO":
        print("   validação de schema:", resumo(amostra(grp, cen, "id", "schema")))

print("\n== Decomposição do overhead e testes (§9, §10), cenário NENHUMA")
for a, b in [("id", "lendo"), ("lendo", "limpando"), ("limpando", "limpa"), ("limpa", "hist"),
             ("hist", "copiados"), ("copiados", "ok"), ("lendo", "ok"), ("id", "ok")]:
    base, trat = amostra("BASELINE", "NENHUMA", a, b), amostra("TRATAMENTO", "NENHUMA", a, b)
    mw = stats.mannwhitneyu(trat, base, alternative="two-sided")
    w = stats.ttest_ind(trat, base, equal_var=False)
    print(f"{a}→{b}: B {st.mean(base):.2f} ({st.stdev(base):.2f})  T {st.mean(trat):.2f} ({st.stdev(trat):.2f})  "
          f"Δ {st.mean(trat) - st.mean(base):+.2f}  Δmediana {st.median(trat) - st.median(base):+.1f}  "
          f"U={mw.statistic:.1f} p={mw.pvalue:.3g}  t={w.statistic:.3f} gl={w.df:.1f} p={w.pvalue:.3g}")

print("\n== Spearman iteração × tempo (§10)")
for grp in ("BASELINE", "TRATAMENTO"):
    for a, b in [("id", "ok"), ("limpa", "hist")]:
        pares = sorted((int(r["iteracao"]), ms(k, a, b)) for k, r in cargas.items()
                       if r["grupo"] == grp and r["tipo_falha_injetada"] == "NENHUMA")
        v = [p[1] for p in pares]
        s = stats.spearmanr([p[0] for p in pares], v)
        print(f"{grp} {a}→{b}: rho={s.statistic:.3f} p={s.pvalue:.3f} "
              f"10 primeiras={st.mean(v[:10]):.1f} 10 últimas={st.mean(v[-10:]):.1f}")

print("\n== Linha do tempo da falha de conexão (§3.4)")
pg = []
for l in open(EV + "energia-postgres_20260926.log", encoding="utf-8", errors="replace"):
    m = re.search(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3}) UTC \[\d+\] LOG:  "
                  r"(received fast shutdown request|database system is ready to accept connections)", l)
    if m:
        pg.append((D.strptime(m.group(1), "%Y-%m-%d %H:%M:%S.%f"), "parada" if "shutdown" in m.group(2) else "pronto"))
seqs, s = [], {}
for t, orig, msg in sched:
    if orig != "db_utils":
        continue
    mt = re.match(r"Tentativa (\d)/3 ", msg)
    if mt:
        s = {} if mt.group(1) == "1" else s
        s["t" + mt.group(1)] = t
    mc = re.match(r"Conexão estabelecida após 3 tentativa\(s\) em (\d+) ms", msg)
    if mc and "t3" in s:
        s["ok"] = t; s["ms"] = int(mc.group(1)); seqs.append(s); s = {}
iv = defaultdict(list)
for s in seqs:
    parada = max(t for t, e in pg if e == "parada" and t < s["t1"])
    pronto = min(t for t, e in pg if e == "pronto" and t > parada)
    sec = lambda a, b: (b - a).total_seconds()
    for nome, a, b in [("parada→t1", parada, s["t1"]), ("t1→t2", s["t1"], s["t2"]), ("t2→t3", s["t2"], s["t3"]),
                       ("t3→ok", s["t3"], s["ok"]), ("parada→pronto", parada, pronto), ("pronto→t3", pronto, s["t3"])]:
        iv[nome].append(sec(a, b))
    iv["tempo_recuperacao_s"].append(s["ms"] / 1000)
print("sequências:", len(seqs))
for k, v in iv.items():
    print(f"{k}: média={st.mean(v):.3f} dp={st.stdev(v):.3f} mín={min(v):.3f} máx={max(v):.3f}")
falhas = [int(x) for _, _, msg in sched
          for x in re.findall(r"Falha ao conectar após 1 tentativa\(s\) em (\d+) ms: could not translate", msg)]
print("falhas de DNS (BASELINE):", resumo(falhas))
```

---

## Anexo B. Coleta das evidências

Os arquivos de `docs/evidencias/` foram coletados em 2026-09-26, depois das 60 execuções NENHUMA:

```bash
docker logs projeto_tcc_energia-airflow-scheduler-1 > docs/evidencias/scheduler_20260926.log 2>&1
docker logs energia-postgres > docs/evidencias/energia-postgres_20260926.log 2>&1
docker exec -i energia-postgres psql -U energia_user -d energia_db -c "\copy (SELECT e.*, c.* FROM public.tbl_experimento e LEFT JOIN public.tbl_controle_carga c ON c.id_carga=e.id_carga_afetada ORDER BY e.id_experimento) TO STDOUT WITH CSV HEADER" > docs/evidencias/tbl_experimento_controle_carga_20260926.csv
cd docs/evidencias && sha256sum *.log *.csv > SHA256SUMS.txt
```

Os `docker logs` só existem enquanto o contêiner existir: `docker compose down` ou a recriação do contêiner os
apaga. As cópias versionadas são a fonte primária para reproduzir a análise. Os arquivos de
`docs/evidencias/` são versionados sem conversão de fim de linha (`.gitattributes`: `docs/evidencias/** -text`),
para que `sha256sum -c SHA256SUMS.txt` passe num clone novo. Eles não devem ser editados.
