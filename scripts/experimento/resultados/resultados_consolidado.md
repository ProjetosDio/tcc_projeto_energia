# Resultados do experimento comparativo (BASELINE x TRATAMENTO)

Gerado automaticamente por `scripts/experimento/exportar_resultados.py` a partir das consultas em `sql/consultas/02_analise_experimento.sql`.

## Taxa de sucesso por grupo e tipo de falha

Para cada combinação (grupo, tipo de falha injetada), total de execuções e fração que o runner observou como concluída com sucesso (e.observacoes='success'). LEFT JOIN: falhas graves (ex.: CONEXAO no BASELINE) podem nem chegar a gravar uma carga em tbl_controle_carga, e não podem sumir da contagem.

| grupo | tipo_falha_injetada | total_execucoes | sucessos | taxa_sucesso_pct |
| --- | --- | --- | --- | --- |
| BASELINE | ARQUIVO_AUSENTE | 30 | 0 | 0.00 |
| TRATAMENTO | ARQUIVO_AUSENTE | 30 | 30 | 100.00 |
| BASELINE | CONEXAO | 30 | 0 | 0.00 |
| TRATAMENTO | CONEXAO | 30 | 30 | 100.00 |
| BASELINE | DADOS_NULOS | 30 | 0 | 0.00 |
| TRATAMENTO | DADOS_NULOS | 30 | 30 | 100.00 |
| BASELINE | NENHUMA | 30 | 30 | 100.00 |
| TRATAMENTO | NENHUMA | 30 | 30 | 100.00 |
| BASELINE | SCHEMA | 30 | 0 | 0.00 |
| TRATAMENTO | SCHEMA | 30 | 0 | 0.00 |

## MTTR (tempo médio de recuperação) por tipo de falha

Apenas grupo TRATAMENTO, apenas cargas com resolvido_automaticamente=TRUE: quão rápido cada mecanismo de autocorreção resolveu a falha.

| tipo_falha | execucoes_recuperadas | mttr_ms | mttr_s | min_ms | max_ms |
| --- | --- | --- | --- | --- | --- |
| CONEXAO | 30 | 11066.00 | 11.066 | 11030 | 11101 |
| DADOS_NULOS | 30 | 6.20 | 0.006 | 6 | 8 |
| ARQUIVO_AUSENTE | 30 | 6.00 | 0.006 | 6 | 6 |

## Percentual de falhas resolvidas automaticamente (TRATAMENTO)

Das execuções em que uma falha foi injetada (exclui o controle NENHUMA), fração que terminou auto-resolvida.

| tipo_falha_injetada | execucoes | auto_resolvidas | pct_auto_resolvido |
| --- | --- | --- | --- |
| CONEXAO | 30 | 30 | 100.00 |
| SCHEMA | 30 | 0 | 0.00 |
| DADOS_NULOS | 30 | 30 | 100.00 |
| ARQUIVO_AUSENTE | 30 | 30 | 100.00 |

## Overhead médio de execução (sem falha injetada)

Compara o tempo total de carga entre BASELINE e TRATAMENTO apenas nas execuções de controle (tipo_falha_injetada='NENHUMA'), isolando o custo dos próprios mecanismos de autocorreção.

| grupo | execucoes | tempo_medio_ms |
| --- | --- | --- |
| TRATAMENTO | 30 | 35.65 |
| BASELINE | 30 | 27.19 |

## Consolidado: BASELINE x TRATAMENTO por tipo de falha

Tabela principal do TCC: sucessos e taxa de sucesso de cada grupo lado a lado, por tipo de falha injetada. Sucesso = e.observacoes='success'; LEFT JOIN para não descartar execuções sem carga registrada.

| Tipo de Falha | Baseline Sucesso | Tratamento Sucesso | Baseline % | Tratamento % |
| --- | --- | --- | --- | --- |
| ARQUIVO_AUSENTE | 0 | 30 | 0.0 | 100.0 |
| CONEXAO | 0 | 30 | 0.0 | 100.0 |
| DADOS_NULOS | 0 | 30 | 0.0 | 100.0 |
| SCHEMA | 0 | 0 | 0.0 | 0.0 |
