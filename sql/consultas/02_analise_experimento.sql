-- Consultas de análise do experimento comparativo BASELINE x TRATAMENTO.
-- Usadas para gerar os números da seção "Resultados e Discussão" do TCC.
-- Cada carga (tbl_controle_carga) é ligada à sua execução de experimento
-- (tbl_experimento) via id_carga_afetada, preenchido pelo runner.

-- 1. Taxa de sucesso por grupo e tipo de falha.
-- Para cada combinação (grupo, tipo de falha injetada), quantas execuções
-- houve e qual fração o runner observou como concluída com sucesso
-- (e.observacoes='success', vindo do polling da DAG na API do Airflow).
-- Critério baseado em tbl_experimento, não em tbl_controle_carga: falhas
-- graves (ex.: CONEXAO no BASELINE) podem nem chegar a gravar uma carga,
-- e LEFT JOIN garante que essas execuções continuem contadas como
-- tentativas (e como não-sucesso), em vez de somem da contagem.
SELECT grupo, tipo_falha_injetada,
       COUNT(*) as total_execucoes,
       SUM(CASE WHEN e.observacoes='success' THEN 1 ELSE 0 END) as sucessos,
       ROUND(100.0 * SUM(CASE WHEN e.observacoes='success' THEN 1 ELSE 0 END)
             / COUNT(*), 2) as taxa_sucesso_pct
FROM public.tbl_experimento e
LEFT JOIN public.tbl_controle_carga tcc ON tcc.id_carga = e.id_carga_afetada
GROUP BY grupo, tipo_falha_injetada
ORDER BY tipo_falha_injetada, grupo;


-- 2. MTTR (tempo médio de recuperação) por tipo de falha.
-- Só considera o grupo TRATAMENTO e cargas efetivamente auto-resolvidas
-- (resolvido_automaticamente=TRUE), medindo quão rápido cada mecanismo
-- de autocorreção resolveu a falha (tempo_recuperacao_ms).
SELECT tipo_falha, COUNT(*) as execucoes_recuperadas,
       ROUND(AVG(tempo_recuperacao_ms)::numeric, 2) as mttr_ms,
       ROUND(AVG(tempo_recuperacao_ms)::numeric / 1000, 3) as mttr_s,
       MIN(tempo_recuperacao_ms) as min_ms,
       MAX(tempo_recuperacao_ms) as max_ms
FROM public.tbl_experimento e
JOIN public.tbl_controle_carga tcc ON tcc.id_carga = e.id_carga_afetada
WHERE e.grupo = 'TRATAMENTO'
  AND tcc.resolvido_automaticamente = TRUE
  AND tcc.tempo_recuperacao_ms IS NOT NULL
GROUP BY tipo_falha;


-- 3. Percentual de falhas resolvidas automaticamente (só grupo TRATAMENTO).
-- Das execuções em que uma falha foi de fato injetada (exclui o controle
-- NENHUMA), qual fração terminou com resolvido_automaticamente=TRUE.
SELECT tipo_falha_injetada,
       COUNT(*) as execucoes,
       SUM(CASE WHEN tcc.resolvido_automaticamente THEN 1 ELSE 0 END) as auto_resolvidas,
       ROUND(100.0 * SUM(CASE WHEN tcc.resolvido_automaticamente THEN 1 ELSE 0 END)
             / COUNT(*), 2) as pct_auto_resolvido
FROM public.tbl_experimento e
JOIN public.tbl_controle_carga tcc ON tcc.id_carga = e.id_carga_afetada
WHERE e.grupo = 'TRATAMENTO' AND e.tipo_falha_injetada != 'NENHUMA'
GROUP BY tipo_falha_injetada;


-- 4. Overhead médio de execução (custo dos mecanismos de autocorreção
-- quando NADA falha). Compara o tempo total de carga (data_fim - data_inicio)
-- entre BASELINE e TRATAMENTO apenas nas execuções de controle
-- (tipo_falha_injetada='NENHUMA'), isolando o overhead dos próprios
-- mecanismos (validação de schema, checagem de campos, etc.) do tempo
-- gasto se recuperando de falhas reais.
SELECT grupo,
       COUNT(*) as execucoes,
       ROUND(AVG(EXTRACT(EPOCH FROM (data_fim - data_inicio)) * 1000)::numeric, 2) as tempo_medio_ms
FROM public.tbl_experimento e
LEFT JOIN public.tbl_controle_carga tcc ON tcc.id_carga = e.id_carga_afetada
WHERE e.tipo_falha_injetada = 'NENHUMA'
GROUP BY grupo;


-- 5. Consolidado (tabela principal do TCC): BASELINE vs TRATAMENTO lado a
-- lado, por tipo de falha injetada, com contagem de sucessos e taxa de
-- sucesso percentual de cada grupo — o resumo mais direto do ganho trazido
-- pela autocorreção. Sucesso = e.observacoes='success' (ver nota da
-- query 1); LEFT JOIN para não descartar execuções sem carga registrada.
SELECT tipo_falha_injetada as "Tipo de Falha",
       SUM(CASE WHEN grupo='BASELINE' AND e.observacoes='success' THEN 1 ELSE 0 END) as "Baseline Sucesso",
       SUM(CASE WHEN grupo='TRATAMENTO' AND e.observacoes='success' THEN 1 ELSE 0 END) as "Tratamento Sucesso",
       ROUND(100.0 * SUM(CASE WHEN grupo='BASELINE' AND e.observacoes='success' THEN 1 ELSE 0 END)
             / NULLIF(SUM(CASE WHEN grupo='BASELINE' THEN 1 ELSE 0 END), 0), 1) as "Baseline %",
       ROUND(100.0 * SUM(CASE WHEN grupo='TRATAMENTO' AND e.observacoes='success' THEN 1 ELSE 0 END)
             / NULLIF(SUM(CASE WHEN grupo='TRATAMENTO' THEN 1 ELSE 0 END), 0), 1) as "Tratamento %"
FROM public.tbl_experimento e
LEFT JOIN public.tbl_controle_carga tcc ON tcc.id_carga = e.id_carga_afetada
WHERE tipo_falha_injetada != 'NENHUMA'
GROUP BY tipo_falha_injetada
ORDER BY tipo_falha_injetada;
