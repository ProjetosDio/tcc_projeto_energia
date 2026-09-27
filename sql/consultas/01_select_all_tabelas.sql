-- ============================================================
-- Consulta geral: SELECT * de todas as tabelas e views
-- Projeto: TCC Energia
-- ============================================================


-- ============================================================
-- SCHEMA: public
-- ============================================================

-- public.tbl_controle_carga
SELECT * FROM public.tbl_controle_carga LIMIT 100;


-- ============================================================
-- SCHEMA: bronze
-- ============================================================

-- bronze.tbl_carga_raw
SELECT * FROM bronze.tbl_carga_raw LIMIT 100;

-- bronze.tbl_carga_raw_hist
SELECT * FROM bronze.tbl_carga_raw_hist LIMIT 100;


-- ============================================================
-- SCHEMA: silver — Tabelas de Domínio
-- ============================================================

-- silver.tbl_tipo_status_fatura
SELECT * FROM silver.tbl_tipo_status_fatura LIMIT 100;

-- silver.tbl_tipo_status_parecer
SELECT * FROM silver.tbl_tipo_status_parecer LIMIT 100;

-- silver.tbl_tipo_classe_consumo
SELECT * FROM silver.tbl_tipo_classe_consumo LIMIT 100;

-- silver.tbl_tipo_subclasse_consumo
SELECT * FROM silver.tbl_tipo_subclasse_consumo LIMIT 100;

-- silver.tbl_tipo_status_comercial
SELECT * FROM silver.tbl_tipo_status_comercial LIMIT 100;

-- silver.tbl_tipo_cliente
SELECT * FROM silver.tbl_tipo_cliente LIMIT 100;

-- silver.tbl_tipo_categoria_tarifaria
SELECT * FROM silver.tbl_tipo_categoria_tarifaria LIMIT 100;

-- silver.tbl_tipo_entidade
SELECT * FROM silver.tbl_tipo_entidade LIMIT 100;


-- ============================================================
-- SCHEMA: silver — Dimensões
-- ============================================================

-- silver.tbl_municipio
SELECT * FROM silver.tbl_municipio LIMIT 100;

-- silver.tbl_distribuidora
SELECT * FROM silver.tbl_distribuidora LIMIT 100;

-- silver.tbl_endereco
SELECT * FROM silver.tbl_endereco LIMIT 100;

-- silver.tbl_cliente
SELECT * FROM silver.tbl_cliente LIMIT 100;

-- silver.tbl_cliente_pf
SELECT * FROM silver.tbl_cliente_pf LIMIT 100;

-- silver.tbl_cliente_pj
SELECT * FROM silver.tbl_cliente_pj LIMIT 100;

-- silver.tbl_perfil_regulatorio_instalacao
SELECT * FROM silver.tbl_perfil_regulatorio_instalacao LIMIT 100;

-- silver.tbl_instalacao
SELECT * FROM silver.tbl_instalacao LIMIT 100;

-- silver.tbl_escopo
SELECT * FROM silver.tbl_escopo LIMIT 100;

-- silver.tbl_usuario_escopo
SELECT * FROM silver.tbl_usuario_escopo LIMIT 100;


-- ============================================================
-- SCHEMA: silver — Fatos
-- ============================================================

-- silver.tbl_fatura
SELECT * FROM silver.tbl_fatura LIMIT 100;

-- silver.tbl_pagamento
SELECT * FROM silver.tbl_pagamento LIMIT 100;

-- silver.tbl_imposto_taxa
SELECT * FROM silver.tbl_imposto_taxa LIMIT 100;

-- silver.tbl_ciclo_leitura
SELECT * FROM silver.tbl_ciclo_leitura LIMIT 100;

-- silver.tbl_refaturamento
SELECT * FROM silver.tbl_refaturamento LIMIT 100;

-- silver.tbl_cip
SELECT * FROM silver.tbl_cip LIMIT 100;


-- ============================================================
-- SCHEMA: silver — Leis e Documentos
-- ============================================================

-- silver.tbl_lei
SELECT * FROM silver.tbl_lei LIMIT 100;

-- silver.tbl_lei_documento
SELECT * FROM silver.tbl_lei_documento LIMIT 100;

-- silver.tbl_lei_log
SELECT * FROM silver.tbl_lei_log LIMIT 100;


-- ============================================================
-- SCHEMA: silver — Simulação CIP
-- ============================================================

-- silver.tbl_simulacao_cip
SELECT * FROM silver.tbl_simulacao_cip LIMIT 100;

-- silver.tbl_simulacao_faixa_consumo
SELECT * FROM silver.tbl_simulacao_faixa_consumo LIMIT 100;

-- silver.tbl_simulacao_resultado
SELECT * FROM silver.tbl_simulacao_resultado LIMIT 100;


-- ============================================================
-- SCHEMA: silver — Auditoria
-- ============================================================

-- silver.tbl_auditoria_evento
SELECT * FROM silver.tbl_auditoria_evento LIMIT 100;


-- ============================================================
-- SCHEMA: gold — Views
-- ============================================================

-- gold.vw_base_cip
SELECT * FROM gold.vw_base_cip LIMIT 100;

-- gold.vw_consumo_por_bairro
SELECT * FROM gold.vw_consumo_por_bairro LIMIT 100;

-- gold.vw_consumo_mensal
SELECT * FROM gold.vw_consumo_mensal LIMIT 100;

-- gold.vw_media_conta_por_bairro
SELECT * FROM gold.vw_media_conta_por_bairro LIMIT 100;

-- gold.vw_arrecadacao_cip_por_classe
SELECT * FROM gold.vw_arrecadacao_cip_por_classe LIMIT 100;

-- gold.vw_arrecadacao_cip_por_subclasse
SELECT * FROM gold.vw_arrecadacao_cip_por_subclasse LIMIT 100;

-- gold.vw_arrecadacao_cip_total
SELECT * FROM gold.vw_arrecadacao_cip_total LIMIT 100;

-- gold.vw_faturamento_cip
SELECT * FROM gold.vw_faturamento_cip LIMIT 100;

-- gold.vw_ticket_medio_cip_por_classe
SELECT * FROM gold.vw_ticket_medio_cip_por_classe LIMIT 100;

-- gold.vw_ticket_medio_cip_por_municipio
SELECT * FROM gold.vw_ticket_medio_cip_por_municipio LIMIT 100;

-- gold.vw_consumo_por_classe_subclasse
SELECT * FROM gold.vw_consumo_por_classe_subclasse LIMIT 100;

-- gold.vw_ranking_bairros_consumo
SELECT * FROM gold.vw_ranking_bairros_consumo LIMIT 100;

-- gold.vw_perda_arrecadacao_cip
SELECT * FROM gold.vw_perda_arrecadacao_cip LIMIT 100;

-- gold.vw_perda_arrecadacao_cip_por_bairro
SELECT * FROM gold.vw_perda_arrecadacao_cip_por_bairro LIMIT 100;
