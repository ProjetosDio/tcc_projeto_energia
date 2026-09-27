-- Base enriquecida com dimensoes e marcadores de pagamento
CREATE OR REPLACE VIEW gold.vw_base_cip AS
WITH pagamento AS (
	SELECT
		p.id_fatura,
		SUM(p.vlr_arrecadado) AS total_pago
	FROM silver.tbl_pagamento p
	GROUP BY p.id_fatura
)
SELECT
	f.id_fatura,
	f.id_cliente,
	f.id_instalacao,
	i.codigo_instalacao,
	c.nome AS nome_cliente,
	cls.descricao_classe_consumo AS classe,
	sub.descricao_subclasse_consumo AS subclasse,
	e.bairro,
	m.nome_municipio AS municipio,
	f.mes_competencia,
	f.mes_referencia,
	to_date(f.mes_competencia || '01','YYYYMMDD') AS dt_mes,
	ROUND(f.consumo_medido, 2) AS consumo_medido,
	ROUND(f.consumo_faturado, 2) AS consumo_faturado,
    ROUND(f.valor_fatura, 2) AS valor_fatura,
	ROUND(COALESCE(cp.valor_cip, 0), 2) AS valor_cip_faturado,
	ROUND(COALESCE(pg.total_pago, 0), 2) AS valor_pago_total,
	ROUND(COALESCE(cp.valor_cip, 0)* LEAST(COALESCE(pg.total_pago, 0) / NULLIF(f.valor_fatura, 0), 1),2) AS valor_cip_arrecadado
     
FROM silver.tbl_fatura f
JOIN silver.tbl_instalacao i ON i.id_instalacao = f.id_instalacao
LEFT JOIN silver.tbl_tipo_subclasse_consumo sub ON sub.id_subclasse = i.id_subclasse
LEFT JOIN silver.tbl_tipo_classe_consumo cls ON cls.id_classe = sub.id_classe
LEFT JOIN silver.tbl_endereco e ON e.id_endereco = i.id_endereco
LEFT JOIN silver.tbl_municipio m ON m.id_municipio = e.id_municipio
LEFT JOIN silver.tbl_cliente c ON c.id_cliente = f.id_cliente
LEFT JOIN silver.tbl_cip cp ON cp.id_fatura = f.id_fatura
LEFT JOIN pagamento pg ON pg.id_fatura = f.id_fatura;

-- 1. Consumo Energetico por Bairro (kWh medido)
CREATE OR REPLACE VIEW gold.vw_consumo_por_bairro AS
SELECT
	dt_mes,
	bairro,
	ROUND(SUM(consumo_medido), 2) AS kwh_consumido
FROM gold.vw_base_cip
GROUP BY dt_mes, bairro;


-- 2. Consumo Mensal de Energia (kWh total do municipio)
CREATE OR REPLACE VIEW gold.vw_consumo_mensal AS
SELECT
    dt_mes,
    municipio,
    ROUND(SUM(consumo_medido), 2) AS kwh_consumido
FROM gold.vw_base_cip
GROUP BY dt_mes, municipio;

-- 3. Media da Conta de Luz por Bairro
CREATE OR REPLACE VIEW gold.vw_media_conta_por_bairro AS
SELECT
	dt_mes,
	bairro,
	COUNT(*) AS total_registros,
	ROUND(SUM(valor_fatura), 2) AS total_fatura,
	ROUND(AVG(valor_fatura), 2) AS valor_medio_fatura
FROM gold.vw_base_cip
GROUP BY dt_mes, bairro;

-- 4. Arrecadacao da CIP por Classe (pago)
CREATE OR REPLACE VIEW gold.vw_arrecadacao_cip_por_classe AS
SELECT
	dt_mes,
	classe,
	ROUND(SUM(valor_cip_arrecadado), 2) AS cip_arrecadado
FROM gold.vw_base_cip
GROUP BY dt_mes, classe;

-- 5. Arrecadacao da CIP por Subclasse (pago)
CREATE OR REPLACE VIEW gold.vw_arrecadacao_cip_por_subclasse AS
SELECT
	dt_mes,
	classe,
	subclasse,
	ROUND(SUM(valor_cip_arrecadado), 2) AS cip_arrecadado
FROM gold.vw_base_cip
GROUP BY dt_mes, classe, subclasse;

-- 6. Arrecadacao Total da CIP (pago - Visão municipal) 
CREATE OR REPLACE VIEW gold.vw_arrecadacao_cip_total AS
SELECT
    dt_mes,
    municipio,
    ROUND(SUM(valor_cip_arrecadado), 2) AS cip_arrecadado
FROM gold.vw_base_cip
GROUP BY dt_mes, municipio;

-- 7. Faturamento da CIP (gerado nas faturas)
CREATE OR REPLACE VIEW gold.vw_faturamento_cip AS
SELECT
	dt_mes,
	ROUND(SUM(valor_cip_faturado), 2) AS cip_faturado
FROM gold.vw_base_cip
GROUP BY dt_mes;

-- 8.1 Ticket Medio da CIP por classe
CREATE OR REPLACE VIEW gold.vw_ticket_medio_cip_por_classe AS
SELECT
	dt_mes,
	classe,
	COUNT(DISTINCT id_cliente) AS total_clientes,
	ROUND(SUM(valor_cip_faturado), 2) AS total_cip_faturado,
	ROUND(SUM(valor_cip_faturado) / NULLIF(COUNT(DISTINCT id_cliente),0), 2) AS ticket_medio_cip
FROM gold.vw_base_cip
GROUP BY dt_mes, classe;

-- 8.2 Ticket Medio da CIP por municipio
CREATE OR REPLACE VIEW gold.vw_ticket_medio_cip_por_municipio AS
SELECT
	dt_mes,
	municipio,
	COUNT(DISTINCT id_cliente) AS total_clientes,
	ROUND(SUM(valor_cip_faturado), 2) AS total_cip_faturado,
	ROUND(SUM(valor_cip_faturado) / NULLIF(COUNT(DISTINCT id_cliente),0), 2) AS ticket_medio_cip
FROM gold.vw_base_cip
GROUP BY dt_mes, municipio;

-- 9. Consumo por Classe/Subclasse (kWh medido)
CREATE OR REPLACE VIEW gold.vw_consumo_por_classe_subclasse AS
SELECT
	dt_mes,
	classe,
	subclasse,
	ROUND(SUM(consumo_medido), 2) AS kwh_consumido
FROM gold.vw_base_cip
GROUP BY dt_mes, classe, subclasse;

-- 10. Ranking dos Bairros que mais Consomem (kWh)
CREATE OR REPLACE VIEW gold.vw_ranking_bairros_consumo AS
SELECT
	dt_mes,
	bairro,
	ROUND(SUM(consumo_medido), 2) AS kwh_consumido,
	RANK() OVER (PARTITION BY dt_mes ORDER BY SUM(consumo_medido) DESC) AS posicao
FROM gold.vw_base_cip
GROUP BY dt_mes, bairro;

-- 11. Perda de Arrecadacao por Inadimplencia por municipio (CIP faturado - CIP pago)
CREATE OR REPLACE VIEW gold.vw_perda_arrecadacao_cip AS
SELECT
	dt_mes,
	municipio,
	ROUND(SUM(valor_cip_faturado - valor_cip_arrecadado), 2) AS perda_cip
FROM gold.vw_base_cip
GROUP BY dt_mes, municipio;

-- 12. Perda por Bairro (inadimplencia segmentada)
CREATE OR REPLACE VIEW gold.vw_perda_arrecadacao_cip_por_bairro AS
SELECT
	dt_mes,
	bairro,
	ROUND(SUM(valor_cip_faturado - valor_cip_arrecadado), 2) AS perda_cip
FROM gold.vw_base_cip
GROUP BY dt_mes, bairro;

-- Observacao sobre filtros: todos os filtros (periodo, bairro, classe, subclasse) devem ser aplicados pelas ferramentas de visualizacao sobre as views acima usando as colunas dt_mes, bairro, classe e subclasse.
