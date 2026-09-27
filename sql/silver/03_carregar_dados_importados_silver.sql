-- Domínios básicos necessários para as cargas
INSERT INTO silver.tbl_tipo_cliente (descricao_tipo_cliente)
VALUES ('PF'), ('PJ')
ON CONFLICT (descricao_tipo_cliente) DO NOTHING;

INSERT INTO silver.tbl_tipo_status_fatura (descricao_status_fatura)
SELECT DISTINCT
    fb.situacao
FROM bronze.tbl_carga_raw fb
WHERE fb.situacao IS NOT NULL
ON CONFLICT (descricao_status_fatura) DO NOTHING;

INSERT INTO silver.tbl_tipo_status_comercial (codigo_status_comercial, descricao_status_comercial)
SELECT DISTINCT
    fb.status_comercial,
    fb.status_comercial
FROM bronze.tbl_carga_raw fb
WHERE fb.status_comercial IS NOT NULL
ON CONFLICT (codigo_status_comercial) DO NOTHING;


-- 6.1 Domínio: Categoria Tarifária
INSERT INTO silver.tbl_tipo_categoria_tarifaria (codigo_categoria_tarifaria)
SELECT DISTINCT
    fb.categoria_tarifa
FROM bronze.tbl_carga_raw fb
WHERE fb.categoria_tarifa IS NOT NULL
ON CONFLICT (codigo_categoria_tarifaria) DO NOTHING;

-- 6.2 Domínio: Perfil Regulatório da Instalação
INSERT INTO silver.tbl_perfil_regulatorio_instalacao (
    etapa,
    grupo,
    cliente_livre,
    fase,
    perimetro,
    micro_gerador,
    irrigante,
    motivo_bloqueio_contrato,
    calendario_fabrica,
    id_categoria_tarifaria
)
SELECT DISTINCT
    fb.etapa,
    fb.grupo,
    CASE WHEN fb.cliente_livre = 'S' THEN TRUE ELSE FALSE END,
    fb.fase,
    fb.perimetro,
    CASE WHEN fb.micro_gerador = 'S' THEN TRUE ELSE FALSE END,
    CASE WHEN fb.irrigante = 'S' THEN TRUE ELSE FALSE END,
    fb.motivo_bloqueio_contrato,
    fb.calendario_fabrica,
    ct.id_categoria_tarifaria
FROM bronze.tbl_carga_raw fb
LEFT JOIN silver.tbl_tipo_categoria_tarifaria ct
    ON ct.codigo_categoria_tarifaria = fb.categoria_tarifa
ON CONFLICT DO NOTHING;


-- 1. Tabela de Municípios
INSERT INTO silver.tbl_municipio (nome_municipio, estado)
SELECT DISTINCT
    b.municipio,
    NULL::CHAR(2) -- Sem UF na fonte; manter nulo
FROM bronze.tbl_carga_raw b
WHERE b.municipio IS NOT NULL
ON CONFLICT (nome_municipio) DO NOTHING;

-- 2. Tabela de Distribuidoras
INSERT INTO silver.tbl_distribuidora (nome)
VALUES ('DISTRIBUIDORA_PADRAO')
ON CONFLICT DO NOTHING;

-- 3. Tabela de Endereços
INSERT INTO silver.tbl_endereco (logradouro, numero, complemento, bairro, cep, id_municipio, latitude, longitude)
SELECT DISTINCT
    b.endereco,
    b.numero,
    b.complemento,
    b.bairro,
    b.cep,
    m.id_municipio,
    b.latitude,
    b.longitude
FROM bronze.tbl_carga_raw b
JOIN silver.tbl_municipio m ON m.nome_municipio = b.municipio
WHERE b.endereco IS NOT NULL
ON CONFLICT (logradouro, numero, complemento, cep) DO NOTHING;

-- 4. Tabela de Clientes
WITH tc_pf AS (
    SELECT id_tipo_cliente FROM silver.tbl_tipo_cliente WHERE descricao_tipo_cliente = 'PF'
),
tc_pj AS (
    SELECT id_tipo_cliente FROM silver.tbl_tipo_cliente WHERE descricao_tipo_cliente = 'PJ'
)
INSERT INTO silver.tbl_cliente (nome, id_tipo_cliente)
SELECT DISTINCT
    b.nome,
    CASE WHEN b.cpf IS NOT NULL AND b.cpf <> '' THEN tc_pf.id_tipo_cliente ELSE tc_pj.id_tipo_cliente END
FROM bronze.tbl_carga_raw b
CROSS JOIN tc_pf
CROSS JOIN tc_pj
WHERE b.nome IS NOT NULL
ON CONFLICT DO NOTHING;

-- 5. Tabela de Clientes PF/PJ
-- PF
INSERT INTO silver.tbl_cliente_pf (id_cliente, cpf)
SELECT DISTINCT
    c.id_cliente,
    b.cpf
FROM bronze.tbl_carga_raw b
JOIN silver.tbl_cliente c ON c.nome = b.nome
WHERE b.cpf IS NOT NULL AND b.cpf <> ''
ON CONFLICT (cpf) DO NOTHING;

-- PJ
INSERT INTO silver.tbl_cliente_pj (id_cliente, cnpj)
SELECT DISTINCT
    c.id_cliente,
    b.cnpj
FROM bronze.tbl_carga_raw b
JOIN silver.tbl_cliente c ON c.nome = b.nome
WHERE b.cnpj IS NOT NULL AND b.cnpj <> ''
ON CONFLICT (cnpj) DO NOTHING;


-- 6. Tabela de Instalações 
INSERT INTO silver.tbl_instalacao (
    codigo_instalacao,
    conta_contrato,
    id_subclasse,
    data_ligacao_cc,
    id_status_comercial,
    contrato_ativo,
    id_perfil_regulatorio,
    id_endereco,
    id_distribuidora
)
SELECT DISTINCT
    fb.instalacao,
    fb.conta_contrato,
    sc.id_subclasse,
    fb.data_ligacao_cc,
    stc.id_status_comercial,
    CASE WHEN fb.contrato_ativo IN ('S','SIM','1','TRUE','T') THEN TRUE ELSE FALSE END,
    pri.id_perfil_regulatorio,
    e.id_endereco,
    d.id_distribuidora
FROM bronze.tbl_carga_raw fb
JOIN silver.tbl_tipo_classe_consumo cc
    ON cc.descricao_classe_consumo = fb.classe_principal
JOIN silver.tbl_tipo_subclasse_consumo sc
    ON sc.id_classe = cc.id_classe
    AND sc.descricao_subclasse_consumo = fb.subclasse
JOIN silver.tbl_tipo_status_comercial stc
    ON stc.codigo_status_comercial = fb.status_comercial
JOIN silver.tbl_distribuidora d
    ON d.nome = 'DISTRIBUIDORA_PADRAO'
JOIN silver.tbl_endereco e
    ON e.logradouro = fb.endereco 
    AND e.numero = fb.numero 
    AND e.complemento = fb.complemento
    AND e.cep = fb.cep
JOIN silver.tbl_perfil_regulatorio_instalacao pri
    ON pri.etapa = fb.etapa
    AND pri.grupo = fb.grupo
    AND pri.cliente_livre = CASE WHEN fb.cliente_livre = 'S' THEN TRUE ELSE FALSE END
    AND pri.fase = fb.fase
    AND pri.perimetro = fb.perimetro
    AND pri.micro_gerador = CASE WHEN fb.micro_gerador = 'S' THEN TRUE ELSE FALSE END
    AND pri.irrigante = CASE WHEN fb.irrigante = 'S' THEN TRUE ELSE FALSE END
    AND pri.motivo_bloqueio_contrato = fb.motivo_bloqueio_contrato
    AND pri.calendario_fabrica = fb.calendario_fabrica
    AND pri.id_categoria_tarifaria = (
        SELECT id_categoria_tarifaria
        FROM silver.tbl_tipo_categoria_tarifaria
        WHERE codigo_categoria_tarifaria = fb.categoria_tarifa
    )
WHERE fb.instalacao IS NOT NULL
ON CONFLICT (codigo_instalacao) DO NOTHING;


-- 7. Tabela de Faturas
INSERT INTO silver.tbl_fatura (
    id_cliente,
    id_instalacao,
    mes_competencia,
    mes_referencia,
    documento_impressao,
    numero_fatura,
    cnr,
    consumo_faturado,
    consumo_medido,
    tarifa,
    preco,
    receita_consumo_faturado,
    receita_bandeiras,
    valor_fatura,
    data_vencimento_original,
    situacao,
    cancelamento,
    id_status_fatura
)
SELECT DISTINCT
    c.id_cliente,
    i.id_instalacao,
    fb.mes_competencia,
    fb.mes_referencia,
    fb.documento_impressao,
    fb.fatura,
    fb.cnr,
    fb.consumo_faturado,
    fb.consumo_medido,
    fb.tarifa,
    fb.preco,
    fb.receita_consumo_faturado,
    fb.receita_bandeiras,
    fb.valor_fatura,
    fb.data_vencimento_original,
    fb.situacao,
    CASE WHEN fb.cancelamento IN ('S','SIM','1','TRUE','T') THEN TRUE ELSE FALSE END,
    sf.id_status_fatura
FROM bronze.tbl_carga_raw fb
JOIN silver.tbl_cliente c ON c.nome = fb.nome
JOIN silver.tbl_instalacao i ON i.codigo_instalacao = fb.instalacao
JOIN silver.tbl_tipo_status_fatura sf
    ON sf.descricao_status_fatura = fb.situacao
WHERE fb.fatura IS NOT NULL
ON CONFLICT DO NOTHING;


-- 8. Tabela de Pagamentos
INSERT INTO silver.tbl_pagamento (
    id_fatura,
    data_pagamento,
    meio_pagamento,
    vlr_arrecadado
)
SELECT DISTINCT
    f.id_fatura,
    fb.data_pagamento,
    fb.meio_pagamento,
    fb.vlr_arrecadado
FROM bronze.tbl_carga_raw fb
JOIN silver.tbl_fatura f
    ON f.numero_fatura = fb.fatura
WHERE fb.data_pagamento IS NOT NULL
ON CONFLICT (id_fatura, data_pagamento) DO NOTHING;


-- 9. Impostos e CIP
INSERT INTO silver.tbl_imposto_taxa (
    id_fatura,
    pis,
    cofins,
    icms
)
SELECT DISTINCT
    f.id_fatura,
    fb.pis,
    fb.cofins,
    fb.icms
FROM bronze.tbl_carga_raw fb
JOIN silver.tbl_fatura f
    ON f.numero_fatura = fb.fatura
ON CONFLICT (id_fatura) DO NOTHING;


INSERT INTO silver.tbl_ciclo_leitura (
    id_fatura,
    inicio_calculo,
    fim_calculo,
    quantidade_dias
)
SELECT DISTINCT
    f.id_fatura,
    fb.inicio_calculo,
    fb.fim_calculo,
    fb.quantidade_dias
FROM bronze.tbl_carga_raw fb
JOIN silver.tbl_fatura f
    ON f.numero_fatura = fb.fatura
WHERE fb.inicio_calculo IS NOT NULL
ON CONFLICT DO NOTHING;

-- 10. Tabela de Refaturamento
INSERT INTO silver.tbl_refaturamento (
    id_fatura_original,
    id_fatura_nova,
    data_refaturamento,
    valor_fatura_sub,
    cip_sub
)
SELECT DISTINCT
    f.id_fatura,
    f.id_fatura, -- Sem fatura nova na fonte; vincula à original
    fb.data_refaturamento,
    fb.valor_fatura_sub,
    fb.cip_sub
FROM bronze.tbl_carga_raw fb
JOIN silver.tbl_fatura f
    ON f.numero_fatura = fb.fatura
WHERE fb.refaturamento IN ('S','SIM','1','TRUE','T')
ON CONFLICT DO NOTHING;

-- 11. Tabela de CIP
INSERT INTO silver.tbl_cip (
    id_fatura,
    valor_cip
)
SELECT DISTINCT
    f.id_fatura,
    fb.cip
FROM bronze.tbl_carga_raw fb
JOIN silver.tbl_fatura f
    ON f.numero_fatura = fb.fatura
WHERE fb.cip IS NOT NULL
ON CONFLICT DO NOTHING;