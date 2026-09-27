-- Tabela Bronze: Dados brutos da carga
CREATE TABLE IF NOT EXISTS bronze.tbl_carga_raw (
    id_raw SERIAL PRIMARY KEY,

    ordem INTEGER,
    
    -- Período
    mes_competencia VARCHAR(6),
    mes_referencia  VARCHAR(6),

    nome             VARCHAR(255),
    cpf              VARCHAR(14),
    cnpj             VARCHAR(18),

    municipio        VARCHAR(150),
    localidade       VARCHAR(150),
    endereco         VARCHAR(255),
    numero           VARCHAR(50),
    complemento      VARCHAR(255),
    cep              VARCHAR(15),
    bairro           VARCHAR(150),

    calendario_fabrica VARCHAR(10),

    longitude NUMERIC(18,12),
    latitude  NUMERIC(18,12),

    classe_principal   VARCHAR(100),
    subclasse          VARCHAR(100),
    categoria_tarifa   VARCHAR(50),

    data_ligacao_cc DATE,

    status_comercial VARCHAR(10),

    conta_contrato VARCHAR(20),
    instalacao     VARCHAR(20),

    etapa VARCHAR(10),
    grupo VARCHAR(10),

    cliente_livre VARCHAR(10),
    fase          VARCHAR(10),
    perimetro     VARCHAR(10),

    micro_gerador VARCHAR(10),
    irrigante     VARCHAR(10),

    motivo_bloqueio_contrato VARCHAR(255),

    contrato_ativo VARCHAR(5),

    meio_pagamento      VARCHAR(10),
    documento_impressao VARCHAR(30),

    fatura VARCHAR(30),
    cnr    VARCHAR(30),

    receita_bandeiras NUMERIC(18,2),

    consumo_faturado NUMERIC(18,6),
    consumo_medido   NUMERIC(18,6),

    data_vencimento_original DATE,
    data_pagamento           DATE,

    inicio_calculo DATE,
    fim_calculo    DATE,

    quantidade_dias INTEGER,

    cancelamento  VARCHAR(10),
    refaturamento VARCHAR(10),

    data_refaturamento DATE,

    tarifa NUMERIC(18,8),
    preco  NUMERIC(18,8),

    pis    NUMERIC(18,2),
    cofins NUMERIC(18,2),
    icms   NUMERIC(18,2),

    receita_consumo_faturado NUMERIC(18,2),
    valor_fatura             NUMERIC(18,2),
    vlr_arrecadado           NUMERIC(18,2),

    situacao        VARCHAR(20),
    valor_fatura_sub NUMERIC(18,2),
    cip             NUMERIC(18,2),
    cip_sub         NUMERIC(18,2),

    carga_id      INTEGER REFERENCES public.tbl_controle_carga(id_carga),
    arquivo_origem VARCHAR(255),
    data_carga     TIMESTAMP DEFAULT NOW()
);

-- Tabela Bronze Histórico: Mantém histórico de todas as cargas
CREATE TABLE IF NOT EXISTS bronze.tbl_carga_raw_hist (
    id_raw SERIAL PRIMARY KEY,

    ordem INTEGER,
    
    -- Período
    mes_competencia VARCHAR(6),
    mes_referencia  VARCHAR(6),

    nome             VARCHAR(255),
    cpf              VARCHAR(14),
    cnpj             VARCHAR(18),

    municipio        VARCHAR(150),
    localidade       VARCHAR(150),
    endereco         VARCHAR(255),
    numero           VARCHAR(50),
    complemento      VARCHAR(255),
    cep              VARCHAR(15),
    bairro           VARCHAR(150),

    calendario_fabrica VARCHAR(10),

    longitude NUMERIC(18,12),
    latitude  NUMERIC(18,12),

    classe_principal   VARCHAR(100),
    subclasse          VARCHAR(100),
    categoria_tarifa   VARCHAR(50),

    data_ligacao_cc DATE,

    status_comercial VARCHAR(10),

    conta_contrato VARCHAR(20),
    instalacao     VARCHAR(20),

    etapa VARCHAR(10),
    grupo VARCHAR(10),

    cliente_livre VARCHAR(10),
    fase          VARCHAR(10),
    perimetro     VARCHAR(10),

    micro_gerador VARCHAR(10),
    irrigante     VARCHAR(10),

    motivo_bloqueio_contrato VARCHAR(255),

    contrato_ativo VARCHAR(5),

    meio_pagamento      VARCHAR(10),
    documento_impressao VARCHAR(30),

    fatura VARCHAR(30),
    cnr    VARCHAR(30),

    receita_bandeiras NUMERIC(18,2),

    consumo_faturado NUMERIC(18,6),
    consumo_medido   NUMERIC(18,6),

    data_vencimento_original DATE,
    data_pagamento           DATE,

    inicio_calculo DATE,
    fim_calculo    DATE,

    quantidade_dias INTEGER,

    cancelamento  VARCHAR(10),
    refaturamento VARCHAR(10),

    data_refaturamento DATE,

    tarifa NUMERIC(18,8),
    preco  NUMERIC(18,8),

    pis    NUMERIC(18,2),
    cofins NUMERIC(18,2),
    icms   NUMERIC(18,2),

    receita_consumo_faturado NUMERIC(18,2),
    valor_fatura             NUMERIC(18,2),
    vlr_arrecadado           NUMERIC(18,2),

    situacao        VARCHAR(20),
    valor_fatura_sub NUMERIC(18,2),
    cip             NUMERIC(18,2),
    cip_sub         NUMERIC(18,2),

    carga_id      INTEGER REFERENCES public.tbl_controle_carga(id_carga),
    arquivo_origem VARCHAR(255),
    data_carga     TIMESTAMP DEFAULT NOW()
);

