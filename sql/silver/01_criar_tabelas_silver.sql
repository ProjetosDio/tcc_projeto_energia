--Tabelas de dominio
CREATE TABLE IF NOT EXISTS silver.tbl_tipo_status_fatura (
    id_status_fatura SERIAL PRIMARY KEY,
    descricao_status_fatura VARCHAR(30) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.tbl_tipo_status_parecer (
    id_status_parecer SERIAL PRIMARY KEY,
    descricao_status_parecer VARCHAR(30) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.tbl_tipo_classe_consumo (
    id_classe SERIAL PRIMARY KEY,
    descricao_classe_consumo VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.tbl_tipo_subclasse_consumo (
    id_subclasse SERIAL PRIMARY KEY,
    id_classe INTEGER NOT NULL REFERENCES silver.tbl_tipo_classe_consumo(id_classe) ON DELETE CASCADE,
    descricao_subclasse_consumo VARCHAR(100) NOT NULL,
    UNIQUE(id_classe, descricao_subclasse_consumo)
);

CREATE TABLE IF NOT EXISTS silver.tbl_tipo_status_comercial (
    id_status_comercial SERIAL PRIMARY KEY,
    codigo_status_comercial VARCHAR(10) UNIQUE NOT NULL,
    descricao_status_comercial VARCHAR(50) NOT NULL
);

-- Dimensão: Município
CREATE TABLE IF NOT EXISTS silver.tbl_municipio (
    id_municipio SERIAL PRIMARY KEY,
    nome_municipio VARCHAR(150) NOT NULL UNIQUE,
    estado CHAR(2),
    criado_em TIMESTAMP DEFAULT NOW()
);

-- Dimensão: Distribuidora
CREATE TABLE IF NOT EXISTS silver.tbl_distribuidora (
    id_distribuidora SERIAL PRIMARY KEY,
    nome VARCHAR(150) NOT NULL UNIQUE,
    criado_em TIMESTAMP DEFAULT NOW()
);

-- Dimensão: Endereço
CREATE TABLE IF NOT EXISTS silver.tbl_endereco (
    id_endereco SERIAL PRIMARY KEY,
    logradouro VARCHAR(255) NOT NULL,
    numero VARCHAR(50),
    complemento VARCHAR(255),
    bairro VARCHAR(150),
    cep VARCHAR(15),
    id_municipio INTEGER REFERENCES silver.tbl_municipio(id_municipio) ON DELETE SET NULL,
    latitude  NUMERIC(18,12),
    longitude NUMERIC(18,12),
    criado_em TIMESTAMP DEFAULT NOW(),
    UNIQUE(logradouro, numero, complemento, cep)
);

-- Dimensão: Tipo Cliente
CREATE TABLE IF NOT EXISTS silver.tbl_tipo_cliente (
    id_tipo_cliente SERIAL PRIMARY KEY,
    descricao_tipo_cliente VARCHAR(30) UNIQUE NOT NULL
);


CREATE TABLE IF NOT EXISTS silver.tbl_cliente (
    id_cliente SERIAL PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    id_tipo_cliente INTEGER REFERENCES silver.tbl_tipo_cliente(id_tipo_cliente),
    criado_em TIMESTAMP DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS silver.tbl_cliente_pf (
    id_cliente INTEGER PRIMARY KEY REFERENCES silver.tbl_cliente(id_cliente) ON DELETE CASCADE,
    cpf VARCHAR(14) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS silver.tbl_cliente_pj (
    id_cliente INTEGER PRIMARY KEY REFERENCES silver.tbl_cliente(id_cliente) ON DELETE CASCADE,
    cnpj VARCHAR(18) NOT NULL UNIQUE
);

-- Tabela de categoria tarifária
CREATE TABLE IF NOT EXISTS silver.tbl_tipo_categoria_tarifaria (
    id_categoria_tarifaria SERIAL PRIMARY KEY,
    codigo_categoria_tarifaria VARCHAR(20) UNIQUE NOT NULL,
    descricao_categoria_tarifaria VARCHAR(255),
    criado_em TIMESTAMP DEFAULT NOW()
);

-- Dimensão: Perfil Regulatório da Instalação
CREATE TABLE IF NOT EXISTS silver.tbl_perfil_regulatorio_instalacao (
    id_perfil_regulatorio SERIAL PRIMARY KEY,
    etapa VARCHAR(10),
    grupo VARCHAR(10),
    cliente_livre BOOLEAN,
    fase VARCHAR(10),
    perimetro VARCHAR(10),
    micro_gerador BOOLEAN,
    irrigante BOOLEAN,
    motivo_bloqueio_contrato VARCHAR(255),
    calendario_fabrica VARCHAR(10),
    id_categoria_tarifaria INTEGER REFERENCES silver.tbl_tipo_categoria_tarifaria(id_categoria_tarifaria),
    UNIQUE(etapa, grupo, cliente_livre, fase, perimetro, micro_gerador, irrigante, motivo_bloqueio_contrato, calendario_fabrica, id_categoria_tarifaria)
);


-- Dimensão: Instalação
CREATE TABLE IF NOT EXISTS silver.tbl_instalacao (
    id_instalacao SERIAL PRIMARY KEY,
    codigo_instalacao VARCHAR(20) UNIQUE NOT NULL,
    conta_contrato VARCHAR(20),
    id_subclasse INTEGER REFERENCES silver.tbl_tipo_subclasse_consumo(id_subclasse),
    data_ligacao_cc DATE,
    id_status_comercial INTEGER REFERENCES silver.tbl_tipo_status_comercial(id_status_comercial),
    contrato_ativo BOOLEAN,
    id_perfil_regulatorio INTEGER REFERENCES silver.tbl_perfil_regulatorio_instalacao(id_perfil_regulatorio),
    id_endereco INTEGER REFERENCES silver.tbl_endereco(id_endereco),
    id_distribuidora INTEGER REFERENCES silver.tbl_distribuidora(id_distribuidora),
    criado_em TIMESTAMP DEFAULT NOW()
);


-- Fato: Fatura
CREATE TABLE IF NOT EXISTS silver.tbl_fatura (
    id_fatura SERIAL PRIMARY KEY,
    id_cliente INTEGER REFERENCES silver.tbl_cliente(id_cliente),
    id_instalacao INTEGER REFERENCES silver.tbl_instalacao(id_instalacao),
    mes_competencia VARCHAR(6),
    mes_referencia  VARCHAR(6),
    documento_impressao VARCHAR(30),
    numero_fatura       VARCHAR(30),
    cnr                 VARCHAR(30),
    consumo_faturado NUMERIC(18,2),
    consumo_medido   NUMERIC(18,2),
    tarifa NUMERIC(18,8),
    preco  NUMERIC(18,8),
    receita_consumo_faturado NUMERIC(18,2),
    receita_bandeiras        NUMERIC(18,2),
    valor_fatura      NUMERIC(18,2),
    data_vencimento_original DATE,
    situacao VARCHAR(20),
    cancelamento BOOLEAN,
    id_status_fatura INTEGER REFERENCES silver.tbl_tipo_status_fatura(id_status_fatura),
    criado_em TIMESTAMP DEFAULT NOW()
);

-- Fato: Pagamento
CREATE TABLE IF NOT EXISTS silver.tbl_pagamento (
    id_pagamento SERIAL PRIMARY KEY,
    id_fatura INTEGER NOT NULL REFERENCES silver.tbl_fatura(id_fatura) ON DELETE CASCADE,
    data_pagamento DATE NOT NULL,
    meio_pagamento VARCHAR(20),
    vlr_arrecadado NUMERIC(18,2) NOT NULL,
    criado_em TIMESTAMP DEFAULT NOW(),
    UNIQUE (id_fatura, data_pagamento)
);


-- Fato: Impostos e Taxas
CREATE TABLE IF NOT EXISTS silver.tbl_imposto_taxa (
    id_imposto_taxa SERIAL PRIMARY KEY,
    id_fatura INTEGER NOT NULL REFERENCES silver.tbl_fatura(id_fatura) ON DELETE CASCADE UNIQUE,
    pis    NUMERIC(18,2),
    cofins NUMERIC(18,2),
    icms   NUMERIC(18,2),
    criado_em TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.tbl_ciclo_leitura (
    id_ciclo SERIAL PRIMARY KEY,
    id_fatura INTEGER NOT NULL REFERENCES silver.tbl_fatura(id_fatura) ON DELETE CASCADE,
    inicio_calculo DATE,
    fim_calculo DATE,
    quantidade_dias INTEGER,
    criado_em TIMESTAMP DEFAULT NOW()
);


-- Fato: Refaturamento
CREATE TABLE IF NOT EXISTS silver.tbl_refaturamento (
    id_refaturamento SERIAL PRIMARY KEY,
    id_fatura_original INTEGER NOT NULL REFERENCES silver.tbl_fatura(id_fatura) ON DELETE CASCADE,
    id_fatura_nova INTEGER NOT NULL REFERENCES silver.tbl_fatura(id_fatura) ON DELETE CASCADE,
    data_refaturamento DATE NOT NULL,
    valor_fatura_sub NUMERIC(18,2),
    cip_sub NUMERIC(18,2)
);


CREATE TABLE IF NOT EXISTS silver.tbl_cip (
    id_cip SERIAL PRIMARY KEY,
    id_fatura INTEGER NOT NULL REFERENCES silver.tbl_fatura(id_fatura) ON DELETE CASCADE,
    valor_cip NUMERIC(18,2) NOT NULL,
    valor_cip_arrecadado NUMERIC(18,2),
    preco_unitario NUMERIC(18,6),
    criado_em TIMESTAMP DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS silver.tbl_lei (
    id_lei BIGSERIAL PRIMARY KEY,
    id_municipio INTEGER REFERENCES silver.tbl_municipio(id_municipio),
    id_distribuidora INTEGER REFERENCES silver.tbl_distribuidora(id_distribuidora),
    nome_lei VARCHAR(255) NOT NULL,
    numero_lei VARCHAR(50),
    data_publicacao DATE,
    data_inicio_vigencia DATE NOT NULL,
    data_fim_vigencia DATE,
    id_status_parecer INTEGER REFERENCES silver.tbl_tipo_status_parecer(id_status_parecer),
    observacao_parecer TEXT,
    ativo BOOLEAN DEFAULT TRUE,
    criado_em TIMESTAMP DEFAULT NOW(),
    criado_por VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS silver.tbl_lei_documento (
    id_documento BIGSERIAL PRIMARY KEY,
    id_lei BIGINT NOT NULL REFERENCES silver.tbl_lei(id_lei) ON DELETE CASCADE,
    data_evento DATE NOT NULL,
    tipo_documento VARCHAR(50) NOT NULL,
    nome_arquivo VARCHAR(255) NOT NULL,
    id_ged TEXT NOT NULL,
    data_upload TIMESTAMP NOT NULL DEFAULT NOW(),
    enviado_por VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.tbl_lei_log (
    id_log BIGSERIAL PRIMARY KEY,
    id_lei BIGINT NOT NULL REFERENCES silver.tbl_lei(id_lei),
    descricao TEXT NOT NULL,
    status_anterior INTEGER REFERENCES silver.tbl_tipo_status_parecer(id_status_parecer),
    status_novo INTEGER REFERENCES silver.tbl_tipo_status_parecer(id_status_parecer),
    criado_em TIMESTAMP DEFAULT NOW(),
    id_usuario_auth VARCHAR(100) NOT NULL,
    nome_responsavel VARCHAR(255) NOT NULL
);

--Município ou distribuidora
CREATE TABLE IF NOT EXISTS silver.tbl_tipo_entidade (
    id_tipo_entidade SERIAL PRIMARY KEY,
    codigo VARCHAR(20) UNIQUE NOT NULL,
    descricao VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.tbl_escopo (
    id_escopo BIGSERIAL PRIMARY KEY,
    id_tipo_entidade INTEGER NOT NULL REFERENCES silver.tbl_tipo_entidade(id_tipo_entidade),
    id_municipio INTEGER REFERENCES silver.tbl_municipio(id_municipio),
    id_distribuidora INTEGER REFERENCES silver.tbl_distribuidora(id_distribuidora),
    criado_em TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.tbl_usuario_escopo (
    id_usuario_escopo SERIAL PRIMARY KEY,
    id_usuario_auth VARCHAR(100) NOT NULL,
    id_escopo BIGINT REFERENCES silver.tbl_escopo(id_escopo),
    criado_em TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.tbl_auditoria_evento (
    id_auditoria BIGSERIAL PRIMARY KEY,
    id_usuario_auth VARCHAR(100) NOT NULL,
    entidade VARCHAR(50), 
    id_entidade BIGINT,
    acao VARCHAR(30) NOT NULL,
    descricao TEXT,
    endpoint_path VARCHAR(255),
    metodo_http VARCHAR(10),
    criado_em TIMESTAMP DEFAULT NOW()
);

--Simulação CIP
CREATE TABLE IF NOT EXISTS silver.tbl_simulacao_cip (
    id_simulacao BIGSERIAL PRIMARY KEY,
    nome_simulacao VARCHAR(255) NOT NULL,
    id_municipio INTEGER REFERENCES silver.tbl_municipio(id_municipio),
    id_distribuidora INTEGER REFERENCES silver.tbl_distribuidora(id_distribuidora),
    base_dados_referencia VARCHAR(100) NOT NULL,
    data_inicio DATE,
    data_fim DATE,
    usa_media_12_meses BOOLEAN DEFAULT FALSE,
    id_classe INTEGER REFERENCES silver.tbl_tipo_classe_consumo(id_classe),
    id_subclasse INTEGER REFERENCES silver.tbl_tipo_subclasse_consumo(id_subclasse),
    bandeira VARCHAR(30),
    tipo_contrato VARCHAR(20) NOT NULL,
    valor_cip_fixo NUMERIC(18,2),
    percentual_cip NUMERIC(8,4),
    incluir_icms BOOLEAN,
    criado_por_usuario_auth VARCHAR(100) NOT NULL,
    criado_em TIMESTAMP DEFAULT NOW(),
    atualizado_em TIMESTAMP DEFAULT NOW(),
    ativo BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS silver.tbl_simulacao_faixa_consumo (
    id_faixa BIGSERIAL PRIMARY KEY,
    id_simulacao BIGINT REFERENCES silver.tbl_simulacao_cip(id_simulacao),
    consumo_inicial NUMERIC(18,2) NOT NULL,
    consumo_final NUMERIC(18,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.tbl_simulacao_resultado (
    id_resultado BIGSERIAL PRIMARY KEY,
    id_simulacao BIGINT REFERENCES silver.tbl_simulacao_cip(id_simulacao),
    total_tarifa NUMERIC(18,2),
    total_tarifa_bandeira NUMERIC(18,2),
    total_registros INTEGER,
    previsao_arrecadacao_cip NUMERIC(18,2),
    custo_medio_por_imovel NUMERIC(18,2),
    criado_em TIMESTAMP DEFAULT NOW()
);

-- Índices para melhor performance
CREATE INDEX IF NOT EXISTS idx_endereco_municipio ON silver.tbl_endereco(id_municipio);
CREATE INDEX IF NOT EXISTS idx_instalacao_endereco ON silver.tbl_instalacao(id_endereco);
CREATE INDEX IF NOT EXISTS idx_instalacao_distribuidora ON silver.tbl_instalacao(id_distribuidora);
CREATE INDEX IF NOT EXISTS idx_fatura_cliente ON silver.tbl_fatura(id_cliente);
CREATE INDEX IF NOT EXISTS idx_fatura_instalacao ON silver.tbl_fatura(id_instalacao);
CREATE INDEX IF NOT EXISTS idx_fatura_mes ON silver.tbl_fatura(mes_competencia);
CREATE INDEX IF NOT EXISTS idx_lei_municipio ON silver.tbl_lei (id_municipio);
CREATE INDEX IF NOT EXISTS idx_lei_distribuidora ON silver.tbl_lei (id_distribuidora);
CREATE INDEX IF NOT EXISTS idx_lei_status ON silver.tbl_lei (id_status_parecer);
CREATE INDEX IF NOT EXISTS idx_lei_log_lei ON silver.tbl_lei_log (id_lei);
CREATE INDEX IF NOT EXISTS idx_lei_log_data ON silver.tbl_lei_log (criado_em);
CREATE INDEX IF NOT EXISTS idx_cip_fatura ON silver.tbl_cip (id_fatura);
CREATE INDEX IF NOT EXISTS idx_lei_documento_lei ON silver.tbl_lei_documento (id_lei);
CREATE INDEX IF NOT EXISTS idx_refaturamento_fatura_original ON silver.tbl_refaturamento (id_fatura_original);
CREATE INDEX IF NOT EXISTS idx_refaturamento_fatura_nova ON silver.tbl_refaturamento (id_fatura_nova);
CREATE INDEX IF NOT EXISTS idx_ciclo_leitura_fatura ON silver.tbl_ciclo_leitura (id_fatura);
CREATE INDEX IF NOT EXISTS idx_auditoria_entidade ON silver.tbl_auditoria_evento (entidade, id_entidade);
CREATE INDEX IF NOT EXISTS idx_auditoria_usuario ON silver.tbl_auditoria_evento (id_usuario_auth);
CREATE INDEX IF NOT EXISTS idx_auditoria_data ON silver.tbl_auditoria_evento (criado_em);