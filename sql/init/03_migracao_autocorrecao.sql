-- Migração para suporte a métricas de autocorreção no ETL

-- a) Novas colunas em public.tbl_controle_carga
DO $$
BEGIN
    BEGIN
        ALTER TABLE public.tbl_controle_carga
            ADD COLUMN tipo_falha VARCHAR(30); -- 'CONEXAO', 'SCHEMA', 'DADOS_NULOS', 'ARQUIVO_AUSENTE', 'NENHUMA'
    EXCEPTION
        WHEN duplicate_column THEN NULL;
    END;

    BEGIN
        ALTER TABLE public.tbl_controle_carga
            ADD COLUMN tentativas_recuperacao INTEGER DEFAULT 0;
    EXCEPTION
        WHEN duplicate_column THEN NULL;
    END;

    BEGIN
        ALTER TABLE public.tbl_controle_carga
            ADD COLUMN tempo_recuperacao_ms BIGINT;
    EXCEPTION
        WHEN duplicate_column THEN NULL;
    END;

    BEGIN
        ALTER TABLE public.tbl_controle_carga
            ADD COLUMN resolvido_automaticamente BOOLEAN DEFAULT FALSE;
    EXCEPTION
        WHEN duplicate_column THEN NULL;
    END;

    BEGIN
        ALTER TABLE public.tbl_controle_carga
            ADD COLUMN mecanismo_utilizado VARCHAR(50); -- 'RETRY_BACKOFF', 'QUARENTENA', 'FALLBACK_ARQUIVO', 'VALIDACAO_SCHEMA', 'NENHUM'
    EXCEPTION
        WHEN duplicate_column THEN NULL;
    END;

    BEGIN
        ALTER TABLE public.tbl_controle_carga
            ADD COLUMN qtd_registros_quarentena INTEGER DEFAULT 0;
    EXCEPTION
        WHEN duplicate_column THEN NULL;
    END;
END $$;

-- b) Tabela de quarentena de registros da camada bronze
CREATE TABLE IF NOT EXISTS bronze.tbl_carga_quarentena (
    id_quarentena    SERIAL PRIMARY KEY,
    id_carga         INTEGER REFERENCES public.tbl_controle_carga(id_carga),
    linha_origem     INTEGER,
    motivo_rejeicao  TEXT,
    conteudo_linha   JSONB,
    data_quarentena  TIMESTAMP DEFAULT NOW()
);

-- c) Tabela de experimentos (injeção de falhas para avaliação da autocorreção)
CREATE TABLE IF NOT EXISTS public.tbl_experimento (
    id_experimento     SERIAL PRIMARY KEY,
    grupo              VARCHAR(20), -- 'BASELINE' ou 'TRATAMENTO'
    tipo_falha_injetada VARCHAR(30),
    id_carga_afetada   INTEGER REFERENCES public.tbl_controle_carga(id_carga),
    iteracao           INTEGER,
    timestamp_injecao  TIMESTAMP DEFAULT NOW(),
    observacoes        TEXT
);
