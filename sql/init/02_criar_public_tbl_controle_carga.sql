CREATE TABLE IF NOT EXISTS public.tbl_controle_carga (
    id_carga       SERIAL PRIMARY KEY,
    nome_arquivo   VARCHAR(255) NOT NULL,
    data_inicio    TIMESTAMP NOT NULL DEFAULT NOW(),
    data_fim       TIMESTAMP,
    qtd_registros  INTEGER,
    status         VARCHAR(20),   -- ex.: 'SUCESSO', 'ERRO'
    mensagem_erro  TEXT
);
