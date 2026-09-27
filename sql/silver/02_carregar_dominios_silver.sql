
--Classes Principais
INSERT INTO silver.tbl_tipo_classe_consumo (descricao_classe_consumo) VALUES
('Comercial, Serviços e Outras A'),
('Comercial, Servicos e Outras A'),
('Consumo Próprio'),
('Industrial'),
('Poder Publico'),
('Residencial'),
('Rural'),
('Serviço Público')
ON CONFLICT DO NOTHING;


--Subclasses com vínculo

--Comercial
INSERT INTO silver.tbl_tipo_subclasse_consumo (id_classe, descricao_subclasse_consumo)
SELECT id_classe, unnest(ARRAY[
    'Comerc. Administração Condominial',
    'Comerc. Associação e Entidades Filant.',
    'Comerc. Iluminação em Rodovia',
    'Comerc. Outros Serviços e Atividades',
    'Comerc. Semáforos, Radares e Câmeras',
    'Comerc. Serv. Comunicações e Telecom',
    'Comerc. Templos Religiosos',
    'Comercial'
])
FROM silver.tbl_tipo_classe_consumo
WHERE descricao_classe_consumo = 'Comercial, Serviços e Outras A'
ON CONFLICT (id_classe, descricao_subclasse_consumo) DO NOTHING;

--Comercial (sem acento - conforme dados do CSV)
INSERT INTO silver.tbl_tipo_subclasse_consumo (id_classe, descricao_subclasse_consumo)
SELECT id_classe, unnest(ARRAY[
    'Comerc. Administração Condominial',
    'Comerc. Associação e Entidades Filant.',
    'Comerc. Iluminação em Rodovia',
    'Comerc. Outros Serviços e Atividades',
    'Comerc. Semáforos, Radares e Câmeras',
    'Comerc. Serv. Comunicações e Telecom',
    'Comerc. Templos Religiosos',
    'Comercial'
])
FROM silver.tbl_tipo_classe_consumo
WHERE descricao_classe_consumo = 'Comercial, Servicos e Outras A'
ON CONFLICT (id_classe, descricao_subclasse_consumo) DO NOTHING;


--Consumo Próprio
INSERT INTO silver.tbl_tipo_subclasse_consumo (id_classe, descricao_subclasse_consumo)
SELECT id_classe, unnest(ARRAY[
    'Consumo Próprio - Canteiro de Obras',
    'Consumo Próprio - Escritórios',
    'Consumo Próprio - Rec veículos elétricos',
    'Consumo Próprio - Sub-Estação',
    'Consumo Próprio - Usinas'
])
FROM silver.tbl_tipo_classe_consumo
WHERE descricao_classe_consumo = 'Consumo Próprio'
ON CONFLICT (id_classe, descricao_subclasse_consumo) DO NOTHING;


--Industrial
INSERT INTO silver.tbl_tipo_subclasse_consumo (id_classe, descricao_subclasse_consumo)
SELECT id_classe, unnest(ARRAY[
    'Industrial',
    'Industrial B-Optante'
])
FROM silver.tbl_tipo_classe_consumo
WHERE descricao_classe_consumo = 'Industrial'
ON CONFLICT (id_classe, descricao_subclasse_consumo) DO NOTHING;


--Poder Público
INSERT INTO silver.tbl_tipo_subclasse_consumo (id_classe, descricao_subclasse_consumo)
SELECT id_classe, unnest(ARRAY[
    'Poder Publico Federal',
    'Poder Publico Estadual B-Optante',
    'Poder Público Estadual',
    'Residencial Pleno',
    'Poder Público Municipal',
    'Poder Público Municipal B-Optante',
    'Poder Publico Federal B-Optante'
])
FROM silver.tbl_tipo_classe_consumo
WHERE descricao_classe_consumo = 'Poder Publico'
ON CONFLICT (id_classe, descricao_subclasse_consumo) DO NOTHING;


--Residencial
INSERT INTO silver.tbl_tipo_subclasse_consumo (id_classe, descricao_subclasse_consumo)
SELECT id_classe, unnest(ARRAY[
    'Residencial Pleno',
    'Resid. Baixa Renda',
    'Resid. Baixa Renda Indígena',
    'Rural B-Optante',
    'Rural Residencial Rural',
    'Rural Agropecuária',
    'Resid. Baixa Renda BPC',
    'Resid. Baixa Renda Multifamiliar',
    'Resid. Baixa Renda Quilombola'

])
FROM silver.tbl_tipo_classe_consumo
WHERE descricao_classe_consumo = 'Residencial'
ON CONFLICT (id_classe, descricao_subclasse_consumo) DO NOTHING;

--Rural
INSERT INTO silver.tbl_tipo_subclasse_consumo (id_classe, descricao_subclasse_consumo)
SELECT id_classe, unnest(ARRAY[
    'Rural Agropecuária',
    'Rural Residencial Rural',
    'Rural Coletividade Rural',
    'Agroindustrial',
    'Rural Serviço Público de Irrigação Rural',
    'Rural Escola Agrotécnica',
    'Rural Agropecuária Urbana'
])
FROM silver.tbl_tipo_classe_consumo
WHERE descricao_classe_consumo = 'Rural'
ON CONFLICT (id_classe, descricao_subclasse_consumo) DO NOTHING;

--Serviço Público
INSERT INTO silver.tbl_tipo_subclasse_consumo (id_classe, descricao_subclasse_consumo)
SELECT id_classe, unnest(ARRAY[
    'Serviço Público Água Esgoto e Saneamento',
    'Serviço Público B-Optante',
    'Serviço Público Resíduo B-Optante',
    'Serviço Público Tração Elétrica',
    'Serviço Público AES B-Optante'
])
FROM silver.tbl_tipo_classe_consumo
WHERE descricao_classe_consumo = 'Serviço Público'
ON CONFLICT (id_classe, descricao_subclasse_consumo) DO NOTHING;

-- Domínio: Status Comercial
INSERT INTO silver.tbl_tipo_status_comercial (codigo_status_comercial, descricao_status_comercial)
VALUES
('LG', 'Ligado'),
('BL', 'Bloqueado'),
('DS', 'Desligado')
ON CONFLICT (codigo_status_comercial) DO NOTHING;

-- Domínio: Tipo de Entidade
INSERT INTO silver.tbl_tipo_entidade (codigo, descricao) VALUES
('MUNICIPIO', 'Município'),
('DISTRIBUIDORA', 'Distribuidora')
ON CONFLICT DO NOTHING;

-- Domínio: Status do Parecer
insert into silver.tbl_tipo_status_parecer (descricao_status_parecer) VALUES
('AGUARDANDO PARECER'),
('ACEITO'),
('RECUSADO')
ON CONFLICT (descricao_status_parecer) DO NOTHING;