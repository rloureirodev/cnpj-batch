-- ====================================
-- V1 - Tabelas STAGE para COPY (TODAS AS COLUNAS VARCHAR)
-- ====================================

-- ---------- stage_empresa ----------
DROP TABLE IF EXISTS stage_empresa;
CREATE TABLE stage_empresa (
    cnpj_basico                  VARCHAR(255),
    razao_social                 VARCHAR(4000),
    natureza_juridica            VARCHAR(255),
    qualificacao_responsavel     VARCHAR(255),
    capital_social               VARCHAR(255),
    porte                        VARCHAR(255),
    ente_federativo_responsavel  VARCHAR(4000),
    hash                         VARCHAR(255)
);

-- ---------- stage_estabelecimento ----------
DROP TABLE IF EXISTS stage_estabelecimento;
CREATE TABLE stage_estabelecimento (
    cnpj_basico                     VARCHAR(255),
    cnpj_ordem                      VARCHAR(255),
    cnpj_dv                         VARCHAR(255),
    identificador_matriz_filial     VARCHAR(255),
    nome_fantasia                   VARCHAR(4000),
    situacao_cadastral              VARCHAR(255),
    data_situacao_cadastral         VARCHAR(255),
    motivo_situacao_cadastral       VARCHAR(255),
    nome_cidade_exterior            VARCHAR(4000),
    pais                            VARCHAR(255),
    data_inicio_atividade           VARCHAR(255),
    cnae_fiscal_principal           VARCHAR(255),
    cnae_fiscal_secundaria          VARCHAR(4000),
    tipo_logradouro                 VARCHAR(4000),
    logradouro                      VARCHAR(4000),
    numero                          VARCHAR(255),
    complemento                     VARCHAR(4000),
    bairro                          VARCHAR(4000),
    cep                             VARCHAR(255),
    uf                              VARCHAR(50),
    municipio                       VARCHAR(255),
    ddd1                            VARCHAR(50),
    telefone1                       VARCHAR(255),
    ddd2                            VARCHAR(50),
    telefone2                       VARCHAR(255),
    ddd_fax                         VARCHAR(50),
    fax                             VARCHAR(255),
    email                           VARCHAR(4000),
    situacao_especial               VARCHAR(4000),
    data_situacao_especial          VARCHAR(255),
    hash                            VARCHAR(255)
);

-- ---------- stage_socios ----------
DROP TABLE IF EXISTS stage_socios;
CREATE TABLE stage_socios (
    cnpj_basico                     VARCHAR(255),
    identificador_de_socio          VARCHAR(255),
    nome_do_socio                   VARCHAR(4000),
    cnpj_cpf_do_socio               VARCHAR(255),
    qualificacao_socio              VARCHAR(255),
    data_entrada_sociedade          VARCHAR(255),
    pais                            VARCHAR(255),
    representante_legal             VARCHAR(255),
    nome_representante              VARCHAR(4000),
    qualificacao_representante      VARCHAR(255),
    faixa_etaria                    VARCHAR(255),
    hash                            VARCHAR(255)
);

-- ---------- stage_simples ----------
DROP TABLE IF EXISTS stage_simples;
CREATE TABLE stage_simples (
    cnpj_basico                     VARCHAR(255),
    opcao_pelo_simples              VARCHAR(255),
    data_opcao_pelo_simples         VARCHAR(255),
    data_exclusao_do_simples        VARCHAR(255),
    opcao_pelo_mei                  VARCHAR(255),
    data_opcao_pelo_mei             VARCHAR(255),
    data_exclusao_do_mei            VARCHAR(255),
    hash                            VARCHAR(255)
);

-- ---------- stage_municipios ----------
DROP TABLE IF EXISTS stage_municipios;
CREATE TABLE stage_municipios (
    codigo                         VARCHAR(255),
    nome                           VARCHAR(4000),
    uf                             VARCHAR(255),
    hash                            VARCHAR(255)
);

-- ---------- stage_cnaes ----------
DROP TABLE IF EXISTS stage_cnaes;
CREATE TABLE stage_cnaes (
    codigo                         VARCHAR(255),
    descricao                      VARCHAR(4000),
    hash                            VARCHAR(255)
);

-- ---------- stage_qualificacoes ----------
DROP TABLE IF EXISTS stage_qualificacoes;
CREATE TABLE stage_qualificacoes (
    codigo                         VARCHAR(255),
    descricao                      VARCHAR(4000),
    hash                            VARCHAR(255)
);

-- ---------- stage_naturezas ----------
DROP TABLE IF EXISTS stage_naturezas;
CREATE TABLE stage_naturezas (
    codigo                         VARCHAR(255),
    descricao                      VARCHAR(4000),
    hash                            VARCHAR(255)
);

-- ---------- stage_paises ----------
DROP TABLE IF EXISTS stage_paises;
CREATE TABLE stage_paises (
    codigo                         VARCHAR(255),
    nome                           VARCHAR(4000),
    hash                            VARCHAR(255)
);

-- ---------- stage_motivos ----------
DROP TABLE IF EXISTS stage_motivos;
CREATE TABLE stage_motivos (
    codigo                         VARCHAR(255),
    descricao                      VARCHAR(4000),
    hash                            VARCHAR(255)
);

-- Índices opcionais (não obrigatórios)
CREATE INDEX IF NOT EXISTS idx_stage_empresa_cnpj ON stage_empresa(cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_stage_estab_cnpj ON stage_estabelecimento(cnpj_basico);
