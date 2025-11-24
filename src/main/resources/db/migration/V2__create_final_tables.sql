-- ==========================================
-- V2 - Tabelas finais normalizadas
-- ==========================================

--DROP TABLE IF EXISTS empresa;

CREATE TABLE empresa (
    cnpj_basico VARCHAR(8) PRIMARY KEY,
    razao_social VARCHAR(255),
    natureza_juridica INTEGER,
    qualificacao_responsavel INTEGER,
    capital_social NUMERIC(18,2),
    porte INTEGER,
    ente_federativo_responsavel VARCHAR(255),
    hash VARCHAR(32),
    atualizado_em TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX empresa_hash_uk ON empresa(hash);


--DROP TABLE IF EXISTS estabelecimento;

CREATE TABLE estabelecimento (
    cnpj_basico                 VARCHAR(8),
    cnpj_ordem                  VARCHAR(4),
    cnpj_dv                     VARCHAR(2),

    identificador_matriz_filial INTEGER,
    nome_fantasia               VARCHAR(4000),
    situacao_cadastral          INTEGER,

    data_situacao_cadastral     VARCHAR(8),
    motivo_situacao_cadastral   INTEGER,

    nome_cidade_exterior        VARCHAR(4000),
    pais                        INTEGER,

    data_inicio_atividade       VARCHAR(8),

    cnae_fiscal_principal       INTEGER,
    cnae_fiscal_secundaria      TEXT,

    tipo_logradouro             VARCHAR(4000),
    logradouro                  VARCHAR(4000),
    numero                      VARCHAR(255),
    complemento                 VARCHAR(4000),
    bairro                      VARCHAR(4000),

    cep                         VARCHAR(20),
    uf                          VARCHAR(2),
    municipio                   INTEGER,

    ddd1                        VARCHAR(50),
    telefone1                   VARCHAR(255),
    ddd2                        VARCHAR(50),
    telefone2                   VARCHAR(255),
    ddd_fax                     VARCHAR(50),
    fax                         VARCHAR(255),

    email                       VARCHAR(4000),
    situacao_especial           VARCHAR(4000),
    data_situacao_especial      VARCHAR(8),

    hash                        VARCHAR(32),
    atualizado_em               TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
);

CREATE UNIQUE INDEX est_hash_uk ON estabelecimento(hash);

--DROP TABLE IF EXISTS socio;

CREATE TABLE socio (
    cnpj_basico VARCHAR(8),
    identificador_de_socio INTEGER,
    nome_do_socio VARCHAR(255),
    cnpj_cpf_do_socio VARCHAR(20),
    qualificacao_socio INTEGER,

    data_entrada_sociedade VARCHAR(8),
    pais VARCHAR(255),
    representante_legal VARCHAR(255),
    nome_representante VARCHAR(255),
    qualificacao_representante VARCHAR(255),
    faixa_etaria VARCHAR(50),

    hash VARCHAR(32),
    atualizado_em TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY (cnpj_basico, identificador_de_socio, nome_do_socio)
);

CREATE UNIQUE INDEX socio_hash_uk ON socio(hash);


--DROP TABLE IF EXISTS simples;

CREATE TABLE simples (
    cnpj_basico VARCHAR(8) PRIMARY KEY,

    opcao_pelo_simples VARCHAR(1),
    data_opcao_pelo_simples VARCHAR(8),
    data_exclusao_do_simples VARCHAR(8),

    opcao_mei VARCHAR(1),
    data_opcao_mei VARCHAR(8),
    data_exclusao_mei VARCHAR(8),

    hash VARCHAR(32),
    atualizado_em TIMESTAMP DEFAULT NOW()
);
CREATE UNIQUE INDEX simples_hash_uk ON simples(hash);
