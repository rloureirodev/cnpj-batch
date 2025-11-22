-- ==========================================
-- V2 - Tabelas finais normalizadas
-- ==========================================

DROP TABLE IF EXISTS empresa;
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


DROP TABLE IF EXISTS estabelecimento;
CREATE TABLE estabelecimento (
    id SERIAL PRIMARY KEY,
    cnpj_basico VARCHAR(8),
    cnpj_ordem VARCHAR(4),
    cnpj_dv VARCHAR(2),
    identificador_matriz_filial INTEGER,
    nome_fantasia VARCHAR(255),
    situacao_cadastral INTEGER,
    cep VARCHAR(20),
    uf VARCHAR(2),
    municipio INTEGER,
    hash VARCHAR(32),
    atualizado_em TIMESTAMP DEFAULT NOW()
);


DROP TABLE IF EXISTS socio;
CREATE TABLE socio (
    id SERIAL PRIMARY KEY,
    cnpj_basico VARCHAR(8),
    identificador_de_socio INTEGER,
    nome_do_socio VARCHAR(255),
    cnpj_cpf_do_socio VARCHAR(20),
    qualificacao_socio INTEGER,
    hash VARCHAR(32),
    atualizado_em TIMESTAMP DEFAULT NOW()
);


DROP TABLE IF EXISTS simples;
CREATE TABLE simples (
    id SERIAL PRIMARY KEY,
    cnpj_basico VARCHAR(8),
    opcao_pelo_simples VARCHAR(1),
    data_opcao_pelo_simples DATE,
    data_exclusao_do_simples DATE,
    hash VARCHAR(32),
    atualizado_em TIMESTAMP DEFAULT NOW()
);
