CREATE TABLE municipio (
    codigo VARCHAR(32) PRIMARY KEY,
    nome VARCHAR(255),
    atualizado_em TIMESTAMP DEFAULT NOW()
);


CREATE TABLE cnae (
    codigo VARCHAR(32) PRIMARY KEY,
    descricao VARCHAR(255),
    atualizado_em TIMESTAMP DEFAULT NOW()
);

CREATE TABLE natureza (
    codigo VARCHAR(32) PRIMARY KEY,
    descricao VARCHAR(255),
    atualizado_em TIMESTAMP DEFAULT NOW()
);

CREATE TABLE motivo (
    codigo VARCHAR(32) PRIMARY KEY,
    descricao VARCHAR(255),
    atualizado_em TIMESTAMP DEFAULT NOW()
);

CREATE TABLE pais (
    codigo VARCHAR(32) PRIMARY KEY,
    nome VARCHAR(255),
    atualizado_em TIMESTAMP DEFAULT NOW()
);

CREATE TABLE qualificacao (
    codigo VARCHAR(32) PRIMARY KEY,
    descricao VARCHAR(255),
    atualizado_em TIMESTAMP DEFAULT NOW()
);
