CREATE TABLE municipio (
    codigo VARCHAR(32) PRIMARY KEY,
    nome VARCHAR(255),
    hash VARCHAR(32),
    atualizado_em TIMESTAMP DEFAULT NOW()
);


CREATE TABLE cnae (
    codigo INTEGER PRIMARY KEY,
    descricao VARCHAR(255),
    hash VARCHAR(32),
    atualizado_em TIMESTAMP DEFAULT NOW()
);

CREATE TABLE natureza (
    codigo INTEGER PRIMARY KEY,
    descricao VARCHAR(255),
    hash VARCHAR(32),
    atualizado_em TIMESTAMP DEFAULT NOW()
);

CREATE TABLE motivo (
    codigo INTEGER PRIMARY KEY,
    descricao VARCHAR(255),
    hash VARCHAR(32),
    atualizado_em TIMESTAMP DEFAULT NOW()
);

CREATE TABLE pais (
    codigo INTEGER PRIMARY KEY,
    nome VARCHAR(255),
    hash VARCHAR(32),
    atualizado_em TIMESTAMP DEFAULT NOW()
);

CREATE TABLE qualificacao (
    codigo INTEGER PRIMARY KEY,
    descricao VARCHAR(255),
    hash VARCHAR(32),
    atualizado_em TIMESTAMP DEFAULT NOW()
);
