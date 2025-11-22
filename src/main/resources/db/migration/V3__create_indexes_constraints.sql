-- ==========================================
-- V3 - Índices e Constraints
-- ==========================================

-- Empresa
CREATE UNIQUE INDEX IF NOT EXISTS ux_empresa_cnpj ON empresa(cnpj_basico);

-- Estabelecimento (chave de negócio)
CREATE UNIQUE INDEX IF NOT EXISTS ux_estabelecimento_key 
    ON estabelecimento(cnpj_basico, cnpj_ordem, cnpj_dv);

-- Socio (chave de negócio)
CREATE UNIQUE INDEX IF NOT EXISTS ux_socio_key 
    ON socio(cnpj_basico, identificador_de_socio, cnpj_cpf_do_socio);

-- Simples (chave de negócio)
CREATE UNIQUE INDEX IF NOT EXISTS ux_simples_key 
    ON simples(cnpj_basico, data_opcao_pelo_simples);
