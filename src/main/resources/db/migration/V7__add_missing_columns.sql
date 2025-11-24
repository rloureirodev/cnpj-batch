-- ================================
-- V7 - Adiciona campos faltantes
-- ================================

---------------------------
-- 1) TABELA SOCIO
---------------------------

ALTER TABLE socio
    ADD COLUMN IF NOT EXISTS data_entrada_sociedade DATE,
    ADD COLUMN IF NOT EXISTS pais INTEGER,
    ADD COLUMN IF NOT EXISTS representante_legal VARCHAR(20),
    ADD COLUMN IF NOT EXISTS nome_do_representante VARCHAR(255),
    ADD COLUMN IF NOT EXISTS qualificacao_representante INTEGER,
    ADD COLUMN IF NOT EXISTS faixa_etaria INTEGER;


---------------------------
-- 2) TABELA SIMPLES
---------------------------

ALTER TABLE simples
    ADD COLUMN IF NOT EXISTS opcao_mei VARCHAR(1),
    ADD COLUMN IF NOT EXISTS data_opcao_mei DATE,
    ADD COLUMN IF NOT EXISTS data_exclusao_mei DATE;
