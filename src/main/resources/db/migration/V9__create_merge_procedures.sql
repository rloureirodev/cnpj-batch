-- =======================================================
-- V8 - Procedures de merge completas e altamente performáticas
-- =======================================================

------------------------------------------------------------
-- MERGE EMPRESA
------------------------------------------------------------
CREATE OR REPLACE FUNCTION proc_merge_empresa()
RETURNS void AS $$
BEGIN
    -- INSERT NOVOS
    INSERT INTO empresa (
        cnpj_basico,
        razao_social,
        natureza_juridica,
        qualificacao_responsavel,
        capital_social,
        porte,
        ente_federativo_responsavel,
        hash
    )
    SELECT
        s.cnpj_basico,
        s.razao_social,
        s.natureza_juridica,
        s.qualificacao_responsavel,
        COALESCE(s.capital_social, 0),
        s.porte,
        s.ente_federativo_responsavel,
        md5(
            coalesce(s.razao_social,'') ||
            coalesce(s.natureza_juridica,'') ||
            coalesce(s.qualificacao_responsavel,'') ||
            coalesce(s.capital_social,'') ||
            coalesce(s.porte,'') ||
            coalesce(s.ente_federativo_responsavel,'')
        )
    FROM stage_empresa s
    LEFT JOIN empresa f ON f.cnpj_basico = s.cnpj_basico
    WHERE f.cnpj_basico IS NULL;

    -- UPDATE ALTERADOS
    UPDATE empresa f
    SET
        razao_social = s.razao_social,
        natureza_juridica = s.natureza_juridica,
        qualificacao_responsavel = s.qualificacao_responsavel,
        capital_social = COALESCE(s.capital_social,0),
        porte = s.porte,
        ente_federativo_responsavel = s.ente_federativo_responsavel,
        hash = md5(
            coalesce(s.razao_social,'') ||
            coalesce(s.natureza_juridica,'') ||
            coalesce(s.qualificacao_responsavel,'') ||
            coalesce(s.capital_social,'') ||
            coalesce(s.porte,'') ||
            coalesce(s.ente_federativo_responsavel,'')
        ),
        atualizado_em = now()
    FROM stage_empresa s
    WHERE f.cnpj_basico = s.cnpj_basico
      AND f.hash IS DISTINCT FROM s.hash;
END;
$$ LANGUAGE plpgsql;


------------------------------------------------------------
-- MERGE ESTABELECIMENTO
------------------------------------------------------------
CREATE OR REPLACE FUNCTION proc_merge_estabelecimento()
RETURNS void AS $$
BEGIN
    -- INSERT NOVOS
    INSERT INTO estabelecimento (
        cnpj_basico, cnpj_ordem, cnpj_dv,
        identificador_matriz_filial, nome_fantasia, situacao_cadastral,
        data_situacao_cadastral, motivo_situacao_cadastral, nome_cidade_exterior,
        pais, data_inicio_atividade, cnae_fiscal_principal, cnae_fiscal_secundaria,
        tipo_logradouro, logradouro, numero, complemento, bairro,
        cep, uf, municipio, ddd1, telefone1, ddd2, telefone2,
        ddd_fax, fax, email, situacao_especial, data_situacao_especial,
        hash
    )
    SELECT
        s.cnpj_basico, s.cnpj_ordem, s.cnpj_dv,
        s.identificador_matriz_filial, s.nome_fantasia, s.situacao_cadastral,
        s.data_situacao_cadastral, s.motivo_situacao_cadastral, s.nome_cidade_exterior,
        s.pais, s.data_inicio_atividade, s.cnae_fiscal_principal, s.cnae_fiscal_secundaria,
        s.tipo_logradouro, s.logradouro, s.numero, s.complemento, s.bairro,
        s.cep, s.uf, s.municipio, s.ddd1, s.telefone1, s.ddd2, s.telefone2,
        s.ddd_fax, s.fax, s.email, s.situacao_especial, s.data_situacao_especial,
        md5(
            coalesce(s.nome_fantasia,'') ||
            coalesce(s.situacao_cadastral,'') ||
            coalesce(s.data_situacao_cadastral,'') ||
            coalesce(s.motivo_situacao_cadastral,'') ||
            coalesce(s.pais,'') ||
            coalesce(s.data_inicio_atividade,'') ||
            coalesce(s.cnae_fiscal_principal,'') ||
            coalesce(s.cnae_fiscal_secundaria,'') ||
            coalesce(s.tipo_logradouro,'') ||
            coalesce(s.logradouro,'') ||
            coalesce(s.numero,'') ||
            coalesce(s.complemento,'') ||
            coalesce(s.bairro,'') ||
            coalesce(s.cep,'') ||
            coalesce(s.uf,'') ||
            coalesce(s.municipio,'') ||
            coalesce(s.ddd1,'') ||
            coalesce(s.telefone1,'') ||
            coalesce(s.ddd2,'') ||
            coalesce(s.telefone2,'') ||
            coalesce(s.ddd_fax,'') ||
            coalesce(s.fax,'') ||
            coalesce(s.email,'') ||
            coalesce(s.situacao_especial,'') ||
            coalesce(s.data_situacao_especial,'')
        )
    FROM stage_estabelecimento s
    LEFT JOIN estabelecimento f
    ON f.cnpj_basico = s.cnpj_basico
    AND f.cnpj_ordem = s.cnpj_ordem
    AND f.cnpj_dv = s.cnpj_dv
    WHERE f.cnpj_basico IS NULL;

    -- UPDATE ALTERADOS
    UPDATE estabelecimento f
    SET
        identificador_matriz_filial = s.identificador_matriz_filial,
        nome_fantasia = s.nome_fantasia,
        situacao_cadastral = s.situacao_cadastral,
        data_situacao_cadastral = s.data_situacao_cadastral,
        motivo_situacao_cadastral = s.motivo_situacao_cadastral,
        nome_cidade_exterior = s.nome_cidade_exterior,
        pais = s.pais,
        data_inicio_atividade = s.data_inicio_atividade,
        cnae_fiscal_principal = s.cnae_fiscal_principal,
        cnae_fiscal_secundaria = s.cnae_fiscal_secundaria,
        tipo_logradouro = s.tipo_logradouro,
        logradouro = s.logradouro,
        numero = s.numero,
        complemento = s.complemento,
        bairro = s.bairro,
        cep = s.cep,
        uf = s.uf,
        municipio = s.municipio,
        ddd1 = s.ddd1,
        telefone1 = s.telefone1,
        ddd2 = s.ddd2,
        telefone2 = s.telefone2,
        ddd_fax = s.ddd_fax,
        fax = s.fax,
        email = s.email,
        situacao_especial = s.situacao_especial,
        data_situacao_especial = s.data_situacao_especial,
        hash = md5(
            coalesce(s.nome_fantasia,'') ||
            coalesce(s.situacao_cadastral,'') ||
            coalesce(s.data_situacao_cadastral,'') ||
            coalesce(s.motivo_situacao_cadastral,'') ||
            coalesce(s.pais,'') ||
            coalesce(s.data_inicio_atividade,'') ||
            coalesce(s.cnae_fiscal_principal,'') ||
            coalesce(s.cnae_fiscal_secundaria,'') ||
            coalesce(s.tipo_logradouro,'') ||
            coalesce(s.logradouro,'') ||
            coalesce(s.numero,'') ||
            coalesce(s.complemento,'') ||
            coalesce(s.bairro,'') ||
            coalesce(s.cep,'') ||
            coalesce(s.uf,'') ||
            coalesce(s.municipio,'') ||
            coalesce(s.ddd1,'') ||
            coalesce(s.telefone1,'') ||
            coalesce(s.ddd2,'') ||
            coalesce(s.telefone2,'') ||
            coalesce(s.ddd_fax,'') ||
            coalesce(s.fax,'') ||
            coalesce(s.email,'') ||
            coalesce(s.situacao_especial,'') ||
            coalesce(s.data_situacao_especial,'')
        ),
        atualizado_em = now()
    FROM stage_estabelecimento s
    WHERE f.cnpj_basico = s.cnpj_basico
      AND f.cnpj_ordem = s.cnpj_ordem
      AND f.cnpj_dv = s.cnpj_dv
      AND f.hash IS DISTINCT FROM s.hash;
END;
$$ LANGUAGE plpgsql;


------------------------------------------------------------
-- MERGE SOCIO
------------------------------------------------------------
CREATE OR REPLACE FUNCTION proc_merge_socio()
RETURNS void AS $$
BEGIN
    INSERT INTO socio (
        cnpj_basico, identificador_de_socio, nome_do_socio,
        cnpj_cpf_do_socio, qualificacao_socio, hash
    )
    SELECT
        s.cnpj_basico, s.identificador_de_socio, s.nome_do_socio,
        s.cnpj_cpf_do_socio, s.qualificacao_socio,
        md5(
            coalesce(s.cnpj_cpf_do_socio,'') ||
            coalesce(s.qualificacao_socio,'')
        )
    FROM stage_socios s
    LEFT JOIN socio f
    ON f.cnpj_basico = s.cnpj_basico
    AND f.identificador_de_socio = s.identificador_de_socio
    AND f.nome_do_socio = s.nome_do_socio
    WHERE f.cnpj_basico IS NULL;

    UPDATE socio f
    SET
        cnpj_cpf_do_socio = s.cnpj_cpf_do_socio,
        qualificacao_socio = s.qualificacao_socio,
        hash = md5(
            coalesce(s.cnpj_cpf_do_socio,'') ||
            coalesce(s.qualificacao_socio,'')
        ),
        atualizado_em = now()
    FROM stage_socios s
    WHERE f.cnpj_basico = s.cnpj_basico
    AND f.identificador_de_socio = s.identificador_de_socio
    AND f.nome_do_socio = s.nome_do_socio
    AND f.hash IS DISTINCT FROM s.hash;
END;
$$ LANGUAGE plpgsql;


------------------------------------------------------------
-- MERGE SIMPLES
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE cnpj.proc_merge_simples_block(
    p_inicio BIGINT,
    p_fim BIGINT
)
LANGUAGE plpgsql
AS $procedure$
BEGIN
    ------------------------------------------------------------
    -- INSERT (apenas do bloco)
    ------------------------------------------------------------
    INSERT INTO cnpj.simples (
        cnpj_basico,
        opcao_pelo_simples,
        data_opcao_pelo_simples,
        data_exclusao_do_simples,
        hash
    )
    SELECT
        s.cnpj_basico,
        s.opcao_pelo_simples,
        to_date(s.data_opcao_pelo_simples, 'YYYYMMDD'),
        to_date(s.data_exclusao_do_simples, 'YYYYMMDD'),
        md5(
            coalesce(s.cnpj_basico,'') ||
            coalesce(s.opcao_pelo_simples,'') ||
            coalesce(s.data_opcao_pelo_simples,'') ||
            coalesce(s.data_exclusao_do_simples,'')
        )
    FROM cnpj.stage_simples s
    LEFT JOIN cnpj.simples f ON f.cnpj_basico = s.cnpj_basico
    WHERE f.cnpj_basico IS NULL
      AND s.id BETWEEN p_inicio AND p_fim;

    ------------------------------------------------------------
    -- UPDATE (apenas do bloco)
    ------------------------------------------------------------
    UPDATE cnpj.simples f
    SET
        opcao_pelo_simples = s.opcao_pelo_simples,
        data_opcao_pelo_simples = to_date(s.data_opcao_pelo_simples,'YYYYMMDD'),
        data_exclusao_do_simples = to_date(s.data_exclusao_do_simples,'YYYYMMDD'),
        hash = md5(
            coalesce(s.cnpj_basico,'') ||
            coalesce(s.opcao_pelo_simples,'') ||
            coalesce(s.data_opcao_pelo_simples,'') ||
            coalesce(s.data_exclusao_do_simples,'')
        ),
        atualizado_em = now()
    FROM cnpj.stage_simples s
    WHERE f.cnpj_basico = s.cnpj_basico
      AND s.id BETWEEN p_inicio AND p_fim
      AND f.hash IS DISTINCT FROM
          md5(
              coalesce(s.cnpj_basico,'') ||
              coalesce(s.opcao_pelo_simples,'') ||
              coalesce(s.data_opcao_pelo_simples,'') ||
              coalesce(s.data_exclusao_do_simples,'')
          );
END;
$procedure$;


------------------------------------------------------------
-- MERGE MUNICIPIOS
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE proc_merge_municipios()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO municipio (codigo, nome, hash)
    SELECT
        trim(s.codigo),
        s.nome,
        md5(coalesce(s.nome,''))
    FROM stage_municipios s
    LEFT JOIN municipio f ON f.codigo = trim(s.codigo)
    WHERE f.codigo IS NULL;

    UPDATE municipio f
    SET
        nome = s.nome,
        hash = md5(coalesce(s.nome,'')),
        atualizado_em = now()
    FROM stage_municipios s
    WHERE f.codigo = trim(s.codigo)
      AND f.hash IS DISTINCT FROM md5(coalesce(s.nome,''));
END;
$$ ;


------------------------------------------------------------
-- MERGE CNAES
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE cnpj.proc_merge_cnaes()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO cnae (codigo, descricao, hash)
    SELECT
        trim(s.codigo)::int,
        s.descricao,
        md5(coalesce(s.descricao,''))
    FROM stage_cnaes s
    LEFT JOIN cnae f ON f.codigo = trim(s.codigo)::int
    WHERE f.codigo IS NULL;

    UPDATE cnae f
    SET
        descricao = s.descricao,
        hash = md5(coalesce(s.descricao,'')),
        atualizado_em = now()
    FROM stage_cnaes s
    WHERE f.codigo = trim(s.codigo)::int
      AND f.hash IS DISTINCT FROM md5(coalesce(s.descricao,''));
END;
$procedure$
;
------------------------------------------------------------
-- MERGE PAISES
------------------------------------------------------------

CREATE OR REPLACE PROCEDURE cnpj.proc_merge_paises()
 LANGUAGE plpgsql
AS $procedure$
DECLARE etapa TEXT := 'inicio';
BEGIN
    etapa := 'INSERT';
    INSERT INTO cnpj.pais (codigo, nome, hash)
    SELECT
        s.codigo::int,
        s.nome,
        md5(coalesce(s.nome,''))
    FROM cnpj.stage_paises s
    LEFT JOIN cnpj.pais f ON f.codigo = s.codigo::int
    WHERE f.codigo IS NULL;

    etapa := 'UPDATE';
    UPDATE cnpj.pais f
    SET
        nome = s.nome,
        hash = md5(coalesce(s.nome,'')),
        atualizado_em = now()
    FROM cnpj.stage_paises s
    WHERE f.codigo = s.codigo::int
      AND f.hash IS DISTINCT FROM md5(coalesce(s.nome,''));

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Erro em proc_merge_paises na etapa %: %', etapa, SQLERRM;
END;
$procedure$
;

------------------------------------------------------------
-- MERGE NATUREZAS
------------------------------------------------------------


CREATE OR REPLACE PROCEDURE cnpj.proc_merge_naturezas()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO cnpj.natureza (codigo, descricao, hash)
    SELECT
        s.codigo::int,
        s.descricao,
        md5(coalesce(s.descricao, ''))
    FROM cnpj.stage_naturezas s
    LEFT JOIN cnpj.natureza f ON f.codigo = s.codigo::int
    WHERE f.codigo IS NULL;

    UPDATE cnpj.natureza f
    SET
        descricao = s.descricao,
        hash = md5(coalesce(s.descricao,'')),
        atualizado_em = now()
    FROM cnpj.stage_naturezas s
    WHERE f.codigo = s.codigo::int
      AND f.hash IS DISTINCT FROM md5(coalesce(s.descricao,''));
END;
$$;
------------------------------------------------------------
-- MERGE QUALIFICACOES
------------------------------------------------------------

CREATE OR REPLACE PROCEDURE cnpj.proc_merge_qualificacoes()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO cnpj.qualificacao (codigo, descricao, hash)
    SELECT
        s.codigo::int,
        s.descricao,
        md5(coalesce(s.descricao,''))
    FROM cnpj.stage_qualificacoes s
    LEFT JOIN cnpj.qualificacao f ON f.codigo = s.codigo::int
    WHERE f.codigo IS NULL;

    UPDATE cnpj.qualificacao f
    SET
        descricao = s.descricao,
        hash = md5(coalesce(s.descricao,'')),
        atualizado_em = now()
    FROM cnpj.stage_qualificacoes s
    WHERE f.codigo = s.codigo::int
      AND f.hash IS DISTINCT FROM md5(coalesce(s.descricao,''));
END;
$$;
------------------------------------------------------------
-- MERGE MOTIVOS
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE cnpj.proc_merge_motivos()
 LANGUAGE plpgsql
AS $procedure$
DECLARE etapa TEXT := 'inicio';
BEGIN
    etapa := 'INSERT';
    INSERT INTO cnpj.motivo (codigo, descricao, hash)
    SELECT
        s.codigo::int,
        s.descricao,
        md5(coalesce(s.descricao,''))
    FROM cnpj.stage_motivos s
    LEFT JOIN cnpj.motivo f ON f.codigo = s.codigo::int
    WHERE f.codigo IS NULL;

    etapa := 'UPDATE';
    UPDATE cnpj.motivo f
    SET
        descricao = s.descricao,
        hash = md5(coalesce(s.descricao,'')),
        atualizado_em = now()
    FROM cnpj.stage_motivos s
    WHERE f.codigo = s.codigo::int
      AND f.hash IS DISTINCT FROM md5(coalesce(s.descricao,''));

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Erro em proc_merge_motivos na etapa %: %', etapa, SQLERRM;
END;
$procedure$
;

