-- =======================================================
-- V8 - Procedures de merge completas e altamente performáticas
-- =======================================================

------------------------------------------------------------
-- MERGE EMPRESA BLOCK
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE cnpj.proc_merge_empresa_block(
    p_inicio BIGINT,
    p_fim BIGINT
)
LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- INSERT NOVOS
    INSERT INTO cnpj.empresa (
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
	    trim(coalesce(s.cnpj_basico,'')),
	    trim(coalesce(s.razao_social,'')),
	    trim(coalesce(s.natureza_juridica,'')),
	    trim(coalesce(s.qualificacao_responsavel,'')),
	    trim(coalesce(s.capital_social,'')),
	    trim(coalesce(s.porte,'')),
	    trim(coalesce(s.ente_federativo_responsavel,'')),
        md5(
			trim(coalesce(s.cnpj_basico,'')) ||
	        trim(coalesce(s.razao_social,'')) ||
	        trim(coalesce(s.natureza_juridica,'')) ||
	        trim(coalesce(s.qualificacao_responsavel,'')) ||
	        trim(coalesce(s.capital_social,'')) ||
	        trim(coalesce(s.porte,'')) ||
	        trim(coalesce(s.ente_federativo_responsavel,''))
        )
    FROM cnpj.stage_empresa s
    LEFT JOIN cnpj.empresa f ON f.cnpj_basico = s.cnpj_basico
    WHERE f.cnpj_basico IS NULL
      AND s.id BETWEEN p_inicio AND p_fim;

    -- UPDATE ALTERADOS
    UPDATE cnpj.empresa f
    SET
        razao_social = s.razao_social,
        natureza_juridica = s.natureza_juridica,
        qualificacao_responsavel = s.qualificacao_responsavel,
        capital_social = COALESCE(s.capital_social,''),
        porte = s.porte,
        ente_federativo_responsavel = s.ente_federativo_responsavel,
        hash = md5(
			coalesce(s.cnpj_basico,'') ||
            coalesce(s.razao_social,'') ||
            coalesce(s.natureza_juridica,'') ||
            coalesce(s.qualificacao_responsavel,'') ||
            coalesce(s.capital_social,'') ||
            coalesce(s.porte,'') ||
            coalesce(s.ente_federativo_responsavel,'')
        ),
        atualizado_em = now()
    FROM cnpj.stage_empresa s
    WHERE f.cnpj_basico = s.cnpj_basico
      AND s.id BETWEEN p_inicio AND p_fim
      AND f.hash IS DISTINCT FROM md5(
		    coalesce(s.cnpj_basico,'') ||
            coalesce(s.razao_social,'') ||
            coalesce(s.natureza_juridica,'') ||
            coalesce(s.qualificacao_responsavel,'') ||
            coalesce(s.capital_social,'') ||
            coalesce(s.porte,'') ||
            coalesce(s.ente_federativo_responsavel,'')
        );
END;
$procedure$;


------------------------------------------------------------
-- MERGE ESTABELECIMENTO BLOCK
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE cnpj.proc_merge_estabelecimento_block(
    p_inicio BIGINT,
    p_fim BIGINT
)
LANGUAGE plpgsql
AS $procedure$
BEGIN
    ------------------------------------------------------------
    -- INSERT
    ------------------------------------------------------------
    INSERT INTO cnpj.estabelecimento (
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
        trim(coalesce(s.cnpj_basico,'')),
        trim(coalesce(s.cnpj_ordem,'')),
        trim(coalesce(s.cnpj_dv,'')),
        trim(coalesce(s.identificador_matriz_filial,'')),
        trim(coalesce(s.nome_fantasia,'')),
        trim(coalesce(s.situacao_cadastral,'')),
        trim(coalesce(s.data_situacao_cadastral,'')),
        trim(coalesce(s.motivo_situacao_cadastral,'')),
        trim(coalesce(s.nome_cidade_exterior,'')),
        trim(coalesce(s.pais,'')),
        trim(coalesce(s.data_inicio_atividade,'')),
        trim(coalesce(s.cnae_fiscal_principal,'')),
        trim(coalesce(s.cnae_fiscal_secundaria,'')),
        trim(coalesce(s.tipo_logradouro,'')),
        trim(coalesce(s.logradouro,'')),
        trim(coalesce(s.numero,'')),
        trim(coalesce(s.complemento,'')),
        trim(coalesce(s.bairro,'')),
        trim(coalesce(s.cep,'')),
        trim(coalesce(s.uf,'')),
        trim(coalesce(s.municipio,'')),
        trim(coalesce(s.ddd1,'')),
        trim(coalesce(s.telefone1,'')),
        trim(coalesce(s.ddd2,'')),
        trim(coalesce(s.telefone2,'')),
        trim(coalesce(s.ddd_fax,'')),
        trim(coalesce(s.fax,'')),
        trim(coalesce(s.email,'')),
        trim(coalesce(s.situacao_especial,'')),
        trim(coalesce(s.data_situacao_especial,'')),
        md5(
            trim(coalesce(s.nome_fantasia,'')) ||
            trim(coalesce(s.situacao_cadastral,'')) ||
            trim(coalesce(s.data_situacao_cadastral,'')) ||
            trim(coalesce(s.motivo_situacao_cadastral,'')) ||
            trim(coalesce(s.pais,'')) ||
            trim(coalesce(s.data_inicio_atividade,'')) ||
            trim(coalesce(s.cnae_fiscal_principal,'')) ||
            trim(coalesce(s.cnae_fiscal_secundaria,'')) ||
            trim(coalesce(s.tipo_logradouro,'')) ||
            trim(coalesce(s.logradouro,'')) ||
            trim(coalesce(s.numero,'')) ||
            trim(coalesce(s.complemento,'')) ||
            trim(coalesce(s.bairro,'')) ||
            trim(coalesce(s.cep,'')) ||
            trim(coalesce(s.uf,'')) ||
            trim(coalesce(s.municipio,'')) ||
            trim(coalesce(s.ddd1,'')) ||
            trim(coalesce(s.telefone1,'')) ||
            trim(coalesce(s.ddd2,'')) ||
            trim(coalesce(s.telefone2,'')) ||
            trim(coalesce(s.ddd_fax,'')) ||
            trim(coalesce(s.fax,'')) ||
            trim(coalesce(s.email,'')) ||
            trim(coalesce(s.situacao_especial,'')) ||
            trim(coalesce(s.data_situacao_especial,''))
        )
    FROM cnpj.stage_estabelecimento s
    LEFT JOIN cnpj.estabelecimento f
      ON f.cnpj_basico = trim(coalesce(s.cnpj_basico,'')) 
     AND f.cnpj_ordem  = trim(coalesce(s.cnpj_ordem,'')) 
     AND f.cnpj_dv     = trim(coalesce(s.cnpj_dv,''))
    WHERE f.cnpj_basico IS NULL
      AND s.id BETWEEN p_inicio AND p_fim;

    ------------------------------------------------------------
    -- UPDATE
    ------------------------------------------------------------
    UPDATE cnpj.estabelecimento f
    SET
        identificador_matriz_filial = trim(coalesce(s.identificador_matriz_filial,'')),
        nome_fantasia = trim(coalesce(s.nome_fantasia,'')),
        situacao_cadastral = trim(coalesce(s.situacao_cadastral,'')),
        data_situacao_cadastral = trim(coalesce(s.data_situacao_cadastral,'')),
        motivo_situacao_cadastral = trim(coalesce(s.motivo_situacao_cadastral,'')),
        nome_cidade_exterior = trim(coalesce(s.nome_cidade_exterior,'')),
        pais = trim(coalesce(s.pais,'')),
        data_inicio_atividade = trim(coalesce(s.data_inicio_atividade,'')),
        cnae_fiscal_principal = trim(coalesce(s.cnae_fiscal_principal,'')),
        cnae_fiscal_secundaria = trim(coalesce(s.cnae_fiscal_secundaria,'')),
        tipo_logradouro = trim(coalesce(s.tipo_logradouro,'')),
        logradouro = trim(coalesce(s.logradouro,'')),
        numero = trim(coalesce(s.numero,'')),
        complemento = trim(coalesce(s.complemento,'')),
        bairro = trim(coalesce(s.bairro,'')),
        cep = trim(coalesce(s.cep,'')),
        uf = trim(coalesce(s.uf,'')),
        municipio = trim(coalesce(s.municipio,'')),
        ddd1 = trim(coalesce(s.ddd1,'')),
        telefone1 = trim(coalesce(s.telefone1,'')),
        ddd2 = trim(coalesce(s.ddd2,'')),
        telefone2 = trim(coalesce(s.telefone2,'')),
        ddd_fax = trim(coalesce(s.ddd_fax,'')),
        fax = trim(coalesce(s.fax,'')),
        email = trim(coalesce(s.email,'')),
        situacao_especial = trim(coalesce(s.situacao_especial,'')),
        data_situacao_especial = trim(coalesce(s.data_situacao_especial,'')),
        hash =
            md5(
                trim(coalesce(s.nome_fantasia,'')) ||
                trim(coalesce(s.situacao_cadastral,'')) ||
                trim(coalesce(s.data_situacao_cadastral,'')) ||
                trim(coalesce(s.motivo_situacao_cadastral,'')) ||
                trim(coalesce(s.pais,'')) ||
                trim(coalesce(s.data_inicio_atividade,'')) ||
                trim(coalesce(s.cnae_fiscal_principal,'')) ||
                trim(coalesce(s.cnae_fiscal_secundaria,'')) ||
                trim(coalesce(s.tipo_logradouro,'')) ||
                trim(coalesce(s.logradouro,'')) ||
                trim(coalesce(s.numero,'')) ||
                trim(coalesce(s.complemento,'')) ||
                trim(coalesce(s.bairro,'')) ||
                trim(coalesce(s.cep,'')) ||
                trim(coalesce(s.uf,'')) ||
                trim(coalesce(s.municipio,'')) ||
                trim(coalesce(s.ddd1,'')) ||
                trim(coalesce(s.telefone1,'')) ||
                trim(coalesce(s.ddd2,'')) ||
                trim(coalesce(s.telefone2,'')) ||
                trim(coalesce(s.ddd_fax,'')) ||
                trim(coalesce(s.fax,'')) ||
                trim(coalesce(s.email,'')) ||
                trim(coalesce(s.situacao_especial,'')) ||
                trim(coalesce(s.data_situacao_especial,''))
            ),
        atualizado_em = now()
    FROM cnpj.stage_estabelecimento s
    WHERE f.cnpj_basico = trim(coalesce(s.cnpj_basico,'')) 
      AND f.cnpj_ordem = trim(coalesce(s.cnpj_ordem,'')) 
      AND f.cnpj_dv = trim(coalesce(s.cnpj_dv,'')) 
      AND s.id BETWEEN p_inicio AND p_fim
      AND f.hash IS DISTINCT FROM
            md5(
                trim(coalesce(s.nome_fantasia,'')) ||
                trim(coalesce(s.situacao_cadastral,'')) ||
                trim(coalesce(s.data_situacao_cadastral,'')) ||
                trim(coalesce(s.motivo_situacao_cadastral,'')) ||
                trim(coalesce(s.pais,'')) ||
                trim(coalesce(s.data_inicio_atividade,'')) ||
                trim(coalesce(s.cnae_fiscal_principal,'')) ||
                trim(coalesce(s.cnae_fiscal_secundaria,'')) ||
                trim(coalesce(s.tipo_logradouro,'')) ||
                trim(coalesce(s.logradouro,'')) ||
                trim(coalesce(s.numero,'')) ||
                trim(coalesce(s.complemento,'')) ||
                trim(coalesce(s.bairro,'')) ||
                trim(coalesce(s.cep,'')) ||
                trim(coalesce(s.uf,'')) ||
                trim(coalesce(s.municipio,'')) ||
                trim(coalesce(s.ddd1,'')) ||
                trim(coalesce(s.telefone1,'')) ||
                trim(coalesce(s.ddd2,'')) ||
                trim(coalesce(s.telefone2,'')) ||
                trim(coalesce(s.ddd_fax,'')) ||
                trim(coalesce(s.fax,'')) ||
                trim(coalesce(s.email,'')) ||
                trim(coalesce(s.situacao_especial,'')) ||
                trim(coalesce(s.data_situacao_especial,''))
            );
END;
$procedure$;

------------------------------------------------------------
-- MERGE SOCIO BLOCK
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE cnpj.proc_merge_socio_block(
    p_inicio BIGINT,
    p_fim BIGINT
)
LANGUAGE plpgsql
AS $procedure$
BEGIN
    ------------------------------------------------------------
    -- INSERT (somente do bloco)
    ------------------------------------------------------------
    INSERT INTO cnpj.socio (
        cnpj_basico,
        identificador_de_socio,
        nome_do_socio,
        cnpj_cpf_do_socio,
        qualificacao_socio,
        data_entrada_sociedade,
        pais,
        representante_legal,
        nome_representante,
        qualificacao_representante,
        faixa_etaria,
        hash
    )
    SELECT
        trim(coalesce(s.cnpj_basico,'')),
        trim(coalesce(s.identificador_de_socio,'')),
        trim(coalesce(s.nome_do_socio,'')),
        trim(coalesce(s.cnpj_cpf_do_socio,'')),
        trim(coalesce(s.qualificacao_socio,'')),
        trim(coalesce(s.data_entrada_sociedade,'')),
        trim(coalesce(s.pais,'')),
        trim(coalesce(s.representante_legal,'')),
        trim(coalesce(s.nome_representante,'')),
        trim(coalesce(s.qualificacao_representante,'')),
        trim(coalesce(s.faixa_etaria,'')),
        md5(
            trim(coalesce(s.cnpj_cpf_do_socio,'')) ||
            trim(coalesce(s.qualificacao_socio,'')) ||
            trim(coalesce(s.data_entrada_sociedade,'')) ||
            trim(coalesce(s.pais,'')) ||
            trim(coalesce(s.representante_legal,'')) ||
            trim(coalesce(s.nome_representante,'')) ||
            trim(coalesce(s.qualificacao_representante,'')) ||
            trim(coalesce(s.faixa_etaria,''))
        )
    FROM cnpj.stage_socios s
    LEFT JOIN cnpj.socio f
      ON f.cnpj_basico = trim(coalesce(s.cnpj_basico,'')) 
     AND f.identificador_de_socio = trim(coalesce(s.identificador_de_socio,'')) 
     AND f.nome_do_socio = trim(coalesce(s.nome_do_socio,''))
    WHERE f.cnpj_basico IS NULL
      AND s.id BETWEEN p_inicio AND p_fim;

    ------------------------------------------------------------
    -- UPDATE (somente do bloco)
    ------------------------------------------------------------
    UPDATE cnpj.socio f
    SET
        cnpj_cpf_do_socio        = trim(coalesce(s.cnpj_cpf_do_socio,'')),
        qualificacao_socio       = trim(coalesce(s.qualificacao_socio,'')),
        data_entrada_sociedade   = trim(coalesce(s.data_entrada_sociedade,'')),
        pais                     = trim(coalesce(s.pais,'')),
        representante_legal      = trim(coalesce(s.representante_legal,'')),
        nome_representante       = trim(coalesce(s.nome_representante,'')),
        qualificacao_representante = trim(coalesce(s.qualificacao_representante,'')),
        faixa_etaria            = trim(coalesce(s.faixa_etaria,'')),
        hash = md5(
            trim(coalesce(s.cnpj_cpf_do_socio,'')) ||
            trim(coalesce(s.qualificacao_socio,'')) ||
            trim(coalesce(s.data_entrada_sociedade,'')) ||
            trim(coalesce(s.pais,'')) ||
            trim(coalesce(s.representante_legal,'')) ||
            trim(coalesce(s.nome_representante,'')) ||
            trim(coalesce(s.qualificacao_representante,'')) ||
            trim(coalesce(s.faixa_etaria,''))
        ),
        atualizado_em = now()
    FROM cnpj.stage_socios s
    WHERE f.cnpj_basico = trim(coalesce(s.cnpj_basico,'')) 
      AND f.identificador_de_socio = trim(coalesce(s.identificador_de_socio,'')) 
      AND f.nome_do_socio = trim(coalesce(s.nome_do_socio,'')) 
      AND s.id BETWEEN p_inicio AND p_fim
      AND f.hash IS DISTINCT FROM md5(
            trim(coalesce(s.cnpj_cpf_do_socio,'')) ||
            trim(coalesce(s.qualificacao_socio,'')) ||
            trim(coalesce(s.data_entrada_sociedade,'')) ||
            trim(coalesce(s.pais,'')) ||
            trim(coalesce(s.representante_legal,'')) ||
            trim(coalesce(s.nome_representante,'')) ||
            trim(coalesce(s.qualificacao_representante,'')) ||
            trim(coalesce(s.faixa_etaria,''))
        );
END;
$procedure$;

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
    -- INSERT NOVOS (somente do bloco)
    ------------------------------------------------------------
    INSERT INTO cnpj.simples (
        cnpj_basico,
        opcao_pelo_simples,
        data_opcao_pelo_simples,
        data_exclusao_do_simples,
        opcao_mei,
        data_opcao_mei,
        data_exclusao_mei,
        hash
    )
    SELECT
        trim(coalesce(s.cnpj_basico,'')),
        trim(coalesce(s.opcao_pelo_simples,'')),
        trim(coalesce(s.data_opcao_pelo_simples,'')),
        trim(coalesce(s.data_exclusao_do_simples,'')),
        trim(coalesce(s.opcao_pelo_mei,'')),
        trim(coalesce(s.data_opcao_mei,'')),
        trim(coalesce(s.data_exclusao_do_mei,'')),
        md5(
            trim(coalesce(s.cnpj_basico,'')) ||
            trim(coalesce(s.opcao_pelo_simples,'')) ||
            trim(coalesce(s.data_opcao_pelo_simples,'')) ||
            trim(coalesce(s.data_exclusao_do_simples,'')) ||
            trim(coalesce(s.opcao_pelo_mei,'')) ||
            trim(coalesce(s.data_opcao_mei,'')) ||
            trim(coalesce(s.data_exclusao_do_mei,''))
        )
    FROM cnpj.stage_simples s
    LEFT JOIN cnpj.simples f
        ON f.cnpj_basico = trim(coalesce(s.cnpj_basico,''))
    WHERE f.cnpj_basico IS NULL
      AND s.id BETWEEN p_inicio AND p_fim;

    ------------------------------------------------------------
    -- UPDATE ALTERADOS (somente do bloco)
    ------------------------------------------------------------
    UPDATE cnpj.simples f
    SET
        opcao_pelo_simples      = trim(coalesce(s.opcao_pelo_simples,'')),
        data_opcao_pelo_simples = trim(coalesce(s.data_opcao_pelo_simples,'')),
        data_exclusao_do_simples = trim(coalesce(s.data_exclusao_do_simples,'')),
        opcao_mei               = trim(coalesce(s.opcao_pelo_mei,'')),
        data_opcao_mei          = trim(coalesce(s.data_opcao_mei,'')),
        data_exclusao_mei       = trim(coalesce(s.data_exclusao_do_mei,'')),
        hash = md5(
            trim(coalesce(s.cnpj_basico,'')) ||
            trim(coalesce(s.opcao_pelo_simples,'')) ||
            trim(coalesce(s.data_opcao_pelo_simples,'')) ||
            trim(coalesce(s.data_exclusao_do_simples,'')) ||
            trim(coalesce(s.opcao_pelo_mei,'')) ||
            trim(coalesce(s.data_opcao_mei,'')) ||
            trim(coalesce(s.data_exclusao_do_mei,''))
        ),
        atualizado_em = now()
    FROM cnpj.stage_simples s
    WHERE f.cnpj_basico = trim(coalesce(s.cnpj_basico,''))
      AND s.id BETWEEN p_inicio AND p_fim
      AND f.hash IS DISTINCT FROM md5(
            trim(coalesce(s.cnpj_basico,'')) ||
            trim(coalesce(s.opcao_pelo_simples,'')) ||
            trim(coalesce(s.data_opcao_pelo_simples,'')) ||
            trim(coalesce(s.data_exclusao_do_simples,'')) ||
            trim(coalesce(s.opcao_pelo_mei,'')) ||
            trim(coalesce(s.data_opcao_mei,'')) ||
            trim(coalesce(s.data_exclusao_do_mei,''))
        );
END;
$procedure$;


------------------------------------------------------------
-- MERGE MUNICIPIOS BLOCK
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE cnpj.proc_merge_municipios_block(
    p_inicio BIGINT,
    p_fim BIGINT
)
LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO cnpj.municipio (codigo, nome, hash)
    SELECT
        trim(s.codigo),
        s.nome,
        md5(coalesce(s.nome,''))
    FROM cnpj.stage_municipios s
    LEFT JOIN cnpj.municipio f ON f.codigo = trim(s.codigo)
    WHERE f.codigo IS NULL
      AND s.id BETWEEN p_inicio AND p_fim;

    UPDATE cnpj.municipio f
    SET
        nome = s.nome,
        hash = md5(coalesce(s.nome,'')),
        atualizado_em = now()
    FROM cnpj.stage_municipios s
    WHERE f.codigo = trim(s.codigo)
      AND f.hash IS DISTINCT FROM md5(coalesce(s.nome,''))
      AND s.id BETWEEN p_inicio AND p_fim;
END;
$procedure$;


------------------------------------------------------------
-- MERGE CNAES BLOCK
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE cnpj.proc_merge_cnaes_block(
    p_inicio BIGINT,
    p_fim BIGINT
)
LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO cnpj.cnae (codigo, descricao, hash)
    SELECT
        s.codigo,
        s.descricao,
        md5(coalesce(s.descricao,''))
    FROM cnpj.stage_cnaes s
    LEFT JOIN cnpj.cnae f ON f.codigo = s.codigo
    WHERE f.codigo IS NULL
      AND s.id BETWEEN p_inicio AND p_fim;

    UPDATE cnpj.cnae f
    SET
        descricao = s.descricao,
        hash = md5(coalesce(s.descricao,'')),
        atualizado_em = now()
    FROM cnpj.stage_cnaes s
    WHERE f.codigo = s.codigo
      AND f.hash IS DISTINCT FROM md5(coalesce(s.descricao,''))
      AND s.id BETWEEN p_inicio AND p_fim;
END;
$procedure$;

------------------------------------------------------------
-- MERGE PAISES BLOCK - ok
------------------------------------------------------------

CREATE OR REPLACE PROCEDURE cnpj.proc_merge_paises_block(
    p_inicio BIGINT,
    p_fim BIGINT
)
LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO cnpj.pais (codigo, nome, hash)
    SELECT
        s.codigo,
        s.nome,
        md5(coalesce(s.nome,''))
    FROM cnpj.stage_paises s
    LEFT JOIN cnpj.pais f ON f.codigo = s.codigo
    WHERE f.codigo IS NULL
      AND s.id BETWEEN p_inicio AND p_fim;

    UPDATE cnpj.pais f
    SET
        nome = s.nome,
        hash = md5(coalesce(s.nome,'')),
        atualizado_em = now()
    FROM cnpj.stage_paises s
    WHERE f.codigo = s.codigo
      AND f.hash IS DISTINCT FROM md5(coalesce(s.nome,''))
      AND s.id BETWEEN p_inicio AND p_fim;
END;
$procedure$;


------------------------------------------------------------
-- MERGE NATUREZAS BLOCK
------------------------------------------------------------

CREATE OR REPLACE PROCEDURE cnpj.proc_merge_naturezas_block(
    p_inicio BIGINT,
    p_fim BIGINT
)
LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO cnpj.natureza (codigo, descricao, hash)
    SELECT
        s.codigo,
        s.descricao,
        md5(coalesce(s.descricao,''))
    FROM cnpj.stage_naturezas s
    LEFT JOIN cnpj.natureza f ON f.codigo = s.codigo
    WHERE f.codigo IS NULL
      AND s.id BETWEEN p_inicio AND p_fim;

    UPDATE cnpj.natureza f
    SET
        descricao = s.descricao,
        hash = md5(coalesce(s.descricao,'')),
        atualizado_em = now()
    FROM cnpj.stage_naturezas s
    WHERE f.codigo = s.codigo
      AND f.hash IS DISTINCT FROM md5(coalesce(s.descricao,''))
      AND s.id BETWEEN p_inicio AND p_fim;
END;
$procedure$;

------------------------------------------------------------
-- MERGE QUALIFICACOES BLOCK
------------------------------------------------------------

CREATE OR REPLACE PROCEDURE cnpj.proc_merge_qualificacoes_block(
    p_inicio BIGINT,
    p_fim BIGINT
)
LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO cnpj.qualificacao (codigo, descricao, hash)
    SELECT
        s.codigo,
        s.descricao,
        md5(coalesce(s.descricao,''))
    FROM cnpj.stage_qualificacoes s
    LEFT JOIN cnpj.qualificacao f ON f.codigo = s.codigo
    WHERE f.codigo IS NULL
      AND s.id BETWEEN p_inicio AND p_fim;

    UPDATE cnpj.qualificacao f
    SET
        descricao = s.descricao,
        hash = md5(coalesce(s.descricao,'')),
        atualizado_em = now()
    FROM cnpj.stage_qualificacoes s
    WHERE f.codigo = s.codigo
      AND f.hash IS DISTINCT FROM md5(coalesce(s.descricao,''))
      AND s.id BETWEEN p_inicio AND p_fim;
END;
$procedure$;

------------------------------------------------------------
-- MERGE MOTIVOS BLOCK - ok
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE cnpj.proc_merge_motivos_block(
    p_inicio BIGINT,
    p_fim BIGINT
)
LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO cnpj.motivo (codigo, descricao, hash)
    SELECT
        s.codigo,
        s.descricao,
        md5(coalesce(s.descricao,''))
    FROM cnpj.stage_motivos s
    LEFT JOIN cnpj.motivo f ON f.codigo = s.codigo
    WHERE f.codigo IS NULL
      AND s.id BETWEEN p_inicio AND p_fim;

    UPDATE cnpj.motivo f
    SET
        descricao = s.descricao,
        hash = md5(coalesce(s.descricao,'')),
        atualizado_em = now()
    FROM cnpj.stage_motivos s
    WHERE f.codigo = s.codigo
      AND f.hash IS DISTINCT FROM md5(coalesce(s.descricao,''))
      AND s.id BETWEEN p_inicio AND p_fim;
END;
$procedure$;

