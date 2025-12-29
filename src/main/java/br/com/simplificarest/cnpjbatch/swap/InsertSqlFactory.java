package br.com.simplificarest.cnpjbatch.swap;

public class InsertSqlFactory {

	public static String buildInsert(String finalTable, String newTable, String stageTable) {

		switch (finalTable) {
		case "cnpj.empresa":
			return insertEmpresa(newTable, stageTable);

		case "cnpj.estabelecimento":
			return insertEstabelecimento(newTable, stageTable);

		case "cnpj.socio":
			return insertSocio(newTable, stageTable);

		case "cnpj.simples":
			return insertSimples(newTable, stageTable);

		case "cnpj.municipio":
		case "cnpj.cnae":
		case "cnpj.natureza":
		case "cnpj.motivo":
		case "cnpj.pais":
		case "cnpj.qualificacao":
			return insertLookup(newTable, stageTable);

		default:
			throw new IllegalArgumentException("Sem SQL para tabela: " + finalTable);
		}
	}

	private static String insertEmpresa(String newTable, String stage) {
		return """
					INSERT INTO %s (
					    cnpj_basico,
					    razao_social,
					    natureza_juridica,
					    qualificacao_responsavel,
					    capital_social,
					    porte,
					    ente_federativo_responsavel,
					    atualizado_em
					)
					SELECT
					    cnpj_basico,
					    razao_social,
					    natureza_juridica,
					    qualificacao_responsavel,
					    capital_social,
					    porte,
					    ente_federativo_responsavel,
					    now()
					FROM %s;
				""".formatted(newTable, stage);
	}

	private static String insertEstabelecimento(String newTable, String stage) {
		return """
					INSERT INTO %s (
					    cnpj_basico, cnpj_ordem, cnpj_dv,
					    identificador_matriz_filial, nome_fantasia, situacao_cadastral,
					    data_situacao_cadastral, motivo_situacao_cadastral, nome_cidade_exterior,
					    pais, data_inicio_atividade, cnae_fiscal_principal, cnae_fiscal_secundaria,
					    tipo_logradouro, logradouro, numero, complemento, bairro,
					    cep, uf, municipio, ddd1, telefone1, ddd2, telefone2,
					    ddd_fax, fax, email, situacao_especial, data_situacao_especial,
					    atualizado_em
					)
					SELECT
					    cnpj_basico, cnpj_ordem, cnpj_dv,
					    identificador_matriz_filial, nome_fantasia, situacao_cadastral,
					    data_situacao_cadastral, motivo_situacao_cadastral, nome_cidade_exterior,
					    pais, data_inicio_atividade,
					    NULLIF(cnae_fiscal_principal, '')::int,
					    cnae_fiscal_secundaria,
					    tipo_logradouro, logradouro, numero, complemento, bairro,
					    cep, uf, municipio, ddd1, telefone1, ddd2, telefone2,
					    ddd_fax, fax, email, situacao_especial, data_situacao_especial,
					    now()
					FROM %s;
				"""
				.formatted(newTable, stage);
	}

	private static String insertSocio(String newTable, String stage) {
		return """
					INSERT INTO %s (
					    cnpj_basico, identificador_de_socio, nome_do_socio,
					    cnpj_cpf_do_socio, qualificacao_socio, data_entrada_sociedade,
					    pais, representante_legal, nome_representante, qualificacao_representante,
					    faixa_etaria, atualizado_em
					)
					SELECT
					    cnpj_basico, identificador_de_socio, nome_do_socio,
					    cnpj_cpf_do_socio, qualificacao_socio, data_entrada_sociedade,
					    pais, representante_legal, nome_representante, qualificacao_representante,
					    faixa_etaria, now()
					FROM %s;
				""".formatted(newTable, stage);
	}

	private static String insertSimples(String newTable, String stage) {
		return """
					INSERT INTO %s (
					    cnpj_basico,
					    opcao_pelo_simples,
					    data_opcao_pelo_simples,
					    data_exclusao_do_simples,
					    opcao_mei,
					    data_opcao_mei,
					    data_exclusao_mei,
					    atualizado_em
					)
					SELECT
					    cnpj_basico,
					    opcao_pelo_simples,
					    data_opcao_pelo_simples,
					    data_exclusao_do_simples,
					    opcao_pelo_mei AS opcao_mei,
					    data_opcao_pelo_mei AS data_opcao_mei,
					    data_exclusao_do_mei AS data_exclusao_mei,
					    now()
					FROM %s;
				""".formatted(newTable, stage);
	}

	private static String insertLookup(String newTable, String stage) {
		return """
					INSERT INTO %s (
					    codigo, descricao, atualizado_em
					)
					SELECT
					    codigo,
					    descricao,
					    now()
					FROM %s;
				""".formatted(newTable, stage);

	}

	// demais métodos...
}
