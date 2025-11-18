package br.com.simplificarest.cnpjbatch.enm;

import java.util.Arrays;
import java.util.Optional;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum CnpjFileType {

	 CNAE("CNAECSV", "stg_cnae", "Cnaes"),
	 MOTIVO("MOTICSV", "stg_motivo", "Motivos"),
	 MUNICIPIO("MUNICCSV", "stg_municipio", "Municipios"),
	 NATUREZA("NATJUCSV", "stg_natureza_juridica", "Naturezas"),
	 PAIS("PAISCSV", "stg_pais", "Paises"),
	 QUALIFICACAO("QUALSCSV", "stg_qualificacao", "Qualificacoes"),
	 EMPRESA("EMPRECSV", "stg_empresa", "Empresas"),
	 ESTABELECIMENTO("ESTABELE", "stg_estabelecimento", "Estabelecimentos"),
	 SOCIO("SOCIOCSV", "stg_socio", "Socios"),
	 SIMPLES("SIMPLES.CSV", "stg_simples", "Simples");

	private final String identifier; // CSV interno
	private final String stageTable; // tabela stage
	private final String zipPrefix; // prefixo do ZIP

	public static Optional<CnpjFileType> fromFileName(String name) {
		return Arrays.stream(values()).filter(t -> name.startsWith(t.zipPrefix)).findFirst();
	}
}
