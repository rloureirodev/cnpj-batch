package br.com.simplificarest.cnpjbatch.batch.config;

import java.util.LinkedHashMap;
import java.util.Map;

public class TableSwapConfig {

    // key = tabela final qualificada (schema.name)
    // value = tabela stage qualificada
    public static final Map<String, String> TABLES = new LinkedHashMap<>();
    static {
        TABLES.put("cnpj.empresa", "cnpj.stage_empresa");
        TABLES.put("cnpj.estabelecimento", "cnpj.stage_estabelecimento");
        TABLES.put("cnpj.socio", "cnpj.stage_socios");
        TABLES.put("cnpj.simples", "cnpj.stage_simples");
        TABLES.put("cnpj.municipio", "cnpj.stage_municipios");
        TABLES.put("cnpj.cnae", "cnpj.stage_cnaes");
        TABLES.put("cnpj.natureza", "cnpj.stage_naturezas");
        TABLES.put("cnpj.motivo", "cnpj.stage_motivos");
        TABLES.put("cnpj.pais", "cnpj.stage_paises");
        TABLES.put("cnpj.qualificacao", "cnpj.stage_qualificacoes");
    }

    public static final String NEW_POSTFIX = "_new";
    public static final String OLD_POSTFIX = "_old";

    // advisory lock key any bigint
    public static final long ADVISORY_LOCK_KEY = 123456789L;
}