package br.com.simplificarest.cnpjbatch.batch.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import br.com.simplificarest.cnpjdomain.enm.CnpjFileType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class StagePreparationService {

    private final JdbcTemplate jdbc;

    public void prepararStages() {

        for (CnpjFileType tipo : CnpjFileType.values()) {

            String tabela = tipo.getStageTable();

            log.info("Preparando stage {}", tabela);

            adicionarIdSeNaoExistir(tabela);
            criarIndiceId(tabela);
        }
    }

    private void adicionarIdSeNaoExistir(String tabela) {
        String checkIdExists = """
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_schema = 'cnpj' 
              AND table_name = ? 
              AND column_name = 'id'
            """;

        Integer existe = jdbc.queryForObject(checkIdExists, Integer.class, tabela);

        if (existe != null && existe == 0) {
            log.info("Adicionando coluna id BIGSERIAL em {}", tabela);

            jdbc.execute("ALTER TABLE cnpj." + tabela + " ADD COLUMN id BIGSERIAL");
        }
    }

    private void criarIndiceId(String tabela) {
        String indexName = "idx_" + tabela + "_id";

        String existsSql = """
            SELECT COUNT(*) 
            FROM pg_indexes 
            WHERE schemaname = 'cnpj' 
              AND tablename = ? 
              AND indexname = ?
            """;

        Integer exists = jdbc.queryForObject(existsSql, Integer.class, tabela, indexName);

        if (exists != null && exists == 0) {
            log.info("Criando índice {} para {}", indexName, tabela);
            jdbc.execute("CREATE INDEX " + indexName + " ON cnpj." + tabela + "(id)");
        }
    }
}
