package br.com.simplificarest.cnpjbatch.batch.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import br.com.simplificarest.cnpjdomain.enm.BatchStage;
import br.com.simplificarest.cnpjdomain.enm.CnpjFileType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class StagePreparationService {

    private final JdbcTemplate jdbc;
    private final BatchProcessLogService logService;

    public void prepararStages(String anoMes) {

        log.info("==========================================================");
        log.info(" INICIANDO PREPARAÇÃO DAS TABELAS STAGE");
        log.info("==========================================================");

        for (CnpjFileType tipo : CnpjFileType.values()) {

            String tabela = tipo.getStageTable();
            long logId = 0;

            try {
                log.info("----------------------------------------------------------");
                log.info(">>> Preparando stage {}", tabela);

                logId = logService.start(
                        BatchStage.PREPARACAO,
                        "cnpjJob",
                        anoMes,
                        null,
                        null,
                        tabela,
                        ""
                );

                prepararTabela(tabela);

                logService.ok(logId);

                log.info("<<< Stage {} finalizada com sucesso", tabela);
                log.info("----------------------------------------------------------");

            } catch (Exception e) {
                logService.error(logId, e);
                log.error("ERRO ao preparar stage {}: {}", tabela, e.getMessage());
                throw e;
            }
        }

        log.info("==========================================================");
        log.info(" TODAS AS STAGES FORAM PREPARADAS");
        log.info("==========================================================");
    }


    private void prepararTabela(String tabela) {

        // 1. contar linhas
        log.info("[{}] Obtendo contagem de linhas...", tabela);
        Long count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM cnpj." + tabela,
                Long.class
        );
        log.info("[{}] Total de linhas: {}", tabela, count);

        // 2. criar tabela temporária com ID
        log.info("[{}] Criando tabela temporária com ID via CTAS...", tabela);
        long inicio = System.currentTimeMillis();

        String sql = """
        	    CREATE TABLE cnpj.%s_tmp AS
        	    SELECT row_number() OVER () AS id, *
        	    FROM cnpj.%s
        	""".formatted(tabela, tabela);

        	jdbc.execute(sql);
        	
        long fim = System.currentTimeMillis();
        log.info("[{}] CTAS concluído em {} ms", tabela, (fim - inicio));

        // 3. substituir tabela original
        log.info("[{}] Removendo tabela original...", tabela);
        jdbc.execute("DROP TABLE cnpj." + tabela);

        log.info("[{}] Renomeando tabela _tmp para tabela original...", tabela);
        jdbc.execute("ALTER TABLE cnpj." + tabela + "_tmp RENAME TO " + tabela);

        // 4. criar índices
        log.info("[{}] Criando índice IDX_{}_ID...", tabela, tabela);
        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_" + tabela + "_id ON cnpj." + tabela + "(id)");

        log.info("[{}] Índices criados", tabela);
    }
}
