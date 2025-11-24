package br.com.simplificarest.cnpjbatch.service.merge;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import br.com.simplificarest.cnpjbatch.batch.service.BatchProcessLogService;
import br.com.simplificarest.cnpjdomain.enm.BatchStage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class MergeExecutorService {
	
	private final JdbcTemplate jdbc;
    private final BatchProcessLogService logService;

    public long countStage(String table) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM cnpj." + table, Long.class);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void executarBloco(String procedure, long inicio, long fim, String anoMes) {

        long logId = logService.start(
                BatchStage.MERGE,
                "cnpjJob",
                anoMes,
                null,
                null,
                procedure + "(" + inicio + "," + fim + ")",
                null
        );

        try {
            log.info("Executando {} com bloco {} - {}", procedure, inicio, fim);

            jdbc.update("CALL cnpj." + procedure + "(?, ?)", inicio, fim);

            logService.ok(logId);
            log.info("Concluído {} bloco {} - {}", procedure, inicio, fim);

        } catch (Exception e) {
            logService.error(logId, e);
            log.error("Erro ao executar {} bloco {} - {}: {}", procedure, inicio, fim, e.getMessage());
            throw e;
        }
    }

//    private final JdbcTemplate jdbc;
//    private final BatchProcessLogService logService;
//
//    @Transactional(propagation = Propagation.REQUIRES_NEW)
//    public void executar(String procedure, String anoMes) {
//        long logId = logService.start(
//                BatchStage.MERGE, "cnpjJob", anoMes, null, null, procedure, null
//        );
//
//        try {
//        	log.info("Executando merge com a procedure {}" ,procedure);
//            jdbc.execute("CALL " + procedure + "()");
//            logService.ok(logId);
//            log.info("Procedure executada {}" ,procedure);
//        } catch (Exception e) {
//            logService.error(logId, e);
//            log.error("Erro ao executar {}: {}", procedure, e.getMessage());
//            throw e;
//        }
//    }
}