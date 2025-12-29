package br.com.simplificarest.cnpjbatch.swap;

import org.springframework.stereotype.Service;

import br.com.simplificarest.cnpjbatch.batch.config.TableSwapConfig;
import br.com.simplificarest.cnpjbatch.batch.service.BatchProcessLogService;
import br.com.simplificarest.cnpjdomain.enm.BatchStage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class BuildNewTablesService {

    private final BuildNewTablesWorker worker;
    private final BatchProcessLogService logService;

    public void buildAll(String anoMes) {

    	for (var e : TableSwapConfig.TABLES.entrySet()) {

    	    String finalTable = e.getKey();
    	    String stageTable = e.getValue();
    	    String newTable = finalTable + TableSwapConfig.NEW_POSTFIX;

    	    if (logService.isBuildNewFinished(newTable, anoMes)) {
    	        log.info("BUILD_NEW: pulando {}, já concluído antes", newTable);
    	        continue;
    	    }

    	    long logId = logService.start(
    	            BatchStage.BUILD_NEW,
    	            "cnpjJob",
    	            anoMes,
    	            null,
    	            null,
    	            newTable,
    	            null
    	    );

    	    try {
    	        worker.buildOne(finalTable, stageTable);
    	        logService.ok(logId);
    	    } catch (Exception ex) {
    	        logService.error(logId, ex);
    	        throw ex;
    	    }
    	}
    }
}

//    public void buildAll() {
//        for (var e : TableSwapConfig.TABLES.entrySet()) {
//            worker.buildOne(e.getKey(), e.getValue());
//        }
//    }

