package br.com.simplificarest.cnpjbatch.swap;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import br.com.simplificarest.cnpjbatch.batch.service.BatchProcessLogService;
import br.com.simplificarest.cnpjdomain.enm.BatchStage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class SwapOrchestrationService {

    private final BuildNewTablesService buildService;
    private final FinalIndexService finalIndexService;
    private final SwapTablesService swapService;
    private final StageCleanerService cleaner;
    private final BatchProcessLogService logService;

    private static final String JOB = "cnpjJob"; // pode vir de parâmetro


    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void buildNewTables(String anoMes) {
        long id = logService.start(BatchStage.BUILD_NEW, JOB, anoMes, "", "", "", "");
        try {
            buildService.buildAll(anoMes);
            logService.ok(id);
        } catch (Exception e) {
            logService.error(id, e);
            throw e;
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void indexFinal(String anoMes) {
        long id = logService.start(BatchStage.FINAL_INDEX, JOB, anoMes, "", "", "", "");
        try {
            finalIndexService.createFinalIndexes();
            logService.ok(id);
        } catch (Exception e) {
            logService.error(id, e);
            throw e;
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void swap(String anoMes) {
        long id = logService.start(BatchStage.SWAP, JOB, anoMes, "", "", "", "");
        try {
            swapService.swapAll();
            logService.ok(id);
        } catch (Exception e) {
            logService.error(id, e);
            throw e;
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void cleanStage(String anoMes) {
        long id = logService.start(BatchStage.CLEANUP_STAGE, JOB, anoMes, "", "", "", "");
        try {
            cleaner.cleanStage();
            logService.ok(id);
        } catch (Exception e) {
            logService.error(id, e);
            throw e;
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void dropOlds(String anoMes) {
        long id = logService.start(BatchStage.DROP_OLD, JOB, anoMes, "", "", "", "");
        try {
            swapService.dropOlds();
            logService.ok(id);
        } catch (Exception e) {
            logService.error(id, e);
            throw e;
        }
    }
}