package br.com.simplificarest.cnpjbatch.batch.service;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import br.com.simplificarest.cnpjdomain.enm.BatchStage;
import br.com.simplificarest.cnpjdomain.entities.BatchProcessControl;
import br.com.simplificarest.cnpjdomain.repository.BatchProcessControlRepository;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class BatchProcessLogService {

    private final BatchProcessControlRepository repo;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public long start(BatchStage etapa, String job, String anoMes,
                      String zip, String csv, String tabela, String hash) {

        BatchProcessControl log = new BatchProcessControl();
        log.setJobName(job);
        log.setAnoMes(anoMes);
        log.setEtapa(etapa.name());
        log.setStatus("RUNNING");
        log.setZipFile(zip != null ? zip : "");
        log.setCsvFile(csv != null ? csv : "");
        log.setTabelaStage(tabela != null ? tabela : "");
        log.setHash(hash != null ? hash : "");
        log.setStartTime(LocalDateTime.now());

        repo.saveAndFlush(log);
        return log.getId();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void ok(long id) {
        repo.findById(id).ifPresent(x -> {
            x.setStatus("OK");
            x.setEndTime(LocalDateTime.now());
            repo.saveAndFlush(x);
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void error(long id, Exception e) {
        repo.findById(id).ifPresent(x -> {
            x.setStatus("ERROR");
            x.setEndTime(LocalDateTime.now());
            x.setErrorMessage(e.getMessage());
            x.setErrorStack(stack(e));
            repo.saveAndFlush(x);
        });
    }

    private String stack(Exception e) {
        return Arrays.stream(e.getStackTrace())
                     .map(StackTraceElement::toString)
                     .collect(Collectors.joining("\n"));
    }
    
    public boolean jaProcessado(String job, String anoMes, String tabela, String hash) {
        Long count = repo.countByJobNameAndAnoMesAndTabelaStageAndHashAndStatus(
                job, anoMes, tabela, hash, "OK"
        );
        return count != null && count > 0;
    }
    
    
    public boolean isBuildNewFinished(String newTable, String anoMes) {
        Long count = repo.countByEtapaAndTabelaStageAndAnoMesAndStatus(
                BatchStage.BUILD_NEW.name(),
                newTable,
                anoMes,
                "OK"
        );
        return count != null && count > 0;
    }
    
    
    
}