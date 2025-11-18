package br.com.simplificarest.cnpjbatch.config;

import java.time.LocalDateTime;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ImportJobListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("=== INÍCIO DO JOB {} às {} ===",
                jobExecution.getJobInstance().getJobName(),
                LocalDateTime.now());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        log.info("=== FIM DO JOB {} às {} | STATUS: {} ===",
                jobExecution.getJobInstance().getJobName(),
                LocalDateTime.now(),
                jobExecution.getStatus());
    }
}