package br.com.simplificarest.cnpjbatch.controller;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/batch")
@RequiredArgsConstructor
public class BatchController {

    private final JobLauncher jobLauncher;
    private final JobExplorer jobExplorer;
    private final Job cnpjJob;

    @GetMapping("/run")
    public String run(@RequestParam(required = false) String anoMes) {

        // impede execução paralela
        if (isJobRunning("cnpjJob")) {
            return "Já existe uma execução do batch em andamento.";
        }

        try {
            JobParameters params = new JobParametersBuilder()
                    .addString("anoMes", anoMes != null ? anoMes : "") // parâmetro opcional
                    .addLong("startAt", System.currentTimeMillis())
                    .toJobParameters();

            jobLauncher.run(cnpjJob, params);

            return "Batch iniciado";
        } catch (Exception e) {
            e.printStackTrace();
            return "Erro: " + e.getMessage();
        }
    }

    private boolean isJobRunning(String jobName) {
        return jobExplorer.findRunningJobExecutions(jobName).size() > 0;
    }
}