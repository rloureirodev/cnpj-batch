package br.com.simplificarest.cnpjbatch.batch.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import br.com.simplificarest.cnpjbatch.batch.service.ArquivoService;
import br.com.simplificarest.cnpjbatch.batch.service.FileProcessorService;
import br.com.simplificarest.cnpjbatch.batch.service.StagePreparationService;
import br.com.simplificarest.cnpjbatch.service.merge.MergeOrchestrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class CnpjBatchJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager tx;
    private final ArquivoService arquivoService;
    private final FileProcessorService processor; // NOVO
    private final MergeOrchestrationService mergeMasterService;
    private final StagePreparationService stagePrepService;

    private static final Path BASE = Paths.get("G:", "temp", "cnpj");

    @Bean
    public Job cnpjJob() {
        return new JobBuilder("cnpjJob", jobRepository)
                //.start(processarArquivosStep())
        		.start(prepareStagesStep())
                .next(mergeStep())
                .next(cleanupStep())
                .build();
    }
    
    

    @Bean
    public Step processarArquivosStep() {
        return new StepBuilder("processarArquivosStep", jobRepository)
                .tasklet((contribution, context) -> {

                    var params = context.getStepContext().getJobParameters();
                    String anoMes = params.get("anoMes") != null
                            ? params.get("anoMes").toString()
                            : LocalDate.now().minusMonths(1)
                                    .format(DateTimeFormatter.ofPattern("yyyy-MM"));

                    List<String> arquivos = arquivoService
                            .listarArquivosDoMes(anoMes)
                            .stream()
                            .sorted()
                            .toList();

                    Path zipDir = BASE.resolve(anoMes).resolve("zip");
                    Path csvDir = BASE.resolve(anoMes).resolve("csv");

                    Files.createDirectories(zipDir);
                    Files.createDirectories(csvDir);

                    for (String url : arquivos) {
                        processor.processarUmArquivo(url, anoMes, zipDir, csvDir);
                    }

                    return RepeatStatus.FINISHED;
                }, tx)
                .build();
    }
    
    @Bean
    public Step prepareStagesStep() {
        return new StepBuilder("prepareStagesStep", jobRepository)
                .tasklet((c, t) -> {

                    stagePrepService.prepararStages(); // CHAVE

                    return RepeatStatus.FINISHED;
                }, tx)
                .build();
    }
    
    @Bean
    public Step mergeStep() {
        return new StepBuilder("mergeStep", jobRepository)
                .tasklet((c, t) -> {

                    var params = t.getStepContext().getJobParameters();
                    String anoMes = params.get("anoMes").toString();

                    mergeMasterService.executarTodos(anoMes);

                    return RepeatStatus.FINISHED;
                }, tx)
                .build();
    }



    @Bean
    public Step cleanupStep() {
        return new StepBuilder("cleanupStep", jobRepository)
                .tasklet((c, t) -> RepeatStatus.FINISHED, tx)
                .build();
    }
}
