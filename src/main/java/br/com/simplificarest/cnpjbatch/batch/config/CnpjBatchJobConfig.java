package br.com.simplificarest.cnpjbatch.batch.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Consumer;

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
import br.com.simplificarest.cnpjbatch.swap.SwapOrchestrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class CnpjBatchJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager tx;
    private final ArquivoService arquivoService;
    private final FileProcessorService processor;
    private final SwapOrchestrationService swap;

    private static final Path BASE = Paths.get("D:", "CNPJ", "temp", "cnpj");
    
    

    @Bean
    Job cnpjJob() {
        return new JobBuilder("cnpjJob", jobRepository)
                //.start(processarArquivosStep())
                .start(buildStep())
                .next(indexFinalStep())
                .next(swapTablesStep())
                .next(cleanStageStep())
                .next(dropOldsStep())
                .next(cleanupStep())
                .build();
    }

    @Bean
    Step processarArquivosStep() {
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


    @Bean Step buildStep() {
        return step("buildStep", anoMes -> swap.buildNewTables(anoMes));
    }

    @Bean Step indexFinalStep() {
        return step("indexFinalStep", anoMes -> swap.indexFinal(anoMes));
    }

    @Bean Step swapTablesStep() {
        return step("swapTablesStep", anoMes -> swap.swap(anoMes));
    }

    @Bean Step cleanStageStep() {
        return step("cleanStageStep", anoMes -> swap.cleanStage(anoMes));
    }

    @Bean Step dropOldsStep() {
        return step("dropOldsStep", anoMes -> swap.dropOlds(anoMes));
    }


    @Bean
    Step cleanupStep() {
        return new StepBuilder("cleanupStep", jobRepository)
                .tasklet((c, t) -> RepeatStatus.FINISHED, tx)
                .build();
    }

//    private Step step(String name, Runnable fn) {
//        return new StepBuilder(name, jobRepository)
//                .tasklet((c, t) -> {
//                    fn.run();
//                    return RepeatStatus.FINISHED;
//                }, tx)
//                .build();
//    }
    
    private Step step(String name, Consumer<String> fn) {
        return new StepBuilder(name, jobRepository)
                .tasklet((c, t) -> {

                    var params = t.getStepContext().getJobParameters();
                    String anoMes = params.get("anoMes") != null
                            ? params.get("anoMes").toString()
                            : LocalDate.now().minusMonths(1)
                                    .format(DateTimeFormatter.ofPattern("yyyy-MM"));

                    fn.accept(anoMes);
                    return RepeatStatus.FINISHED;
                }, tx)
                .build();
    }
}