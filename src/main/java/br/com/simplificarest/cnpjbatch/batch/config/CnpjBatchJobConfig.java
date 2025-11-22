package br.com.simplificarest.cnpjbatch.batch.config;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
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
import br.com.simplificarest.cnpjbatch.batch.service.BatchProcessLogService;
import br.com.simplificarest.cnpjbatch.batch.service.CopyService;
import br.com.simplificarest.cnpjbatch.batch.service.DownloadService;
import br.com.simplificarest.cnpjbatch.batch.service.FileProcessorService;
import br.com.simplificarest.cnpjbatch.batch.service.StageHashService;
import br.com.simplificarest.cnpjbatch.batch.service.UnzipService;
import br.com.simplificarest.cnpjdomain.enm.BatchStage;
import br.com.simplificarest.cnpjdomain.enm.CnpjFileType;
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

    private static final Path BASE = Paths.get("C:", "temp", "cnpj");

    @Bean
    public Job cnpjJob() {
        return new JobBuilder("cnpjJob", jobRepository)
                .start(processarArquivosStep())
                .next(hashStep())
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
    public Step hashStep() {
        return new StepBuilder("hashStep", jobRepository)
                .tasklet((contribution, ctx) -> {
                    processor.aplicarHashes();
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
