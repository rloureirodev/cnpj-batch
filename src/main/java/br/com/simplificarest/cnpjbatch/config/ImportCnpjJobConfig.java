package br.com.simplificarest.cnpjbatch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import br.com.simplificarest.cnpjbatch.tasklet.*;
import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
public class ImportCnpjJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager txManager;

//    private final MergeEmpresaTasklet mergeEmpresaTasklet;
//    private final MergeEstabelecimentoTasklet mergeEstabelecimentoTasklet;
//    private final MergeSocioTasklet mergeSocioTasklet;
//    private final MergeSimplesTasklet mergeSimplesTasklet;
    
    private final DownloadFilesTasklet downloadFilesTasklet;
    
    private final ImportCnaeTasklet importCnaeTasklet;
    
    private final ImportMotivoTasklet importMotivoTasklet;
    private final ImportMunicipioTasklet importMunicipioTasklet;
    private final ImportNaturezaTasklet importNaturezaTasklet;
    private final ImportPaisTasklet importPaisTasklet;
    private final ImportQualificacaoTasklet importQualificacaoTasklet;

    private final ImportEmpresaTasklet importEmpresaTasklet;
    private final ImportEstabelecimentoTasklet importEstabelecimentoTasklet;
    private final ImportSocioTasklet importSocioTasklet;
    private final ImportSimplesTasklet importSimplesTasklet;

//    @Bean
//    Job importCnpjJob() {
//        return new JobBuilder("importCnpjJob", jobRepository)
//            .start(downloadFilesStep())
//            .next(importCnaeStep())
//            .next(importMotivoStep())
//            .next(importMunicipioStep())
//            .next(importNaturezaStep())
//            .next(importPaisStep())
//            .next(importQualificacaoStep())
//            .next(importEmpresaStep())
//            .next(importEstabelecimentostep())
//            .next(importSocioStep())
//            .next(importSimplesStep())
//            .build();
//    }
    
    @Bean
    Job importCnpjJob(ImportJobListener listener) {
        return new JobBuilder("importCnpjJob", jobRepository)
        	.listener(listener)
            //.start(importCnaeStep())
            //.next(importMotivoStep())
            //.next(importMunicipioStep())
            //.next(importNaturezaStep())
            //.next(importPaisStep())
            //.next(importQualificacaoStep())
            //.next(importEmpresaStep())
            .start(importEstabelecimentostep())
            .next(importSocioStep())
            .next(importSimplesStep())
            .build();
    }

    @Bean
    Step downloadFilesStep() {
        return new StepBuilder("downloadFilesTasklet", jobRepository)
                .tasklet(downloadFilesTasklet, txManager)
                .build();
    }

    @Bean
    Step importCnaeStep() {
        return new StepBuilder("importCnaeTasklet", jobRepository)
                .tasklet(importCnaeTasklet, txManager)
                .build();
    }

    @Bean
    Step importMotivoStep() {
        return new StepBuilder("importMotivoTasklet", jobRepository)
                .tasklet(importMotivoTasklet, txManager)
                .build();
    }

    @Bean
    Step importMunicipioStep() {
        return new StepBuilder("importMunicipioTasklet", jobRepository)
                .tasklet(importMunicipioTasklet, txManager)
                .build();
    }

    @Bean
    Step importNaturezaStep() {
        return new StepBuilder("importNaturezaTasklet", jobRepository)
                .tasklet(importNaturezaTasklet, txManager)
                .build();
    }

    @Bean
    Step importPaisStep() {
        return new StepBuilder("importPaisTasklet", jobRepository)
                .tasklet(importPaisTasklet, txManager)
                .build();
    }

    @Bean
    Step importQualificacaoStep() {
        return new StepBuilder("importQualificacaoTasklet", jobRepository)
                .tasklet(importQualificacaoTasklet, txManager)
                .build();
    }

    @Bean
    Step importEmpresaStep() {
        return new StepBuilder("importEmpresaTasklet", jobRepository)
                .tasklet(importEmpresaTasklet, txManager)
                .build();
    }

    @Bean
    Step importEstabelecimentostep() {
        return new StepBuilder("importEstabelecimentoTasklet", jobRepository)
                .tasklet(importEstabelecimentoTasklet, txManager)
                .build();
    }

    @Bean
    Step importSocioStep() {
        return new StepBuilder("importSocioTasklet", jobRepository)
                .tasklet(importSocioTasklet, txManager)
                .build();
    }

    @Bean
    Step importSimplesStep() {
        return new StepBuilder("importSimplesTasklet", jobRepository)
                .tasklet(importSimplesTasklet, txManager)
                .build();
    }
    
    
    
    
//    @Bean
//    public Step importCsvStep() {
//        return new StepBuilder("importCsvStep", jobRepository)
//                .tasklet(importStageTasklet, txManager)
//                .build();
//    }
//
//
//    @Bean
//    public Step mergeEmpresaStep() {
//        return new StepBuilder("mergeEmpresaStep", jobRepository)
//                .tasklet(mergeEmpresaTasklet, txManager)
//                .build();
//    }
//
//    @Bean
//    public Step mergeEstabelecimentoStep() {
//        return new StepBuilder("mergeEstabelecimentoStep", jobRepository)
//                .tasklet(mergeEstabelecimentoTasklet, txManager)
//                .build();
//    }
//
//    @Bean
//    public Step mergeSocioStep() {
//        return new StepBuilder("mergeSocioStep", jobRepository)
//                .tasklet(mergeSocioTasklet, txManager)
//                .build();
//    }
//
//    @Bean
//    public Step mergeSimplesStep() {
//        return new StepBuilder("mergeSimplesStep", jobRepository)
//                .tasklet(mergeSimplesTasklet, txManager)
//                .build();
//    }
}
