package br.com.simplificarest.cnpjbatch.config;

import java.util.Optional;

import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class BatchExecutorConfig {

	@Bean("cnpjExecutor")
    public ThreadPoolTaskExecutor cnpjExecutor() {
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();

        exec.setCorePoolSize(3); // 10 núcleos físicos
        exec.setMaxPoolSize(13);  // 12 lógicos
        exec.setQueueCapacity(10);

        // Prefixo será substituído dinamicamente via TaskDecorator
        exec.setThreadNamePrefix("cnpj-");

        exec.setTaskDecorator(runnable -> () -> {
            // Pega o nome da Step em execução
            String stepName = Optional.ofNullable(
                StepSynchronizationManager.getContext()
            ).map(c -> c.getStepExecution().getStepName())
             .orElse("unknown");

            String originalName = Thread.currentThread().getName();
            Thread.currentThread().setName(stepName + "-" + originalName);

            try {
                runnable.run();
            } finally {
                // Restaura nome original
                Thread.currentThread().setName(originalName);
            }
        });

        exec.initialize();
        return exec;
    }
}