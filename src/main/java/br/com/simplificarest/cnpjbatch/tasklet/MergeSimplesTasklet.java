package br.com.simplificarest.cnpjbatch.tasklet;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class MergeSimplesTasklet implements Tasklet {

    private final EntityManager entityManager;

    @Override
    @Transactional
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {

        entityManager.createNativeQuery("""
            INSERT INTO simples (
                cnpj_basico, opcao_simples, data_opcao_simples, data_exclusao_simples,
                opcao_mei, data_opcao_mei, data_exclusao_mei
            )
            SELECT
                cnpj_basico, opcao_simples, data_opcao_simples, data_exclusao_simples,
                opcao_mei, data_opcao_mei, data_exclusao_mei
            FROM stg_simples
            ON CONFLICT (cnpj_basico) DO UPDATE
                SET opcao_simples = EXCLUDED.opcao_simples,
                    data_opcao_simples = EXCLUDED.data_opcao_simples,
                    data_exclusao_simples = EXCLUDED.data_exclusao_simples,
                    opcao_mei = EXCLUDED.opcao_mei,
                    data_opcao_mei = EXCLUDED.data_opcao_mei,
                    data_exclusao_mei = EXCLUDED.data_exclusao_mei
        """).executeUpdate();

        return RepeatStatus.FINISHED;
    }
}
