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
public class MergeEmpresaTasklet implements Tasklet {

    private final EntityManager entityManager;

    @Override
    @Transactional
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        entityManager.createNativeQuery("""
            INSERT INTO empresa (cnpj_basico, razao_social, natureza_juridica, qualificacao, capital_social, porte)
            SELECT s.cnpj_basico, s.razao_social, s.natureza_juridica, s.qualificacao, s.capital_social, s.porte
            FROM stage_empresa s
            ON CONFLICT (cnpj_basico) DO UPDATE
            SET   razao_social      = EXCLUDED.razao_social,
                  natureza_juridica = EXCLUDED.natureza_juridica,
                  qualificacao      = EXCLUDED.qualificacao,
                  capital_social    = EXCLUDED.capital_social,
                  porte             = EXCLUDED.porte
        """).executeUpdate();

        return RepeatStatus.FINISHED;
    }
}