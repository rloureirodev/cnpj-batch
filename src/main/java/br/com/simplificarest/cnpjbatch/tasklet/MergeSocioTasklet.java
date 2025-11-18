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
public class MergeSocioTasklet implements Tasklet {

    private final EntityManager entityManager;

    @Override
    @Transactional
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {

        entityManager.createNativeQuery("""
            INSERT INTO socio (
                empresa_cnpj_basico,
                identificador,
                nome,
                documento,
                qualificacao,
                data_entrada,
                pais,
                representante_documento,
                representante_nome,
                representante_qualificacao,
                faixa_etaria
            )
            SELECT
                s.cnpj_basico,
                s.identificador_socio,
                s.nome_socio,
                s.cpf_cnpj_socio,
                s.qualificacao_socio,
                s.data_entrada_sociedade,
                s.pais,
                s.cpf_representante_legal,
                s.nome_representante,
                s.qualificacao_representante,
                s.faixa_etaria
            FROM stg_socio s
        """).executeUpdate();

        return RepeatStatus.FINISHED;
    }
}
