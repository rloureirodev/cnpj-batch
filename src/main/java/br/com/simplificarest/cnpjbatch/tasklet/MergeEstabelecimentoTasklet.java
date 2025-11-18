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
public class MergeEstabelecimentoTasklet implements Tasklet {

    private final EntityManager entityManager;

    @Override
    @Transactional
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {

        entityManager.createNativeQuery("""
            INSERT INTO estabelecimento (
                cnpj,
                empresa_cnpj_basico,
                matriz,
                nome_fantasia,
                situacao_cadastral,
                data_situacao_cadastral,
                motivo_situacao,
                nome_cidade_exterior,
                pais,
                data_inicio_atividade,
                cnae_principal,
                cnae_secundaria,
                tipo_logradouro,
                logradouro,
                numero,
                complemento,
                bairro,
                cep,
                uf,
                municipio,
                telefone1,
                telefone2,
                fax,
                email,
                situacao_especial,
                data_situacao_especial
            )
            SELECT
                CONCAT(s.cnpj_basico, s.cnpj_ordem, s.cnpj_dv),
                s.cnpj_basico,
                CASE WHEN s.identificador_matriz_filial = '1' THEN TRUE ELSE FALSE END,
                s.nome_fantasia,
                s.situacao_cadastral,
                s.data_situacao_cadastral,
                s.motivo_situacao_cadastral,
                s.nome_cidade_exterior,
                s.pais,
                s.data_inicio_atividade,
                s.cnae_principal,
                s.cnae_secundaria,
                s.tipo_logradouro,
                s.logradouro,
                s.numero,
                s.complemento,
                s.bairro,
                s.cep,
                s.uf,
                s.municipio,
                CONCAT(s.ddd1, s.telefone1),
                CONCAT(s.ddd2, s.telefone2),
                CONCAT(s.dddFax, s.fax),
                s.email,
                s.situacao_especial,
                s.data_situacao_especial
            FROM stg_estabelecimento s
            ON CONFLICT (cnpj) DO UPDATE
                SET nome_fantasia = EXCLUDED.nome_fantasia
        """).executeUpdate();

        return RepeatStatus.FINISHED;
    }
}
