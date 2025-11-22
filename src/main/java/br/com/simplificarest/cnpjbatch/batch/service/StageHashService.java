package br.com.simplificarest.cnpjbatch.batch.service;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class StageHashService {

    private final JdbcTemplate jdbc;

    // EMPRESAS
    public void hashEmpresa() {
        jdbc.update("""
            UPDATE stage_empresa SET hash = md5(
                coalesce(cnpj_basico,'') || '|' ||
                coalesce(razao_social,'') || '|' ||
                coalesce(natureza_juridica::text,'') || '|' ||
                coalesce(qualificacao_responsavel::text,'') || '|' ||
                coalesce(capital_social::text,'') || '|' ||
                coalesce(porte::text,'') || '|' ||
                coalesce(ente_federativo_responsavel,'')
            )
        """);
    }

    // ESTABELECIMENTO
    public void hashEstabelecimento() {
        jdbc.update("""
            UPDATE stage_estabelecimento SET hash = md5(
                coalesce(cnpj_basico,'') || '|' ||
                coalesce(cnpj_ordem,'') || '|' ||
                coalesce(cnpj_dv,'') || '|' ||
                coalesce(identificador_matriz_filial::text,'') || '|' ||
                coalesce(nome_fantasia,'') || '|' ||
                coalesce(situacao_cadastral::text,'') || '|' ||
                coalesce(cep,'') || '|' ||
                coalesce(uf,'') || '|' ||
                coalesce(municipio::text,'')
            )
        """);
    }

    // SOCIOS
    public void hashSocios() {
        jdbc.update("""
            UPDATE stage_socios SET hash = md5(
                coalesce(cnpj_basico,'') || '|' ||
                coalesce(identificador_de_socio::text,'') || '|' ||
                coalesce(nome_do_socio,'') || '|' ||
                coalesce(cnpj_cpf_do_socio,'') || '|' ||
                coalesce(qualificacao_socio::text,'')
            )
        """);
    }

    // SIMPLES
    public void hashSimples() {
        jdbc.update("""
            UPDATE stage_simples SET hash = md5(
                coalesce(cnpj_basico,'') || '|' ||
                coalesce(opcao_pelo_simples,'') || '|' ||
                coalesce(data_opcao_pelo_simples::text,'') || '|' ||
                coalesce(data_exclusao_do_simples::text,'')
            )
        """);
    }
}
