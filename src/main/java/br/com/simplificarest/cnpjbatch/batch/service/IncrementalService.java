package br.com.simplificarest.cnpjbatch.batch.service;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class IncrementalService {

    private final JdbcTemplate jdbc;

    // NOVOS
    public List<Map<String,Object>> novosEmpresa() {
        return jdbc.queryForList("""
            SELECT s.* 
            FROM stage_empresa s
            LEFT JOIN empresa e ON e.cnpj_basico = s.cnpj_basico
            WHERE e.cnpj_basico IS NULL
        """);
    }

    // ALTERADOS
    public List<Map<String,Object>> alteradosEmpresa() {
        return jdbc.queryForList("""
            SELECT s.*
            FROM stage_empresa s
            JOIN empresa e ON e.cnpj_basico = s.cnpj_basico
            WHERE s.hash IS DISTINCT FROM e.hash
        """);
    }

    // INSERIR NOVOS
    public void inserirNovosEmpresa(List<Map<String,Object>> lista) {
        if (lista == null || lista.isEmpty()) return;

        String sql = """
            INSERT INTO empresa (
                cnpj_basico, razao_social, natureza_juridica, 
                qualificacao_responsavel, capital_social, porte, 
                ente_federativo_responsavel, hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """;

        jdbc.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {

                Map<String,Object> r = lista.get(i);

                ps.setString(1, (String) r.get("cnpj_basico"));
                ps.setString(2, (String) r.get("razao_social"));
                ps.setObject(3, r.get("natureza_juridica"));
                ps.setObject(4, r.get("qualificacao_responsavel"));

                if (r.get("capital_social") == null)
                    ps.setBigDecimal(5, BigDecimal.ZERO);
                else
                    ps.setBigDecimal(5, new BigDecimal(r.get("capital_social").toString()));

                ps.setObject(6, r.get("porte"));
                ps.setString(7, (String) r.get("ente_federativo_responsavel"));
                ps.setString(8, (String) r.get("hash"));
            }

            @Override
            public int getBatchSize() {
                return lista.size();
            }
        });
    }

    // ATUALIZADOS
    public void atualizarAlteradosEmpresa(List<Map<String,Object>> lista) {
        if (lista == null || lista.isEmpty()) return;

        String sql = """
            UPDATE empresa
            SET razao_social = ?, 
                natureza_juridica = ?, 
                qualificacao_responsavel = ?, 
                capital_social = ?, 
                porte = ?, 
                ente_federativo_responsavel = ?, 
                hash = ?, 
                atualizado_em = now()
            WHERE cnpj_basico = ?
        """;

        jdbc.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {

                Map<String,Object> r = lista.get(i);

                ps.setString(1, (String) r.get("razao_social"));
                ps.setObject(2, r.get("natureza_juridica"));
                ps.setObject(3, r.get("qualificacao_responsavel"));

                if (r.get("capital_social") == null)
                    ps.setBigDecimal(4, BigDecimal.ZERO);
                else
                    ps.setBigDecimal(4, new BigDecimal(r.get("capital_social").toString()));

                ps.setObject(5, r.get("porte"));
                ps.setString(6, (String) r.get("ente_federativo_responsavel"));
                ps.setString(7, (String) r.get("hash"));
                ps.setString(8, (String) r.get("cnpj_basico"));
            }

            @Override
            public int getBatchSize() {
                return lista.size();
            }
        });
    }
}
