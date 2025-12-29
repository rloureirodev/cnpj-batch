package br.com.simplificarest.cnpjbatch.swap;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import br.com.simplificarest.cnpjbatch.batch.config.TableSwapConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class CreateIndexesService {

    private final JdbcTemplate jdbc;

    public void createAll() {
        for (String table : TableSwapConfig.TABLES.keySet()) {
            createIndexesFor(table + TableSwapConfig.NEW_POSTFIX);
        }
    }

    private void createIndexesFor(String newTable) {
        String sn = simple(newTable);
        log.info("INDEX: criando índices para {}", newTable);

        switch (sn) {
            case "empresa_new":
                concurrent("idx_" + sn + "_pk",
                        "CREATE INDEX CONCURRENTLY idx_" + sn + "_pk ON " + newTable + "(cnpj_basico)");
                break;

            case "estabelecimento_new":
                concurrent("idx_" + sn + "_pk",
                        "CREATE INDEX CONCURRENTLY idx_" + sn + "_pk ON " + newTable + "(cnpj_basico, cnpj_ordem, cnpj_dv)");
                break;

            case "socio_new":
                concurrent("idx_" + sn + "_pk",
                        "CREATE INDEX CONCURRENTLY idx_" + sn + "_pk ON " + newTable + "(cnpj_basico, identificador_de_socio, nome_do_socio)");
                break;

            case "simples_new":
                concurrent("idx_" + sn + "_pk",
                        "CREATE INDEX CONCURRENTLY idx_" + sn + "_pk ON " + newTable + "(cnpj_basico)");
                break;

            default:
                concurrent("idx_" + sn + "_codigo",
                        "CREATE INDEX CONCURRENTLY idx_" + sn + "_codigo ON " + newTable + "(codigo)");
        }
    }

    private void concurrent(String name, String sql) {
        try {
            jdbc.execute(sql);
        } catch (Exception e) {
            log.warn("Erro criando índice {}: {}", name, e.getMessage());
        }
    }

    private String simple(String q) {
        return q.contains(".") ? q.substring(q.indexOf('.') + 1) : q;
    }
}
