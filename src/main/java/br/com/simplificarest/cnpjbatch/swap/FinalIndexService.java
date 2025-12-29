package br.com.simplificarest.cnpjbatch.swap;

import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import br.com.simplificarest.cnpjbatch.batch.config.TableSwapConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class FinalIndexService {

    private final JdbcTemplate jdbc;

    /**
     * Cria índices nas tabelas NEW após o INSERT.
     * Aqui também evitamos UNIQUE. 
     */
    public void createFinalIndexes() {

        for (Map.Entry<String, String> e : TableSwapConfig.TABLES.entrySet()) {

            String finalTable = e.getKey();
            String newTable = finalTable + TableSwapConfig.NEW_POSTFIX;
            String sn = simple(newTable);

            log.info("FINAL INDEX: criando índices para {}", newTable);

            switch (finalTable) {

                case "cnpj.empresa":
                	 // PK já cria índice único
                    jdbc.execute("ALTER TABLE " + newTable + " ADD PRIMARY KEY (cnpj_basico)");
                    // índice secundário opcional (somente se API pesquisar por razão social)
                    jdbc.execute("CREATE INDEX IF NOT EXISTS idx_" + sn + "_razao ON " + newTable + " (razao_social)");
                    break;

                case "cnpj.estabelecimento":
                    jdbc.execute("""
                            ALTER TABLE %s
                            ADD PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
                        """.formatted(newTable));
                        // para consultas comuns
                        jdbc.execute("CREATE INDEX IF NOT EXISTS idx_" + sn + "_basico ON " + newTable + "(cnpj_basico)");
                    break;

                case "cnpj.socio":
                    jdbc.execute("CREATE INDEX IF NOT EXISTS idx_" + sn + "_cnpj ON " + newTable + " (cnpj_basico)");
                    jdbc.execute("CREATE INDEX IF NOT EXISTS idx_" + sn + "_doc ON " + newTable + " (cnpj_cpf_do_socio)");
                    break;

                case "cnpj.simples":
                    jdbc.execute("ALTER TABLE " + newTable + " ADD PRIMARY KEY (cnpj_basico)");
                    break;

                default:
                    jdbc.execute("ALTER TABLE " + newTable + " ADD PRIMARY KEY (codigo)");
            }

            log.info("FINAL INDEX: índices criados para {}", newTable);
        }
    }

    private String simple(String q) {
        return q.contains(".") ? q.substring(q.indexOf('.') + 1) : q;
    }
}
