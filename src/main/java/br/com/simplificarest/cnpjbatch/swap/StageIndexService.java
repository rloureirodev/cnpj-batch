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
public class StageIndexService {

    private final JdbcTemplate jdbc;

    /**
     * Cria índices que ajudam o INSERT SELECT mas não prejudicam o COPY.
     * Usamos apenas índices simples — nada de UNIQUE.
     */
    public void createStageIndexes() {

        for (Map.Entry<String, String> e : TableSwapConfig.TABLES.entrySet()) {

            String stage = e.getValue();
            String sn = simple(stage);

            log.info("STAGE INDEX: criando índices para {}", stage);

            switch (stage) {

                case "cnpj.stage_empresa":
                    jdbc.execute("CREATE INDEX IF NOT EXISTS idx_" + sn + "_basico ON " + stage + " (cnpj_basico)");
                    break;

                case "cnpj.stage_estabelecimento":
                    jdbc.execute("""
                        CREATE INDEX IF NOT EXISTS idx_%s_pk 
                        ON %s (cnpj_basico, cnpj_ordem, cnpj_dv)
                    """.formatted(sn, stage));
                    break;

                case "cnpj.stage_socios":
                    jdbc.execute("""
                        CREATE INDEX IF NOT EXISTS idx_%s_basico 
                        ON %s (cnpj_basico)
                    """.formatted(sn, stage));
                    break;

                case "cnpj.stage_simples":
                    jdbc.execute("""
                        CREATE INDEX IF NOT EXISTS idx_%s_basico 
                        ON %s (cnpj_basico)
                    """.formatted(sn, stage));
                    break;

                // tabelas lookup
                default:
                    jdbc.execute("CREATE INDEX IF NOT EXISTS idx_" + sn + "_codigo ON " + stage + " (codigo)");
            }

            log.info("STAGE INDEX: índices criados para {}", stage);
        }
    }

    private String simple(String q) {
        return q.contains(".") ? q.substring(q.indexOf('.') + 1) : q;
    }
}
