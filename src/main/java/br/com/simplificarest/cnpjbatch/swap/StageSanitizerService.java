package br.com.simplificarest.cnpjbatch.swap;

import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import br.com.simplificarest.cnpjbatch.batch.config.TableSwapConfig;

@Slf4j
@Service
@RequiredArgsConstructor
public class StageSanitizerService {

    private final JdbcTemplate jdbc;

    public void sanitizeAllStages() {
        for (Map.Entry<String, String> e : TableSwapConfig.TABLES.entrySet()) {
            sanitizeStage(e.getValue());
        }
    }

    private void sanitizeStage(String stage) {
        log.info("SANITIZE: {}", stage);

        String columnSql = buildColumnSanitize(stage);

        String sql = """
            UPDATE %s SET
            %s
        """.formatted(stage, columnSql);

        jdbc.execute(sql);
    }

    private String buildColumnSanitize(String stage) {
        var cols = jdbc.queryForList("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = ?
        """, String.class, stage.replace("cnpj.", ""));

        StringBuilder sb = new StringBuilder();

        for (String c : cols) {
            if (c.equalsIgnoreCase("id")) continue;

            sb.append("    ")
              .append(c)
              .append(" = trim(nullif(replace(replace(replace(")
              .append(c)
              .append(", '\\u0000',''), '\\u00A0',''), '\\u200B',''), '')),\n");
        }

        sb.setLength(sb.length() - 2);
        return sb.toString();
    }
}
