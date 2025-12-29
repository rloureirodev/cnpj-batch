package br.com.simplificarest.cnpjbatch.swap;

import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import br.com.simplificarest.cnpjbatch.batch.config.TableSwapConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Trunca as tabelas stage após swap.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StageCleanerService {

    private final JdbcTemplate jdbc;

    public void cleanStage() {
        for (Map.Entry<String, String> e : TableSwapConfig.TABLES.entrySet()) {
            String stage = e.getValue();
            log.info("CLEAN: truncando {}", stage);
            jdbc.execute("TRUNCATE TABLE " + stage);
        }
    }
}
