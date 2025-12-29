package br.com.simplificarest.cnpjbatch.swap;

import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import br.com.simplificarest.cnpjbatch.batch.config.TableSwapConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Faz o swap atomico:
 * 1) adquire advisory lock
 * 2) rename tabela -> tabela_old
 * 3) rename tabela_new -> tabela
 * 4) drop tabela_old (opcional ou atrasado)
 *
 * Rename é atomic e rápido (metadados).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SwapTablesService {

    private final JdbcTemplate jdbc;

    @Transactional
    public void swapAll() {
        log.info("SWAP: acquiring advisory lock {}", TableSwapConfig.ADVISORY_LOCK_KEY);
        jdbc.execute("SELECT pg_advisory_lock(" + TableSwapConfig.ADVISORY_LOCK_KEY + ")");
        try {
            for (Map.Entry<String, String> e : TableSwapConfig.TABLES.entrySet()) {
                String finalQualified = e.getKey();                 // schema.name
                String newQualified = finalQualified + TableSwapConfig.NEW_POSTFIX;
                String oldQualified = finalQualified + TableSwapConfig.OLD_POSTFIX;

                String schema = schemaOf(finalQualified);
                String finalName = nameOf(finalQualified);
                String newName = nameOf(newQualified);
                String oldName = nameOf(oldQualified);

                log.info("SWAP: {} <-- {}", finalQualified, newQualified);

                // rename final -> old (if exists)
                jdbc.execute("ALTER TABLE IF EXISTS " + finalQualified + " RENAME TO " + oldName);

                // rename new -> final
                // newQualified may not exist (error tolerated)
                jdbc.execute("ALTER TABLE IF EXISTS " + schema + "." + newName + " RENAME TO " + finalName);

                log.info("SWAP: swap realizado para {}", finalQualified);
            }
        } finally {
            log.info("SWAP: releasing advisory lock {}", TableSwapConfig.ADVISORY_LOCK_KEY);
            jdbc.execute("SELECT pg_advisory_unlock(" + TableSwapConfig.ADVISORY_LOCK_KEY + ")");
        }
    }

    @Transactional
    public void dropOlds() {
        for (String finalQualified : TableSwapConfig.TABLES.keySet()) {
            String schema = schemaOf(finalQualified);
            String oldName = nameOf(finalQualified + TableSwapConfig.OLD_POSTFIX);
            String qualified = schema + "." + oldName;
            log.info("DROP: attempting to drop {}", qualified);
            jdbc.execute("DROP TABLE IF EXISTS " + qualified);
        }
    }

    private String schemaOf(String qualified) {
        int i = qualified.indexOf('.');
        return i > -1 ? qualified.substring(0, i) : "public";
    }

    private String nameOf(String qualified) {
        int i = qualified.indexOf('.');
        return i > -1 ? qualified.substring(i + 1) : qualified;
    }
}
