package br.com.simplificarest.cnpjbatch.swap;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import br.com.simplificarest.cnpjbatch.batch.config.TableSwapConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class BuildNewTablesWorker {

    private final JdbcTemplate jdbc;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void buildOne(String finalTable, String stageTable) {

        String newTable = finalTable + TableSwapConfig.NEW_POSTFIX;

        log.info("BUILD_ONE: drop {}", newTable);
        jdbc.execute("DROP TABLE IF EXISTS " + newTable);

        log.info("BUILD_ONE: create table {}", newTable);
        jdbc.execute("CREATE TABLE " + newTable + " AS TABLE " + finalTable + " WITH NO DATA");

        log.info("BUILD_ONE: insert dados stage={} → new={}", stageTable, newTable);
        jdbc.execute(InsertSqlFactory.buildInsert(finalTable, newTable, stageTable));

        log.info("BUILD_ONE: commit OK {}", newTable);
    }
}
