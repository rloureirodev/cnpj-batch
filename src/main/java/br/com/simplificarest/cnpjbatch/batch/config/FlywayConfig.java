package br.com.simplificarest.cnpjbatch.batch.config;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FlywayConfig {

    @Bean
    public Flyway flyway(DataSource dataSource) {

        Flyway flyway = Flyway.configure()
                .locations("classpath:db/migration")
                .baselineOnMigrate(true)
                .dataSource(dataSource)
                .load();

        flyway.migrate(); // <- obrigatório

        return flyway;
    }
}
