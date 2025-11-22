package br.com.simplificarest.cnpjbatch.batch.service;

import lombok.RequiredArgsConstructor;
import org.flywaydb.core.Flyway;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class FlywaySyncService {

    private final Flyway flyway;

    public void executarSync() {
        flyway.migrate();
    }
}
