package br.com.simplificarest.cnpjbatch.service;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@Service
public class StageCsvImportHelper {

    @PersistenceContext
    private EntityManager em;

    // cada chamada grava e comita (quando called de threads diferentes, cada thread terá sua tx)
    @Transactional
    public <T> void saveBatch(List<T> list) {
        var session = em.unwrap(org.hibernate.Session.class);
        final int batchSize = 1000;
        int i = 0;
        for (T item : list) {
            setDefaultStageFields(item);
            session.persist(item);
            i++;
            if (i % batchSize == 0) {
                session.flush();
                session.clear();
            }
        }
        session.flush();
        session.clear();
    }

    private void setDefaultStageFields(Object entity) {
        try {
            Field merged = entity.getClass().getDeclaredField("merged");
            merged.setAccessible(true);
            if (merged.get(entity) == null) merged.set(entity, false);

            Field importedAt = entity.getClass().getDeclaredField("importedAt");
            importedAt.setAccessible(true);
            if (importedAt.get(entity) == null) importedAt.set(entity, LocalDateTime.now());
        } catch (NoSuchFieldException ignored) {
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}