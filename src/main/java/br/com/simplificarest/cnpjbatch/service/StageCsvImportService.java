package br.com.simplificarest.cnpjbatch.service;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class StageCsvImportService {

    private final StageCsvImportHelper helper;
    private static final int BATCH_SIZE = 50000;
    
    private final Map<Class<?>, Field[]> cachedFields = new ConcurrentHashMap<>();

    public <T> void importCsv(InputStream is, Class<T> entityClass, String[] mapping) throws Exception {
        log.info("Importando CSV para entidade {}", entityClass.getSimpleName());

        ColumnPositionMappingStrategy<T> strategy = new ColumnPositionMappingStrategy<>();
        strategy.setType(entityClass);
        strategy.setColumnMapping(mapping);

        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(is, StandardCharsets.ISO_8859_1))
                .withCSVParser(new CSVParserBuilder().withSeparator(';').build())
                .build()) {

            CsvToBean<T> csv = new CsvToBeanBuilder<T>(reader)
                    .withMappingStrategy(strategy)
                    .withSkipLines(0)
                    .build();

            List<T> buffer = new ArrayList<>(BATCH_SIZE);
            long total = 0;

            for (T row : csv) {
                buffer.add(row);
                if (buffer.size() == BATCH_SIZE) {
                    helper.saveBatch(buffer);
                    total += buffer.size();
                    buffer.clear();
                    log.info("Commit parcial: {} registros importados", total);
                }
            }

            if (!buffer.isEmpty()) {
                helper.saveBatch(buffer);
                total += buffer.size();
            }

            log.info("IMPORT FINALIZADO: {} registros", total);
        }
    }
    public void importCsvFast(InputStream is, Class<?> entityClass, String[] mapping) throws Exception {
        log.info("Fast import (Univocity): {}", entityClass.getSimpleName());

        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setDelimiter(';');
        settings.getFormat().setQuote('"');
        settings.setHeaderExtractionEnabled(false);
        settings.setNullValue("");
        settings.setEmptyValue("");
        settings.setReadInputOnSeparateThread(false);
        settings.setInputBufferSize(64 * 1024);
        settings.setMaxCharsPerColumn(4096);

        CsvParser parser = new CsvParser(settings);

        // cache pré-calculado dos fields, REUTILIZADO pelo loop
        Field[] fields = cachedFields.computeIfAbsent(entityClass, c -> {
            Field[] arr = new Field[mapping.length];
            try {
                for (int i = 0; i < mapping.length; i++) {
                    Field f = c.getDeclaredField(mapping[i]);
                    f.setAccessible(true);
                    arr[i] = f;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return arr;
        });

        List<Object> buffer = new ArrayList<>(BATCH_SIZE);
        long total = 0;

        for (String[] cols : parser.iterate(is, StandardCharsets.ISO_8859_1)) {

            Object entity = entityClass.getConstructor().newInstance();

            for (int i = 0; i < fields.length && i < cols.length; i++) {
                fields[i].set(entity, clean(cols[i]));
            }

            buffer.add(entity);

            if (buffer.size() == BATCH_SIZE) {
                helper.saveBatch(buffer);
                total += buffer.size();
                buffer.clear();
                log.info("Commit parcial: {}", total);
            }
        }

        if (!buffer.isEmpty()) {
            helper.saveBatch(buffer);
            total += buffer.size();
        }

        log.info("Import finalizado (Univocity): {}", total);
    }
  
    private String clean(String v) {
        if (v == null) return null;
        return v.replace("\"", "").trim();
    }

}
