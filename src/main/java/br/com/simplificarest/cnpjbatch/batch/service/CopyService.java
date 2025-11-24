package br.com.simplificarest.cnpjbatch.batch.service;

import java.io.BufferedReader;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class CopyService {

    private final DataSource dataSource;
    private final JdbcTemplate jdbc;

    public void copyCsvToTable(Path file, String table, String hashIgnorado) {

        try (Connection conn = dataSource.getConnection()) {

            List<String> columns = jdbc.queryForList("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = ?
                ORDER BY ordinal_position
            """, String.class, table);

            //columns.remove("hash");
            String colList = String.join(",", columns);

            PGConnection pg = conn.unwrap(PGConnection.class);

            String sql =
                    "COPY " + table + " (" + colList + ") FROM STDIN " +
                    "WITH (FORMAT CSV, DELIMITER ';', QUOTE '\"', NULL '', ENCODING 'LATIN1')";

            CopyIn copyIn = pg.getCopyAPI().copyIn(sql);


            try (InputStream raw = Files.newInputStream(file);
                    InputStream clean = sanitize(raw)) {

                   byte[] buffer = new byte[8 * 1024 * 1024]; // 4MB buffer
                   int read;

                   while ((read = clean.read(buffer)) != -1) {
                       copyIn.writeToCopy(buffer, 0, read);
                   }
               }

            copyIn.endCopy();
            log.info("COPY finalizado para {} (arquivo {})", table, file.getFileName());

        } catch (Exception e) {
        	e.printStackTrace();
            throw new IllegalStateException("Erro ao executar COPY na tabela " + table, e);
            
        }
    }
    
    public boolean hashJaImportada(String table, String hash) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM " + table + " WHERE hash = ?",
                Integer.class,
                hash
        );
        return count != null && count > 0;
    }
    
   private InputStream sanitize(InputStream raw) {
        return new FilterInputStream(raw) {

            @Override
            public int read() throws IOException {
                int b;
                do {
                    b = super.read();
                } while (b == 0); // remove bytes NULL
                return b;
            }

            @Override
            public int read(byte[] buffer, int off, int len) throws IOException {
                int count = super.read(buffer, off, len);
                if (count <= 0) return count;

                for (int i = off; i < off + count; i++) {
                    if (buffer[i] == 0) {
                        buffer[i] = ' '; // substitui byte NULL por espaço
                    }
                    // remove espaços invisíveis \u200B, \u00A0 etc
                    if (buffer[i] == '\u00A0' || buffer[i] == '\u200B') {
                        buffer[i] = ' ';
                    }
                }
                return count;
            }
        };
    }
}