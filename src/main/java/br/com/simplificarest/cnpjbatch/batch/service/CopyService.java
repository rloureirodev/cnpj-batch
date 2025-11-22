package br.com.simplificarest.cnpjbatch.batch.service;

import java.io.BufferedReader;
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

//    public void copyCsvToTable(Path file, String table, String hash) {
//
//        try (Connection conn = dataSource.getConnection()) {
//
//            List<String> columns = jdbc.queryForList("""
//                SELECT column_name
//                FROM information_schema.columns
//                WHERE table_name = ?
//                ORDER BY ordinal_position
//            """, String.class, table);
//
//            // tabela tem hash -> remove
//            columns.remove("hash");
//
//            String colList = String.join(",", columns);
//
//            PGConnection pg = conn.unwrap(PGConnection.class);
//
//            String sql =
//                "COPY " + table + " (" + colList + ") FROM STDIN " +
//                "WITH (FORMAT CSV, DELIMITER ';', QUOTE '\"', NULL '', ENCODING 'LATIN1')";
//
//            CopyIn copyIn = pg.getCopyAPI().copyIn(sql);
//
//            try (BufferedReader reader = new BufferedReader(
//                    new InputStreamReader(Files.newInputStream(file), "ISO-8859-1"))) {
//
//                String line;
//                while ((line = reader.readLine()) != null) {
//
//                    // Corrige apenas decimais entre delimitadores
//                    String fixed = line.replaceAll(
//                        "(?<=^|;)(\\d+),(\\d+)(?=;|$)",
//                        "$1.$2"
//                    );
//
//
//                    byte[] bytes = (fixed + "\n").getBytes("ISO-8859-1");
//                    copyIn.writeToCopy(bytes, 0, bytes.length);
//                }
//            }
//
//            copyIn.endCopy();
//            log.info("Arquivo {} inserido na tabela {}", file.getFileName(), table);
//
//        } catch (Exception e) {
//            throw new IllegalStateException("Erro ao executar COPY na tabela " + table, e);
//        }
//    }
    public void copyCsvToTable(Path file, String table, String hashIgnorado) {

        try (Connection conn = dataSource.getConnection()) {

            List<String> columns = jdbc.queryForList("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = ?
                ORDER BY ordinal_position
            """, String.class, table);

            columns.remove("hash");
            String colList = String.join(",", columns);

            PGConnection pg = conn.unwrap(PGConnection.class);

            String sql =
                    "COPY " + table + " (" + colList + ") FROM STDIN " +
                    "WITH (FORMAT CSV, DELIMITER ';', QUOTE '\"', NULL '', ENCODING 'LATIN1')";

            CopyIn copyIn = pg.getCopyAPI().copyIn(sql);

            // ------ LEITURA EM BLOCO OTIMIZADA ------
            try (InputStream in = Files.newInputStream(file)) {

                byte[] buffer = new byte[16 * 1024 * 1024]; // 4 MB
                int read;

                while ((read = in.read(buffer)) != -1) {
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
}