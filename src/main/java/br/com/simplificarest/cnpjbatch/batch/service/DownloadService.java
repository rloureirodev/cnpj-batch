package br.com.simplificarest.cnpjbatch.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

@Service
@Slf4j
public class DownloadService {

    public Path download(String url, Path destino) {
        try {
            log.info("Baixando arquivo: {}", url);

            Files.createDirectories(destino.getParent());

            try (var in = new BufferedInputStream(new URL(url).openStream());
                 var out = new FileOutputStream(destino.toFile())) {

                byte[] buffer = new byte[64 * 1024];
                int n;

                while ((n = in.read(buffer)) != -1) {
                    out.write(buffer, 0, n);
                }
            }

            log.info("Download concluído: {}", destino);
            return destino;

        } catch (Exception e) {
            throw new RuntimeException("Erro ao baixar: " + url, e);
        }
    }
}
