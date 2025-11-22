package br.com.simplificarest.cnpjbatch.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Service
@Slf4j
public class UnzipService {

    public Path unzip(Path zipFile, Path destinoDir) {
        try {
            Files.createDirectories(destinoDir);
            log.info("Descompactando: {} → {}", zipFile, destinoDir);

            try (var fis = new FileInputStream(zipFile.toFile());
                 var zis = new ZipInputStream(fis)) {

                ZipEntry entry;

                while ((entry = zis.getNextEntry()) != null) {

                    Path novoArquivo = destinoDir.resolve(entry.getName());

                    if (entry.isDirectory()) {
                        Files.createDirectories(novoArquivo);
                        continue;
                    }

                    Files.createDirectories(novoArquivo.getParent());

                    try (var bos = new BufferedOutputStream(Files.newOutputStream(novoArquivo))) {
                        byte[] buffer = new byte[64 * 1024];
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            bos.write(buffer, 0, len);
                        }
                    }
                }
            }

            log.info("Descompactação concluída.");
            return destinoDir;

        } catch (Exception e) {
            throw new RuntimeException("Erro ao descompactar arquivo " + zipFile, e);
        }
    }
}
