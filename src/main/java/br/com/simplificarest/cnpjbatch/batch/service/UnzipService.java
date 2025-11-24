package br.com.simplificarest.cnpjbatch.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

@Service
@Slf4j
public class UnzipService {
	
	public Path unzip(Path zipFile, Path destinoDir) {
	    try {
	        Files.createDirectories(destinoDir);
	        log.info("Descompactando (ZipFile): {} → {}", zipFile, destinoDir);

	        try (ZipFile zip = new ZipFile(zipFile.toFile())) {

	            zip.stream().forEach(entry -> {
	                try {
	                    Path out = destinoDir.resolve(entry.getName());

	                    if (entry.isDirectory()) {
	                        Files.createDirectories(out);
	                        return;
	                    }

	                    Files.createDirectories(out.getParent());

	                    try (InputStream is = zip.getInputStream(entry);
	                         OutputStream os = Files.newOutputStream(out)) {

	                        byte[] buffer = new byte[16 * 1024 * 1024]; // 16 MB
	                        int read;
	                        while ((read = is.read(buffer)) > 0) {
	                            os.write(buffer, 0, read);
	                        }
	                    }

	                } catch (Exception e) {
	                    throw new RuntimeException("Erro extraindo " + entry.getName(), e);
	                }
	            });
	        }

	        log.info("Descompactação concluída.");
	        return destinoDir;

	    } catch (Exception e) {
	        throw new RuntimeException("Erro ao descompactar arquivo " + zipFile, e);
	    }
	}
	
//    public Path unzip(Path zipFile, Path destinoDir) {
//        try {
//            Files.createDirectories(destinoDir);
//            log.info("Descompactando: {} → {}", zipFile, destinoDir);
//
//            try (var fis = new FileInputStream(zipFile.toFile());
//                 var zis = new ZipInputStream(fis)) {
//
//                ZipEntry entry;
//
//                while ((entry = zis.getNextEntry()) != null) {
//
//                    Path novoArquivo = destinoDir.resolve(entry.getName());
//
//                    if (entry.isDirectory()) {
//                        Files.createDirectories(novoArquivo);
//                        continue;
//                    }
//
//                    Files.createDirectories(novoArquivo.getParent());
//
//                    try (var bos = new BufferedOutputStream(Files.newOutputStream(novoArquivo))) {
//                        byte[] buffer = new byte[64 * 1024];
//                        int len;
//                        while ((len = zis.read(buffer)) > 0) {
//                            bos.write(buffer, 0, len);
//                        }
//                    }
//                }
//            }
//
//            log.info("Descompactação concluída.");
//            return destinoDir;
//
//        } catch (Exception e) {
//            throw new RuntimeException("Erro ao descompactar arquivo " + zipFile, e);
//        }
//    }
}
