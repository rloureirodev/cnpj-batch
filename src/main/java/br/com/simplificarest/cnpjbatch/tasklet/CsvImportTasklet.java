package br.com.simplificarest.cnpjbatch.tasklet;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.task.TaskExecutor;

import br.com.simplificarest.cnpjbatch.enm.CnpjFileType;
import br.com.simplificarest.cnpjbatch.service.StageCsvImportService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

//@Slf4j
//@RequiredArgsConstructor
//public abstract class CsvImportTasklet<T> implements Tasklet {
//
//    private final StageCsvImportService importService;
//    private final Class<T> type;
//    private final CnpjFileType fileType;
//    private final String[] columnMapping;
//
//    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
//        log.info("Iniciando importação CSV para tipo: {}", type.getSimpleName());
//
//        Path dir = Path.of("D:/temp/cnpj/2025-11");
//
//        try (Stream<Path> files = Files.list(dir)) {
//            files.forEach(f -> {
//                String filename = f.getFileName().toString();
//                if (filename.toUpperCase().contains(fileType.getZipPrefix().toUpperCase()) 
//                        && filename.toLowerCase().endsWith(".zip")) {
//                    log.info("Arquivo filtrado para processamento: {}", filename);
//                    processZip(f);
//                } else {
//                    log.info("Arquivo ignorado: {}", filename);
//                }
//            });
//        }
//
//        log.info("Finalizada importação CSV para tipo: {}", type.getSimpleName());
//        return RepeatStatus.FINISHED;
//    }
//
//    private void processZip(Path zipPath) {
//        log.info("Iniciando processamento do ZIP: {}", zipPath.getFileName());
//
//        try (ZipFile zipFile = new ZipFile(zipPath.toFile())) {
//
//            Enumeration<? extends ZipEntry> entries = zipFile.entries();
//            while (entries.hasMoreElements()) {
//                ZipEntry entry = entries.nextElement();
//
//                if (entry.isDirectory()) continue;
//
//                if (entry.getName().toUpperCase().contains(fileType.getIdentifier())) {
//                    log.info("Encontrado arquivo no ZIP: {}", entry.getName());
//                    processEntry(zipFile, entry);
//                    break; // só existe 1 arquivo relevante por ZIP
//                }
//            }
//
//        } catch (Exception e) {
//            log.error("Erro ao processar ZIP: {}", zipPath.getFileName(), e);
//        }
//
//        log.info("Finalizado ZIP: {}", zipPath.getFileName());
//    }
//
//    private void processEntry(ZipFile zipFile, ZipEntry entry) {
//        log.info("Lendo CSV dentro do ZIP: {}", entry.getName());
//
//        try (InputStream is = zipFile.getInputStream(entry)) {
//        	importService.importCsvFast(is, type, columnMapping);
//            log.info("Importação concluída para CSV: {}", entry.getName());
//        } catch (Exception e) {
//            log.error("Erro ao importar CSV: {}", entry.getName(), e);
//        }
//    }
//}

@Slf4j
@RequiredArgsConstructor
public abstract class CsvImportTasklet<T> implements Tasklet {

    private final StageCsvImportService importService;
    private final Class<T> type;
    private final CnpjFileType fileType;
    private final String[] columnMapping;
    private final TaskExecutor executor;

    // Evita reprocessar mesmo ZIP se o Step reiniciar
    private static final Set<String> processed = ConcurrentHashMap.newKeySet();

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("Iniciando importação (paralela) para tipo: {}", type.getSimpleName());

        Path dir = Path.of("D:/temp/cnpj/2025-11");

        List<Path> zipFiles = Files.list(dir)
                .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".zip"))
                .filter(p -> p.getFileName().toString().startsWith(fileType.getZipPrefix()))
                .sorted() // garante ordem determinística
                .toList();

        if (zipFiles.isEmpty()) {
            log.warn("Nenhum ZIP encontrado para {}", type.getSimpleName());
            return RepeatStatus.FINISHED;
        }

        CountDownLatch latch = new CountDownLatch(zipFiles.size());
        
        String stepName = chunkContext.getStepContext().getStepName();
        
        for (Path zip : zipFiles) {

            // 🔥 EVITA DUPLICAÇÃO
            if (!processed.add(zip.toString())) {
                log.warn("IGNORANDO ZIP JA PROCESSADO: {}", zip.getFileName());
                latch.countDown(); // importante para não travar await()
                continue;
            }

            executor.execute(() -> {
                try {
                	Thread.currentThread().setName(stepName + "-" + zip.getFileName());
                    processZip(zip);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        log.info("Finalizada importação paralela para {}", type.getSimpleName());
        return RepeatStatus.FINISHED;
    }

    private void processZip(Path zipPath) {
        log.info("Thread {} processando ZIP {}", 
                Thread.currentThread().getName(), 
                zipPath.getFileName());

        try (ZipFile zipFile = new ZipFile(zipPath.toFile())) {

            ZipEntry target = zipFile.stream()
                    .filter(e -> !e.isDirectory())
                    .filter(e -> e.getName().toUpperCase().contains(fileType.getIdentifier()))
                    .findFirst()
                    .orElse(null);

            if (target == null) {
                log.warn("Nenhum arquivo {} encontrado dentro do ZIP {}", 
                        fileType.getIdentifier(), 
                        zipPath.getFileName());
                return;
            }

            try (InputStream is = zipFile.getInputStream(target)) {
                importService.importCsvFast(is, type, columnMapping);
            }

            log.info("ZIP {} processado", zipPath.getFileName());

        } catch (Exception e) {
            log.error("Erro processando ZIP " + zipPath.getFileName(), e);
        }
    }
}
