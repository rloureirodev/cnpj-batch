package br.com.simplificarest.cnpjbatch.batch.service;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;

import org.springframework.stereotype.Service;

import br.com.simplificarest.cnpjdomain.enm.BatchStage;
import br.com.simplificarest.cnpjdomain.enm.CnpjFileType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class FileProcessorService {

    private final DownloadService downloadService;
    private final UnzipService unzipService;
    private final CopyService copyService;
    private final BatchProcessLogService logService;
    private final StageHashService stageHashService;

    // --------- PROCESSA UM ARQUIVO COMPLETO ---------
    public void processarUmArquivo(String url, String anoMes, Path zipDir, Path csvDir) {

        String nome = extrairNome(url);
        CnpjFileType type = CnpjFileType.fromFileName(nome).orElse(null);

        if (type == null) {
            log.warn("Ignorando arquivo não mapeado: {}", nome);
            return;
        }

        String tabela = type.getStageTable();
        Path zipLocal = zipDir.resolve(nome);
        Path extractDir = csvDir.resolve(nome.replace(".zip", ""));

        try {
        	Path csv = prepararCsv(url, nome, anoMes, tabela, zipLocal, extractDir, type);
        	realizarCopy(csv, tabela, nome, anoMes); // <-- envia ZIP
        } catch (Exception e) {
            log.error("Erro processando arquivo {}: {}", nome, e.getMessage());
        }
    }

    // --------- DOWNLOAD + UNZIP + LOCALIZA CSV ---------
    private Path prepararCsv(String url, String nome, String anoMes, String tabela,
                             Path zipLocal, Path extractDir, CnpjFileType type) throws Exception {

        // DOWNLOAD
        if (!Files.exists(zipLocal)) {
            long id = logService.start(BatchStage.DOWNLOAD, "cnpjJob", anoMes, nome, null, tabela, null);
            downloadService.download(url, zipLocal);
            logService.ok(id);
        } else {
            long id = logService.start(BatchStage.SKIP_DOWNLOAD, "cnpjJob", anoMes, nome, null, tabela, null);
            logService.ok(id);
        }

        // UNZIP
        if (!Files.exists(extractDir)) {
            long id = logService.start(BatchStage.UNZIP, "cnpjJob", anoMes, nome, null, tabela, null);
            unzipService.unzip(zipLocal, extractDir);
            logService.ok(id);
        } else {
            long id = logService.start(BatchStage.SKIP_UNZIP, "cnpjJob", anoMes, nome, null, tabela, null);
            logService.ok(id);
        }

        // LOCALIZA CSV
        try (var walk = Files.walk(extractDir)) {
            return walk
                    .filter(p -> p.getFileName().toString().toUpperCase().endsWith(type.getIdentifier()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("CSV não encontrado em " + extractDir));
        }
    }

    // --------- COPY COM HASH ---------
    private void realizarCopy(Path csv, String tabela, String nome, String anoMes) throws Exception {

        Path cleaned = removeBom(csv);
        String hash = gerarHash(csv);

        if (logService.jaProcessado("cnpjJob", anoMes, tabela, hash)) {
            long id = logService.start(BatchStage.SKIP_COPY, "cnpjJob", anoMes, nome,
                    cleaned.toString(), tabela, hash);
            logService.ok(id);
            log.info("CSV ja processado {} ",csv.getFileName());
            return;
        }

        long id = logService.start(BatchStage.COPY, "cnpjJob", anoMes, nome,
                cleaned.toString(), tabela, hash);

        try {
        	log.info("Iniciando Copy para tabela {} com CSV {} ",tabela, csv.getFileName());
            copyService.copyCsvToTable(cleaned, tabela, hash);
            logService.ok(id);
        } catch (Exception e) {
            logService.error(id, e);
            throw e;
        }
    }

    // --------- HASH NAS TABELAS ---------
    public void aplicarHashes() {
        stageHashService.hashEmpresa();
        stageHashService.hashEstabelecimento();
        stageHashService.hashSocios();
        stageHashService.hashSimples();
    }


    // --------- HELPERS ---------

    private String extrairNome(String url) {
        return url.substring(url.lastIndexOf('/') + 1);
    }

    private Path removeBom(Path csv) throws Exception {

        try (var in = Files.newInputStream(csv)) {
            byte[] b = new byte[3];
            if (in.read(b) == 3 &&
                (b[0] & 0xFF) == 0xEF &&
                (b[1] & 0xFF) == 0xBB &&
                (b[2] & 0xFF) == 0xBF) {

                Path cleaned = csv.getParent().resolve("cleaned-" + csv.getFileName());

                try (var in2 = Files.newInputStream(csv);
                     var out = Files.newOutputStream(cleaned)) {

                    in2.skip(3);
                    byte[] buffer = new byte[8192];
                    int r;
                    while ((r = in2.read(buffer)) != -1) {
                        out.write(buffer, 0, r);
                    }
                }
                return cleaned;
            }
        }
        return csv;
    }

    private String gerarHash(Path file) throws Exception {
        try (InputStream is = Files.newInputStream(file)) {
            MessageDigest md = MessageDigest.getInstance("MD5");

            byte[] buffer = new byte[8192];
            int read;
            while ((read = is.read(buffer)) != -1) {
                md.update(buffer, 0, read);
            }

            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        }
    }
}