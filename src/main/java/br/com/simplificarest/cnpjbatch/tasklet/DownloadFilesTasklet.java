package br.com.simplificarest.cnpjbatch.tasklet;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class DownloadFilesTasklet implements Tasklet {

    private static final String BASE_URL =
            "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/";

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        String month = "2025-11";
        String targetDir = "D:/temp/cnpj/" + month;

        Files.createDirectories(Path.of(targetDir));

        // Lê HTML e extrai links
        Document doc = Jsoup.connect(BASE_URL + month + "/").get();

        List<String> links = doc.select("a[href$=.zip]")
                .stream()
                .map(e -> e.attr("href"))
                .toList();

        for (String file : links) {
            String url = BASE_URL + month + "/" + file;
            String dest = targetDir + "/" + file;
            download(url, dest);
        }

        return RepeatStatus.FINISHED;
    }

    private void download(String url, String dest) throws Exception {
        System.out.println("Downloading: " + url);
        HttpClient.newHttpClient().send(
                HttpRequest.newBuilder().uri(URI.create(url)).build(),
                HttpResponse.BodyHandlers.ofFile(Path.of(dest))
        );
    }
}