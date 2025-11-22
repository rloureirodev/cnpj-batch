package br.com.simplificarest.cnpjbatch.batch.service;

import org.jsoup.Jsoup;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class ArquivoService {

    public List<String> listarArquivosDoMes(String anoMes) {
        try {
            String url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/" + anoMes + "/";

            var doc = Jsoup.connect(url).get();

            return doc.select("a")
                    .eachAttr("href")
                    .stream()
                    .filter(href -> href.toLowerCase().endsWith(".zip"))
                    .map(href -> url + href)
                    .collect(Collectors.toList());

        } catch (Exception e) {
            throw new RuntimeException("Erro ao listar arquivos do mês " + anoMes, e);
        }
    }
}
