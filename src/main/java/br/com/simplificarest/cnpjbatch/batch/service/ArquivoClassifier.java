package br.com.simplificarest.cnpjbatch.batch.service;

import org.springframework.stereotype.Component;

@Component
public class ArquivoClassifier {

    public String getStageTable(String fileName) {
        String f = fileName.toLowerCase();

        if (f.startsWith("empresas")) return "stage_empresa";
        if (f.startsWith("estabelecimentos")) return "stage_estabelecimento";
        if (f.startsWith("socios")) return "stage_socios";
        if (f.startsWith("simples")) return "stage_simples";

        if (f.startsWith("municipios")) return "stage_municipios";
        if (f.startsWith("cnaes")) return "stage_cnaes";
        if (f.startsWith("naturezas")) return "stage_naturezas";
        if (f.startsWith("qualificacoes")) return "stage_qualificacoes";
        if (f.startsWith("paises")) return "stage_paises";
        if (f.startsWith("motivos")) return "stage_motivos";

        return null;
    }
}
