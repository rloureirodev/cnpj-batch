package br.com.simplificarest.cnpjbatch.service.merge;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.stereotype.Service;

import br.com.simplificarest.cnpjdomain.enm.CnpjFileType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class MergeOrchestrationService {

    private final MergeExecutorService executor;
    
    public void executarTodos(String anoMes) {

        Map<String, CnpjFileType> map = Map.of(
                "proc_merge_municipios",      CnpjFileType.MUNICIPIO,
                "proc_merge_cnaes",           CnpjFileType.CNAE,
                "proc_merge_paises",          CnpjFileType.PAIS,
                "proc_merge_naturezas",       CnpjFileType.NATUREZA,
                "proc_merge_qualificacoes",   CnpjFileType.QUALIFICACAO,
                "proc_merge_motivos",         CnpjFileType.MOTIVO,
                "proc_merge_empresa",         CnpjFileType.EMPRESA
                //"proc_merge_estabelecimento", CnpjFileType.ESTABELECIMENTO,
                //"proc_merge_socio",           CnpjFileType.SOCIO,
                //"proc_merge_simples",         CnpjFileType.SIMPLES
        );

        List<String> procedures = List.of(
                "proc_merge_municipios",
                "proc_merge_cnaes",
                "proc_merge_paises",
                "proc_merge_naturezas",
                "proc_merge_qualificacoes",
                "proc_merge_motivos",
                "proc_merge_empresa",
                "proc_merge_estabelecimento",
                "proc_merge_socio",
                "proc_merge_simples"
        );

        procedures.forEach(proc -> {
            log.info("Iniciando execução paralela da procedure [{}] para anoMes={}", proc, anoMes);
            executarParalelo(proc, map.get(proc).getStageTable(), anoMes);
        });
    }

    private void executarParalelo(String procedure, String stageTable, String anoMes) {

        long total = executor.countStage(stageTable);
        long passo = 5_000_000L;
        int threads = 4;

        log.info(
            "Executando {} de forma paralela | stage={} | total={} registros | passo={} | threads={}",
            procedure, stageTable, total, passo, threads
        );

        ExecutorService pool = Executors.newFixedThreadPool(threads);

        for (long inicio = 0; inicio < total; inicio += passo) {
            long fim = Math.min(inicio + passo - 1, total - 1);

            long blocoInicio = inicio;
            long blocoFim = fim;

            pool.submit(() -> {

                String tname = Thread.currentThread().getName();
                long start = System.currentTimeMillis();

                log.info(
                    "[THREAD {}] Iniciando bloco {} ({}) intervalo [{} - {}]",
                    tname, procedure, stageTable, blocoInicio, blocoFim
                );

                executor.executarBloco(procedure + "_block", blocoInicio, blocoFim, anoMes);

                long elapsed = System.currentTimeMillis() - start;

                log.info(
                    "[THREAD {}] Finalizado bloco {} intervalo [{} - {}] em {} ms ({} segundos)",
                    tname, procedure, blocoInicio, blocoFim, elapsed, (elapsed / 1000)
                );
            });
        }

        pool.shutdown();
        log.info("ThreadPool encerrado para procedure {}", procedure);
    }



//    public void executarTodos(String anoMes) {
//    	executor.executar("proc_merge_municipios", anoMes);
//    	executor.executar("proc_merge_cnaes", anoMes);
//    	executor.executar("proc_merge_paises", anoMes);
//        executor.executar("proc_merge_naturezas", anoMes);
//        executor.executar("proc_merge_qualificacoes", anoMes);
//        executor.executar("proc_merge_motivos", anoMes);
//        //executor.executar("proc_merge_empresa", anoMes);
//        //executor.executar("proc_merge_estabelecimento", anoMes);
//        //executor.executar("proc_merge_socio", anoMes);
//        executor.executar("proc_merge_simples", anoMes);
//    }
    
    
}