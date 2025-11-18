package br.com.simplificarest.cnpjbatch.tasklet;

import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import br.com.simplificarest.cnpjbatch.enm.CnpjFileType;
import br.com.simplificarest.cnpjbatch.entities.stage.StgQualificacao;
import br.com.simplificarest.cnpjbatch.mappingcsv.CsvColumnMappings;
import br.com.simplificarest.cnpjbatch.service.StageCsvImportService;
@Component
public class ImportQualificacaoTasklet extends CsvImportTasklet<StgQualificacao> {

	public ImportQualificacaoTasklet(StageCsvImportService importService,  TaskExecutor executor) {
		super(importService, StgQualificacao.class, CnpjFileType.QUALIFICACAO, CsvColumnMappings.QUALIFICACAO, executor);
		// TODO Auto-generated constructor stub
	}
//	public ImportQualificacaoTasklet(StageCsvImportService s) {
//		super(s, StgQualificacao.class, CnpjFileType.QUALIFICACAO,CsvColumnMappings.QUALIFICACAO);
//	}
}
