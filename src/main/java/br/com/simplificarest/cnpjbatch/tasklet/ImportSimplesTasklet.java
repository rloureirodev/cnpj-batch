package br.com.simplificarest.cnpjbatch.tasklet;

import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import br.com.simplificarest.cnpjbatch.enm.CnpjFileType;
import br.com.simplificarest.cnpjbatch.entities.stage.StgSimples;
import br.com.simplificarest.cnpjbatch.mappingcsv.CsvColumnMappings;
import br.com.simplificarest.cnpjbatch.service.StageCsvImportService;
@Component
public class ImportSimplesTasklet extends CsvImportTasklet<StgSimples> {

	public ImportSimplesTasklet(StageCsvImportService importService, TaskExecutor executor) {
		super(importService, StgSimples.class, CnpjFileType.SIMPLES, CsvColumnMappings.SIMPLES, executor);
		// TODO Auto-generated constructor stub
	}
//	public ImportSimplesTasklet(StageCsvImportService s) {
//		super(s, StgSimples.class, CnpjFileType.SIMPLES,CsvColumnMappings.SIMPLES);
//	}
}
