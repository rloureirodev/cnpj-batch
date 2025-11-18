package br.com.simplificarest.cnpjbatch.tasklet;

import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import br.com.simplificarest.cnpjbatch.enm.CnpjFileType;
import br.com.simplificarest.cnpjbatch.entities.stage.StgNaturezaJuridica;
import br.com.simplificarest.cnpjbatch.mappingcsv.CsvColumnMappings;
import br.com.simplificarest.cnpjbatch.service.StageCsvImportService;

@Component
public class ImportNaturezaTasklet extends CsvImportTasklet<StgNaturezaJuridica> {

	public ImportNaturezaTasklet(StageCsvImportService importService, TaskExecutor executor) {
		super(importService, StgNaturezaJuridica.class, CnpjFileType.NATUREZA, CsvColumnMappings.NATUREZA_JURIDICA, executor);
		// TODO Auto-generated constructor stub
	}
//	public ImportNaturezaTasklet(StageCsvImportService s) {
//		super(s, StgNaturezaJuridica.class, CnpjFileType.NATUREZA,CsvColumnMappings.NATUREZA_JURIDICA);
//	}
}
