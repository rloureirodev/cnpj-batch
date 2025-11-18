package br.com.simplificarest.cnpjbatch.tasklet;

import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import br.com.simplificarest.cnpjbatch.enm.CnpjFileType;
import br.com.simplificarest.cnpjbatch.entities.stage.StgPais;
import br.com.simplificarest.cnpjbatch.mappingcsv.CsvColumnMappings;
import br.com.simplificarest.cnpjbatch.service.StageCsvImportService;
@Component
public class ImportPaisTasklet extends CsvImportTasklet<StgPais> {

	public ImportPaisTasklet(StageCsvImportService importService,  TaskExecutor executor) {
		super(importService, StgPais.class, CnpjFileType.PAIS, CsvColumnMappings.PAIS, executor);
	}
//	public ImportPaisTasklet(StageCsvImportService s) {
//		super(s, StgPais.class, CnpjFileType.PAIS,CsvColumnMappings.PAIS);
//	}
}
