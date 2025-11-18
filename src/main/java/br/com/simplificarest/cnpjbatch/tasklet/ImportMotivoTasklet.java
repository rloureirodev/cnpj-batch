package br.com.simplificarest.cnpjbatch.tasklet;

import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import br.com.simplificarest.cnpjbatch.enm.CnpjFileType;
import br.com.simplificarest.cnpjbatch.entities.stage.StgMotivo;
import br.com.simplificarest.cnpjbatch.mappingcsv.CsvColumnMappings;
import br.com.simplificarest.cnpjbatch.service.StageCsvImportService;

@Component
public class ImportMotivoTasklet extends CsvImportTasklet<StgMotivo> {

	public ImportMotivoTasklet(StageCsvImportService importService, TaskExecutor executor) {
		super(importService, StgMotivo.class, CnpjFileType.MOTIVO, CsvColumnMappings.MOTIVO, executor);
		// TODO Auto-generated constructor stub
	}
//	public ImportMotivoTasklet(StageCsvImportService s) {
//		super(s, StgMotivo.class, CnpjFileType.MOTIVO,CsvColumnMappings.MOTIVO);
//	}
}
