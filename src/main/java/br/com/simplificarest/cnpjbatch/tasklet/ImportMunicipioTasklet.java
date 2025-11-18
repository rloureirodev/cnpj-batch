package br.com.simplificarest.cnpjbatch.tasklet;

import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import br.com.simplificarest.cnpjbatch.enm.CnpjFileType;
import br.com.simplificarest.cnpjbatch.entities.stage.StgMunicipio;
import br.com.simplificarest.cnpjbatch.mappingcsv.CsvColumnMappings;
import br.com.simplificarest.cnpjbatch.service.StageCsvImportService;
@Component
public class ImportMunicipioTasklet extends CsvImportTasklet<StgMunicipio> {

	public ImportMunicipioTasklet(StageCsvImportService importService, TaskExecutor executor) {
		super(importService, StgMunicipio.class, CnpjFileType.MUNICIPIO, CsvColumnMappings.MUNICIPIO,executor);
	}
//	public ImportMunicipioTasklet(StageCsvImportService s) {
//		super(s, StgMunicipio.class, CnpjFileType.MUNICIPIO,CsvColumnMappings.MUNICIPIO);
//	}
}
