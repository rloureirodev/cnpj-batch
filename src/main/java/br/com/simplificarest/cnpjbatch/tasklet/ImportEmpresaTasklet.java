package br.com.simplificarest.cnpjbatch.tasklet;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import br.com.simplificarest.cnpjbatch.enm.CnpjFileType;
import br.com.simplificarest.cnpjbatch.entities.stage.StgEmpresa;
import br.com.simplificarest.cnpjbatch.mappingcsv.CsvColumnMappings;
import br.com.simplificarest.cnpjbatch.service.StageCsvImportService;


@Component
@Transactional
public class ImportEmpresaTasklet extends CsvImportTasklet<StgEmpresa> {
//	public ImportEmpresaTasklet(StageCsvImportService s) {
//		super(s, StgEmpresa.class, CnpjFileType.EMPRESA,CsvColumnMappings.EMPRESA);
//	}
	
	public ImportEmpresaTasklet(StageCsvImportService service, @Qualifier("cnpjExecutor") TaskExecutor executor) {
		super(service, StgEmpresa.class, CnpjFileType.EMPRESA, CsvColumnMappings.EMPRESA, executor);
	}
}
