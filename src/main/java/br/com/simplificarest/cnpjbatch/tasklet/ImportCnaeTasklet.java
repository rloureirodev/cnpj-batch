package br.com.simplificarest.cnpjbatch.tasklet;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import br.com.simplificarest.cnpjbatch.enm.CnpjFileType;
import br.com.simplificarest.cnpjbatch.entities.stage.StgCnae;
import br.com.simplificarest.cnpjbatch.mappingcsv.CsvColumnMappings;
import br.com.simplificarest.cnpjbatch.service.StageCsvImportService;

@Component
public class ImportCnaeTasklet extends CsvImportTasklet<StgCnae> {
	
	
public ImportCnaeTasklet(StageCsvImportService importService, @Qualifier("cnpjExecutor") TaskExecutor executor) {
		super(importService, StgCnae.class, CnpjFileType.CNAE, CsvColumnMappings.CNAE, executor);
	}

//    public ImportCnaeTasklet(StageCsvImportService s) {
//        super(s, StgCnae.class, CnpjFileType.CNAE, CsvColumnMappings.CNAE);
//    }

}