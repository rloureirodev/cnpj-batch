package br.com.simplificarest.cnpjbatch.tasklet;

import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import br.com.simplificarest.cnpjbatch.enm.CnpjFileType;
import br.com.simplificarest.cnpjbatch.entities.stage.StgEstabelecimento;
import br.com.simplificarest.cnpjbatch.mappingcsv.CsvColumnMappings;
import br.com.simplificarest.cnpjbatch.service.StageCsvImportService;
@Component
public class ImportEstabelecimentoTasklet extends CsvImportTasklet<StgEstabelecimento> {

	public ImportEstabelecimentoTasklet(StageCsvImportService importService,TaskExecutor executor) {
		super(importService, StgEstabelecimento.class, CnpjFileType.ESTABELECIMENTO, CsvColumnMappings.ESTABELECIMENTO, executor);
	}
//	public ImportEstabelecimentoTasklet(StageCsvImportService s) {
//		super(s, StgEstabelecimento.class, CnpjFileType.ESTABELECIMENTO,CsvColumnMappings.ESTABELECIMENTO);
//	}
}
