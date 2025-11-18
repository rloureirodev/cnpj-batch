package br.com.simplificarest.cnpjbatch.tasklet;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import br.com.simplificarest.cnpjbatch.enm.CnpjFileType;
import br.com.simplificarest.cnpjbatch.entities.stage.StgSocio;
import br.com.simplificarest.cnpjbatch.mappingcsv.CsvColumnMappings;
import br.com.simplificarest.cnpjbatch.service.StageCsvImportService;


@Component
public class ImportSocioTasklet extends CsvImportTasklet<StgSocio> {
//	public ImportSocioTasklet(StageCsvImportService s) {
//		super(s, StgSocio.class, CnpjFileType.SOCIO,CsvColumnMappings.SOCIO);
//	}
	public ImportSocioTasklet(StageCsvImportService service, @Qualifier("cnpjExecutor") TaskExecutor executor) {
        super(service, StgSocio.class, CnpjFileType.SOCIO, CsvColumnMappings.SOCIO, executor);
    }
}
