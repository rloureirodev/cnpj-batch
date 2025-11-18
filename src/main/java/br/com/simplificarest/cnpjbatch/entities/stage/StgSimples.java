package br.com.simplificarest.cnpjbatch.entities.stage;

import java.time.LocalDateTime;
import java.util.UUID;

import com.opencsv.bean.CsvBindByPosition;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "stg_simples")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StgSimples {
	
	@Id
	private String id = UUID.randomUUID().toString();

	//@CsvBindByPosition(position = 0)
    private String cnpjBasico;
	//@CsvBindByPosition(position = 1)
    private String opcaoSimples;
	//@CsvBindByPosition(position = 2)
    private String dataOpcaoSimples;
	//@CsvBindByPosition(position = 3)
    private String dataExclusaoSimples;
	//@CsvBindByPosition(position = 4)
    private String opcaoMei;
	//@CsvBindByPosition(position = 5)
    private String dataOpcaoMei;
	//@CsvBindByPosition(position = 6)
    private String dataExclusaoMei;
	
	@Column(name = "imported_at", nullable = false)
	private LocalDateTime importedAt = LocalDateTime.now();

	@Column(name = "merged", nullable = false)
	private Boolean merged = false;

	@Column(name = "merge_at")
	private LocalDateTime mergeAt;

}


