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
@Table(name = "stg_cnae")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StgCnae {
	
	@Id
	private String id = UUID.randomUUID().toString();
	
	//@CsvBindByPosition(position = 0)
	private String codigo;
	
	//@CsvBindByPosition(position = 1)
	private String descricao;
	
	//@Column(name = "imported_at", nullable = false)
	private LocalDateTime importedAt = LocalDateTime.now();

	//@Column(name = "merged", nullable = false)
	private Boolean merged = false;

	//@Column(name = "merge_at")
	private LocalDateTime mergeAt;

}
