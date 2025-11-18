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
@Table(name = "stg_socio")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StgSocio {
	
	@Id
	private String id = UUID.randomUUID().toString();

	//@CsvBindByPosition(position = 0)
    private String cnpjBasico;
	
	//@CsvBindByPosition(position = 1)
    private String identificadorSocio;
	
	//@CsvBindByPosition(position = 3)
    private String cpfCnpjSocio;
	
	//@CsvBindByPosition(position = 2)
	private String nomeSocio;
	
	//@CsvBindByPosition(position = 4)
	private String qualificacaoSocio;
	
	//@CsvBindByPosition(position = 5)
	private String dataEntradaSociedade;
	
	//@CsvBindByPosition(position = 6)
	private String pais;
	
	//@CsvBindByPosition(position = 7)
	private String cpfRepresentanteLegal;
	
	//@CsvBindByPosition(position = 8)
	private String nomeRepresentante;
	
	//@CsvBindByPosition(position = 9)
	private String qualificacaoRepresentante;
	
	//@CsvBindByPosition(position = 10)
	private String faixaEtaria;
	
	@Column(name = "imported_at", nullable = false)
	private LocalDateTime importedAt = LocalDateTime.now();

	@Column(name = "merged", nullable = false)
	private Boolean merged = false;

	@Column(name = "merge_at")
	private LocalDateTime mergeAt;

}
