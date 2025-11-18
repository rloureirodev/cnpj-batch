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
@Table(name = "stg_empresa")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StgEmpresa {
	
	@Id
	private String id = UUID.randomUUID().toString();

    //@CsvBindByPosition(position = 0)
    @Column(name = "cnpj_basico", length = 20)
    private String cnpjBasico;

    //@CsvBindByPosition(position = 1)
    @Column(name = "razao_social", length = 255)
    private String razaoSocial;

    //@CsvBindByPosition(position = 2)
    @Column(name = "natureza_juridica", length = 10)
    private String naturezaJuridica;

    //@CsvBindByPosition(position = 3)
    @Column(name = "qualificacao_responsavel", length = 10)
    private String qualificacaoResponsavel;

    //@CsvBindByPosition(position = 4)
    @Column(name = "capital_social", length = 50)
    private String capitalSocial;

    //@CsvBindByPosition(position = 5)
    @Column(name = "porte", length = 10)
    private String porte;

    //@CsvBindByPosition(position = 6)
    @Column(name = "ente_federativo_responsavel", length = 50)
    private String enteFederativoResponsavel;
    
    @Column(name = "imported_at", nullable = false)
    private LocalDateTime importedAt = LocalDateTime.now();

    @Column(name = "merged", nullable = false)
    private Boolean merged = false;

    @Column(name = "merge_at")
    private LocalDateTime mergeAt;

}
