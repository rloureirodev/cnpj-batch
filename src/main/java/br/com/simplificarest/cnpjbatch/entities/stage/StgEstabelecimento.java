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
@Table(name = "stg_estabelecimento")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StgEstabelecimento {
	
	@Id
	private String id = UUID.randomUUID().toString();
	
    ////@CsvBindByPosition(position = 0)
    @Column(length = 500)
	private String cnpjBasico;
    
	@Column(length = 500)
	//@CsvBindByPosition(position = 1)
    private String cnpjOrdem;
	
	@Column(length = 500)
	//@CsvBindByPosition(position = 2)
    private String cnpjDv;

	//@CsvBindByPosition(position = 3)
    private String identificadorMatrizFilial;
	
	@Column(length = 500)
	//@CsvBindByPosition(position = 4)
    private String nomeFantasia;
	
	//@CsvBindByPosition(position = 5)
    private String situacaoCadastral;
	
	//@CsvBindByPosition(position = 6)
    private String dataSituacaoCadastral;
	
	@Column(length = 500)
	//@CsvBindByPosition(position = 7)
    private String motivoSituacaoCadastral;
	
	@Column(length = 500)
	////@CsvBindByPosition(position = 8)
    private String nomeCidadeExterior;
	
	//@CsvBindByPosition(position = 9)
    private String pais;
	
	//@CsvBindByPosition(position = 10)
    private String dataInicioAtividade;
	
	@Column(length = 500)
	//@CsvBindByPosition(position = 11)
    private String cnaePrincipal;
	
	@Column(columnDefinition = "TEXT")
	private String cnaeSecundaria;
    
    //@CsvBindByPosition(position = 13)
    private String tipoLogradouro;
    
    @Column(length = 500)
    //@CsvBindByPosition(position = 14)
    private String logradouro;
    
    //@CsvBindByPosition(position = 15)
    private String numero;
    
    @Column(length = 500)
    //@CsvBindByPosition(position = 16)
    private String complemento;
    
    @Column(length = 500)
    //@CsvBindByPosition(position = 17)
    private String bairro;
    
    //@CsvBindByPosition(position = 18)
    private String cep;
    
    //@CsvBindByPosition(position = 19)
    private String uf;
    
    @Column(length = 500)
    //@CsvBindByPosition(position = 20)
    private String municipio;
    
    //@CsvBindByPosition(position = 21)
    private String ddd1;
    
    //@CsvBindByPosition(position = 22)
    private String telefone1;
    
    //@CsvBindByPosition(position = 23)
    private String ddd2;
    
    //@CsvBindByPosition(position = 24)
    private String telefone2;
    
    //@CsvBindByPosition(position = 25)
    private String dddFax;
    
    //@CsvBindByPosition(position = 26)
    private String fax;
    
    //@CsvBindByPosition(position = 27)
    private String email;
    
    @Column(length = 500)
    //@CsvBindByPosition(position = 28)
    private String situacaoEspecial;
    
    //@CsvBindByPosition(position = 29)
    private String dataSituacaoEspecial;
    
    @Column(name = "imported_at", nullable = false)
    private LocalDateTime importedAt = LocalDateTime.now();

    @Column(name = "merged", nullable = false)
    private Boolean merged = false;

    @Column(name = "merge_at")
    private LocalDateTime mergeAt;


}
