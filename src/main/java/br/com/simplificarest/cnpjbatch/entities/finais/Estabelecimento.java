package br.com.simplificarest.cnpjbatch.entities.finais;

import java.time.LocalDate;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "estabelecimento")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Estabelecimento {

    @Id
    private String cnpj; // cnpjBasico + ordem + dv montado

    @ManyToOne
    private Empresa empresa;

    private Boolean matriz;
    private String nomeFantasia;
    private Integer situacaoCadastral;
    private LocalDate dataSituacaoCadastral;
    private Integer motivoSituacao;
    private String nomeCidadeExterior;
    private String pais;
    private LocalDate dataInicioAtividade;

    private String cnaePrincipal;

    private String cnaeSecundaria;

    private String tipoLogradouro;
    private String logradouro;
    private String numero;
    private String complemento;
    private String bairro;
    private String cep;
    private String uf;
    private Integer municipio;
    private String telefone1;
    private String telefone2;
    private String fax;
    private String email;
    private String situacaoEspecial;
    private LocalDate dataSituacaoEspecial;
}
