package br.com.simplificarest.cnpjbatch.entities.finais;

import java.math.BigDecimal;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "empresa")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Empresa {

    @Id
    private String cnpjBasico;

    private String razaoSocial;

    private String naturezaJuridica;

    @ManyToOne
    private Qualificacao qualificacao;

    private BigDecimal capitalSocial;

    private String porte;

    private String enteFederativoResponsavel;
}
