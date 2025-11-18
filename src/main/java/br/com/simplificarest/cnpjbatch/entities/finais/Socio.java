package br.com.simplificarest.cnpjbatch.entities.finais;

import java.time.LocalDate;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "socio")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Socio {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    private Empresa empresa;

    private Integer identificador;
    private String nome;
    private String documento;
    private Integer qualificacao;
    private LocalDate dataEntrada;
    private String pais;
    private String representanteDocumento;
    private String representanteNome;
    private Integer representanteQualificacao;
    private Integer faixaEtaria;
}
