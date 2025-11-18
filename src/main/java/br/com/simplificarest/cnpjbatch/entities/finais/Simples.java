package br.com.simplificarest.cnpjbatch.entities.finais;

import java.time.LocalDate;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "simples")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Simples {

    @Id
    private String cnpjBasico;

    private Boolean opcaoSimples;
    private LocalDate dataOpcaoSimples;
    private LocalDate dataExclusaoSimples;
    private Boolean opcaoMei;
    private LocalDate dataOpcaoMei;
    private LocalDate dataExclusaoMei;
}
