package br.com.simplificarest.cnpjbatch.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import br.com.simplificarest.cnpjbatch.entities.finais.Estabelecimento;

public interface EstabelecimentoRepository extends JpaRepository<Estabelecimento, String> { }
