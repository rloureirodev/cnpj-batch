package br.com.simplificarest.cnpjbatch.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import br.com.simplificarest.cnpjbatch.entities.finais.Simples;

public interface SimplesRepository extends JpaRepository<Simples, String> { }
