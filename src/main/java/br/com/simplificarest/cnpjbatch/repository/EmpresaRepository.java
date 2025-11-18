package br.com.simplificarest.cnpjbatch.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import br.com.simplificarest.cnpjbatch.entities.finais.Empresa;

public interface EmpresaRepository extends JpaRepository<Empresa, String> { }
