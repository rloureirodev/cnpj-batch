package br.com.simplificarest.cnpjbatch.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import br.com.simplificarest.cnpjbatch.entities.finais.Socio;

public interface SocioRepository extends JpaRepository<Socio, Long> { }
