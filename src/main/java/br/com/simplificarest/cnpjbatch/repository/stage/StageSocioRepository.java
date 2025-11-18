package br.com.simplificarest.cnpjbatch.repository.stage;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import br.com.simplificarest.cnpjbatch.entities.stage.StgSocio;

@Repository
public interface StageSocioRepository extends JpaRepository<StgSocio, String> {}