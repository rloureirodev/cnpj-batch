package br.com.simplificarest.cnpjbatch.repository.stage;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import br.com.simplificarest.cnpjbatch.entities.stage.StgMunicipio;

@Repository
public interface StageMunicipioRepository extends JpaRepository<StgMunicipio, String> {}