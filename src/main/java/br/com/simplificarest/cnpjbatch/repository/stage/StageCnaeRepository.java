package br.com.simplificarest.cnpjbatch.repository.stage;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import br.com.simplificarest.cnpjbatch.entities.stage.StgCnae;

@Repository
public interface StageCnaeRepository extends JpaRepository<StgCnae, String> {}