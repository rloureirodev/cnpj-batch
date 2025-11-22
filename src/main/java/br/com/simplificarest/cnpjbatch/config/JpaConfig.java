package br.com.simplificarest.cnpjbatch.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@EnableJpaRepositories(basePackages = "br.com.simplificarest.cnpjdomain.repository")
@EntityScan(basePackages = "br.com.simplificarest.cnpjdomain.entities")
public class JpaConfig {
}