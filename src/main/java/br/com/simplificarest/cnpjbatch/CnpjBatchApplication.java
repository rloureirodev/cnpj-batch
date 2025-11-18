package br.com.simplificarest.cnpjbatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CnpjBatchApplication {

	public static void main(String[] args) {
		System.out.println("Loaded: " + CnpjBatchApplication.class.getClassLoader()
		        .getResource("application.properties"));
		SpringApplication.run(CnpjBatchApplication.class, args);
	}

}


