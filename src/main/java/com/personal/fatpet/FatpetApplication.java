package com.personal.fatpet;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.personal.fatpet"})
public class FatpetApplication {

	public static void main(String[] args) {
		SpringApplication.run(FatpetApplication.class, args);
	}
}
