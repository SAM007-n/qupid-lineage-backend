package com.lineage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class SqlDependencyExtractorApplication {

    public static void main(String[] args) {
        SpringApplication.run(SqlDependencyExtractorApplication.class, args);
    }
} 