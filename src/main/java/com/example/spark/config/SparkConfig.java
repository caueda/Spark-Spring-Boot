package com.example.spark.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
@Setter
@ConfigurationProperties
public class SparkConfig {

    private String folder;
    private String var1;
    private String var2;
//    @Value("${externalLocation:'classpath:/'}")
    private String externalLocation;
}

