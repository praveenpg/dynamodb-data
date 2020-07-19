package org.leo.ddb.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EntityValidationInitializer {
    @Value("${sfly.ddb.entities.basePackage:com.shutterfly}")
    private String dtoBasePackage;

    @Bean
    EntityValidationConfig entityValidationConfig() {
        return new EntityValidationConfig(dtoBasePackage);
    }
}
