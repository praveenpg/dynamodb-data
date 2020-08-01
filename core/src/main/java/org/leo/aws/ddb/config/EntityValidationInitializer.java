package org.leo.aws.ddb.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EntityValidationInitializer {
    @Value("${org.leo.aws.ddb.entities.basePackage:org.leo}")
    private String dtoBasePackage;

    @Bean
    EntityValidationConfig entityValidationConfig() {
        return new EntityValidationConfig(dtoBasePackage);
    }
}
