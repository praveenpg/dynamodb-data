package org.leo.ddb.repositories;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Map;

@Configuration
class DataMapperConfigInitializer {
    @Value("${sfly.ddb.entities.basePackage:com.shutterfly}")
    private String dtoBasePackage;

    private final Map<Class, DataMapper> dataMapperMap;
    private final Environment environment;


    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    public DataMapperConfigInitializer(@Qualifier("dataMapperMap") final Map<Class, DataMapper> dataMapperMap, final Environment environment) {
        this.dataMapperMap = dataMapperMap;
        this.environment = environment;
    }

    @Bean
    DataMapperConfigCleanUp dataMapperConfigCleanUp() {
        return new DataMapperConfigCleanUp(dtoBasePackage, dataMapperMap, environment);
    }
}
