package org.leo.ddb.repositories;


import org.leo.ddb.utils.model.ApplicationContextUtils;
import org.leo.ddb.utils.model.Tuple;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings("SpringFacetCodeInspection")
@Configuration
public class DataMapperConfig {
    private final ApplicationContext applicationContext;
    private final Environment environment;

    public DataMapperConfig(final ApplicationContext applicationContext, final Environment environment) {
        this.applicationContext = applicationContext;
        this.environment = environment;
    }

    @Bean
    public Map<Class, DataMapper> dataMapperMap() {
        final Map<String, DataMapper> beans = applicationContext.getBeansOfType(DataMapper.class);

        ApplicationContextUtils.setApplicationContext(applicationContext);
        ApplicationContextUtils.setEnvironment(environment);

        return new HashMap<>(beans.values().stream()
                .map(bean -> Tuple.of(getParameterType(bean), bean))
                .collect(Collectors.toMap(Tuple::_1, Tuple::_2)));
    }

    private Class getParameterType(final DataMapper<?> dataMapper) {
        return dataMapper.getParameterType();
    }
}
