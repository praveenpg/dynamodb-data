package org.leo.aws.ddb.repositories;


import org.leo.aws.ddb.utils.ApplicationContextUtils;
import org.leo.aws.ddb.utils.Tuple;
import org.leo.aws.ddb.utils.Tuples;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings({"SpringFacetCodeInspection", "rawtypes"})
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
        try {
            final Map<String, DataMapper> beans = applicationContext.getBeansOfType(DataMapper.class);
            final Method method = ApplicationContextUtils.class.getDeclaredMethod("init", ApplicationContext.class, Environment.class);

            method.invoke(null, applicationContext, environment);

            return new HashMap<>(beans.values().stream()
                    .map(bean -> Tuples.of(getParameterType(bean), bean))
                    .collect(Collectors.toMap(Tuple::_1, Tuple::_2)));
        } catch (final Exception ex) {
            throw new BeanInitializationException("Exception while create dataMapperMap", ex);
        }
    }

    private Class getParameterType(final DataMapper<?> dataMapper) {
        return dataMapper.getParameterType();
    }
}
