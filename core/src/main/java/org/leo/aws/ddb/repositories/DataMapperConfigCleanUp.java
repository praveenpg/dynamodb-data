package org.leo.aws.ddb.repositories;


import org.leo.aws.ddb.annotations.DDBTable;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Set;

@SuppressWarnings({"rawtypes", "unchecked"})
public class DataMapperConfigCleanUp {
    private final String dtoBasePackage;
    private final Map<Class, DataMapper> dataMapperMap;
    private final Environment environment;

    public DataMapperConfigCleanUp(final String dtoBasePackage, final Map<Class, DataMapper> dataMapperMap, final Environment environment) {
        this.dtoBasePackage = dtoBasePackage;
        this.dataMapperMap = dataMapperMap;
        this.environment = environment;
    }

    @PostConstruct
    public void mapDataObjectsWithoutMapper() {
        final Reflections reflections = new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage(
                dtoBasePackage, ClasspathHelper.contextClassLoader(),
                ClasspathHelper.staticClassLoader())));
        final Set<Class<?>> annotated = reflections.getTypesAnnotatedWith(DDBTable.class);

        annotated.forEach(a -> {
            final DataMapper dataMapper = dataMapperMap.get(a);

            if(dataMapper == null) {
                dataMapperMap.put(a, new AbstractDataMapper() {
                    @Override
                    public Class getParameterType() {
                        return a;
                    }
                });
            }
        });

        dataMapperMap.forEach((key, value) -> MapperUtils.INSTANCE.setDbAttributes(value.getParameterType(), environment, value));
    }
}
