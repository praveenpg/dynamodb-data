package org.leo.ddb.config;



import org.leo.ddb.annotations.DDBTable;
import org.leo.ddb.exceptions.DbException;
import org.reflections.Reflections;

import javax.annotation.PostConstruct;
import java.lang.reflect.Constructor;
import java.text.MessageFormat;
import java.util.Set;

public class EntityValidationConfig {
    private final String dtoBasePackage;

    public EntityValidationConfig(final String dtoBasePackage) {
        this.dtoBasePackage = dtoBasePackage;
    }

    @PostConstruct
    public void validateEntities() {
        final Reflections reflections = new Reflections(dtoBasePackage);
        final Set<Class<?>> entityClasses = reflections.getTypesAnnotatedWith(DDBTable.class);

        entityClasses.forEach(entityClass -> {
            try {
                final Constructor constructor = entityClass.getDeclaredConstructor();

                if(constructor == null) {
                    throw new NoSuchMethodException();
                }
            } catch (final NoSuchMethodException e) {
                throw new DbException(MessageFormat.format(
                        "Entity [{0}] does not have a default constructor. All entities should have at least one default no-args constructor",
                        entityClass.getName()));
            }
        });
    }
}
