package org.leo.ddb.repositories;



import org.leo.ddb.annotations.MappedBy;
import org.leo.ddb.annotations.PK;
import org.leo.ddb.utils.model.Tuple;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Map;

@SuppressWarnings({"unused", "WeakerAccess"})
final class AttributeMapper<T> {
    private final Class<T> mappedClass;
    private final Map<String, Tuple<Field, MappedBy>> mappedFields;
    private final Constructor<T> constructor;
    private final Map<PK.Type, Tuple<String, Field>> primaryKeyMapping;
    private final Map<String, GSI> globalSecondaryIndexMap;
    private final String tableName;
    private final Tuple<String, Field> dateUpdatedField;
    private final Tuple<String, Field> dateCreatedField;
    private final Tuple<Field, MappedBy> versionAttributeField;

    private AttributeMapper(final Class<T> mappedClass,
                            final Map<String, Tuple<Field, MappedBy>> mappedFields,
                            final Constructor<T> constructor,
                            final Map<PK.Type, Tuple<String, Field>> primaryKeyMapping,
                            final String tableName,
                            final Tuple<String, Field> dateUpdatedField,
                            final Tuple<String, Field> dateCreatedField,
                            final Map<String, GSI> globalSecondaryIndexMap,
                            final Tuple<Field, MappedBy> versionAttributeField) {

        this.mappedClass = mappedClass;
        this.mappedFields = mappedFields;
        this.constructor = constructor;
        this.primaryKeyMapping = primaryKeyMapping;
        this.tableName = tableName;
        this.dateUpdatedField = dateUpdatedField;
        this.dateCreatedField = dateCreatedField;
        this.globalSecondaryIndexMap = globalSecondaryIndexMap;
        this.versionAttributeField = versionAttributeField;
    }

    public Class<T> getMappedClass() {
        return mappedClass;
    }

    public Constructor<T> getConstructor() {
        return constructor;
    }

    public Map<PK.Type, Tuple<String, Field>> getPrimaryKeyMapping() {
        return primaryKeyMapping;
    }

    public String getTableName() {
        return tableName;
    }

    public Tuple<String, Field> getDateUpdatedField() {
        return dateUpdatedField;
    }

    public Tuple<String, Field> getDateCreatedField() {
        return dateCreatedField;
    }


    public Map<String, GSI> getGlobalSecondaryIndexMap() {
        return globalSecondaryIndexMap;
    }

    public Map<String, Tuple<Field, MappedBy>> getMappedFields() {
        return mappedFields;
    }

    public Tuple<Field, MappedBy> getVersionAttributeField() {
        return versionAttributeField;
    }

    public static<T> Builder<T> builder() {
        return new AttributeMapperBuilder<>();
    }

    @SuppressWarnings("UnusedReturnValue")
    public interface Builder<T> {
        Builder<T> mappedClass(Class<T> mappedClass);

        Builder<T> mappedFields(Map<String, Tuple<Field, MappedBy>> mappedFields);

        Builder<T> constructor(Constructor<T> constructor);
        Builder<T> primaryKeyMapping(Map<PK.Type, Tuple<String, Field>> primaryKeyMapping);
        Builder<T> tableName(String tableName);
        Builder<T> dateUpdatedField(Tuple<String, Field> dateUpdatedField);
        Builder<T> dateCreatedField(Tuple<String, Field> dateCreatedField);
        Builder<T> globalSecondaryIndexMap(Map<String, GSI> globalSecondaryIndexMap);
        Builder<T> versionAttributeField(Tuple<Field, MappedBy> versionAttributeField);

        AttributeMapper<T> build();
    }

    private static class AttributeMapperBuilder<T> implements Builder<T> {
        private Class<T> mappedClass;
        private Map<String, Tuple<Field, MappedBy>> mappedFields;
        private Constructor<T> constructor;
        private Map<PK.Type, Tuple<String, Field>> primaryKeyMapping;
        private String tableName;
        private Tuple<String, Field> dateUpdatedField;
        private Tuple<String, Field> dateCreatedField;
        private Map<String, GSI> globalSecondaryIndexMap;
        private Tuple<Field, MappedBy> versionAttributeField;

        @Override
        public Builder<T> mappedClass(final Class<T> mappedClass) {
            this.mappedClass = mappedClass;
            return this;
        }

        @Override
        public Builder<T> mappedFields(final Map<String, Tuple<Field, MappedBy>> mappedFields) {
            this.mappedFields = mappedFields;
            return this;
        }

        @Override
        public Builder<T> constructor(final Constructor<T> constructor) {
            this.constructor = constructor;
            return this;
        }

        @Override
        public Builder<T> primaryKeyMapping(final Map<PK.Type, Tuple<String, Field>> primaryKeyMapping) {
            this.primaryKeyMapping = primaryKeyMapping;
            return this;
        }

        @Override
        public Builder<T> tableName(final String tableName) {
            this.tableName = tableName;
            return this;
        }

        @Override
        public Builder<T> dateUpdatedField(final Tuple<String, Field> dateUpdatedField) {
            this.dateUpdatedField = dateUpdatedField;
            return this;
        }

        @Override
        public Builder<T> dateCreatedField(final Tuple<String, Field> dateCreatedField) {
            this.dateCreatedField = dateCreatedField;
            return this;
        }

        @Override
        public Builder<T> globalSecondaryIndexMap(final Map<String, GSI> globalSecondaryIndexMap) {
            this.globalSecondaryIndexMap = globalSecondaryIndexMap;

            return this;
        }

        @Override
        public Builder<T> versionAttributeField(final Tuple<Field, MappedBy> versionAttributeField) {
            this.versionAttributeField = versionAttributeField;

            return this;
        }

        @Override
        public AttributeMapper<T> build() {
            return new AttributeMapper<>(mappedClass,
                    mappedFields,
                    constructor,
                    primaryKeyMapping,
                    tableName,
                    dateUpdatedField,
                    dateCreatedField,
                    globalSecondaryIndexMap, versionAttributeField);
        }
    }
}
