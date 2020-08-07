package org.leo.aws.ddb.repositories;

import org.leo.aws.ddb.annotations.KeyType;

import java.lang.reflect.Field;

class EmbeddedIdMapping {
    private final String fieldName;
    private final Field field;
    private final Class<?> embeddedIdClass;
    private final KeyType keyType;
    private final String embeddedIdFieldName;
    private final Field embeddedIdField;

    private EmbeddedIdMapping(String fieldName, Field field, Class<?> embeddedIdClass, KeyType keyType, String embeddedIdFieldName, Field embeddedIdField) {
        this.fieldName = fieldName;
        this.field = field;
        this.embeddedIdClass = embeddedIdClass;
        this.keyType = keyType;
        this.embeddedIdFieldName = embeddedIdFieldName;
        this.embeddedIdField = embeddedIdField;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Field getField() {
        return field;
    }

    public Class<?> getEmbeddedIdClass() {
        return embeddedIdClass;
    }

    public KeyType getKeyType() {
        return keyType;
    }

    public String getEmbeddedIdFieldName() {
        return embeddedIdFieldName;
    }

    public Field getEmbeddedIdField() {
        return embeddedIdField;
    }

    public static Builder builder() {
        return new BuilderImpl();
    }

    public interface Builder {
        Builder fieldName(String fieldName);
        Builder field(Field field);
        Builder embeddedIdClass(Class<?> embeddedIdClass);
        Builder keyType(KeyType keyType);
        Builder embeddedIdFieldName(String embeddedIdFieldName);
        Builder embeddedIdField(final Field embeddedIdField);

        EmbeddedIdMapping build();
    }

    private static class BuilderImpl implements Builder {

        private String fieldName;
        private Field field;
        private Class<?> embeddedIdClass;
        private KeyType keyType;
        private String embeddedIdFieldName;
        private Field embeddedIdField;

        @Override
        public Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        @Override
        public Builder field(Field field) {
            this.field = field;
            return this;
        }

        @Override
        public Builder embeddedIdClass(Class<?> embeddedIdClass) {
            this.embeddedIdClass = embeddedIdClass;
            return this;
        }

        @Override
        public Builder keyType(KeyType keyType) {
            this.keyType = keyType;
            return this;
        }

        @Override
        public Builder embeddedIdFieldName(String embeddedIdFieldName) {
            this.embeddedIdFieldName = embeddedIdFieldName;
            return this;
        }

        @Override
        public Builder embeddedIdField(Field embeddedIdField) {
            this.embeddedIdField = embeddedIdField;
            return this;
        }

        @Override
        public EmbeddedIdMapping build() {
            return new EmbeddedIdMapping(fieldName, field, embeddedIdClass, keyType, embeddedIdFieldName, embeddedIdField);
        }
    }
}
