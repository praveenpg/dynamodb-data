package org.leo.aws.ddb.repositories;


import org.leo.aws.ddb.annotations.DbAttribute;
import org.leo.aws.ddb.annotations.KeyType;
import org.leo.aws.ddb.exceptions.DbException;
import org.leo.aws.ddb.model.PrimaryKey;
import org.leo.aws.ddb.utils.DbUtils;
import org.leo.aws.ddb.utils.Tuple;
import org.leo.aws.ddb.utils.Tuple4;
import org.leo.aws.ddb.utils.Tuples;
import org.leo.aws.ddb.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

interface DataMapper<T> {
    @SuppressWarnings("unused")
    Logger LOGGER = LoggerFactory.getLogger(DataMapper.class);

    /**
     *
     * @param attributeValues Attribute values returned by the AWS DynamoDB SDK
     * @return The entity object representing the attribute value passed.
     */
    @SuppressWarnings("unchecked")
    default T mapFromValue(final Map<String, AttributeValue> attributeValues) {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) MapperUtils.getInstance().getAttributeMappingMap().get(getParameterType().getName());
        final Constructor<T> constructor = fieldMapping.getConstructor();
        final T mappedObject = Utils.constructObject(constructor);
        final Map<String, Tuple<Field, DbAttribute>> fieldMap = fieldMapping.getMappedFields();

        attributeValues.entrySet().stream()
                .filter(a -> (fieldMap.get(a.getKey()) != null))
                .forEach(entry -> ReflectionUtils.setField(fieldMap.get(entry.getKey())._1(), mappedObject,
                        DbUtils.attributeToModel(fieldMap.get(entry.getKey())._1()).call(entry.getValue())));

        return mappedObject;
    }

    /**
     * Method constructs Attribute name to value map that is required by the AWS DDB SDK to make updates.
     * @param input Entity Object
     * @return Attribute name to value mapping.
     */
    @SuppressWarnings("unchecked")
    default Map<String, AttributeValue> mapToValue(final T input) {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) MapperUtils.getInstance().getAttributeMappingMap().get(getParameterType().getName());
        final Map<String, Tuple<Field, DbAttribute>> fieldMap = fieldMapping.getMappedFields();

        final Map<String, AttributeValue> ret = fieldMapping.getMappedFields().keySet().stream()
                .map(key -> Tuples.of(key, ReflectionUtils.getField(fieldMap.get(key)._1(), input)))
                .filter(a -> a._2() != null)
                .map(t -> Tuples.of(t._1(), DbUtils.modelToAttributeValue(fieldMap.get(t._1())._1(), t._2()).call(AttributeValue.builder()).build()))
                .collect(Collectors.toMap(Tuple::_1, Tuple::_2));

        return ret;
    }

    @SuppressWarnings("unchecked")
    default Tuple<Field, DbAttribute> getVersionedAttribute() {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) MapperUtils.getInstance().getAttributeMappingMap().get(getParameterType().getName());

        return fieldMapping.getVersionAttributeField();
    }

    /**
     * Returns Attribute name to value mapping for the primary key object
     * @param primaryKey Primary Key representing a record.
     * @return Attribute name to value mapping
     */
    default Map<String, AttributeValue> getPrimaryKey(final PrimaryKey primaryKey) {
        final Map<String, AttributeValue> key = new HashMap<>();

        //TODO Handle partitionKey value of non-string data types
        key.put(primaryKey.getHashKeyName(), getAttributeBuilderFunctionForKeys(primaryKey.getHashKeyValue())
                .apply(AttributeValue.builder()).build());

        if(!StringUtils.isEmpty(primaryKey.getRangeKeyName())) {
            key.put(primaryKey.getRangeKeyName(), getAttributeBuilderFunctionForKeys(primaryKey.getRangeKeyValue())
                    .apply(AttributeValue.builder()).build());
        }

        return key;
    }

    default Function<AttributeValue.Builder, AttributeValue.Builder> getAttributeBuilderFunctionForKeys(final Object keyValue) {
        final Function<AttributeValue.Builder, AttributeValue.Builder> builderFn;

        if(keyValue instanceof String) {
            builderFn = builder -> builder.s((String) keyValue);
        } else if(keyValue instanceof Number) {
            builderFn = builder -> builder.n(Utils.getUnformattedNumber((Number) keyValue));
        } else if(keyValue.getClass().isEnum()){
            builderFn = builder -> builder.s(Utils.invokeMethod(keyValue, "name"));
        }else {
            throw new DbException("Only string and number type supported for hash/range key values");
        }

        return builderFn;
    }

    /**
     * Generate the primary key (hash key/range key combination) from the entity passed.
     */
    @SuppressWarnings("unchecked")
    default PrimaryKey createPKFromItem(final T item) {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) MapperUtils.getInstance().getAttributeMappingMap().get(getParameterType().getName());
        final Map<KeyType, Tuple<String, Field>> pkMap = fieldMapping.getPrimaryKeyMapping();
        final Tuple<String, Field> hashKeyTuple = pkMap.get(KeyType.HASH_KEY);
        final Tuple<String, Field> rangeKeyTuple = pkMap.get(KeyType.RANGE_KEY);
        final PrimaryKey.Builder pkBuilder = PrimaryKey.builder();
        final Object hashKeyValue = DbUtils.serializeValue(hashKeyTuple._2(), ReflectionUtils.getField(hashKeyTuple._2(), item));

        pkBuilder
                .hashKeyValue((hashKeyValue instanceof Long || hashKeyValue instanceof Integer) ? hashKeyValue : String.valueOf(hashKeyValue))
                .hashKeyName(hashKeyTuple._1());

        if(rangeKeyTuple != null) {
            final Object rangeKeyValue = DbUtils.serializeValue(rangeKeyTuple._2(), ReflectionUtils.getField(rangeKeyTuple._2(), item));

            pkBuilder.rangeKeyValue((rangeKeyValue instanceof Long || rangeKeyValue instanceof Integer) ? rangeKeyValue : String.valueOf(rangeKeyValue))
                    .rangeKeyName(rangeKeyTuple._1());
        }

        return pkBuilder.build();
    }

    @SuppressWarnings("unused")
    default void applyTTLLogic(final T item, final Map<String, AttributeValue> attributeValueMap) {
        throw new UnsupportedOperationException();
    }

    /**
     *
     * @return Entity class
     */
    Class<T> getParameterType();

    /**
     *
     * @return name of the DDB table name the entity class represents
     */
    default String tableName() {
        return MapperUtils.getInstance().getAttributeMappingMap().get(getParameterType().getName()).getTableName();
    }

    /**
     *
     * @param input Entity object
     * @return Field values along with the attribute name in the DDB table to which it is mapped to.
     */
    default Stream<Tuple4<String,Object, Field, DbAttribute>> getMappedValues(final T input) {
        final String parameterType = getParameterType().getName();
        return MapperUtils.getInstance().getMappedValues(input, parameterType);
    }

    /**
     *
     * @return Primary key mapping
     */
    default Map<KeyType, Tuple<String, Field>> getPKMapping() {
        return MapperUtils.getInstance().getAttributeMappingMap().get(getParameterType().getName()).getPrimaryKeyMapping();
    }
}
