package org.leo.aws.ddb.repositories;


import org.leo.aws.ddb.annotations.DbAttribute;
import org.leo.aws.ddb.annotations.KeyType;
import org.leo.aws.ddb.model.PrimaryKey;
import org.leo.aws.ddb.utils.DbUtils;
import org.leo.aws.ddb.utils.model.Tuple;
import org.leo.aws.ddb.utils.model.Tuple4;
import org.leo.aws.ddb.utils.model.Tuples;
import org.leo.aws.ddb.utils.model.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

interface DataMapper<T> {
    @SuppressWarnings({"unused", "RedundantSuppression"})
    Logger LOGGER = LoggerFactory.getLogger(DataMapper.class);

    /**
     *
     * @param attributeValues Attribute values returned by the AWS DynamoDB SDK
     * @return The entity object representing the attribute value passed.
     */
    @SuppressWarnings("unchecked")
    default T mapFromValue(final Map<String, AttributeValue> attributeValues) {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) MapperUtils.ATTRIBUTE_MAPPING_MAP.get(getParameterType().getName());
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
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) MapperUtils.ATTRIBUTE_MAPPING_MAP.get(getParameterType().getName());
        final Map<String, Tuple<Field, DbAttribute>> fieldMap = fieldMapping.getMappedFields();

        return fieldMapping.getMappedFields().keySet().stream()
                           .map(key -> Tuples.of(key, ReflectionUtils.getField(fieldMap.get(key)._1(), input)))
                           .filter(a -> a._2() != null)
                           .map(t -> Tuples.of(t._1(), DbUtils.modelToAttributeValue(fieldMap.get(t._1())._1(), t._2()).call(AttributeValue.builder()).build()))
                           .collect(Collectors.toMap(Tuple::_1, Tuple::_2));
    }

    @SuppressWarnings("unchecked")
    default Tuple<Field, DbAttribute> getVersionedAttribute() {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) MapperUtils.ATTRIBUTE_MAPPING_MAP.get(getParameterType().getName());

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
        key.put(primaryKey.getHashKeyName(), AttributeValue.builder().s((String) primaryKey.getHashKeyValue()).build());

        if(!StringUtils.isEmpty(primaryKey.getRangeKeyName())) {
            key.put(primaryKey.getRangeKeyName(), AttributeValue.builder().s((String) primaryKey.getRangeKeyValue()).build());
        }

        return key;
    }

    /**
     * Generate the primary key (hash key/range key combination) from the entity passed.
     */
    @SuppressWarnings("unchecked")
    default PrimaryKey createPKFromItem(final T item) {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) MapperUtils.ATTRIBUTE_MAPPING_MAP.get(getParameterType().getName());
        final Map<KeyType, Tuple<String, Field>> pkMap = fieldMapping.getPrimaryKeyMapping();
        final Tuple<String, Field> hashKeyTuple = pkMap.get(KeyType.HASH_KEY);
        final Tuple<String, Field> rangeKeyTuple = pkMap.get(KeyType.RANGE_KEY);

        return PrimaryKey.builder()
                .hashKeyValue(String.valueOf(DbUtils.serializeValue(hashKeyTuple._2(), ReflectionUtils.getField(hashKeyTuple._2(), item))))
                .hashKeyName(hashKeyTuple._1())
                .rangeKeyValue(String.valueOf(DbUtils.serializeValue(rangeKeyTuple._2(), ReflectionUtils.getField(rangeKeyTuple._2(), item))))
                .rangeKeyName(rangeKeyTuple._1()).build();
    }

    @SuppressWarnings({"unused", "RedundantSuppression"})
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
        return MapperUtils.ATTRIBUTE_MAPPING_MAP.get(getParameterType().getName()).getTableName();
    }

    /**
     *
     * @param input Entity object
     * @return Field values along with the attribute name in the DDB table to which it is mapped to.
     */
    default Stream<Tuple4<String,Object, Field, DbAttribute>> getMappedValues(final T input) {
        final String parameterType = getParameterType().getName();
        return MapperUtils.getMappedValues(input, parameterType);
    }

    /**
     *
     * @return Primary key mapping
     */
    default Map<KeyType, Tuple<String, Field>> getPKMapping() {
        return MapperUtils.ATTRIBUTE_MAPPING_MAP.get(getParameterType().getName()).getPrimaryKeyMapping();
    }
}
