package org.leo.aws.ddb.repositories;


import org.leo.aws.ddb.annotations.KeyType;
import org.leo.aws.ddb.utils.model.Tuple;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Map;

@SuppressWarnings({"unused", "WeakerAccess"})
abstract class AbstractDataMapper<T> implements DataMapper<T> {
    @SuppressWarnings("unchecked")
    @Override
    public Class<T> getParameterType() {
        return (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    /**
     *
     * @return Hash key of the DDB table
     */
    protected String getHashKey() {
        final KeyType keyType = KeyType.HASH_KEY;
        return getPKKey(keyType);
    }

    /**
     * @return Range key of the DDB table
     */
    protected String getRangeKey() {
        final KeyType keyType = KeyType.RANGE_KEY;
        return getPKKey(keyType);
    }

    @SuppressWarnings("unchecked")
    private String getPKKey(final KeyType keyType) {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) MapperUtils.ATTRIBUTE_MAPPING_MAP.get(getParameterType().getName());
        final Map<KeyType, Tuple<String, Field>> pkMapping = fieldMapping.getPrimaryKeyMapping();

        return pkMapping.get(keyType)._2().getName();
    }
}
