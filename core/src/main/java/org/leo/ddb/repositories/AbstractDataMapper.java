package org.leo.ddb.repositories;


import org.leo.ddb.annotations.PK;
import org.leo.ddb.utils.model.Tuple;

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
        final PK.Type keyType = PK.Type.HASH_KEY;
        return getPKKey(keyType);
    }

    /**
     * @return Range key of the DDB table
     */
    protected String getRangeKey() {
        final PK.Type keyType = PK.Type.RANGE_KEY;
        return getPKKey(keyType);
    }

    @SuppressWarnings("unchecked")
    private String getPKKey(final PK.Type keyType) {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) MapperUtils.ATTRIBUTE_MAPPING_MAP.get(getParameterType().getName());
        final Map<PK.Type, Tuple<String, Field>> pkMapping = fieldMapping.getPrimaryKeyMapping();

        return pkMapping.get(keyType)._2().getName();
    }
}
