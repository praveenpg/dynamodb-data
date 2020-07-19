package org.leo.ddb.repositories;

import org.leo.ddb.utils.model.ApplicationContextUtils;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.Map;


@SuppressWarnings("unchecked")
final class DataMapperWrapper {
    private DataMapperWrapper(){}

    static <T> DataMapper<T> getDataMapper(final Class<T> paramType) {
        return (DataMapper<T>) getDataMapperMap().get(paramType);
    }

    static DynamoDbAsyncClient getDynamoDbAsyncClient() {
        return ApplicationContextUtils.getBean(DynamoDbAsyncClient.class);
    }

    private static Map<Class<?>, ? extends DataMapper<?>> getDataMapperMap() {
        return (Map<Class<?>, ? extends DataMapper<?>>) ApplicationContextUtils.getBean("dataMapperMap");
    }
}
