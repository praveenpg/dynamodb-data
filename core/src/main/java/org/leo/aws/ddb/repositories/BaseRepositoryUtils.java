package org.leo.aws.ddb.repositories;

import com.google.common.collect.ImmutableMap;
import org.leo.aws.ddb.annotations.DbAttribute;
import org.leo.aws.ddb.annotations.ProjectionType;
import org.leo.aws.ddb.exceptions.DbException;
import org.leo.aws.ddb.exceptions.OptimisticLockFailureException;
import org.leo.aws.ddb.model.PrimaryKey;
import org.leo.aws.ddb.model.UpdateItem;
import org.leo.aws.ddb.utils.DbUtils;
import org.leo.aws.ddb.utils.Expr;
import org.leo.aws.ddb.utils.exceptions.Issue;
import org.leo.aws.ddb.utils.model.Tuple;
import org.leo.aws.ddb.utils.model.Tuple3;
import org.leo.aws.ddb.utils.model.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.functions.Action2;
import rx.functions.Func0;
import rx.functions.Func1;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.paginators.QueryPublisher;
import software.amazon.awssdk.services.dynamodb.paginators.ScanPublisher;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class BaseRepositoryUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseRepositoryUtils.class);


    static UpdateItemResponse handleUpdateItemException(final PrimaryKey primaryKey, final Throwable e, final String tableName) {
        LOGGER.debug(MessageFormat.format("Record with the following primary key [{0}] exists in table [{1}]", primaryKey, tableName), e);
        if (e instanceof CompletionException && e.getCause() instanceof ConditionalCheckFailedException) {
            throw new OptimisticLockFailureException(e.getCause());
        } else if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        } else {
            throw new DbException(Issue.UNKNOWN_ERROR.name() + " - handleUpdateItemException", e);
        }
    }

    static <T> Integer setVersion(final T item, final Tuple<Field, DbAttribute> versionedAttribute, final UpdateItemRequest.Builder updateItemRequestBuilder) {
        final Integer version;
        if (versionedAttribute != null) {
            final Number versionNum = (Number) ReflectionUtils.getField(versionedAttribute._1(), item);
            final Class<?> versionFieldType;

            version = versionNum != null ? versionNum.intValue() + 1 : 0;
            versionFieldType = versionedAttribute._1().getType();

            if (versionNum == null) {
                updateItemRequestBuilder.expected(ImmutableMap.of(versionedAttribute._2().value(), ExpectedAttributeValue.builder().exists(false).build()));
            } else {
                updateItemRequestBuilder.expected(ImmutableMap.of(versionedAttribute._2().value(), ExpectedAttributeValue.builder()
                        .value(AttributeValue.builder().n(String.valueOf(versionNum.intValue())).build()).build()));
            }

            if (versionFieldType == Integer.class) {
                ReflectionUtils.setField(versionedAttribute._1(), item, version);
            } else {
                ReflectionUtils.setField(versionedAttribute._1(), item, new Long(version));
            }
        } else {
            version = null;
        }

        return version;
    }


    /**
     * @param dbRequestFunc  Function to perform DB write actions
     * @param returnItemFunc Function that return items
     * @return Future
     */
    static <T> Flux<T> batchWriteRequest(final Func1<DataMapper<T>, Stream<WriteRequest>> dbRequestFunc,
                                         final Func0<List<T>> returnItemFunc, final DataMapper<T> dataMapper) {


        return Mono.fromFuture(processBatchWriteRequest(returnItemFunc, ImmutableMap.of(dataMapper.tableName(), dbRequestFunc.call(dataMapper).collect(Collectors.toList())))).flatMapMany(Flux::fromIterable);
    }

    static <T> Mono<T> updateItem(final PrimaryKey primaryKey,
                                  final Map<String, Object> updatedValues,
                                  final Class<T> parameterType,
                                  final DataMapper<T> dataMapper,
                                  final T item) {

        final Tuple<Field, DbAttribute> versionedAttribute = dataMapper.getVersionedAttribute();
        final UpdateItemRequest.Builder updateItemRequestBuilder = UpdateItemRequest.builder();
        final Map<String, Tuple3<String, Field, DbAttribute>> mappedFields = MapperUtils.getMappedValues(parameterType.getName())
                .collect(Collectors.toMap(Tuple3::_1, b -> b));
        final Stream<Tuple<String, AttributeValueUpdate>> mappedValues = updatedValues.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(primaryKey.getHashKeyName()))
                .filter(entry -> !entry.getKey().equals(primaryKey.getRangeKeyName()))
                .peek(a -> {
                    if (mappedFields.get(a.getKey()) != null) {
                        DbUtils.checkForNullFields(mappedFields.get(a.getKey())._3(), a.getValue(), mappedFields.get(a.getKey())._1());
                    }
                })
                .map(a -> {
                    if (mappedFields.get(a.getKey()) != null) {
                        return Tuple.of(a.getKey(), DbUtils.modelToAttributeUpdateValue(mappedFields.get(a.getKey())._2(), a.getValue()).call(AttributeValueUpdate.builder()).build());
                    } else {
                        return Tuple.of(a.getKey(), AttributeValueUpdate.builder().value(AttributeValue.builder().s(String.valueOf(a.getValue())).build()).build());
                    }
                });
        final Map<String, AttributeValueUpdate> mappedUpdateValuesTmp = mappedValues.collect(Collectors.toMap(Tuple::_1, Tuple::_2));
        final Map<String, AttributeValueUpdate> mappedUpdateValues;
        final Integer version = BaseRepositoryUtils.setVersion(item, versionedAttribute, updateItemRequestBuilder);

        if (versionedAttribute != null) {
            mappedUpdateValues = ImmutableMap.<String, AttributeValueUpdate>builder()
                    .putAll(mappedUpdateValuesTmp)
                    .put(versionedAttribute._2().value(), AttributeValueUpdate.builder().value(AttributeValue.builder().n(String.valueOf(version)).build()).build())
                    .build();
        } else {
            mappedUpdateValues = mappedUpdateValuesTmp;
        }

        return Mono.fromFuture(DataMapperWrapper.getDynamoDbAsyncClient().updateItem(updateItemRequestBuilder
                .tableName(dataMapper.tableName())
                .key(dataMapper.getPrimaryKey(primaryKey))
                .attributeUpdates(mappedUpdateValues)
                .returnValues(ReturnValue.ALL_NEW)
                .build())
                .thenApplyAsync(updateItemResponse -> dataMapper.mapFromValue(updateItemResponse.attributes())));
    }

    static <T> Flux<T> findByGlobalSecondaryIndex(final String indexName,
                                                  final Object hashKeyValueObj,
                                                  final Object rangeKeyValue,
                                                  final Function<T, PrimaryKey> primaryKeyFunc,
                                                  final Supplier<Class<T>> dataClassFunc,
                                                  final Function<List<PrimaryKey>, Flux<T>> findByPrimaryKeyFunc,
                                                  final Expr filterExpression) {

        if (hashKeyValueObj instanceof String) {
            final String hashKeyValue;
            final Tuple<ProjectionType, QueryPublisher> queryResponseTuple;
            final Flux<T> returnedDataFromDb;
            final Flux<T> returnData;
            final Class<T> dataClass = dataClassFunc.get();

            if (rangeKeyValue != null && !(rangeKeyValue instanceof String)) {
                throw new DbException("Currently only String types are supported for sortKey Values");
            }

            hashKeyValue = (String) hashKeyValueObj;

            queryResponseTuple = RepositoryUtils.getDataFromIndex(indexName, hashKeyValue, Optional.ofNullable(rangeKeyValue), dataClass, filterExpression);

            returnedDataFromDb = Flux
                    .from(queryResponseTuple._2())
                    .flatMapIterable(QueryResponse::items)
                    .map(a -> DataMapperWrapper.getDataMapper(dataClass).mapFromValue(a));

            if (queryResponseTuple._1() == ProjectionType.ALL) {
                returnData = returnedDataFromDb;
            } else {
                returnData = returnedDataFromDb
                        .map(primaryKeyFunc)
                        .collectList()
                        .flatMapMany(primaryKeys -> !CollectionUtils.isEmpty(primaryKeys) ? findByPrimaryKeyFunc.apply(primaryKeys) : Flux.empty());
            }

            return returnData;
        } else {
            throw new DbException("Currently only String types are supported for hashKey Values");
        }
    }

    static <T> Flux<T> findAll(int pageSize, final Supplier<Class<T>> dataClassFunc) {
        return findAll(null, pageSize, dataClassFunc);
    }

    static <T> Flux<T> findAll(final Expr expr, int pageSize, final Supplier<Class<T>> dataClassFunc) {
        final DataMapper<T> dataMapper = DataMapperWrapper.getDataMapper(dataClassFunc.get());
        final ScanRequest.Builder scanRequestBuilder = ScanRequest
                .builder()
                .tableName(dataMapper.tableName())
                .limit(pageSize);
        final ScanPublisher scanPublisher;

        if (Objects.nonNull(expr)) {
            final Map<String, String> attNameMap = expr.attributeNameMap();
            final Map<String, AttributeValue> attValueMap = expr.attributeValueMap();

            scanRequestBuilder.filterExpression(expr.expression());

            if (!CollectionUtils.isEmpty(attNameMap)) {
                scanRequestBuilder.expressionAttributeNames(attNameMap);
            }

            if (!CollectionUtils.isEmpty(attValueMap)) {
                scanRequestBuilder.expressionAttributeValues(attValueMap);
            }
        }

        scanPublisher = DataMapperWrapper.getDynamoDbAsyncClient().scanPaginator(scanRequestBuilder.build());

        return Flux.from(scanPublisher)
                .flatMapIterable(ScanResponse::items)
                .map(dataMapper::mapFromValue);
    }

    static <T> Mono<T> findByPrimaryKey(final PrimaryKey primaryKey, final Supplier<Class<T>> dataClassFunc) {
        final DataMapper<T> dataMapper = DataMapperWrapper.getDataMapper(dataClassFunc.get());
        final GetItemRequest getItemRequest = GetItemRequest.builder()
                .key(dataMapper.getPrimaryKey(primaryKey))
                .tableName(dataMapper.tableName()).build();

        return Mono.fromCompletionStage(DataMapperWrapper.getDynamoDbAsyncClient().getItem(getItemRequest))
                .flatMap(resp -> resp.item().isEmpty() ? Mono.empty() : Mono.just(dataMapper.mapFromValue(resp.item())));
    }

    static <T> Flux<T> findByPrimaryKeys(final List<PrimaryKey> primaryKeys, final Supplier<Class<T>> dataClassFunc) {
        final DataMapper<T> dataMapper = DataMapperWrapper.getDataMapper(dataClassFunc.get());
        final KeysAndAttributes attributes = KeysAndAttributes.builder().keys(primaryKeys.stream()
                .map(dataMapper::getPrimaryKey)
                .collect(Collectors.toList())).build();
        final Map<String, KeysAndAttributes> andAttributesMap = new HashMap<>();
        final BatchGetItemRequest request;

        andAttributesMap.put(dataMapper.tableName(), attributes);
        request = BatchGetItemRequest.builder().requestItems(andAttributesMap).build();

        return Flux
                .from(DataMapperWrapper.getDynamoDbAsyncClient().batchGetItemPaginator(request))
                .flatMapIterable(resp -> resp.responses().get(dataMapper.tableName()))
                .map(dataMapper::mapFromValue);
    }

    static <T> CompletableFuture<T> saveItem(final T item, final boolean upsert, final Action2<T, Map<String, AttributeValue>> ttlAction, final DataMapper<T> dataMapper) {
        final PrimaryKey primaryKey = dataMapper.createPKFromItem(item);
        final String tableName = dataMapper.tableName();
        final Map<String, AttributeValue> attributeValues;
        final PutItemRequest putItemRequest;
        final PutItemRequest.Builder builder;
        final Tuple<Field, DbAttribute> versionedAttribute = dataMapper.getVersionedAttribute();

        builder = PutItemRequest.builder()
                .tableName(dataMapper.tableName());

        if (!upsert) {
            if (versionedAttribute != null) {
                ReflectionUtils.setField(versionedAttribute._1(), item, BigInteger.ZERO.intValue());
            }

            builder.expected(ImmutableMap.of(
                    primaryKey.getHashKeyName(), ExpectedAttributeValue.builder().exists(false).build(),
                    primaryKey.getRangeKeyName(), ExpectedAttributeValue.builder().exists(false).build()));
        } else {
            if (versionedAttribute != null) {
                final Number versionNum = (Number) ReflectionUtils.getField(versionedAttribute._1(), item);
                final int version;

                if (versionNum == null) {
                    version = BigInteger.ZERO.intValue();

                    builder.expected(ImmutableMap.of(versionedAttribute._2().value(), ExpectedAttributeValue.builder().exists(false).build()));
                } else {
                    version = versionNum.intValue() + 1;

                    builder.expected(ImmutableMap.of(versionedAttribute._2().value(),
                            ExpectedAttributeValue.builder().attributeValueList(AttributeValue.builder().n(String.valueOf(versionNum)).build()).build()));
                }

                ReflectionUtils.setField(versionedAttribute._1(), item, version);
            }
        }

        attributeValues = dataMapper.mapToValue(item);

        ttlAction.call(item, attributeValues);

        builder.item(attributeValues);

        putItemRequest = builder.build();

        return DataMapperWrapper.getDynamoDbAsyncClient().putItem(putItemRequest).thenApplyAsync(putItemResponse -> item)
                .exceptionally(e -> handleCreateItemException(primaryKey, e, tableName));
    }

    static <T> T handleCreateItemException(final PrimaryKey primaryKey, final Throwable e, final String tableName) {
        LOGGER.error(MessageFormat.format("Record with the following primary key [{0}] exists in table [{1}]", primaryKey, tableName), e);

        if (e instanceof CompletionException && e.getCause() instanceof ConditionalCheckFailedException) {
            throw new DbException(Issue.RECORD_ALREADY_EXISTS.name(), e);
        } else if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        } else {
            throw new DbException(Issue.UNKNOWN_ERROR.name() + " - handleCreateItemException", e);
        }
    }

    static <T> Mono<T> updateItem(final T item, final Function<T, PrimaryKey> primaryKeyFunc, final Supplier<Class<T>> dataClassFunc) {
        final PrimaryKey primaryKey = primaryKeyFunc.apply(item);
        final Class<T> dataClass = dataClassFunc.get();
        final DataMapper<T> dataMapper = DataMapperWrapper.getDataMapper(dataClass);
        final Stream<Tuple<String, AttributeValueUpdate>> mappedValues;
        final Tuple<Field, DbAttribute> versionedAttribute = dataMapper.getVersionedAttribute();
        final UpdateItemRequest.Builder updateItemRequestBuilder = UpdateItemRequest.builder();

        BaseRepositoryUtils.setVersion(item, versionedAttribute, updateItemRequestBuilder);

        mappedValues = MapperUtils.getMappedValues(item, dataClass)
                .peek(a -> DbUtils.checkForNullFields(a._4(), a._2(), a._1()))
                .filter(a -> a._1() != null)
                .map(a -> Tuple.of(a._1(), DbUtils.modelToAttributeUpdateValue(a._3(), a._2()).call(AttributeValueUpdate.builder()).build()));

        return Mono.fromFuture(DataMapperWrapper
                .getDynamoDbAsyncClient()
                .updateItem(updateItemRequestBuilder
                        .tableName(dataMapper.tableName())
                        .key(dataMapper.getPrimaryKey(primaryKey))
                        .attributeUpdates(mappedValues.collect(Collectors.toMap(Tuple::_1, Tuple::_2)))
                        .returnValues(ReturnValue.ALL_NEW)
                        .build())
                .exceptionally(e -> BaseRepositoryUtils.handleUpdateItemException(primaryKey, e, dataMapper.tableName()))
                .thenApplyAsync(updateItemResponse -> dataMapper.mapFromValue(updateItemResponse.attributes())))
                .onErrorResume(throwable -> throwable instanceof CompletionException, throwable -> Mono.error(throwable.getCause()));
    }

    static <T> Mono<T> updateItem(final PrimaryKey primaryKey,
                                  final Map<String, Object> updatedValues, Function<PrimaryKey, Mono<T>> findByPrimaryFunc,
                                  final Supplier<Class<T>> dataClassFunc) {

        final Mono<T> itemMono = findByPrimaryFunc.apply(primaryKey);
        final Class<T> parameterType = dataClassFunc.get();
        final DataMapper<T> dataMapper = DataMapperWrapper.getDataMapper(parameterType);

        return itemMono.flatMap(item -> updateItem(primaryKey, updatedValues, parameterType, dataMapper, item));
    }

    static <T> Flux<T> updateItem(final List<UpdateItem> updateItems, final Supplier<Class<T>> paramTypeFunc, final Function<List<PrimaryKey>, Flux<T>> findByPrimaryKeysFunc) {
        final Class<T> parameterType = paramTypeFunc.get();
        final DataMapper<T> dataMapper = DataMapperWrapper.getDataMapper(parameterType);
        final Flux<T> items = findByPrimaryKeysFunc.apply(updateItems.stream().map(UpdateItem::getPrimaryKey).collect(Collectors.toList()));
        final Map<PrimaryKey, UpdateItem> updateItemMap = updateItems.stream().collect(Collectors.toMap(UpdateItem::getPrimaryKey, b -> b));

        //Combine list of mono to flux
        return Flux.concat(items.map(item -> BaseRepositoryUtils.updateItem(dataMapper.createPKFromItem(item),
                updateItemMap.get(dataMapper.createPKFromItem(item)).getUpdatedValues(), parameterType, dataMapper, item)));
    }

    static <T> Flux<T> batchWrite(final List<T> putItems, final List<T> deleteItems, final Supplier<Class<T>> paramTypeFunc) {
        final Func1<DataMapper<T>, Stream<WriteRequest>> putFunc = dataMapper -> putItems.stream().map(item -> WriteRequest.builder()
                .putRequest(PutRequest.builder()
                        .item(dataMapper.mapToValue(item))
                        .build())
                .build());
        final Func1<DataMapper<T>, Stream<WriteRequest>> deleteFunc = dataMapper -> (deleteItems != null ? deleteItems : Collections.<T>emptyList()).stream().map(item -> WriteRequest.builder()
                .deleteRequest(DeleteRequest.builder()
                        .key(dataMapper.getPrimaryKey(dataMapper.createPKFromItem(item)))
                        .build())
                .build());
        final Func1<DataMapper<T>, Stream<WriteRequest>> dbRequestFunc = dataMapper -> Stream.concat(putFunc.call(dataMapper), deleteFunc.call(dataMapper));
        final DataMapper<T> dataMapper = DataMapperWrapper.getDataMapper(paramTypeFunc.get());

        return Mono.fromFuture(processBatchWriteRequest(() -> putItems, ImmutableMap.of(dataMapper.tableName(), dbRequestFunc.call(dataMapper).collect(Collectors.toList())))).flatMapMany(Flux::fromIterable);
    }

    static <T> CompletableFuture<List<T>> processBatchWriteRequest(final Func0<List<T>> returnItemFunc, final Map<String, List<WriteRequest>> requestItems) {
        final BatchWriteItemRequest batchWriteItemRequest;
        final CompletableFuture<BatchWriteItemResponse> response;

        batchWriteItemRequest = BatchWriteItemRequest.builder()
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
                .requestItems(requestItems).build();

        response = DataMapperWrapper.getDynamoDbAsyncClient().batchWriteItem(batchWriteItemRequest);

        return response.thenApplyAsync(res -> {
            final boolean hasUnProcessed = res.hasUnprocessedItems();

            return hasUnProcessed && !CollectionUtils.isEmpty(res.unprocessedItems()) ?
                    Utils.getFromFromFuture(processBatchWriteRequest(returnItemFunc, res.unprocessedItems())) : returnItemFunc.call();
        });
    }

    static <T> Mono<T> deleteItem(final T item, final Supplier<Class<T>> paramTypeFunc) {
        final DataMapper<T> dataMapper = DataMapperWrapper.getDataMapper(paramTypeFunc.get());
        final PrimaryKey primaryKey = dataMapper.createPKFromItem(item);
        final DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
                .tableName(dataMapper.tableName())
                .key(dataMapper.getPrimaryKey(primaryKey)).build();

        return Mono.fromFuture(DataMapperWrapper.getDynamoDbAsyncClient().deleteItem(deleteRequest).thenApplyAsync(deleteItemResponse -> item));
    }
}
