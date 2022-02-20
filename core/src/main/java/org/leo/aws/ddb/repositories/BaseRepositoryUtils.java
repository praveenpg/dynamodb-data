package org.leo.aws.ddb.repositories;

import com.google.common.collect.ImmutableMap;
import org.leo.aws.ddb.annotations.DbAttribute;
import org.leo.aws.ddb.annotations.DdbRepository;
import org.leo.aws.ddb.annotations.ProjectionType;
import org.leo.aws.ddb.exceptions.DbException;
import org.leo.aws.ddb.exceptions.OptimisticLockFailureException;
import org.leo.aws.ddb.model.Page;
import org.leo.aws.ddb.model.PrimaryKey;
import org.leo.aws.ddb.model.UpdateItem;
import org.leo.aws.ddb.utils.*;
import org.leo.aws.ddb.utils.exceptions.Issue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
enum BaseRepositoryUtils {
    INSTANCE;

    private final ConcurrentHashMap<String, Class<?>> repoParameterTypeMap = new ConcurrentHashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseRepositoryUtils.class);

    static BaseRepositoryUtils getInstance() {
        return INSTANCE;
    }

    UpdateItemResponse handleUpdateItemException(final PrimaryKey primaryKey,
                                                 final String tableName, final Throwable e) {

        LOGGER.debug(MessageFormat.format("Record with the following primary key [{0}] exists in table [{1}]", primaryKey, tableName), e);
        if (e instanceof CompletionException && e.getCause() instanceof ConditionalCheckFailedException) {
            throw new OptimisticLockFailureException(e.getCause());
        } else if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        } else {
            throw new DbException(Issue.UNKNOWN_ERROR.name() + " - handleUpdateItemException", e);
        }
    }

    <ENTITY_TYPE> Integer setVersion(final ENTITY_TYPE item,
                                     final Tuple<Field, DbAttribute> versionedAttribute,
                                     final UpdateItemRequest.Builder updateItemRequestBuilder) {

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
                ReflectionUtils.setField(versionedAttribute._1(), item, Long.valueOf(version));
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
    <ENTITY_TYPE> Flux<ENTITY_TYPE> batchWriteRequest(final Func1<DataMapper<ENTITY_TYPE>, Stream<WriteRequest>> dbRequestFunc,
                                                      final Func0<List<ENTITY_TYPE>> returnItemFunc,
                                                      final DataMapper<ENTITY_TYPE> dataMapper) {


        return Flux.defer(() -> Mono.fromFuture(processBatchWriteRequest(returnItemFunc, ImmutableMap.of(dataMapper.tableName(), dbRequestFunc.call(dataMapper)
                        .collect(Collectors.toList()))))
                .flatMapMany(Flux::fromIterable));
    }

    <ENTITY_TYPE> Mono<ENTITY_TYPE> updateItem(final PrimaryKey primaryKey,
                                               final Map<String, Object> updatedValues,
                                               final Class<ENTITY_TYPE> parameterType,
                                               final DataMapper<ENTITY_TYPE> dataMapper,
                                               final ENTITY_TYPE item) {

        return Mono.defer(() -> {
            final Tuple<Field, DbAttribute> versionedAttribute = dataMapper.getVersionedAttribute();
            final UpdateItemRequest.Builder updateItemRequestBuilder = UpdateItemRequest.builder();
            final Map<String, Tuple3<String, Field, DbAttribute>> mappedFields = MapperUtils.getInstance().getMappedValues(parameterType.getName())
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
                            return Tuples.of(a.getKey(), DbUtils.modelToAttributeUpdateValue(mappedFields.get(a.getKey())._2(), a.getValue())
                                    .call(AttributeValueUpdate.builder()).build());
                        } else {
                            return Tuples.of(a.getKey(), AttributeValueUpdate.builder().value(AttributeValue.builder()
                                    .s(String.valueOf(a.getValue())).build()).build());
                        }
                    });
            final Map<String, AttributeValueUpdate> mappedUpdateValuesTmp = mappedValues.collect(Collectors.toMap(Tuple::_1, Tuple::_2));
            final Map<String, AttributeValueUpdate> mappedUpdateValues;
            final Integer version = setVersion(item, versionedAttribute, updateItemRequestBuilder);

            if (versionedAttribute != null) {
                mappedUpdateValues = ImmutableMap.<String, AttributeValueUpdate>builder()
                        .putAll(mappedUpdateValuesTmp)
                        .put(versionedAttribute._2().value(), AttributeValueUpdate.builder().value(AttributeValue.builder()
                                .n(String.valueOf(version)).build()).build())
                        .build();
            } else {
                mappedUpdateValues = mappedUpdateValuesTmp;
            }

            return Mono.fromFuture(DataMapperUtils.getDynamoDbAsyncClient().updateItem(updateItemRequestBuilder
                            .tableName(dataMapper.tableName())
                            .key(dataMapper.getPrimaryKey(primaryKey))
                            .attributeUpdates(mappedUpdateValues)
                            .returnValues(ReturnValue.ALL_NEW)
                            .build())
                    .thenApplyAsync(updateItemResponse -> dataMapper.mapFromAttributeValueToEntity(updateItemResponse.attributes())));
        });
    }

    <ENTITY_TYPE> Flux<ENTITY_TYPE> findByGlobalSecondaryIndex(final String indexName,
                                                               final Object hashKeyValueObj,
                                                               final Object rangeKeyValue,
                                                               final Function<ENTITY_TYPE, PrimaryKey> primaryKeyFunc,
                                                               final Supplier<Class<ENTITY_TYPE>> dataClassFunc,
                                                               final Function<List<PrimaryKey>, Flux<ENTITY_TYPE>> findByPrimaryKeyFunc,
                                                               final Expr filterExpression) {

        return Flux.defer(() -> {
            if (hashKeyValueObj instanceof String) {
                final String hashKeyValue;
                final Tuple<ProjectionType, QueryPublisher> queryResponseTuple;
                final Flux<ENTITY_TYPE> returnedDataFromDb;
                final Flux<ENTITY_TYPE> returnData;
                final Class<ENTITY_TYPE> dataClass = dataClassFunc.get();

                if (rangeKeyValue != null && !(rangeKeyValue instanceof String)) {
                    throw new DbException("Currently only String types are supported for sortKey Values");
                }

                hashKeyValue = (String) hashKeyValueObj;

                queryResponseTuple = getDataFromIndex(indexName, hashKeyValue, rangeKeyValue, dataClass, filterExpression);

                returnedDataFromDb = Flux
                        .from(queryResponseTuple._2())
                        .flatMapIterable(QueryResponse::items)
                        .map(a -> DataMapperUtils.getDataMapper(dataClass).mapFromAttributeValueToEntity(a));

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
        });
    }

    <ENTITY_TYPE> Flux<ENTITY_TYPE> findAll(final int pageSize,
                                            final Supplier<Class<ENTITY_TYPE>> dataClassFunc) {

        return findAll(null, pageSize, dataClassFunc);
    }

    <ENTITY_TYPE> Flux<ENTITY_TYPE> findAll(final Expr expr,
                                            final int pageSize,
                                            final Supplier<Class<ENTITY_TYPE>> dataClassFunc) {

        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(dataClassFunc.get());
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

        scanPublisher = DataMapperUtils.getDynamoDbAsyncClient().scanPaginator(scanRequestBuilder.build());

        return Flux.from(scanPublisher)
                .flatMapIterable(ScanResponse::items)
                .map(dataMapper::mapFromAttributeValueToEntity);
    }

    <ENTITY_TYPE> Mono<ENTITY_TYPE> findByPrimaryKey(final PrimaryKey primaryKey,
                                                     final Supplier<Class<ENTITY_TYPE>> dataClassFunc) {

        return Mono.defer(() -> {
            final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(dataClassFunc.get());
            final GetItemRequest getItemRequest = GetItemRequest.builder()
                    .key(dataMapper.getPrimaryKey(primaryKey))
                    .tableName(dataMapper.tableName()).build();

            return Mono
                    .fromCompletionStage(DataMapperUtils.getDynamoDbAsyncClient().getItem(getItemRequest))
                    .flatMap(resp -> resp.item().isEmpty() ? Mono.empty() : Mono.just(dataMapper.mapFromAttributeValueToEntity(resp.item())));
        });
    }

    <ENTITY_TYPE> Flux<ENTITY_TYPE> findByPrimaryKeys(final List<PrimaryKey> primaryKeys,
                                                      final Supplier<Class<ENTITY_TYPE>> dataClassFunc) {

        return Flux.defer(() -> {
            final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(dataClassFunc.get());
            final List<PrimaryKey> primaryKeysForQuery = new ArrayList<>(new HashSet<>(primaryKeys)); //Adding list of primary keys to a set before querying to remove duplicates.
            final KeysAndAttributes attributes = KeysAndAttributes.builder().keys(primaryKeysForQuery.stream()
                    .map(dataMapper::getPrimaryKey)
                    .collect(Collectors.toList())).build();
            final Map<String, KeysAndAttributes> andAttributesMap = new HashMap<>();
            final BatchGetItemRequest request;

            andAttributesMap.put(dataMapper.tableName(), attributes);
            request = BatchGetItemRequest.builder().requestItems(andAttributesMap).build();

            return Flux
                    .from(DataMapperUtils.getDynamoDbAsyncClient().batchGetItemPaginator(request))
                    .flatMapIterable(resp -> resp.responses().get(dataMapper.tableName()))
                    .map(dataMapper::mapFromAttributeValueToEntity);
        });
    }

    <ENTITY_TYPE> CompletableFuture<ENTITY_TYPE> saveItem(final ENTITY_TYPE item, final boolean upsert,
                                                          final Action2<ENTITY_TYPE, Map<String, AttributeValue>> ttlAction,
                                                          final DataMapper<ENTITY_TYPE> dataMapper) {

        final PrimaryKey primaryKey = dataMapper.createPKFromItem(item);
        final String tableName = dataMapper.tableName();
        final Map<String, AttributeValue> attributeValues;
        final PutItemRequest putItemRequest;
        final PutItemRequest.Builder builder;
        final Tuple<Field, DbAttribute> versionedAttribute = dataMapper.getVersionedAttribute();
        final String rangeKeyName = primaryKey.getRangeKeyName();

        builder = PutItemRequest.builder()
                .tableName(dataMapper.tableName());

        if (!upsert) {
            if (versionedAttribute != null) {
                ReflectionUtils.setField(versionedAttribute._1(), item, BigInteger.ZERO.intValue());
            }

            if (!StringUtils.isEmpty(rangeKeyName)) {
                builder.expected(ImmutableMap.of(
                        primaryKey.getHashKeyName(), ExpectedAttributeValue.builder().exists(false).build(),
                        primaryKey.getRangeKeyName(), ExpectedAttributeValue.builder().exists(false).build()));
            } else {
                builder.expected(ImmutableMap.of(
                        primaryKey.getHashKeyName(), ExpectedAttributeValue.builder().exists(false).build()));
            }
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

        attributeValues = dataMapper.mapFromEntityToAttributeValue(item);

        ttlAction.call(item, attributeValues);

        builder.item(attributeValues);

        putItemRequest = builder.build();

        return DataMapperUtils.getDynamoDbAsyncClient().putItem(putItemRequest).thenApplyAsync(putItemResponse -> item)
                .exceptionally(e -> handleCreateItemException(primaryKey, tableName, e));
    }

    <ENTITY_TYPE> ENTITY_TYPE handleCreateItemException(final PrimaryKey primaryKey,
                                                        final String tableName,
                                                        final Throwable e) {

        LOGGER.error(MessageFormat.format("Record with the following primary key [{0}] exists in table [{1}]", primaryKey, tableName), e);

        if (e instanceof CompletionException && e.getCause() instanceof ConditionalCheckFailedException) {
            throw new DbException(Issue.RECORD_ALREADY_EXISTS.name(), e);
        } else if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        } else {
            throw new DbException(Issue.UNKNOWN_ERROR.name() + " - handleCreateItemException", e);
        }
    }

    <ENTITY_TYPE> Mono<ENTITY_TYPE> updateItem(final ENTITY_TYPE item,
                                               final Function<ENTITY_TYPE, PrimaryKey> primaryKeyFunc,
                                               final Supplier<Class<ENTITY_TYPE>> dataClassFunc) {

        return Mono.defer(() -> {
            final PrimaryKey primaryKey = primaryKeyFunc.apply(item);
            final Class<ENTITY_TYPE> dataClass = dataClassFunc.get();
            final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(dataClass);
            final Stream<Tuple<String, AttributeValueUpdate>> mappedValues;
            final Tuple<Field, DbAttribute> versionedAttribute = dataMapper.getVersionedAttribute();
            final UpdateItemRequest.Builder updateItemRequestBuilder = UpdateItemRequest.builder();

            setVersion(item, versionedAttribute, updateItemRequestBuilder);

            mappedValues = MapperUtils.getInstance().getMappedValues(item, dataClass)
                    .peek(a -> DbUtils.checkForNullFields(a._4(), a._2(), a._1()))
                    .filter(a -> a._1() != null)
                    .map(a -> Tuples.of(a._1(), DbUtils.modelToAttributeUpdateValue(a._3(), a._2()).call(AttributeValueUpdate.builder()).build()));

            return Mono.fromFuture(DataMapperUtils
                            .getDynamoDbAsyncClient()
                            .updateItem(updateItemRequestBuilder
                                    .tableName(dataMapper.tableName())
                                    .key(dataMapper.getPrimaryKey(primaryKey))
                                    .attributeUpdates(mappedValues.collect(Collectors.toMap(Tuple::_1, Tuple::_2)))
                                    .returnValues(ReturnValue.ALL_NEW)
                                    .build())
                            .exceptionally(e -> handleUpdateItemException(primaryKey, dataMapper.tableName(), e))
                            .thenApplyAsync(updateItemResponse -> dataMapper.mapFromAttributeValueToEntity(updateItemResponse.attributes())))
                    .onErrorResume(throwable -> throwable instanceof CompletionException, throwable -> Mono.error(throwable.getCause()));
        });
    }

    <ENTITY_TYPE> Mono<ENTITY_TYPE> updateItem(final PrimaryKey primaryKey,
                                               final Map<String, Object> updatedValues,
                                               final Function<PrimaryKey, Mono<ENTITY_TYPE>> findByPrimaryFunc,
                                               final Supplier<Class<ENTITY_TYPE>> dataClassFunc) {

        final Mono<ENTITY_TYPE> itemMono = findByPrimaryFunc.apply(primaryKey);
        final Class<ENTITY_TYPE> parameterType = dataClassFunc.get();
        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(parameterType);

        return itemMono.flatMap(item -> updateItem(primaryKey, updatedValues, parameterType, dataMapper, item));
    }

    <ENTITY_TYPE> Flux<ENTITY_TYPE> updateItem(final List<UpdateItem> updateItems, final Supplier<Class<ENTITY_TYPE>> paramTypeFunc,
                                               final Function<List<PrimaryKey>, Flux<ENTITY_TYPE>> findByPrimaryKeysFunc) {

        final Class<ENTITY_TYPE> parameterType = paramTypeFunc.get();
        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(parameterType);
        final Flux<ENTITY_TYPE> items = findByPrimaryKeysFunc.apply(updateItems.stream().map(UpdateItem::getPrimaryKey).collect(Collectors.toList()));
        final Map<PrimaryKey, UpdateItem> updateItemMap = updateItems.stream().collect(Collectors.toMap(UpdateItem::getPrimaryKey, b -> b));
        final Flux<Mono<ENTITY_TYPE>> test = items.map(item -> updateItem(dataMapper.createPKFromItem(item),
                updateItemMap.get(dataMapper.createPKFromItem(item)).getUpdatedValues(), parameterType, dataMapper, item));

        //Combine list of mono to flux
        return Flux.concat(test);
    }

    <ENTITY_TYPE> Flux<ENTITY_TYPE> batchWrite(final List<ENTITY_TYPE> putItems,
                                               final List<ENTITY_TYPE> deleteItems,
                                               final Supplier<Class<ENTITY_TYPE>> paramTypeFunc) {

        return Flux.defer(() -> {
            final Func1<DataMapper<ENTITY_TYPE>, Stream<WriteRequest>> putFunc = dataMapper -> putItems.stream().map(item -> WriteRequest.builder()
                    .putRequest(PutRequest.builder()
                            .item(dataMapper.mapFromEntityToAttributeValue(item))
                            .build())
                    .build());
            final Func1<DataMapper<ENTITY_TYPE>, Stream<WriteRequest>> deleteFunc = dataMapper ->
                    (deleteItems != null ? deleteItems : Collections.<ENTITY_TYPE>emptyList()).stream().map(item -> WriteRequest.builder()
                            .deleteRequest(DeleteRequest.builder()
                                    .key(dataMapper.getPrimaryKey(dataMapper.createPKFromItem(item)))
                                    .build())
                            .build());
            final Func1<DataMapper<ENTITY_TYPE>, Stream<WriteRequest>> dbRequestFunc = dataMapper -> Stream.concat(putFunc.call(dataMapper),
                    deleteFunc.call(dataMapper));
            final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(paramTypeFunc.get());

            return Mono.fromFuture(processBatchWriteRequest(() -> putItems, ImmutableMap.of(dataMapper.tableName(), dbRequestFunc.call(dataMapper)
                            .collect(Collectors.toList()))))
                    .flatMapMany(Flux::fromIterable);
        });
    }

    <ENTITY_TYPE> CompletableFuture<List<ENTITY_TYPE>> processBatchWriteRequest(final Func0<List<ENTITY_TYPE>> returnItemFunc,
                                                                                final Map<String, List<WriteRequest>> requestItems) {

        final BatchWriteItemRequest batchWriteItemRequest;
        final CompletableFuture<BatchWriteItemResponse> response;

        batchWriteItemRequest = BatchWriteItemRequest.builder()
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .returnItemCollectionMetrics(ReturnItemCollectionMetrics.SIZE)
                .requestItems(requestItems).build();

        response = DataMapperUtils.getDynamoDbAsyncClient().batchWriteItem(batchWriteItemRequest);

        return response.thenApplyAsync(res -> {
            final boolean hasUnProcessed = res.hasUnprocessedItems();

            return hasUnProcessed && !CollectionUtils.isEmpty(res.unprocessedItems()) ?
                    Utils.getFromFromFuture(processBatchWriteRequest(returnItemFunc, res.unprocessedItems())) : returnItemFunc.call();
        });
    }

    <ENTITY_TYPE> Mono<ENTITY_TYPE> deleteItem(final ENTITY_TYPE item,
                                               final Supplier<Class<ENTITY_TYPE>> paramTypeFunc) {

        return Mono.defer(() -> {
            final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(paramTypeFunc.get());
            final PrimaryKey primaryKey = dataMapper.createPKFromItem(item);
            final DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
                    .tableName(dataMapper.tableName())
                    .key(dataMapper.getPrimaryKey(primaryKey)).build();

            return Mono.fromFuture(DataMapperUtils.getDynamoDbAsyncClient().deleteItem(deleteRequest).thenApplyAsync(deleteItemResponse -> item));
        });
    }


    @SuppressWarnings({"DuplicatedCode", "OptionalUsedAsFieldOrParameterType"})
    @Deprecated
    <ENTITY_TYPE> Tuple<ProjectionType, QueryPublisher> getDataFromIndex(final String indexName,
                                                                         final String hashKeyValue,
                                                                         final Optional<?> rangeKeyValue,
                                                                         final Class<ENTITY_TYPE> dataClass,
                                                                         final Expr filterExpressions) {

        final AttributeMapper<ENTITY_TYPE> attributeMapper = (AttributeMapper<ENTITY_TYPE>) MapperUtils.getInstance().getAttributeMappingMap()
                .get(dataClass.getName());
        final GSI secondaryIndex = attributeMapper.getGlobalSecondaryIndexMap().get(indexName);

        if (secondaryIndex == null) {
            throw new DbException(MessageFormat.format("Index [{0}] not defined in the data model", indexName));
        } else if (rangeKeyValue.isPresent() && secondaryIndex.getRangeKeyTuple() == null) {
            throw new DbException(MessageFormat.format("Sort Key not defined for index[{0}] in the data model", indexName));
        } else {
            final String keyConditionExpression = "#d = :partition_key" + (rangeKeyValue.isPresent() ? (" and " + secondaryIndex.getRangeKeyTuple()._1()
                    + " = :sort_key_val") : "");
            final QueryRequest request;
            final QueryPublisher queryPublisher;
            final QueryRequest.Builder builder = QueryRequest.builder();
            final Map<String, String> nameMap = new HashMap<>(software.amazon.awssdk.utils.ImmutableMap.<String, String>builder().put("#d",
                    secondaryIndex.getHashKeyTuple()._1()).build());
            final Map<String, AttributeValue> attributeValueMap = new HashMap<>();

            rangeKeyValue.ifPresent(s -> attributeValueMap.put(":sort_key_val", AttributeValue.builder().s(String.valueOf(s)).build()));

            attributeValueMap.put(":partition_key", AttributeValue.builder().s(hashKeyValue).build());

            builder.tableName(attributeMapper.getTableName());
            builder.indexName(secondaryIndex.getName());

            setFilterExpression(filterExpressions, builder, nameMap, attributeValueMap);

            builder.keyConditionExpression(keyConditionExpression);
            builder.expressionAttributeNames(nameMap);
            builder.expressionAttributeValues(attributeValueMap);

            request = builder.build();

            queryPublisher = DataMapperUtils.getDynamoDbAsyncClient().queryPaginator(request);

            return Tuples.of(secondaryIndex.getProjectionType(), queryPublisher);
        }
    }

    @SuppressWarnings("DuplicatedCode")
    <ENTITY_TYPE> Tuple<ProjectionType, QueryPublisher> getDataFromIndex(final String indexName,
                                                                         final String hashKeyValue,
                                                                         final Object rangeKeyValue,
                                                                         final Class<ENTITY_TYPE> dataClass,
                                                                         final Expr filterExpressions) {

        final AttributeMapper<ENTITY_TYPE> attributeMapper = (AttributeMapper<ENTITY_TYPE>) MapperUtils.getInstance().getAttributeMappingMap()
                .get(dataClass.getName());
        final GSI secondaryIndex = attributeMapper.getGlobalSecondaryIndexMap().get(indexName);

        if (secondaryIndex == null) {
            throw new DbException(MessageFormat.format("Index [{0}] not defined in the data model", indexName));
        } else if (rangeKeyValue != null && secondaryIndex.getRangeKeyTuple() == null) {
            throw new DbException(MessageFormat.format("Sort Key not defined for index[{0}] in the data model", indexName));
        } else {
            final String keyConditionExpression = "#d = :partition_key" + (rangeKeyValue != null ? (" and " + secondaryIndex.getRangeKeyTuple()._1()
                    + " = :sort_key_val") : "");
            final QueryRequest request;
            final QueryPublisher queryPublisher;
            final QueryRequest.Builder builder = QueryRequest.builder();
            final Map<String, String> nameMap = new HashMap<>(software.amazon.awssdk.utils.ImmutableMap.<String, String>builder().put("#d",
                    secondaryIndex.getHashKeyTuple()._1()).build());
            final Map<String, AttributeValue> attributeValueMap = new HashMap<>();

            if (rangeKeyValue != null) {
                attributeValueMap.put(":sort_key_val", AttributeValue.builder().s(String.valueOf(rangeKeyValue)).build());
            }

            attributeValueMap.put(":partition_key", AttributeValue.builder().s(hashKeyValue).build());

            builder.tableName(attributeMapper.getTableName());
            builder.indexName(secondaryIndex.getName());

            setFilterExpression(filterExpressions, builder, nameMap, attributeValueMap);

            builder.keyConditionExpression(keyConditionExpression);
            builder.expressionAttributeNames(nameMap);
            builder.expressionAttributeValues(attributeValueMap);

            request = builder.build();

            queryPublisher = DataMapperUtils.getDynamoDbAsyncClient().queryPaginator(request);

            return Tuples.of(secondaryIndex.getProjectionType(), queryPublisher);
        }
    }

    void setFilterExpression(final Expr expr,
                             final QueryRequest.Builder builder,
                             final Map<String, String> nameMap,
                             final Map<String, AttributeValue> attributeValueMap) {

        if (expr != null) {
            final Map<String, String> attNameMap = expr.attributeNameMap();
            final Map<String, AttributeValue> attValueMap = expr.attributeValueMap();

            builder.filterExpression(expr.expression());
            if (!CollectionUtils.isEmpty(attNameMap)) {
                nameMap.putAll(attNameMap);
            }

            if (!CollectionUtils.isEmpty(attValueMap)) {
                attributeValueMap.putAll(attValueMap);
            }
        }
    }

    <ENTITY_TYPE> Class<ENTITY_TYPE> getRepoParameterType(
            final DynamoDbRepository<ENTITY_TYPE> baseRepository) {

        return (Class<ENTITY_TYPE>) repoParameterTypeMap.computeIfAbsent(baseRepository.getClass().getName(), s -> {
            final DdbRepository annotation = baseRepository.getClass().getAnnotation(DdbRepository.class);

            if (annotation != null) {
                return annotation.entityClass();
            } else {
                throw new DbException("Annotation not defined in Repository implementation.");
            }
        });
    }

    <ENTITY_TYPE> Flux<ENTITY_TYPE> findByHashKeyAndRangeKeyStartsWithPagination(final String hashKey,
                                                                                 final Object hashKeyValueObj,
                                                                                 final String rangeKey,
                                                                                 final String rangeKeyValue,
                                                                                 final Page page,
                                                                                 @Nullable final String indexName,
                                                                                 final Class<ENTITY_TYPE> dataClass,
                                                                                 @Nullable final Expr expr) {

        if ((hashKeyValueObj instanceof String) || hashKeyValueObj instanceof Number) {
            final QueryRequest request;
            final QueryPublisher queryResponse;
            final Map<String, String> nameMap = new HashMap<>();
            final Map<String, AttributeValue> attributeValueMap = new HashMap<>();
            final String hashAlias = "#a";
            final String keyConditionExpression;
            final QueryRequest.Builder builder = QueryRequest.builder();
            final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(dataClass);


            if (!StringUtils.isEmpty(rangeKey) && !StringUtils.isEmpty(rangeKeyValue)) {
                keyConditionExpression = MessageFormat.format("{0} = :{1} and begins_with({2}, :sortKeyVal)", hashAlias, hashKey, rangeKey);
            } else {
                keyConditionExpression = hashAlias + " = :" + hashKey;
            }

            nameMap.put(hashAlias, hashKey);

            if (hashKeyValueObj instanceof String) {
                attributeValueMap.put(MessageFormat.format(":{0}", hashKey), AttributeValue.builder().s((String) hashKeyValueObj).build());
            } else {
                attributeValueMap.put(MessageFormat.format(":{0}", hashKey), AttributeValue.builder().n(Utils.getUnformattedNumber((Number) hashKeyValueObj)).build());
            }

            if (!StringUtils.isEmpty(rangeKey) && !StringUtils.isEmpty(rangeKeyValue)) {
                attributeValueMap.put(":sortKeyVal", AttributeValue.builder().s(rangeKeyValue).build());
            }

            if (!StringUtils.isEmpty(indexName)) {
                builder.indexName(indexName);
            }

            setFilterExpression(expr, builder, nameMap, attributeValueMap);

            if (page != null) {
                builder.limit(page.getPageSize());

                if (page.getLastEndKey() != null) {
                    final String lastEndKeyVal = (String) page.getLastEndKey().getRangeKeyValue();

                    if (StringUtils.isEmpty(rangeKeyValue) || lastEndKeyVal.startsWith(rangeKeyValue)) {
                        builder.exclusiveStartKey(dataMapper.getPrimaryKey(page.getLastEndKey()));
                    } else {
                        return Flux.error(new DbException("INVALID_RANGE_KEY_VALUE"));
                    }
                }
            }

            request = builder
                    .tableName(dataMapper.tableName())
                    .keyConditionExpression(keyConditionExpression)
                    .expressionAttributeNames(nameMap)
                    .expressionAttributeValues(attributeValueMap)
                    .build();

            queryResponse = DataMapperUtils.getDynamoDbAsyncClient().queryPaginator(request);

            return Flux.from(queryResponse)
                    .flatMapIterable(QueryResponse::items)
                    .map(dataMapper::mapFromAttributeValueToEntity);
        } else {
            throw new DbException("Currently only String/Number types are supported for hashKey Values");
        }
    }
}
