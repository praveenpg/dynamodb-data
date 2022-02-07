package org.leo.aws.ddb.repositories;


import org.leo.aws.ddb.annotations.DbAttribute;
import org.leo.aws.ddb.annotations.KeyType;
import org.leo.aws.ddb.exceptions.DbException;
import org.leo.aws.ddb.model.Page;
import org.leo.aws.ddb.model.PatchUpdate;
import org.leo.aws.ddb.model.PrimaryKey;
import org.leo.aws.ddb.model.UpdateItem;
import org.leo.aws.ddb.utils.Expr;
import org.leo.aws.ddb.utils.ApplicationContextUtils;
import org.leo.aws.ddb.utils.Tuple;
import org.leo.aws.ddb.utils.Tuple4;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;
import rx.functions.Action2;
import rx.functions.Func1;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings({"unused", "RedundantSuppression"})
public interface AbstractDynamoDbRepository<ENTITY_TYPE, SINGLE_RECORD_TYPE, MULTIPLE_RECORD_TYPE> {
    int DEFAULT_PAGE_SIZE = 20;

    default DynamoDbAsyncClient dynamoDbClient() {
        return DataMapperUtils.getDynamoDbAsyncClient();
    }

    /**
     * @return Range Key name
     */
    default String getRangeKeyName() {
        return DataMapperUtils.getDataMapper(getParameterType()).getPKMapping().get(KeyType.RANGE_KEY)._1();
    }

    /**
     * @return Entity class
     */
    default Class<ENTITY_TYPE> getParameterType() {
        return BaseRepositoryUtils.getInstance().getRepoParameterType(this);
    }

    default PrimaryKey getPrimaryKey(final ENTITY_TYPE item) {
        return DataMapperUtils.getDataMapper(getParameterType()).createPKFromItem(item);
    }

    /**
     * @return hash key name of the entity
     */
    default String getHashKeyName() {
        final Class<ENTITY_TYPE> type = getParameterType();
        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(getParameterType());

        if (dataMapper == null) {
            throw new DbException(MessageFormat.format("Could not find any entity of type [{0}] in the provided entityBasePackage [service.aws.ddb.entityBasePackage: {1}]",
                    type.getName(), ApplicationContextUtils.getInstance().getEnvironment().getProperty("service.aws.ddb.entityBasePackage")));
        }

        return dataMapper.getPKMapping().get(KeyType.HASH_KEY)._1();
    }


    /**
     * Returns map representation without primary keys
     *
     * @param item              entity
     * @param includeNullValues Include null values. If true is sent, all values are returned else null values are eliminated
     * @return Map
     */
    default Map<String, ?> getFieldMappings(final ENTITY_TYPE item, final boolean includeNullValues) {
        final Class<ENTITY_TYPE> paramType = getParameterType();
        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(paramType);
        final PrimaryKey primaryKey = dataMapper.createPKFromItem(item);
        final Stream<Tuple4<String, Object, Field, DbAttribute>> fieldMappings = dataMapper.getMappedValues(item)
                .filter(a -> !a._1().equals(primaryKey.getHashKeyName())).filter(a -> !a._1().equals(primaryKey.getRangeKeyName()));

        return includeNullValues ? fieldMappings.collect(Collectors.toMap(Tuple4::_1, Tuple4::_2)) :
                fieldMappings.filter(c -> c._2() != null).collect(Collectors.toMap(Tuple4::_1, Tuple4::_2));
    }

    /**
     * Returns a list of values matching hash key value passed.
     *
     * @param hashKeyValue Hash Key value
     * @return List of entity objects
     */
    default MULTIPLE_RECORD_TYPE findByHashKey(final Object hashKeyValue) {
        return findByHashKey(getHashKeyName(), hashKeyValue, null, null);
    }

    /**
     * Returns a list of values matching hash key value passed.
     *
     * @param hashKeyName  Hash Key Name
     * @param hashKeyValue Hash Key value
     * @return List of entity objects
     */
    default MULTIPLE_RECORD_TYPE findByHashKey(final String hashKeyName, final Object hashKeyValue) {
        return findByHashKey(hashKeyName, hashKeyValue, null, null);
    }

    /**
     * Returns a flux (list) of values matching hash key value passed.
     *
     * @param hashKeyName  Hash Key Name
     * @param hashKeyValue Hash Key value
     * @return List of entity objects
     */
    default MULTIPLE_RECORD_TYPE findByHashKey(final String hashKeyName,
                                               final Object hashKeyValue,
                                               @Nullable final Page page) {
        return findByHashKey(hashKeyName, hashKeyValue, page, null, null);
    }


    default MULTIPLE_RECORD_TYPE findByHashKey(final String hashKeyName,
                                               final Object hashKeyValueObj,
                                               @Nullable final Expr expr) {

        return findByHashKey(hashKeyName, hashKeyValueObj, null, expr);
    }

    /**
     * Returns a flux representing a list of values which matches the hash key value passed
     *
     * @param hashKeyName     Hash Key used for the table
     * @param hashKeyValueObj The value of the Hash key
     * @param indexName       The GSI index name. This is used if the optional parameter has a value
     * @return A Flux representing the list of values satisfying the above conditions.
     */
    default MULTIPLE_RECORD_TYPE findByHashKey(final String hashKeyName,
                                               final Object hashKeyValueObj,
                                               @Nullable final String indexName,
                                               @Nullable final Expr expr) {

        return findByHashKey(hashKeyName, hashKeyValueObj, null, indexName, expr);
    }


    /**
     * Returns a flux representing a list of values which matches the hash key value passed
     *
     * @param hashKeyName     Hash Key used for the table
     * @param hashKeyValueObj The value of the Hash key
     * @param indexName       The GSI index name. This is used if the optional parameter has a value
     * @return A Flux representing the list of values satisfying the above conditions.
     */
    default MULTIPLE_RECORD_TYPE findByHashKey(final String hashKeyName,
                                               final Object hashKeyValueObj,
                                               @Nullable final Page page,
                                               @Nullable final String indexName,
                                               @Nullable final Expr expr) {

        return findByHashKeyAndRangeKeyStartsWith(hashKeyName, hashKeyValueObj, null, null, page, indexName, expr);
    }

    /**
     * Returns a list of entities whose has hash key matches the value sent and the range key starts with the value sent.
     *
     * @param hashKey         Hash Key used for the table
     * @param hashKeyValueObj The value of the Hash key
     * @param rangeKey        Range key.
     * @param rangeKeyValue   Range Key Value
     * @return List of entities
     */
    default MULTIPLE_RECORD_TYPE findByHashKeyAndRangeKeyStartsWith(final String hashKey,
                                                                    final Object hashKeyValueObj,
                                                                    final String rangeKey,
                                                                    final String rangeKeyValue) {

        return findByHashKeyAndRangeKeyStartsWith(hashKey, hashKeyValueObj, rangeKey, rangeKeyValue, null);
    }

    /**
     * Returns a list of entities whose has hash key matches the value sent and the range key starts with the value sent.
     *
     * @param hashKey         Hash Key used for the table
     * @param hashKeyValueObj The value of the Hash key
     * @param rangeKey        Range key.
     * @param rangeKeyValue   Range Key Value
     * @return List of entities
     */
    default MULTIPLE_RECORD_TYPE findByHashKeyAndRangeKeyStartsWith(final String hashKey,
                                                                    final Object hashKeyValueObj,
                                                                    final String rangeKey,
                                                                    final String rangeKeyValue,
                                                                    @Nullable final Page page) {

        return findByHashKeyAndRangeKeyStartsWith(hashKey, hashKeyValueObj, rangeKey, rangeKeyValue, page, null, null);
    }

    /**
     * @param hashKey         Hash Key used for the table
     * @param hashKeyValueObj The value of the Hash key
     * @param rangeKey        Range key.
     * @param rangeKeyValue   Range Key Value
     * @param indexName       Index name
     * @param expr            filter expression
     * @return list of entities
     */
    default MULTIPLE_RECORD_TYPE findByHashKeyAndRangeKeyStartsWith(final String hashKey,
                                                                    final Object hashKeyValueObj,
                                                                    final String rangeKey,
                                                                    final String rangeKeyValue,
                                                                    @Nullable final String indexName,
                                                                    @Nullable final Expr expr) {

        return findByHashKeyAndRangeKeyStartsWithPagination(hashKey, hashKeyValueObj, rangeKey, rangeKeyValue, null, indexName, expr);
    }


    /**
     * @param hashKey         Hash Key used for the table
     * @param hashKeyValueObj The value of the Hash key
     * @param rangeKey        Range key.
     * @param rangeKeyValue   Range Key Value
     * @param indexName       Index name
     * @param expr            filter expression
     * @return list of entities
     */
    default MULTIPLE_RECORD_TYPE findByHashKeyAndRangeKeyStartsWith(final String hashKey,
                                                                    final Object hashKeyValueObj,
                                                                    final String rangeKey,
                                                                    final String rangeKeyValue,
                                                                    @Nullable final Page page,
                                                                    @Nullable final String indexName,
                                                                    @Nullable final Expr expr) {

        return findByHashKeyAndRangeKeyStartsWithPagination(hashKey, hashKeyValueObj, rangeKey, rangeKeyValue, page, indexName, expr);
    }

    /**
     * @param hashKey         Hash Key used for the table
     * @param hashKeyValueObj The value of the Hash key
     * @param rangeKey        Range key.
     * @param rangeKeyValue   Range Key Value
     * @param page            page details
     * @return records matching the keys and page object
     */
    default MULTIPLE_RECORD_TYPE findByHashKeyAndRangeKeyStartsWithPagination(final String hashKey,
                                                                              final Object hashKeyValueObj,
                                                                              final String rangeKey,
                                                                              final String rangeKeyValue,
                                                                              @Nullable final Page page) {

        return findByHashKeyAndRangeKeyStartsWithPagination(hashKey, hashKeyValueObj, rangeKey, rangeKeyValue, page, null, null);
    }

    /**
     * @param hashKey         Hash Key Name
     * @param hashKeyValueObj Hash Key Value
     * @param rangeKey        Range Key Name
     * @param rangeKeyValue   Range Key Value
     * @param page            Page
     * @param indexName       Index name
     * @param expr            Filter Expression
     * @return Records matching above criteria
     */
    default MULTIPLE_RECORD_TYPE findByHashKeyAndRangeKeyStartsWithPagination(final String hashKey,
                                                                              final Object hashKeyValueObj,
                                                                              final String rangeKey,
                                                                              final String rangeKeyValue,
                                                                              @Nullable final Page page,
                                                                              @Nullable final String indexName,
                                                                              @Nullable final Expr expr) {

        return BaseRepositoryUtils.getInstance().processRepositoryResponseForMultipleRecords(BaseRepositoryUtils.getInstance().findByHashKeyAndRangeKeyStartsWithPagination(hashKey,
                hashKeyValueObj,
                rangeKey,
                rangeKeyValue,
                page,
                indexName,
                getParameterType(),
                expr), this);
    }

    /**
     * Queries by GSI name and the hash key value used for the index
     *
     * @param indexName       Name of the GSI
     * @param hashKeyValueObj hash key value
     * @return list of items satisfying the above criteria
     */
    default MULTIPLE_RECORD_TYPE findByGlobalSecondaryIndex(final String indexName, final Object hashKeyValueObj) {
        return findByGlobalSecondaryIndex(indexName, hashKeyValueObj, null);
    }

    /**
     * @param indexName       Queries by GSI name and the hash key value and range key used for the index
     * @param hashKeyValueObj hash key value
     * @param rangeKeyValue   range key value
     * @return list of items satisfying the above criteria
     */
    default MULTIPLE_RECORD_TYPE findByGlobalSecondaryIndex(@NonNull final String indexName,
                                                            final Object hashKeyValueObj,
                                                            final Object rangeKeyValue) {
        return findByGlobalSecondaryIndex(indexName, hashKeyValueObj, rangeKeyValue, null);
    }


    /**
     * @param indexName       name of the GSI
     * @param hashKeyValueObj Hash Key Value
     * @param expr            Filter Expression
     * @return Multiple records
     */
    default MULTIPLE_RECORD_TYPE findByGlobalSecondaryIndex(@NonNull final String indexName,
                                                            final Object hashKeyValueObj,
                                                            @Nullable final Expr expr) {

        return findByGlobalSecondaryIndex(indexName, hashKeyValueObj, null, expr);
    }

    /**
     * @param indexName       name of the GSI
     * @param hashKeyValueObj Hash Key Value
     * @param rangeKeyValue   Range Key Value
     * @param expr            Filter Expression
     * @return Multiple records
     */
    default MULTIPLE_RECORD_TYPE findByGlobalSecondaryIndex(@NonNull final String indexName,
                                                            final Object hashKeyValueObj,
                                                            final Object rangeKeyValue,
                                                            @Nullable final Expr expr) {

        return BaseRepositoryUtils.getInstance().processRepositoryResponseForMultipleRecords(BaseRepositoryUtils.getInstance().findByGlobalSecondaryIndex(indexName,
                hashKeyValueObj,
                rangeKeyValue,
                this::getPrimaryKey,
                this::getParameterType,
                pks -> BaseRepositoryUtils.getInstance().findByPrimaryKeys(pks, this::getParameterType),
                expr), this);
    }

    /**
     * Returns a single record by primary key (Hash Key + Sort Key[if there is one])
     *
     * @param primaryKey Primary Key Object
     * @return A single record
     */
    default SINGLE_RECORD_TYPE findOne(final PrimaryKey primaryKey) {
        return findByPrimaryKey(primaryKey);
    }

    /**
     * Get all records. Limit set to 20
     *
     * @return list of all records
     */
    default MULTIPLE_RECORD_TYPE findAll() {
        return findAll(DEFAULT_PAGE_SIZE);
    }

    /**
     * Get all records
     *
     * @param limit Page Size
     * @return List of all records
     */
    default MULTIPLE_RECORD_TYPE findAll(final int limit) {
        return BaseRepositoryUtils.getInstance().processRepositoryResponseForMultipleRecords(BaseRepositoryUtils.getInstance().findAll(limit, this::getParameterType), this);
    }

    /**
     * @param expr Filter Expression
     * @return All records that satisfy the filter criteria
     */
    default MULTIPLE_RECORD_TYPE findAll(@Nullable final Expr expr) {
        return findAll(expr, DEFAULT_PAGE_SIZE);
    }

    /**
     * @param expr  @param expr Filter Expression
     * @param limit limit
     * @return All records that satisfy the filter criteria
     */
    default MULTIPLE_RECORD_TYPE findAll(@Nullable final Expr expr, final int limit) {
        return BaseRepositoryUtils.getInstance().processRepositoryResponseForMultipleRecords(BaseRepositoryUtils.getInstance().findAll(expr, limit, this::getParameterType), this);
    }

    /**
     * Returns a record which matches the primary key passed
     *
     * @param primaryKey Primary Key (this should have both the hash key and the sort key populated)
     * @return A mono representing a record which matches the primary key passed.
     */
    default SINGLE_RECORD_TYPE findByPrimaryKey(@NonNull final PrimaryKey primaryKey) {
        return BaseRepositoryUtils.getInstance().processRepositoryResponseForSingleRecord(BaseRepositoryUtils.getInstance().findByPrimaryKey(primaryKey, this::getParameterType), this);
    }

    default SINGLE_RECORD_TYPE findByPrimaryKey(final Object hashKeyValue) {
        final String hashKeyName = getHashKeyName();
        final Tuple<String, Field> rangeKey = DataMapperUtils.getDataMapper(getParameterType()).getPKMapping().get(KeyType.RANGE_KEY);
        final String rangeKeyName = rangeKey == null ? null : rangeKey._1();

        if (!StringUtils.isEmpty(rangeKeyName)) {
            throw new DbException("This method cannot be used for entities that has a range key");
        }

        return findByPrimaryKey(PrimaryKey.builder().hashKeyName(hashKeyName).hashKeyValue(hashKeyValue).build());
    }

    default SINGLE_RECORD_TYPE findByPrimaryKey(final Object hashKeyValue, final Object rangeKeyValue) {
        final String hashKeyName = getHashKeyName();
        final String rangeKeyName = getRangeKeyName();

        if (StringUtils.isEmpty(rangeKeyName)) {
            throw new DbException("This method cannot be used for entities that does not have a range key");
        }

        return findByPrimaryKey(PrimaryKey.builder().hashKeyName(hashKeyName).hashKeyValue(hashKeyValue).rangeKeyName(rangeKeyName).rangeKeyValue(rangeKeyValue).build());
    }

    /**
     * @param primaryKeys List of Primary keys
     * @return multiple records
     */
    default MULTIPLE_RECORD_TYPE findBy(final PrimaryKey... primaryKeys) {
        return findByPrimaryKeys(Arrays.asList(primaryKeys));
    }

    /**
     * @param primaryKeys List of primary keys
     * @return Records matching above criteria
     */
    default MULTIPLE_RECORD_TYPE findByPrimaryKeys(@NonNull final List<PrimaryKey> primaryKeys) {
        return BaseRepositoryUtils.getInstance().processRepositoryResponseForMultipleRecords(BaseRepositoryUtils.getInstance().findByPrimaryKeys(primaryKeys, this::getParameterType), this);
    }

    /**
     * Method to create a record
     *
     * @param item data object representing the record.
     * @return A future
     */
    default SINGLE_RECORD_TYPE putItem(@NonNull final ENTITY_TYPE item) {
        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(getParameterType());
        final Action2<ENTITY_TYPE, Map<String, AttributeValue>> ttlAction = (a, b) -> {
        };

        return putItem(item, ttlAction);
    }

    /**
     * @param item      Item being saved. This is an upsert operation
     * @param ttlAction TTL function
     * @return Record being saved
     */
    default SINGLE_RECORD_TYPE putItem(@NonNull final ENTITY_TYPE item, final Action2<ENTITY_TYPE, Map<String, AttributeValue>> ttlAction) {
        return BaseRepositoryUtils.getInstance().processRepositoryResponseForSingleRecord(
                Mono.defer(() -> Mono.fromFuture(BaseRepositoryUtils.getInstance()
                        .saveItem(item, true,
                                ttlAction,
                                DataMapperUtils.getDataMapper(getParameterType())))),
                this);
    }

    /**
     * Method to create a list of records
     *
     * @param items the list of records to be updated
     * @return A future
     */
    default MULTIPLE_RECORD_TYPE putItem(@NonNull final List<ENTITY_TYPE> items) {
        return BaseRepositoryUtils.getInstance().processRepositoryResponseForMultipleRecords(BaseRepositoryUtils.getInstance().batchWriteRequest(dataMapper -> items.stream().map(item -> WriteRequest.builder()
                .putRequest(PutRequest.builder()
                        .item(dataMapper.mapToValue(item))
                        .build())
                .build()), () -> items, DataMapperUtils.getDataMapper(getParameterType())), this);
    }

    /**
     * Updates a record with the new values set in the entity
     *
     * @param item item updated
     * @return updated item
     */
    default SINGLE_RECORD_TYPE updateItem(@NonNull final ENTITY_TYPE item) {
        return BaseRepositoryUtils.getInstance().processRepositoryResponseForSingleRecord(BaseRepositoryUtils.getInstance().updateItem(item, this::getPrimaryKey, this::getParameterType), this);
    }

    /**
     * @param primaryKey    Hash Key and Sort Keys
     * @param updatedValues Updates
     * @return A future representing the execution of the method
     */
    default SINGLE_RECORD_TYPE updateItem(@NonNull final PrimaryKey primaryKey, @NonNull final Map<String, Object> updatedValues) {
        return BaseRepositoryUtils.getInstance().processRepositoryResponseForSingleRecord(BaseRepositoryUtils.getInstance().updateItem(primaryKey, updatedValues, pk -> BaseRepositoryUtils.getInstance().findByPrimaryKey(pk, this::getParameterType), this::getParameterType), this);
    }

    /**
     * @param updateItem Updated Values
     * @return A future representing the execution of the method
     */
    default SINGLE_RECORD_TYPE updateItem(@NonNull final UpdateItem updateItem) {
        return updateItem(updateItem.getPrimaryKey(), updateItem.getUpdatedValues());
    }

    /**
     * Method updates a list of records/documents. Please note that DynamoDb as of today does not
     * support a batch update. This method updates the records one at a time. It will do a batch update
     * once DynamoDB/AWS SDK adds support without forcing the client application to make a change.
     *
     * @param updateItems updateItem Updated Values
     * @return A future representing the execution of the method
     */
    default MULTIPLE_RECORD_TYPE updateItem(@NonNull final List<UpdateItem> updateItems) {
        return BaseRepositoryUtils.getInstance().processRepositoryResponseForMultipleRecords(BaseRepositoryUtils.getInstance()
                .updateItem(updateItems, this::getParameterType, pks -> BaseRepositoryUtils.getInstance().findByPrimaryKeys(pks, this::getParameterType)), this);
    }

    /**
     * Method updates a list of records/documents. Please note that DynamoDb as of today does not
     * support a batch update. This method updates the records one at a time. It will do a batch update
     * once DynamoDB/AWS SDK adds support without forcing the client application to make a change.
     *
     * @param keyValue     Key Value
     * @param patchUpdates PatchUpdate list
     * @param convertFunc  Function to convert PatchItem to UpdateItem
     * @return Updated records
     */
    default MULTIPLE_RECORD_TYPE updateItems(final String keyValue, @NonNull final List<? extends PatchUpdate> patchUpdates, final Func1<PatchUpdate, UpdateItem> convertFunc) {
        return updateItem(patchUpdates.stream().map(convertFunc::call).collect(Collectors.toList()));
    }

    /**
     * Method to remove a list of records
     *
     * @param items List of items to be removed
     * @return A future
     */
    default MULTIPLE_RECORD_TYPE deleteAllItems(@NonNull final List<ENTITY_TYPE> items) {
        return BaseRepositoryUtils.getInstance().processRepositoryResponseForMultipleRecords(BaseRepositoryUtils.getInstance().batchWriteRequest(dataMapper -> items.stream().map(item -> WriteRequest.builder()
                .deleteRequest(DeleteRequest.builder()
                        .key(dataMapper.getPrimaryKey(dataMapper.createPKFromItem(item)))
                        .build())
                .build()), () -> items, DataMapperUtils.getDataMapper(getParameterType())), this);
    }

    /**
     * Method to remove a list of records
     *
     * @param putItems    List of items to be added
     * @param deleteItems List of items to be removed
     * @return A future
     */
    default MULTIPLE_RECORD_TYPE batchWrite(final List<ENTITY_TYPE> putItems, final List<ENTITY_TYPE> deleteItems) {
        return BaseRepositoryUtils.getInstance().processRepositoryResponseForMultipleRecords(BaseRepositoryUtils.getInstance().batchWrite(putItems, deleteItems, this::getParameterType), this);
    }


    /**
     * Method to remove a specific record
     *
     * @param item Item to be removed
     * @return A future
     */
    default SINGLE_RECORD_TYPE deleteItem(@NonNull final ENTITY_TYPE item) {
        return BaseRepositoryUtils.getInstance().processRepositoryResponseForSingleRecord(BaseRepositoryUtils.getInstance().deleteItem(item, this::getParameterType), this);
    }

    /**
     * Method to create a record
     *
     * @param item data object representing the record.
     * @return A future
     */
    default MULTIPLE_RECORD_TYPE saveItems(@NonNull final List<ENTITY_TYPE> item) {
        return batchWrite(item, Collections.emptyList());
    }

    /**
     * Method to create a record
     *
     * @param item data object representing the record.
     * @return A future
     */
    default SINGLE_RECORD_TYPE saveItem(@NonNull final ENTITY_TYPE item) {
        final DataMapper<ENTITY_TYPE> dataMapper = DataMapperUtils.getDataMapper(getParameterType());

        return saveItem(item, (entity, attributeValueMap) -> {
        });
    }

    /**
     * @param item      Item being saved. This is an insert operation.
     * @param ttlAction TTL action
     * @return Item being saved
     */
    default SINGLE_RECORD_TYPE saveItem(final ENTITY_TYPE item, final Action2<ENTITY_TYPE, Map<String, AttributeValue>> ttlAction) {
        return BaseRepositoryUtils.getInstance().processRepositoryResponseForSingleRecord(
                Mono.defer(() ->
                        Mono.fromFuture(BaseRepositoryUtils.getInstance()
                                .saveItem(item,
                                        false,
                                        ttlAction,
                                        DataMapperUtils.getDataMapper(getParameterType())))),
                this);
    }
}
