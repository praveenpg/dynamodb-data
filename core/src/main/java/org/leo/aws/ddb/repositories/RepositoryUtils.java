package org.leo.aws.ddb.repositories;


import org.leo.aws.ddb.annotations.DdbRepository;
import org.leo.aws.ddb.annotations.GlobalSecondaryIndex;
import org.leo.aws.ddb.exceptions.DbException;
import org.leo.aws.ddb.model.Page;
import org.leo.aws.ddb.utils.Expr;
import org.leo.aws.ddb.utils.model.Tuple;
import org.reactivestreams.Publisher;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.functions.Func1;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.paginators.QueryPublisher;
import software.amazon.awssdk.utils.ImmutableMap;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "unchecked"})
final class RepositoryUtils {
    private static final ConcurrentHashMap<String, Class<?>> REPO_PARAMETER_TYPE_MAP = new ConcurrentHashMap<>();

    private RepositoryUtils() {
    }

    static <T> Tuple<GlobalSecondaryIndex.ProjectionType, QueryPublisher> getDataFromIndex(final String indexName,
                                                                                           final String hashKeyValue,
                                                                                           final Optional<?> rangeKeyValue,
                                                                                           final Class<T> dataClass,
                                                                                           final Expr filterExpressions) {

        final AttributeMapper<T> attributeMapper = (AttributeMapper<T>) MapperUtils.ATTRIBUTE_MAPPING_MAP.get(dataClass.getName());
        final GSI secondaryIndex = attributeMapper.getGlobalSecondaryIndexMap().get(indexName);

        if (secondaryIndex == null) {
            throw new DbException(MessageFormat.format("Index [{0}] not defined in the data model", indexName));
        } else if (rangeKeyValue.isPresent() && secondaryIndex.getRangeKeyTuple() == null) {
            throw new DbException(MessageFormat.format("Sort Key not defined for index[{0}] in the data model", indexName));
        } else {
            final String keyConditionExpression = "#d = :partition_key" + (rangeKeyValue.isPresent() ? (" and " + secondaryIndex.getRangeKeyTuple()._1() + " = :sort_key_val") : "");
            final QueryRequest request;
            final QueryPublisher queryPublisher;
            final QueryRequest.Builder builder = QueryRequest.builder();
            final Map<String, String> nameMap = new HashMap<>(ImmutableMap.<String, String>builder().put("#d", secondaryIndex.getHashKeyTuple()._1()).build());
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

            queryPublisher = DataMapperWrapper.getDynamoDbAsyncClient().queryPaginator(request);

            return Tuple.of(secondaryIndex.getProjectionType(), queryPublisher);
        }
    }

    static void setFilterExpression(final Expr expr,
                                    final QueryRequest.Builder builder,
                                    final Map<String, String> nameMap,
                                    final Map<String, AttributeValue> attributeValueMap) {

        if(expr != null) {
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

    static <T, S, M> Class<T> getRepoParameterType(final DynamoDbRepository<T, S, M> baseRepository) {
        return (Class<T>) REPO_PARAMETER_TYPE_MAP.computeIfAbsent(baseRepository.getClass().getName(), s -> {
            final DdbRepository annotation = baseRepository.getClass().getAnnotation(DdbRepository.class);

            if (annotation != null) {
                return annotation.entityClass();
            } else {
                throw new DbException("Annotation not defined in Repository implementation.");
            }
        });
    }

    static <T> Flux<T> findByHashKeyAndRangeKeyStartsWithPagination(final String hashKey,
                                                                    final Object hashKeyValueObj,
                                                                    final String rangeKey,
                                                                    final String rangeKeyValue,
                                                                    final Page page,
                                                                    @Nullable final String indexName,
                                                                    final Class<T> dataClass,
                                                                    @Nullable final Expr expr) {

        if (hashKeyValueObj instanceof String) {
            final String hashKeyValue = (String) hashKeyValueObj;
            final QueryRequest request;
            final QueryPublisher queryResponse;
            final Map<String, String> nameMap = new HashMap<>();
            final Map<String, AttributeValue> attributeValueMap = new HashMap<>();
            final String hashAlias = "#a";
            final String keyConditionExpression;
            final QueryRequest.Builder builder = QueryRequest.builder();
            final DataMapper<T> dataMapper = DataMapperWrapper.getDataMapper(dataClass);

            if (!StringUtils.isEmpty(rangeKey) && !StringUtils.isEmpty(rangeKeyValue)) {
                keyConditionExpression = MessageFormat.format("{0} = :{1} and begins_with({2}, :sortKeyVal)", hashAlias, hashKey, rangeKey);
            } else {
                keyConditionExpression = hashAlias + " = :" + hashKey;
            }

            nameMap.put(hashAlias, hashKey);
            attributeValueMap.put(MessageFormat.format(":{0}", hashKey), AttributeValue.builder().s(hashKeyValue).build());

            if (!StringUtils.isEmpty(rangeKey) && !StringUtils.isEmpty(rangeKeyValue)) {
                attributeValueMap.put(":sortKeyVal", AttributeValue.builder().s(rangeKeyValue).build());
            }

            if(!StringUtils.isEmpty(indexName)) {
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

            queryResponse = DataMapperWrapper.getDynamoDbAsyncClient().queryPaginator(request);

            return Flux.from(queryResponse)
                    .flatMapIterable(QueryResponse::items)
                    .map(dataMapper::mapFromValue);
        } else {
            throw new DbException("Currently only String types are supported for hashKey Values");
        }
    }

    static <ENTITY_TYPE, SINGLE_RECORD_TYPE, MULTI_RECORD_TYPE> SINGLE_RECORD_TYPE processRepositoryResponseForSingleRecord(final Mono<ENTITY_TYPE> returnedRecord, final DynamoDbRepository<ENTITY_TYPE, SINGLE_RECORD_TYPE, MULTI_RECORD_TYPE> repo) {
        return processRepositoryResult(returnedRecord, repo, record -> (SINGLE_RECORD_TYPE) ((Mono<ENTITY_TYPE>) record).block());
    }

    static <ENTITY_TYPE, SINGLE_RECORD_TYPE, MULTI_RECORD_TYPE> MULTI_RECORD_TYPE processRepositoryResponseForMultipleRecords(final Flux<ENTITY_TYPE> returnedRecords, final DynamoDbRepository<ENTITY_TYPE, SINGLE_RECORD_TYPE, MULTI_RECORD_TYPE> repo) {
        return processRepositoryResult(returnedRecords, repo, records -> (MULTI_RECORD_TYPE) ((Flux<ENTITY_TYPE>) records).collectList().block());
    }

    private static <ENTITY_TYPE, SINGLE_RECORD_TYPE, MULTI_RECORD_TYPE, RETURN_TYPE> RETURN_TYPE processRepositoryResult(final Publisher<ENTITY_TYPE> data, final DynamoDbRepository<ENTITY_TYPE, SINGLE_RECORD_TYPE, MULTI_RECORD_TYPE> repo, final Func1<Publisher<ENTITY_TYPE>, RETURN_TYPE> func) {
        if(repo instanceof BlockingBaseRepository) {
            return func.call(data);
        } else if(repo instanceof NonBlockingBaseRepository){
            return (RETURN_TYPE) data;
        } else {
            throw new DbException("Repository should either implement BlockingBaseRepository or NonBlockingBaseRepository");
        }
    }
}
