package org.leo.aws.ddb.repositories;

import com.google.common.collect.ImmutableList;
import org.leo.aws.ddb.annotations.*;
import org.leo.aws.ddb.exceptions.DbException;
import org.leo.aws.ddb.model.VersionedEntity;
import org.leo.aws.ddb.utils.exceptions.Issue;
import org.leo.aws.ddb.utils.exceptions.UtilsException;
import org.leo.aws.ddb.utils.model.*;
import org.springframework.core.env.Environment;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings({"unchecked", "DuplicatedCode"})
final class MapperUtils {
    static final ConcurrentHashMap<String, AttributeMapper<?>> ATTRIBUTE_MAPPING_MAP = new ConcurrentHashMap<>();
    private final static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private MapperUtils() {
    }

    @SuppressWarnings("ConstantConditions")
    static <T> void setDbAttributes(final Class<T> dataClass, final Environment environment, final DataMapper<T> dataMapper) {
        Utils.executeUsingLock(lock.writeLock(), () -> {
            final String tableName;
            final Constructor<T> constructor = constructor(dataClass);
            final DDBTable table = dataClass.getAnnotation(DDBTable.class);
            final Field[] fields = dataClass.getDeclaredFields();
            final Map<String, Tuple<Field, DbAttribute>> map = new HashMap<>();
            final Map<KeyType, Tuple<String, Field>> primaryKeyMapping = new HashMap<>();
            final Map<String, Tuple<Field, DbAttribute>> versionAttMap = new HashMap<>();
            final ConcurrentHashMap<String, GSI.Builder> globalSecondaryIndexMap = new ConcurrentHashMap<>();
            final AttributeMapper.Builder<T> builder;
            final List<Field> fieldList;
            final List<Field> hashKeyFields;
            final List<Field> rangeKeyFields;

            if(VersionedEntity.class.isAssignableFrom(dataClass)) {
                fieldList = filterFields(ImmutableList.<Field>builder().addAll(Arrays.asList(fields))
                        .add(ReflectionUtils.findField(VersionedEntity.class, "version")).build());
            } else {
                fieldList = filterFields(ImmutableList.copyOf(fields));
            }

            if (table != null) {
                tableName = TableNameResolver.getEnvironmentProperty(table.name(), environment);
            } else {
                tableName = dataMapper.tableName();
            }

            builder = AttributeMapper.builder();

            hashKeyFields = fieldList.stream().filter(a -> (a.isAnnotationPresent(HashKey.class))).collect(Collectors.toList());
            rangeKeyFields = fieldList.stream().filter(a -> (a.isAnnotationPresent(RangeKey.class))).collect(Collectors.toList());

            validatePkAnnotations(hashKeyFields, rangeKeyFields);

            fieldList.forEach(field -> setFieldMappings(map, primaryKeyMapping, globalSecondaryIndexMap, versionAttMap, field, builder));

            if (!CollectionUtils.isEmpty(versionAttMap) && versionAttMap.size() > 1) {
                throw new DbException("Entity cannot have more than one version attribute");
            }

            MapperUtils.ATTRIBUTE_MAPPING_MAP.put(dataClass.getName(), builder.mappedClass(dataClass)
                    .mappedFields(map)
                    .constructor(constructor)
                    .primaryKeyMapping(primaryKeyMapping)
                    .globalSecondaryIndexMap(globalSecondaryIndexMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, b -> b.getValue().build())))
                    .versionAttributeField(!CollectionUtils.isEmpty(versionAttMap) ? versionAttMap.entrySet().iterator().next().getValue() : null)
                    .tableName(tableName)
                    .build());
        });
    }

    private static void validatePkAnnotations(List<Field> hashKeyFields, List<Field> rangeKeyFields) {

        if(CollectionUtils.isEmpty(hashKeyFields)) {
            throw new UtilsException(Issue.INCORRECT_MODEL_ANNOTATION, "An entity class should have at least one hash key");
        }

        if(!CollectionUtils.isEmpty(hashKeyFields) && hashKeyFields.size() > 1) {
            throw new UtilsException(Issue.INCORRECT_MODEL_ANNOTATION, "Cannot have more than 1 field annotated with @HashKey at the entity class");
        }

        if(!CollectionUtils.isEmpty(rangeKeyFields) && rangeKeyFields.size() > 1) {
            throw new UtilsException(Issue.INCORRECT_MODEL_ANNOTATION, "Cannot have more than 1 field annotated with @RangeKey at the entity class");
        }
    }

    private static List<Field> filterFields(final List<Field> fields) {
        return fields.stream()
                .filter(field -> !field.getName().contains("ajc$"))
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .filter(field -> !Modifier.isTransient(field.getModifiers()))
                .collect(Collectors.toList());
    }

    static <T> Stream<Tuple4<String, Object, Field, DbAttribute>> getMappedValues(final T input, final String parameterType) {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) ATTRIBUTE_MAPPING_MAP.get(parameterType);
        final Map<String, Tuple<Field, DbAttribute>> fieldMap = fieldMapping.getMappedFields();
        final Map<KeyType, Tuple<String, Field>> pkMapping = fieldMapping.getPrimaryKeyMapping();
        final Set<String> pkFields = pkMapping.values().stream().map(Tuple::_1).collect(Collectors.toSet());

        return fieldMap.entrySet().stream()
                .map(entry -> Tuples.of(entry.getKey(), entry.getValue()))
                .map(a -> Tuples.of(a._1(), ReflectionUtils.getField(a._2()._1(), input), a._2()._1(), a._2()._2()))
                .filter(a -> !pkFields.contains(a._1()));
    }

    static <T> Stream<Tuple3<String, Field, DbAttribute>> getMappedValues(final String parameterType) {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) ATTRIBUTE_MAPPING_MAP.get(parameterType);
        final Map<String, Tuple<Field, DbAttribute>> fieldMap = fieldMapping.getMappedFields();
        final Map<KeyType, Tuple<String, Field>> pkMapping = fieldMapping.getPrimaryKeyMapping();
        final Set<String> pkFields = pkMapping.values().stream().map(Tuple::_1).collect(Collectors.toSet());

        return fieldMap.entrySet().stream()
                .map(entry -> Tuples.of(entry.getKey(), entry.getValue()))
                .map(a -> Tuples.of(a._1(), a._2()._1(), a._2()._2()))
                .filter(a -> !pkFields.contains(a._1()));
    }


    private static <T> void setFieldMappings(final Map<String, Tuple<Field, DbAttribute>> mappedFields,
                                             final Map<KeyType, Tuple<String, Field>> primaryKeyMapping,
                                             final ConcurrentHashMap<String, GSI.Builder> globalSecondaryIndexMap,
                                             final Map<String, Tuple<Field, DbAttribute>> versionAttMap,
                                             final Field field,
                                             final AttributeMapper.Builder<T> builder) {
        final List<Annotation> annotations;

        field.setAccessible(true);
        annotations = Arrays.stream(field.getAnnotations()).filter(a -> (a instanceof DbAttribute ||
                a instanceof Transient ||
                a instanceof Index ||
                a instanceof Indices ||
                a instanceof DateUpdated) ||
                a instanceof DateCreated ||
                a instanceof HashKey ||
                a instanceof RangeKey ||
                a instanceof VersionAttribute).collect(Collectors.toList());

        if (!isTransient(annotations)) {
            if (isAnnotatedCorrectly(annotations)) {
                final DbAttribute dbAttribute = !CollectionUtils.isEmpty(annotations) ?
                        (DbAttribute) annotations.stream().filter(a -> (a instanceof DbAttribute)).findAny().orElse(null) : null;
                final DateCreated dateCreated = !CollectionUtils.isEmpty(annotations) ?
                        (DateCreated) annotations.stream().filter(a -> (a instanceof DateCreated)).findAny().orElse(null) : null;
                final DateUpdated dateUpdated = !CollectionUtils.isEmpty(annotations) ?
                        (DateUpdated) annotations.stream().filter(a -> (a instanceof DateUpdated)).findAny().orElse(null) : null;
                final HashKey hashKey = !CollectionUtils.isEmpty(annotations) ?
                        (HashKey) annotations.stream().filter(a -> (a instanceof HashKey)).findAny().orElse(null) : null;
                final RangeKey rangeKey = !CollectionUtils.isEmpty(annotations) ?
                        (RangeKey) annotations.stream().filter(a -> (a instanceof RangeKey)).findAny().orElse(null) : null;
                final Index gsiElement = !CollectionUtils.isEmpty(annotations) ?
                        (Index) annotations.stream().filter(a -> (a instanceof Index)).findAny().orElse(null) : null;
                final Indices gsis = !CollectionUtils.isEmpty(annotations) ?
                        (Indices) annotations.stream().filter(a -> (a instanceof Indices)).findAny().orElse(null) : null;
                final VersionAttribute versionAttribute = !CollectionUtils.isEmpty(annotations) ?
                        (VersionAttribute) annotations.stream().filter(a -> (a instanceof VersionAttribute)).findAny().orElse(null) : null;
                final String fieldName = dbAttribute != null ? dbAttribute.value() : field.getName();
                final List<Index> gsiList;
                final String fieldNameVal;
                final Class<?> fieldClass = field.getType();

                if (versionAttribute != null && (fieldClass != int.class && fieldClass != Integer.class && fieldClass != Long.class && fieldClass != long.class)) {
                    throw new DbException("VersionedAttribute field can only be of type integer or long");
                }

                if (gsis != null && gsiElement != null) {
                    throw new DbException(MessageFormat.format("Both GSIs and GSI cannot be used for the same field [{0}.{1}]", field.getDeclaringClass(), field.getName()));
                }

                gsiList = gsis != null ? Arrays.asList(gsis.indices()) : Collections.singletonList(gsiElement);

                if(hashKey != null) {
                    primaryKeyMapping.put(KeyType.HASH_KEY, Tuples.of(fieldName, field));
                } else if(rangeKey != null) {
                    primaryKeyMapping.put(KeyType.RANGE_KEY, Tuples.of(fieldName, field));
                }

                gsiList.stream().filter(Objects::nonNull).forEach(gsi -> setGsiBuilder(globalSecondaryIndexMap, field, fieldName, gsi));

                fieldNameVal = getFieldName(dbAttribute, dateCreated, dateUpdated, field, builder);

                mappedFields.put(fieldNameVal, Tuples.of(field, dbAttribute));

                if (versionAttribute != null) {
                    versionAttMap.put(fieldNameVal, Tuples.of(field, dbAttribute));
                }

            }
        }
    }

    private static void setGsiBuilder(ConcurrentHashMap<String, GSI.Builder> globalSecondaryIndexMap, Field field, String fieldName, Index gsi) {
        final GSI.Builder gsiBuilder = globalSecondaryIndexMap.computeIfAbsent(gsi.name(), s -> GSI.builder(gsi.name())
                .projectionType(gsi.projectionType()));
        final Tuple<String, Field> keyTuple = Tuples.of(fieldName, field);

        switch (gsi.type()) {
            case HASH_KEY:
                gsiBuilder.hashKeyTuple(keyTuple);
                break;
            case RANGE_KEY:
                gsiBuilder.rangeKeyTuple(keyTuple);
                break;
            default:
                throw new DbException("Unrecognized key type");
        }
    }

    static <T> Stream<Tuple4<String, Object, Field, DbAttribute>> getMappedValues(final T input, final Class<T> parameterClass) {
        final DataMapper<T> dataMapper = DataMapperUtils.getDataMapper(parameterClass);

        return dataMapper.getMappedValues(input);
    }

    private static <T> String getFieldName(final DbAttribute dbAttribute,
                                           final DateCreated dateCreated,
                                           final DateUpdated dateUpdated,
                                           final Field field,
                                           final AttributeMapper.Builder<T> builder) {
        final String fieldName;

        if (dbAttribute != null && !StringUtils.isEmpty(dbAttribute.value())) {
            fieldName = dbAttribute.value().trim();
        } else if (dbAttribute != null && StringUtils.isEmpty(dbAttribute.value())) {
            fieldName = field.getName();
        } else {
            fieldName = field.getName();
        }

        if(dateCreated != null) {
            builder.dateCreatedField(Tuples.of(fieldName, field));
        } else if (dateUpdated != null) {
            builder.dateUpdatedField(Tuples.of(fieldName, field));
        }

        return fieldName;
    }


    private static boolean isAnnotatedCorrectly(final List<Annotation> annotations) {
        final List<? extends Annotation> annotationList = CollectionUtils.isEmpty(annotations) ?
                annotations.stream().filter(a -> (a instanceof DateCreated || a instanceof DateUpdated)).collect(Collectors.toList()) : Collections.emptyList();

        //A field cannot have both DateCreated and DateUpdated annotation
        if(CollectionUtils.isEmpty(annotationList) || annotationList.size() == 1) {
            return true;
        } else {
            throw new UtilsException(Issue.INCORRECT_MODEL_ANNOTATION, "A field can only have one of the following annotation: [DateCreated, DateUpdated]");
        }
    }

    private static boolean isTransient(final List<Annotation> annotations) {
        return !CollectionUtils.isEmpty(annotations) && annotations.stream().anyMatch(a -> (a instanceof Transient));
    }

    private static <T> Constructor<T> constructor(final Class<T> dataClass) {
        try {
            final Constructor<T> constructor = dataClass.getDeclaredConstructor();

            constructor.setAccessible(true);

            return constructor;
        } catch (final NoSuchMethodException e) {
            throw new UtilsException(Issue.UNKNOWN_ERROR, "No empty constructor found for " + dataClass.getName(), e);
        }
    }

    private static class TableNameResolver {
        private static final HashMap<String, String> PROPERTY_MAPPING = new HashMap<>();

        private static String getEnvironmentProperty(final String text, final Environment environment) {

            // Check if the text is already been parsed
            if (PROPERTY_MAPPING.containsKey(text)) {

                return PROPERTY_MAPPING.get(text);

            }


            // If the text does not start with $, then no need to do pattern
            if (!text.startsWith("$")) {

                // Add to the mapping with key and value as text
                PROPERTY_MAPPING.put(text, text);

                // If no match, then return the text as it is
                return text;

            }

            // Create the pattern
            final Pattern pattern = Pattern.compile("\\Q${\\E(.+?)\\Q}\\E");

            // Create the matcher
            final Matcher matcher = pattern.matcher(text);

            // If the matching is there, then add it to the map and return the value
            if (matcher.find()) {

                // Store the value
                final String key = matcher.group(1);

                // Get the value
                final String value = environment.getProperty(key);

                // Store the value in the setting
                if (value != null) {

                    // Store in the map
                    PROPERTY_MAPPING.put(text, value);

                    // return the value
                    return value;

                }

            }

            // Add to the mapping with key and value as text
            PROPERTY_MAPPING.put(text, text);

            // If no match, then return the text as it is
            return text;

        }
    }
}
