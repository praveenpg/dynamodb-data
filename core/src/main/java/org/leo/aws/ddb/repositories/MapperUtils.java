package org.leo.aws.ddb.repositories;

import com.google.common.collect.ImmutableList;


import org.leo.aws.ddb.exceptions.DbException;
import org.leo.aws.ddb.model.VersionedEntity;
import org.leo.aws.ddb.utils.exceptions.Issue;
import org.leo.aws.ddb.utils.exceptions.UtilsException;
import org.leo.aws.ddb.utils.model.Tuple;
import org.leo.aws.ddb.utils.model.Tuple3;
import org.leo.aws.ddb.utils.model.Tuple4;
import org.leo.aws.ddb.utils.model.Utils;
import org.leo.aws.ddb.annotations.*;
import org.springframework.core.env.Environment;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;

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
            final Map<String, Tuple<Field, MappedBy>> map = new HashMap<>();
            final Map<PK.Type, Tuple<String, Field>> primaryKeyMapping = new HashMap<>();
            final Map<String, Tuple<Field, MappedBy>> versionAttMap = new HashMap<>();
            final ConcurrentHashMap<String, GSI.Builder> globalSecondaryIndexMap = new ConcurrentHashMap<>();
            final AttributeMapper.Builder<T> builder;
            final List<Field> fieldList;

            if(VersionedEntity.class.isAssignableFrom(dataClass)) {
                fieldList = ImmutableList.<Field>builder().addAll(Arrays.asList(fields))
                        .add(ReflectionUtils.findField(VersionedEntity.class, "version")).build();
            } else {
                fieldList = ImmutableList.copyOf(fields);
            }

            if (table != null) {
                tableName = TableNameResolver.getEnvironmentProperty(table.name(), environment);
            } else {
                tableName = dataMapper.tableName();
            }

            builder = AttributeMapper.builder();

            fieldList.stream()
                    .filter(field -> !field.getName().contains("ajc$"))
                    .filter(field -> !Modifier.isStatic(field.getModifiers()))
                    .filter(field -> !Modifier.isTransient(field.getModifiers()))
                    .forEach(field -> setFieldMappings(map, primaryKeyMapping, field, globalSecondaryIndexMap, versionAttMap, builder));

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

    static <T> Stream<Tuple4<String, Object, Field, MappedBy>> getMappedValues(final T input, final String parameterType) {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) ATTRIBUTE_MAPPING_MAP.get(parameterType);
        final Map<String, Tuple<Field, MappedBy>> fieldMap = fieldMapping.getMappedFields();
        final Map<PK.Type, Tuple<String, Field>> pkMapping = fieldMapping.getPrimaryKeyMapping();
        final Set<String> pkFields = pkMapping.values().stream().map(Tuple::_1).collect(Collectors.toSet());

        return fieldMap.entrySet().stream()
                .map(entry -> Tuple.of(entry.getKey(), entry.getValue()))
                .map(a -> Tuple4.of(a._1(), ReflectionUtils.getField(a._2()._1(), input), a._2()._1(), a._2()._2()))
                .filter(a -> !pkFields.contains(a._1()));
    }

    static <T> Stream<Tuple3<String, Field, MappedBy>> getMappedValues(final String parameterType) {
        final AttributeMapper<T> fieldMapping = (AttributeMapper<T>) ATTRIBUTE_MAPPING_MAP.get(parameterType);
        final Map<String, Tuple<Field, MappedBy>> fieldMap = fieldMapping.getMappedFields();
        final Map<PK.Type, Tuple<String, Field>> pkMapping = fieldMapping.getPrimaryKeyMapping();
        final Set<String> pkFields = pkMapping.values().stream().map(Tuple::_1).collect(Collectors.toSet());

        return fieldMap.entrySet().stream()
                .map(entry -> Tuple.of(entry.getKey(), entry.getValue()))
                .map(a -> Tuple3.of(a._1(), a._2()._1(), a._2()._2()))
                .filter(a -> !pkFields.contains(a._1()));
    }


    private static <T> void setFieldMappings(final Map<String, Tuple<Field, MappedBy>> mappedFields,
                                             final Map<PK.Type, Tuple<String, Field>> primaryKeyMapping,
                                             final Field field,
                                             final ConcurrentHashMap<String, GSI.Builder> globalSecondaryIndexMap,
                                             final Map<String, Tuple<Field, MappedBy>> versionAttMap,
                                             final AttributeMapper.Builder<T> builder) {
        final List<Annotation> annotations;

        field.setAccessible(true);
        annotations = Arrays.stream(field.getAnnotations()).filter(a -> (a instanceof MappedBy ||
                a instanceof Transient ||
                a instanceof PK ||
                a instanceof GlobalSecondaryIndex ||
                a instanceof GlobalSecondaryIndices ||
                a instanceof DateUpdated) ||
                a instanceof DateCreated ||
                a instanceof VersionAttribute).collect(Collectors.toList());

        if (!isTransient(annotations)) {
            if (isAnnotatedCorrectly(annotations)) {
                final MappedBy mappedBy = !CollectionUtils.isEmpty(annotations) ?
                        (MappedBy) annotations.stream().filter(a -> (a instanceof MappedBy)).findAny().orElse(null) : null;
                final DateCreated dateCreated = !CollectionUtils.isEmpty(annotations) ?
                        (DateCreated) annotations.stream().filter(a -> (a instanceof DateCreated)).findAny().orElse(null) : null;
                final DateUpdated dateUpdated = !CollectionUtils.isEmpty(annotations) ?
                        (DateUpdated) annotations.stream().filter(a -> (a instanceof DateUpdated)).findAny().orElse(null) : null;
                final PK primaryKey = !CollectionUtils.isEmpty(annotations) ?
                        (PK) annotations.stream().filter(a -> (a instanceof PK)).findAny().orElse(null) : null;
                final GlobalSecondaryIndex gsiElement = !CollectionUtils.isEmpty(annotations) ?
                        (GlobalSecondaryIndex) annotations.stream().filter(a -> (a instanceof GlobalSecondaryIndex)).findAny().orElse(null) : null;
                final GlobalSecondaryIndices gsis = !CollectionUtils.isEmpty(annotations) ?
                        (GlobalSecondaryIndices) annotations.stream().filter(a -> (a instanceof GlobalSecondaryIndices)).findAny().orElse(null) : null;
                final VersionAttribute versionAttribute = !CollectionUtils.isEmpty(annotations) ?
                        (VersionAttribute) annotations.stream().filter(a -> (a instanceof VersionAttribute)).findAny().orElse(null) : null;
                final String fieldName = mappedBy != null ? mappedBy.value() : field.getName();
                final List<GlobalSecondaryIndex> gsiList;
                final String fieldNameVal;
                final Class<?> fieldClass = field.getType();

                if (versionAttribute != null && (fieldClass != int.class && fieldClass != Integer.class && fieldClass != Long.class && fieldClass != long.class)) {
                    throw new DbException("VersionedAttribute field can only be of type integer or long");
                }

                if (gsis != null && gsiElement != null) {
                    throw new DbException(MessageFormat.format("Both GSIs and GSI cannot be used for the same field [{0}.{1}]", field.getDeclaringClass(), field.getName()));
                }

                gsiList = gsis != null ? Arrays.asList(gsis.indices()) : Collections.singletonList(gsiElement);

                if (primaryKey != null) {
                    primaryKeyMapping.put(primaryKey.type(), Tuple.of(fieldName, field));
                }

                gsiList.forEach(gsi -> {
                    if (gsi != null) {
                        final GSI.Builder gsiBuilder = globalSecondaryIndexMap.computeIfAbsent(gsi.name(), s -> GSI.builder(gsi.name())
                                .projectionType(gsi.projectionType()));
                        final Tuple<String, Field> keyTuple = Tuple.of(fieldName, field);

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
                });
                fieldNameVal = getFieldName(mappedBy, dateCreated, dateUpdated, field, builder);

                mappedFields.put(fieldNameVal, Tuple.of(field, mappedBy));

                if (versionAttribute != null) {
                    versionAttMap.put(fieldNameVal, Tuple.of(field, mappedBy));
                }

            } else {
                throw new UtilsException(Issue.INCORRECT_MODEL_ANNOTATION, "A field can only have one of the following annotation: [MappedBy, DateCreated, DateUpdated]");
            }
        }
    }

    static <T> Stream<Tuple4<String, Object, Field, MappedBy>> getMappedValues(final T input, final Class<T> parameterClass) {
        final DataMapper<T> dataMapper = DataMapperWrapper.getDataMapper(parameterClass);

        return dataMapper.getMappedValues(input);
    }

    private static <T> String getFieldName(final MappedBy mappedBy, final DateCreated dateCreated, final DateUpdated dateUpdated, final Field field, final AttributeMapper.Builder<T> builder) {
        final String fieldName;

        if (mappedBy != null) {
            fieldName = mappedBy.value();
        } else if (dateCreated != null) {
            fieldName = dateCreated.value();
            builder.dateCreatedField(Tuple.of(dateCreated.value(), field));
        } else if (dateUpdated != null) {
            fieldName = dateUpdated.value();
            builder.dateUpdatedField(Tuple.of(dateUpdated.value(), field));
        } else {
            fieldName = field.getName();
        }

        return fieldName;
    }

    private static boolean isAnnotatedCorrectly(final List<Annotation> annotations) {
        final List<? extends Annotation> annotationList = CollectionUtils.isEmpty(annotations) ?
                annotations.stream().filter(a -> (a instanceof MappedBy
                        || a instanceof DateCreated
                        || a instanceof DateUpdated)).collect(Collectors.toList()) : Collections.emptyList();

        return CollectionUtils.isEmpty(annotationList) || annotationList.size() == 1;
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
