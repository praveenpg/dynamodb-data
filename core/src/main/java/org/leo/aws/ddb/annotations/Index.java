package org.leo.aws.ddb.annotations;

import java.lang.annotation.*;

/**
 * Represents a GSI/LSI in a DynamoDb field. This is to be used directly on a field only if the field is a part of a single index.
 * If the field is part of multiple indexes, please use {@link Indices}
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Index {
    /**
     * Name of the index
     * @return Index name
     */
    String name();

    /**
     * Key type
     * @return
     */
    KeyType type();

    /**
     *
     * @return
     */
    ProjectionType projectionType();

}
