package org.leo.aws.ddb.annotations;

import java.lang.annotation.*;

/**
 * Represents a GSI/LSI in a DynamoDb field. This can be used directly on a field only if the field is a part of a single index.
 * or iff the field is part of multiple indexes.
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Indices {
    /**
     * Individual Indexes
     * @return indexes
     */
    Index[] indices();
}
