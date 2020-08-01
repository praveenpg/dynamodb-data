package org.leo.aws.ddb.annotations;

import java.lang.annotation.*;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface GlobalSecondaryIndex {
    /**
     * Name of the index
     * @return
     */
    String name();

    /**
     * Key type
     * @return
     */
    PK.Type type();

    /**
     *
     * @return
     */
    ProjectionType projectionType();

    @SuppressWarnings("unused")
    enum ProjectionType {
        KEYS_ONLY, ALL, INCLUDE
    }
}
