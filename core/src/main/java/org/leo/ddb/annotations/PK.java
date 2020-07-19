package org.leo.ddb.annotations;

import java.lang.annotation.*;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface PK {
    Type type() default Type.HASH_KEY;

    enum Type {
        HASH_KEY, RANGE_KEY
    }
}
