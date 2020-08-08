package org.leo.aws.ddb.annotations;

import java.lang.annotation.*;


/**
 * Please use {@link HashKey} and {@link RangeKey}
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Deprecated
public @interface PK {
    KeyType type() default KeyType.HASH_KEY;

}
