package org.leo.aws.ddb.annotations;

import java.lang.annotation.*;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RangeKey {
    KeyType type() default KeyType.ID;
    String indexName();
}
