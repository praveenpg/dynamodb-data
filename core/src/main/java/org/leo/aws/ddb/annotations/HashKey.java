package org.leo.aws.ddb.annotations;

import java.lang.annotation.*;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface HashKey {
    KeyType type() default KeyType.ID;
    String indexName();
}
