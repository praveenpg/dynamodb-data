package org.leo.ddb.annotations;

import java.lang.annotation.*;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface GlobalSecondaryIndices {
    GlobalSecondaryIndex[] indices();
}
