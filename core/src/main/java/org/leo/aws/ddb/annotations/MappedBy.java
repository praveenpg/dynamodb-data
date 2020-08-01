package org.leo.aws.ddb.annotations;

import java.lang.annotation.*;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MappedBy {
    String value();
    boolean nullable() default true;
}
