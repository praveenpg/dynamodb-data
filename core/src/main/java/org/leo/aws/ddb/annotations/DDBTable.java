package org.leo.aws.ddb.annotations;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DDBTable {
    String name() default "";
}
