package org.leo.ddb.annotations;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface DdbRepository {
    Class<?> entityClass();
    String value() default "";
}
