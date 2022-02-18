package org.leo.aws.ddb.config;

import org.leo.aws.ddb.repositories.DdbAutoConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Import({DdbAutoConfiguration.class})
public @interface EnableDynamoDbData {
}
