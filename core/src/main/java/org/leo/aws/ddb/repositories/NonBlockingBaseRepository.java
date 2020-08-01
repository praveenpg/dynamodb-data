package org.leo.aws.ddb.repositories;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Non blocking repositories should implement this interface.
 */
@SuppressWarnings({"unused"})
public interface NonBlockingBaseRepository<T> extends DynamoDbRepository<T, Mono<T>, Flux<T>> {
}
