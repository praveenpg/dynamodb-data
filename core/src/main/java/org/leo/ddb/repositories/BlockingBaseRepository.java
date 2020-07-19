package org.leo.ddb.repositories;

import java.util.List;

public interface BlockingBaseRepository<T> extends DynamoDbRepository<T, T, List<T>> {
}
