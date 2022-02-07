package org.leo.aws.ddb.repositories;

import java.util.List;

public interface BlockingBaseRepository<T> extends AbstractDynamoDbRepository<T, T, List<T>> {
}
