package org.leo.aws.ddb.utils;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public interface SingleValueComparator extends Comparator {
    Value value(final String name, AttributeValue value);
}
