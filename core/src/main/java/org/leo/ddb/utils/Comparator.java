package org.leo.ddb.utils;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public interface Comparator {
    Value value(final String name, AttributeValue value);

    String expression();
}
