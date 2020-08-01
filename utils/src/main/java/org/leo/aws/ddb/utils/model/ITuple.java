package org.leo.aws.ddb.utils.model;

import java.io.Serializable;

public interface ITuple extends Serializable {
    default <A> ITuple append(final A a) {
        throw new UnsupportedOperationException();
    }

    Iterable<?> toIterable();
}
