package org.leo.aws.ddb.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.function.BiFunction;

public final class Tuple<A, B> implements ITuple {
    private final A first;
    private final B second;

    Tuple(final A first, final B second) {
        this.first = first;
        this.second = second;
    }

    public A _1() {
        return this.first;
    }

    public B _2() {
        return this.second;
    }

    public Tuple<A, B> _1(final A updatedVal) {
        return new Tuple<>(updatedVal, second);
    }

    public Tuple<A, B> _2(final B updatedVal) {
        return new Tuple<>(first, updatedVal);
    }


    public <X, Y> Tuple<X, Y> map(final BiFunction<? super A, ? super B, Tuple<X, Y>> mapper) {
        return mapper.apply(first, second);
    }

    public <C> Tuple3<A, B, C> append(final C third) {
        return Tuples.of(first, second, third);
    }

    public Iterable<?> toIterable() {
        return Collections.unmodifiableList(Arrays.asList(first, second));
    }

    @Override
    public String toString() {
        return "Tuple{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Tuple<?, ?> tuple = (Tuple<?, ?>) o;
        return Objects.equals(first, tuple.first) &&
                Objects.equals(second, tuple.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }
}
