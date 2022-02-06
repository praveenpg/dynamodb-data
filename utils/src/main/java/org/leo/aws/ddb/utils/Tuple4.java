package org.leo.aws.ddb.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

@SuppressWarnings("WeakerAccess")
public final class Tuple4<A, B, C, D> implements ITuple {
    private final Tuple<A, B> firstAndSecond;
    private final Tuple<C, D> thirdAndFourth;

    Tuple4(final A first, final B second, final C third, final D fourth) {
        this.firstAndSecond = Tuples.of(first, second);
        this.thirdAndFourth = Tuples.of(third, fourth);
    }

    public A _1() {
        return this.firstAndSecond._1();
    }

    public B _2() {
        return this.firstAndSecond._2();
    }

    public C _3() {
        return this.thirdAndFourth._1();
    }

    public D _4() {
        return this.thirdAndFourth._2();
    }

    public <E> Tuple5<A, B, C, D, E> append(final E fifth) {
        return Tuples.of(_1(), _2(), _3(), _4(), fifth);
    }

    public Iterable<?> toIterable() {
        return Collections.unmodifiableList(Arrays.asList(_1(), _2(), _3(), _4()));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Tuple4<?, ?, ?, ?> tuple4 = (Tuple4<?, ?, ?, ?>) o;
        return Objects.equals(firstAndSecond, tuple4.firstAndSecond) &&
                Objects.equals(thirdAndFourth, tuple4.thirdAndFourth);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstAndSecond, thirdAndFourth);
    }

    @Override
    public String toString() {
        return "Tuple4{" +
                "first=" + firstAndSecond._1() +
                ", second=" + firstAndSecond._2() +
                ", third=" + thirdAndFourth._1() +
                ", fourth=" + thirdAndFourth._2() +
                '}';
    }
}
