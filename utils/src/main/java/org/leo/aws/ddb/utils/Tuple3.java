package org.leo.aws.ddb.utils;

import java.util.List;
import java.util.Objects;

@SuppressWarnings("WeakerAccess")
public final class Tuple3<A, B, C>  implements ITuple {
    private final Tuple<A, B> firstAndSecond;
    private final C third;

    Tuple3(final A first, final B second, final C third) {
        this.firstAndSecond = Tuples.of(first, second);
        this.third = third;
    }

    public A _1() {
        return this.firstAndSecond._1();
    }

    public B _2() {
        return this.firstAndSecond._2();
    }

    public C _3() {
        return this.third;
    }

    public <D> Tuple4<A, B, C, D> append(final D fourth) {
        return Tuples.of(_1(), _2(), _3(), fourth);
    }

    public Iterable<?> toIterable() {
        return List.of(_1(), _2(), _3());
    }

    public static <A, B, C> Tuple3<A, B, C> of(final A first, final B second, final C third) {
        return new Tuple3<>(first, second, third);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Tuple3<?, ?, ?> tuple3 = (Tuple3<?, ?, ?>) o;
        return Objects.equals(firstAndSecond, tuple3.firstAndSecond) &&
                Objects.equals(third, tuple3.third);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstAndSecond, third);
    }

    @Override
    public String toString() {
        return "Tuple3{" +
                "first=" + firstAndSecond._1() +
                ", second=" + firstAndSecond._2() +
                ", third=" + third +
                '}';
    }
}
