package org.leo.aws.ddb.utils.model;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

@SuppressWarnings("unused")
public final class Tuple6<A, B, C, D, E, F> implements ITuple {
    private final Tuple3<A, B, C> firstThroughThird;
    private final Tuple3<D, E, F> fourthThroughSixth;

    Tuple6(final A first, final B second, final C third, final D fourth, final E fifth, final F sixth) {
        this.firstThroughThird = Tuples.of(first, second, third);
        this.fourthThroughSixth = Tuples.of(fourth, fifth, sixth);
    }

    public A _1() {
        return this.firstThroughThird._1();
    }

    public B _2() {
        return this.firstThroughThird._2();
    }

    public C _3() {
        return this.firstThroughThird._3();
    }

    public D _4() {
        return this.fourthThroughSixth._1();
    }

    public E _5() {
        return this.fourthThroughSixth._2();
    }

    public F _6() {
        return this.fourthThroughSixth._3();
    }

    public <G> Tuple7<A, B, C, D, E, F, G> append(final G seventh) {
        return Tuples.of(_1(), _2(), _3(), _4(), _5(), _6(), seventh);
    }

    public Iterable<?> toIterable() {
        return Collections.unmodifiableList(Arrays.asList(_1(), _2(), _3(), _4(), _5(), _6()));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Tuple6<?, ?, ?, ?, ?, ?> tuple6 = (Tuple6<?, ?, ?, ?, ?, ?>) o;
        return Objects.equals(firstThroughThird, tuple6.firstThroughThird) &&
                Objects.equals(fourthThroughSixth, tuple6.fourthThroughSixth);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstThroughThird, fourthThroughSixth);
    }

    @Override
    public String toString() {
        return "Tuple5{" +
                "first=" + firstThroughThird._1() +
                ", second=" + firstThroughThird._2() +
                ", third=" + firstThroughThird._3() +
                ", fourth=" + fourthThroughSixth._1() +
                ", fifth=" + fourthThroughSixth._2() +
                ", sixth=" + fourthThroughSixth._3() +
                '}';
    }
}
