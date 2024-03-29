package org.leo.aws.ddb.utils;

import java.util.List;
import java.util.Objects;

@SuppressWarnings("unused")
public final class Tuple5<A, B, C, D, E> implements ITuple {
    private final Tuple3<A, B, C> firstThroughThird;
    private final Tuple<D, E> fourthAndFifth;

    Tuple5(final A first, final B second, final C third, final D fourth, final E fifth) {
        this.firstThroughThird = Tuples.of(first, second, third);
        this.fourthAndFifth = Tuples.of(fourth, fifth);
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
        return this.fourthAndFifth._1();
    }

    public E _5() {
        return this.fourthAndFifth._2();
    }

    public <F> Tuple6<A, B, C, D, E, F> append(final F sixth) {
        return Tuples.of(_1(), _2(), _3(), _4(), _5(), sixth);
    }

    public Iterable<?> toIterable() {
        return List.of(_1(), _2(), _3(), _4(), _5());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Tuple5<?, ?, ?, ?, ?> tuple5 = (Tuple5<?, ?, ?, ?, ?>) o;
        return Objects.equals(firstThroughThird, tuple5.firstThroughThird) &&
                Objects.equals(fourthAndFifth, tuple5.fourthAndFifth);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstThroughThird, fourthAndFifth);
    }

    @Override
    public String toString() {
        return "Tuple5{" +
                "first=" + firstThroughThird._1() +
                ", second=" + firstThroughThird._2() +
                ", third=" + firstThroughThird._3() +
                ", fourth=" + fourthAndFifth._1() +
                ", fifth=" + fourthAndFifth._2() +
                '}';
    }
}
