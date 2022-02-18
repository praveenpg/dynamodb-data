package org.leo.aws.ddb.utils;

import java.util.List;
import java.util.Objects;

@SuppressWarnings("unused")
public final class Tuple7<A, B, C, D, E, F, G> implements ITuple {
    private final Tuple3<A, B, C> firstThroughThird;
    private final Tuple4<D, E, F, G> fourthThroughSeventh;

    Tuple7(final A first, final B second, final C third, final D fourth, final E fifth, final F sixth, final G seventh) {
        this.firstThroughThird = Tuples.of(first, second, third);
        this.fourthThroughSeventh = Tuples.of(fourth, fifth, sixth, seventh);
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
        return this.fourthThroughSeventh._1();
    }

    public E _5() {
        return this.fourthThroughSeventh._2();
    }

    public F _6() {
        return this.fourthThroughSeventh._3();
    }

    public G _7() {
        return this.fourthThroughSeventh._4();
    }

    public <H> Tuple8<A, B, C, D, E, F, G, H> append(final H eigth) {
        return Tuples.of(_1(), _2(), _3(), _4(), _5(), _6(), _7(), eigth);
    }

    public Iterable<?> toIterable() {
        return List.of(_1(), _2(), _3(), _4(), _5(), _6(), _7());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Tuple7<?, ?, ?, ?, ?, ?, ?> tuple7 = (Tuple7<?, ?, ?, ?, ?, ?, ?>) o;
        return Objects.equals(firstThroughThird, tuple7.firstThroughThird) &&
                Objects.equals(fourthThroughSeventh, tuple7.fourthThroughSeventh);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstThroughThird, fourthThroughSeventh);
    }

    @Override
    public String toString() {
        return "Tuple5{" +
                "first=" + firstThroughThird._1() +
                ", second=" + firstThroughThird._2() +
                ", third=" + firstThroughThird._3() +
                ", fourth=" + fourthThroughSeventh._1() +
                ", fifth=" + fourthThroughSeventh._2() +
                ", sixth=" + fourthThroughSeventh._3() +
                ", seventh=" + fourthThroughSeventh._4() +
                '}';
    }
}
