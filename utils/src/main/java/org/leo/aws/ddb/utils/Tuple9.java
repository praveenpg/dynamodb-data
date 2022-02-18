package org.leo.aws.ddb.utils;

import java.util.List;
import java.util.Objects;

@SuppressWarnings("unused")
public final class Tuple9<A, B, C, D, E, F, G, H, I> implements ITuple {
    private final Tuple4<A, B, C, D> firstThroughFourth;
    private final Tuple5<E, F, G, H, I> fifthThroughNinth;

    Tuple9(final A first, final B second, final C third, final D fourth, final E fifth, final F sixth, final G seventh, final H eighth, final I ninth) {
        this.firstThroughFourth = Tuples.of(first, second, third, fourth);
        this.fifthThroughNinth = Tuples.of(fifth, sixth, seventh, eighth, ninth);
    }

    public A _1() {
        return this.firstThroughFourth._1();
    }

    public B _2() {
        return this.firstThroughFourth._2();
    }

    public C _3() {
        return this.firstThroughFourth._3();
    }

    public D _4() {
        return this.firstThroughFourth._4();
    }

    public E _5() {
        return this.fifthThroughNinth._1();
    }

    public F _6() {
        return this.fifthThroughNinth._2();
    }

    public G _7() {
        return this.fifthThroughNinth._3();
    }

    public H _8() {
        return this.fifthThroughNinth._4();
    }

    public I _9() {
        return this.fifthThroughNinth._5();
    }

    public Iterable<?> toIterable() {
        return List.of(_1(), _2(), _3(), _4(), _5(), _6(), _7(), _8(), _9());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Tuple9<?, ?, ?, ?, ?, ?, ?, ?, ?> tuple9 = (Tuple9<?, ?, ?, ?, ?, ?, ?, ?, ?>) o;
        return Objects.equals(firstThroughFourth, tuple9.firstThroughFourth) &&
                Objects.equals(fifthThroughNinth, tuple9.fifthThroughNinth);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstThroughFourth, fifthThroughNinth);
    }

    @Override
    public String toString() {
        return "Tuple5{" +
                "first=" + firstThroughFourth._1() +
                ", second=" + firstThroughFourth._2() +
                ", third=" + firstThroughFourth._3() +
                ", fourth=" + firstThroughFourth._4() +
                ", fifth=" + fifthThroughNinth._1() +
                ", sixth=" + fifthThroughNinth._2() +
                ", seventh=" + fifthThroughNinth._3() +
                ", eighth=" + fifthThroughNinth._4() +
                ", ninth=" + fifthThroughNinth._5() +
                '}';
    }
}
