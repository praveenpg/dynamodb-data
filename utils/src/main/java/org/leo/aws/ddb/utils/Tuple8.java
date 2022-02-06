package org.leo.aws.ddb.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

@SuppressWarnings("unused")
public final class Tuple8<A, B, C, D, E, F, G, H> implements ITuple {
    private final Tuple4<A, B, C, D> firstThroughFourth;
    private final Tuple4<E, F, G, H> fifthThroughEighth;

    Tuple8(final A first, final B second, final C third, final D fourth, final E fifth, final F sixth, final G seventh, final H eighth) {
        this.firstThroughFourth = Tuples.of(first, second, third, fourth);
        this.fifthThroughEighth = Tuples.of(fifth, sixth, seventh, eighth);
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
        return this.fifthThroughEighth._1();
    }

    public F _6() {
        return this.fifthThroughEighth._2();
    }

    public G _7() {
        return this.fifthThroughEighth._3();
    }

    public H _8() {
        return this.fifthThroughEighth._4();
    }

    public <I> Tuple9<A, B, C, D, E, F, G, H, I> append(final I ninth) {
        return Tuples.of(_1(), _2(), _3(), _4(), _5(), _6(), _7(), _8(), ninth);
    }

    public Iterable<?> toIterable() {
        return Collections.unmodifiableList(Arrays.asList(_1(), _2(), _3(), _4(), _5(), _6(), _7(), _8()));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Tuple8<?, ?, ?, ?, ?, ?, ?, ?> tuple8 = (Tuple8<?, ?, ?, ?, ?, ?, ?, ?>) o;
        return Objects.equals(firstThroughFourth, tuple8.firstThroughFourth) &&
                Objects.equals(fifthThroughEighth, tuple8.fifthThroughEighth);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstThroughFourth, fifthThroughEighth);
    }

    @Override
    public String toString() {
        return "Tuple5{" +
                "first=" + firstThroughFourth._1() +
                ", second=" + firstThroughFourth._2() +
                ", third=" + firstThroughFourth._3() +
                ", fourth=" + firstThroughFourth._4() +
                ", fifth=" + fifthThroughEighth._1() +
                ", sixth=" + fifthThroughEighth._2() +
                ", seventh=" + fifthThroughEighth._3() +
                ", eighth=" + fifthThroughEighth._4() +
                '}';
    }
}
