package org.leo.aws.ddb.utils.model;

public class Tuples {
    public static <A, B> Tuple<A, B> of(final A first, final B second) {
        return new Tuple<>(first, second);
    }

    public static <A, B, C> Tuple3<A, B, C> of(final A first, final B second, final C third) {
        return new Tuple3<>(first, second, third);
    }

    public static <A, B, C, D> Tuple4<A, B, C, D> of(final A first, final B second, final C third, final D fourth) {
        return new Tuple4<>(first, second, third, fourth);
    }

    public static <A, B, C, D, E> Tuple5<A, B, C, D, E> of(final A first, final B second, final C third, final D fourth, final E fifth) {
        return new Tuple5<>(first, second, third, fourth, fifth);
    }

    public static <A, B, C, D, E, F> Tuple6<A, B, C, D, E, F> of(final A first, final B second, final C third, final D fourth, final E fifth, final F sixth) {
        return new Tuple6<>(first, second, third, fourth, fifth, sixth);
    }

    public static <A, B, C, D, E, F, G> Tuple7<A, B, C, D, E, F, G> of(final A first, final B second, final C third, final D fourth, final E fifth, final F sixth, final G seventh) {
        return new Tuple7<>(first, second, third, fourth, fifth, sixth, seventh);
    }

    public static <A, B, C, D, E, F, G, H> Tuple8<A, B, C, D, E, F, G, H> of(final A first, final B second, final C third, final D fourth, final E fifth, final F sixth, final G seventh, final H eighth) {
        return new Tuple8<>(first, second, third, fourth, fifth, sixth, seventh,eighth);
    }

    public static <A, B, C, D, E, F, G, H, I> Tuple9<A, B, C, D, E, F, G, H, I> of(final A first, final B second, final C third, final D fourth, final E fifth, final F sixth, final G seventh, final H eighth, final I ninth) {
        return new Tuple9<>(first, second, third, fourth, fifth, sixth, seventh,eighth, ninth);
    }
}
