package org.leo.ddb.utils;

public interface Name {
    Comparator gt();

    Comparator lt();

    Comparator gte();

    Comparator lte();

    Comparator eq();

    Comparator notExists();
}
