package org.leo.aws.ddb.utils;

public interface Name {
    Comparator gt();

    Comparator lt();

    Comparator gte();

    Comparator lte();

    Comparator eq();

    Comparator notExists();
}
