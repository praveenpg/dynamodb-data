package org.leo.aws.ddb.utils;

public interface Action2<T1, T2> extends Action {
    void call(T1 t1, T2 t2);
}
