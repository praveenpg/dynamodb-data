package org.leo.aws.ddb.utils;

public interface Func1<T, R> extends Function {
    R call(T t);
}
