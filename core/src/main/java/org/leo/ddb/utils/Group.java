package org.leo.ddb.utils;

public interface Group {
    Operator and();
    Operator or();
    String expression();

    Expr buildFilterExpression();
}
