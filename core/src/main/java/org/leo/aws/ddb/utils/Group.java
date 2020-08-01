package org.leo.aws.ddb.utils;

public interface Group {
    Operator and();
    Operator or();
    String expression();

    Expr buildFilterExpression();
}
