package org.leo.aws.ddb.utils;

public interface Operator {
    String expression();

    Name name(String name, String alias);

    Group group(Expr expr);
}
