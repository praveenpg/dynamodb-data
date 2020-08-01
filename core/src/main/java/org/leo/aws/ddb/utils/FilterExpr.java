package org.leo.aws.ddb.utils;

public interface FilterExpr {
    static FilterExpr getInstance() {
        return FilterExprImpl.getInstance();
    }

    Name name(String name, String alias);

    Group group(Expr expr);

    Name name(String name);
}
