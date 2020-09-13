package org.leo.aws.ddb.utils;

import com.google.common.collect.ImmutableMap;

import org.leo.aws.ddb.utils.exceptions.UtilsException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.lang.reflect.Proxy;
import java.text.MessageFormat;
import java.util.Map;

@SuppressWarnings({"unused", "RedundantSuppression"})
class FilterExprImpl implements FilterExpr {
    public static FilterExpr getInstance() {
        return new FilterExprImpl();
    }

    private NameImpl rootName;
    private GroupImpl rootGroup;

    @Override
    public Name name(final String name, final String alias) {
        this.rootName = new NameImpl(name, alias, this);

        return this.rootName;
    }

    @Override
    public Group group(final Expr expr) {
        return (this.rootGroup = new GroupImpl(this, expr));
    }

    @Override
    public Name name(final String name) {
        return name(name, name);
    }


    private static final class ExprImpl implements Expr {
        private final NameImpl rootNode;
        private final GroupImpl rootGroup;

        private ExprImpl(FilterExprImpl filterExpr) {
            this.rootNode = filterExpr.rootName;
            this.rootGroup = filterExpr.rootGroup;
        }

        @Override
        public String expression() {
            if (rootNode != null) {
                return rootNode.expression();
            } else if (rootGroup != null) {
                return rootGroup.expression();
            } else {
                throw new UtilsException("Invalid Expression");
            }
        }

        @Override
        public Map<String, String> attributeNameMap() {
            if (rootNode != null) {
                return rootNode.attributeNameMap();
            } else if (rootGroup != null) {
                return rootGroup.attributeNameMap();
            } else {
                throw new UtilsException("Invalid Expression");
            }
        }

        @Override
        public Map<String, AttributeValue> attributeValueMap() {
            if (rootNode != null) {
                return rootNode.attributeValueMap();
            } else if (rootGroup != null) {
                return rootGroup.attributeValueMap();
            } else {
                throw new UtilsException("Invalid Expression");
            }
        }
    }

    public static class NameImpl implements Name {
        private final String name;
        private final String alias;
        private final FilterExprImpl filterExpression;
        private AbstractComparator comparator;

        public NameImpl(final String name, final String alias, final FilterExprImpl filterExpression) {
            this.name = name;
            this.alias = alias;
            this.filterExpression = filterExpression;
        }

        @Override
        public Comparator gt() {
            this.comparator = new GreaterThan(filterExpression);

            return this.comparator;
        }

        @Override
        public Comparator lt() {
            return (this.comparator = new LessThan(filterExpression));
        }

        @Override
        public Comparator gte() {
            return (this.comparator = new GreaterThanOrEquals(filterExpression));
        }

        @Override
        public Comparator lte() {
            return (this.comparator = new LessThanOrEquals(filterExpression));
        }

        @Override
        public Comparator eq() {
            return (this.comparator = new Equals(filterExpression));
        }

        @Override
        public Comparator notExists() {
            return (this.comparator = new NotExists(filterExpression));
        }

        String expression() {
            if (!(comparator instanceof NotExists)) {
                final ValueImpl value = comparator.value;
                final Operator operator;

                if (value == null) {
                    throw new UtilsException("Invalid Expression");
                }

                operator = value.operator;

                return MessageFormat.format("#{0}{1}:{2}{3}", alias, comparator.expression(), value.name, operator == null ? "" : operator.expression());
            } else {
                return MessageFormat.format(comparator.expression(), name);
            }
        }

        Map<String, String> attributeNameMap() {
            if (!(comparator instanceof NotExists)) {
                final ValueImpl value = comparator.value;
                final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                final AbstractOperator operator = value.operator;

                builder.put("#" + alias, name);

                if (operator != null) {
                    builder.putAll(operator.attributeNameMap());
                }

                return builder.build();
            } else {
                return ImmutableMap.of();
            }
        }

        Map<String, AttributeValue> attributeValueMap() {
            if (!(comparator instanceof NotExists)) {
                final ImmutableMap.Builder<String, AttributeValue> builder = ImmutableMap.builder();
                final ValueImpl value = comparator.value;
                final AbstractOperator operator = value.operator;

                builder.put(":" + value.name, value.value);

                if (operator != null) {
                    builder.putAll(operator.attributeValueMap());
                }

                return builder.build();
            } else {
                return ImmutableMap.of();
            }
        }

        @Override
        public String toString() {
            return "Name{" +
                    "name='" + name + '\'' +
                    ", alias='" + alias + '\'' +
                    ", comparator=" + comparator +
                    '}';
        }
    }

    private static abstract class AbstractComparator implements Comparator {
        private ValueImpl value;
        private final FilterExprImpl filterExpression;

        protected AbstractComparator(final FilterExprImpl filterExpression) {
            this.filterExpression = filterExpression;
        }

        public Value value(final String name, final AttributeValue value) {
            return (this.value = new ValueImpl(name, value, filterExpression));
        }

        @Override
        public String toString() {
            return "AbstractComparator{" +
                    "value=" + value +
                    '}';
        }
    }

    private static class LessThan extends AbstractComparator {

        public LessThan(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        public String expression() {
            return " < ";
        }

        @Override
        public String toString() {
            return "LessThan{} " + super.toString();
        }
    }

    private static final class GreaterThan extends AbstractComparator {

        private GreaterThan(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        public String expression() {
            return " > ";
        }

        @Override
        public String toString() {
            return "GreaterThan{} " + super.toString();
        }
    }

    private static final class LessThanOrEquals extends AbstractComparator {
        private LessThanOrEquals(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        public String expression() {
            return " <= ";
        }

        @Override
        public String toString() {
            return "LessThanOrEquals{} " + super.toString();
        }
    }

    private static final class GreaterThanOrEquals extends AbstractComparator {
        private GreaterThanOrEquals(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        public String expression() {
            return " >= ";
        }

        @Override
        public String toString() {
            return "GreaterThanOrEquals{} " + super.toString();
        }
    }

    private static final class Equals extends AbstractComparator {

        private Equals(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        public String expression() {
            return " = ";
        }

        @Override
        public String toString() {
            return "Equals{} " + super.toString();
        }
    }

    private static final class NotExists extends AbstractComparator {
        public NotExists(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        public Value value(final String name, final AttributeValue value) {
            throw new UnsupportedOperationException("This is not supported for a \"Not exists\" check");
        }

        @Override
        public String expression() {
            return "attribute_not_exists({0})";
        }
    }

    public static final class ValueImpl implements Value {
        private final AttributeValue value;
        private AbstractOperator operator;
        private final FilterExprImpl filterExpression;
        private final String name;


        private ValueImpl(final String name, final AttributeValue value, final FilterExprImpl filterExpression) {
            this.name = name;
            this.value = value;
            this.filterExpression = filterExpression;
        }

        @Override
        public Name and(final String name, final String alias) {
            this.operator = new And(name, alias, filterExpression);

            return this.operator.nextName;
        }

        @Override
        public Operator and() {
            return (this.operator = new And(filterExpression));
        }

        @Override
        public Name and(final String name) {
            return and(name, name);
        }

        @Override
        public Name or(final String name, final String alias) {
            this.operator = new Or(name, alias, filterExpression);

            return this.operator.nextName;
        }

        @Override
        public Operator or() {
            return (this.operator = new Or(filterExpression));
        }

        @Override
        public Name or(final String name) {
            return or(name, name);
        }

        @Override
        public Expr buildFilterExpression() {
            return new ExprImpl(filterExpression);
        }

        @Override
        public String toString() {
            return "Value{" +
                    "value=" + value +
                    ", comparator=" + operator +
                    '}';
        }
    }

    private static abstract class AbstractOperator implements Operator {
        private NameImpl nextName;
        private final FilterExprImpl filterExpr;
        private GroupImpl group;

        protected AbstractOperator(final String nextName, final String alias, final FilterExprImpl filterExpression) {
            this.nextName = new NameImpl(nextName, alias, filterExpression);
            this.filterExpr = filterExpression;
        }

        protected AbstractOperator(final FilterExprImpl filterExpression) {
            this.filterExpr = filterExpression;
        }

        public String expression() {
            if (nextName == null && group == null) {
                throw new UtilsException("Invalid expression.");
            }

            return getExpression() + (nextName != null ? nextName.expression() : group.expression());
        }

        protected abstract String getExpression();


        @Override
        public String toString() {
            return "AbstractOperator{" +
                    "nextName=" + nextName +
                    '}';
        }


        @Override
        public Name name(final String name, final String alias) {
            return (this.nextName = new NameImpl(name, alias, filterExpr));
        }

        @Override
        public Group group(final Expr expr) {
            return (this.group = new GroupImpl(filterExpr, expr));
        }

        public Map<String, String> attributeNameMap() {
            return nextName != null ? nextName.attributeNameMap() : group.attributeNameMap();
        }

        public Map<String, AttributeValue> attributeValueMap() {
            return nextName != null ? nextName.attributeValueMap() : group.attributeValueMap();
        }
    }

    private static final class And extends AbstractOperator {

        private And(final String name, final String alias, final FilterExprImpl filterExpression) {
            super(name, alias, filterExpression);
        }

        public And(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        protected String getExpression() {
            return " and ";
        }
    }

    private static final class Or extends AbstractOperator {

        private Or(final String name, final String alias, final FilterExprImpl filterExpression) {
            super(name, alias, filterExpression);
        }

        public Or(final FilterExprImpl filterExpression) {
            super(filterExpression);
        }

        @Override
        public String toString() {
            return "Or{} " + super.toString();
        }

        @Override
        protected String getExpression() {
            return " or ";
        }
    }

    private static final class GroupImpl implements Group {
        private AbstractOperator operator;
        private final FilterExprImpl filterExpr;
        private final Expr expr;

        private GroupImpl(final FilterExprImpl filterExpr, final Expr expr) {
            this.filterExpr = filterExpr;
            this.expr = expr;
        }

        @Override
        public Operator and() {
            return (this.operator = new And(filterExpr));
        }

        @Override
        public Operator or() {
            return (this.operator = new Or(filterExpr));
        }

        @Override
        public String expression() {
            return operator != null ?
                    MessageFormat.format("({0}) {1}", expr.expression(), operator.expression())
                    : MessageFormat.format("({0})", expr.expression());
        }

        public Map<String, String> attributeNameMap() {
            final ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder().putAll(expr.attributeNameMap());

            if (operator != null) {
                builder.putAll(operator.attributeNameMap());
            }

            return builder.build();
        }

        public Map<String, AttributeValue> attributeValueMap() {
            final ImmutableMap.Builder<String, AttributeValue> builder = ImmutableMap.<String, AttributeValue>builder().putAll(expr.attributeValueMap());

            if (operator != null) {
                builder.putAll(operator.attributeValueMap());
            }

            return builder.build();
        }

        @Override
        public Expr buildFilterExpression() {
            final Expr expr;

            if (operator != null) {
                throw new UtilsException("Invalid expression.");
            }

            expr = new ExprImpl(this.filterExpr);

            return (Expr) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[]{Expr.class},
                    (proxy, method, args) -> method.invoke(expr, args));
        }
    }
}
