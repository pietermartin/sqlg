package org.umlg.sqlg.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.function.BiPredicate;

public record LqueryArray(String operator) implements BiPredicate<LqueryArray.LqueryQueryArray, LqueryArray.LqueryQueryArray> {

    public record LqueryQueryArray(String[] query, boolean lquery) {}

    public static P<LqueryQueryArray> ancestorOfRightOrEquals(final String[] ltree) {
        return new P<>(new LqueryArray("@>"), new LqueryQueryArray(ltree, false));
    }

    public static P<LqueryQueryArray> descendantOfRightOrEquals(final String[] ltree) {
        return new P<>(new LqueryArray("<@"), new LqueryQueryArray(ltree, false));
    }

    @Override
    public boolean test(LqueryQueryArray s, LqueryQueryArray s2) {
        return false;
    }

    public String getOperator() {
        return operator;
    }
}
