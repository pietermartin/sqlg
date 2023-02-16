package org.umlg.sqlg.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.function.BiPredicate;

public record Lquery(String operator) implements BiPredicate<Lquery.LqueryQuery, Lquery.LqueryQuery> {

    public record LqueryQuery(String query, boolean lquery) {}

    public static P<LqueryQuery> ancestorOfRightOrEquals(final String ltree) {
        return new P<>(new Lquery("@>"), new LqueryQuery(ltree, false));
    }
    
    public static P<LqueryQuery> descendantOfRightOrEquals(final String ltree) {
        return new P<>(new Lquery("<@"), new LqueryQuery(ltree, false));
    }

    public static P<LqueryQuery> lquery(final String lquery) {
        return new P<>(new Lquery("~"), new LqueryQuery(lquery, true));
    }

    @Override
    public boolean test(LqueryQuery s, LqueryQuery s2) {
        return false;
    }

    public Lquery(String operator) {
        this.operator = operator;
    }

    public String getOperator() {
        return operator;
    }
}
