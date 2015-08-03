package org.umlg.sqlg.sql.parse;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;

/**
 * Created by pieter on 2015/08/03.
 */
public class WhereClause {

    private P p;

    private WhereClause(P p) {
        this.p = p;
    }

    public static WhereClause from(P p) {
        return new WhereClause(p);
    }

    public String toSql() {
        if (p.getBiPredicate() instanceof Compare) {
            switch ((Compare) p.getBiPredicate()) {
                case eq:
                    return "=";
                case neq:
                    return "<>";
                case gt:
                    return ">";
                case gte:
                    return ">=";
                case lt:
                    return "<";
                case lte:
                    return "<=";
            }
        }
        throw new IllegalStateException("Unhandled BiPredicate " + p.getBiPredicate().toString());
    }
}
