package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.T;
import org.umlg.sqlg.structure.SqlgGraph;

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

    public String toSql(SqlgGraph sqlgGraph, SchemaTableTree schemaTableTree, HasContainer hasContainer) {
        String result = "";

        String prefix = sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTableTree.getSchemaTable().getSchema());
        prefix += ".";
        prefix += sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTableTree.getSchemaTable().getTable());

        if (p.getBiPredicate() instanceof Compare) {
            if (hasContainer.getKey().equals(T.id.getAccessor())) {
                result += prefix +".\"ID\"";
            } else {
                result += prefix + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            }
            result += compareToSql((Compare) p.getBiPredicate());
            return result;
        } else if (p instanceof OrP) {
            OrP<?> orP = (OrP) p;
            Preconditions.checkState(orP.getPredicates().size() == 2, "Only handling OrP with 2 predicates!");
            P p1 = orP.getPredicates().get(0);
            String key;
            if (hasContainer.getKey().equals(T.id.getAccessor())) {
                key = result + "\"ID\"";
            } else {
                key = result + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            }
            result += prefix + key + compareToSql((Compare) p1.getBiPredicate());
            P p2 = orP.getPredicates().get(1);
            result += " or " + prefix + key + compareToSql((Compare) p2.getBiPredicate());
            return result;
        }
        throw new IllegalStateException("Unhandled BiPredicate " + p.getBiPredicate().toString());
    }

    private static String compareToSql(Compare compare) {
        switch (compare) {
            case eq:
                return " = ?";
            case neq:
                return " <> ?";
            case gt:
                return " > ?";
            case gte:
                return " >= ?";
            case lt:
                return " < ?";
            case lte:
                return " <= ?";
            default:
                throw new RuntimeException("Unknown Compare " + compare.name());
        }
    }

    public void putKeyValueMap(HasContainer hasContainer, Multimap<String, Object> keyValueMap) {
        if (p instanceof OrP) {
            OrP<?> orP = (OrP) p;
            Preconditions.checkState(orP.getPredicates().size() == 2, "Only handling OrP with 2 predicates!");
            P p1 = orP.getPredicates().get(0);
            P p2 = orP.getPredicates().get(1);
            keyValueMap.put(hasContainer.getKey(), p1.getValue());
            keyValueMap.put(hasContainer.getKey(), p2.getValue());
        } else {
            keyValueMap.put(hasContainer.getKey(), hasContainer.getValue());
        }
    }
}
