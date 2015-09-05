package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.T;
import org.umlg.sqlg.predicate.Text;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.List;

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
                result += prefix + ".\"ID\"";
            } else {
                result += prefix + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            }
            result += compareToSql((Compare) p.getBiPredicate());
            return result;
        } else if (p.getBiPredicate() instanceof Contains) {
            if (hasContainer.getKey().equals(T.id.getAccessor())) {
                result += prefix + ".\"ID\"";
            } else {
                result += prefix + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            }
            result += containsToSql((Contains) p.getBiPredicate(), ((List) p.getValue()).size());
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
        } else if (p.getBiPredicate() instanceof Text) {
            prefix += "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            result += textToSql(sqlgGraph.getSqlDialect(), prefix, (Text) p.getBiPredicate());
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

    private static String containsToSql(Contains contains, int size) {
        String result;
        switch (contains) {
            case within:
                result = " in (";
                break;
            case without:
                result = " not in (";
                break;
            default:
                throw new RuntimeException("Unknown Contains" + contains.name());
        }
        for (int i = 0; i < size; i++) {
            result += "?";
            if (i < size - 1 && size > 1) {
                result += ", ";

            }
        }
        result += ")";
        return result;
    }

    private static String textToSql(SqlDialect sqlDialect, String prefix, Text text) {
        if (!sqlDialect.supportsILike()) {
            prefix = "lower(" + prefix + ")";
        }
        String result;
        switch (text) {
            case contains:
                result = " like ?";
                break;
            case ncontains:
                result = " not like ?";
                break;
            case containsCIS:
                if (sqlDialect.supportsILike()) {
                    result = " ilike ?";
                } else {
                    result = " like lower(?)";
                }
                break;
            case ncontainsCIS:
                if (sqlDialect.supportsILike()) {
                    result = " not ilike ?";
                } else {
                    result = " not like lower(?)";
                }
                break;
            case startsWith:
                result = " like ?";
                break;
            case nstartsWith:
                result = " not like ?";
                break;
            case endsWith:
                result = " like ?";
                break;
            case nendsWith:
                result = " not like ?";
                break;
            default:
                throw new RuntimeException("Unknown Contains " + text.name());
        }
        return prefix + result;
    }

    public void putKeyValueMap(HasContainer hasContainer, Multimap<String, Object> keyValueMap) {
        if (p instanceof OrP) {
            OrP<?> orP = (OrP) p;
            Preconditions.checkState(orP.getPredicates().size() == 2, "Only handling OrP with 2 predicates!");
            P p1 = orP.getPredicates().get(0);
            P p2 = orP.getPredicates().get(1);
            keyValueMap.put(hasContainer.getKey(), p1.getValue());
            keyValueMap.put(hasContainer.getKey(), p2.getValue());
        } else if (p.getBiPredicate() == Contains.within || p.getBiPredicate() == Contains.without) {
            List values = (List) hasContainer.getValue();
            for (Object value : values) {
                keyValueMap.put(hasContainer.getKey(), value);
            }
        } else if (p.getBiPredicate() == Text.contains || p.getBiPredicate() == Text.ncontains ||
                p.getBiPredicate() == Text.containsCIS || p.getBiPredicate() == Text.ncontainsCIS) {
            keyValueMap.put(hasContainer.getKey(), "%" + hasContainer.getValue() + "%");
        } else if (p.getBiPredicate() == Text.startsWith || p.getBiPredicate() == Text.nstartsWith) {
            keyValueMap.put(hasContainer.getKey(), hasContainer.getValue() + "%");
        } else if (p.getBiPredicate() == Text.endsWith || p.getBiPredicate() == Text.nendsWith) {
            keyValueMap.put(hasContainer.getKey(), "%" + hasContainer.getValue());
        } else {
            keyValueMap.put(hasContainer.getKey(), hasContainer.getValue());
        }
    }
}
