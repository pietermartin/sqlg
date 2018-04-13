package org.umlg.sqlg.sql.parse;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.T;
import org.umlg.sqlg.predicate.PropertyReference;
import org.umlg.sqlg.predicate.Existence;
import org.umlg.sqlg.predicate.FullText;
import org.umlg.sqlg.predicate.Text;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.util.Collection;

/**
 * Created by pieter on 2015/08/03.
 */
public class WhereClause {

    private static final String LIKE = " like ?";
    private static final String NOT_LIKE = " not like ?";
    private P<?> p;

    private WhereClause(P<?> p) {
        this.p = p;
    }

    public static WhereClause from(P<?> p) {
        return new WhereClause(p);
    }

    String toSql(SqlgGraph sqlgGraph, SchemaTableTree schemaTableTree, HasContainer hasContainer) {
        String result = "";

        String prefix = sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTableTree.getSchemaTable().getSchema());
        prefix += ".";
        prefix += sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTableTree.getSchemaTable().getTable());

        if (p.getValue() instanceof PropertyReference && p.getBiPredicate() instanceof Compare){
        	result += prefix + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
        	String column= prefix + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(((PropertyReference)p.getValue()).getColumnName());
        	result += compareToSql((Compare) p.getBiPredicate(),column);
        	return result;
        } else if (p.getBiPredicate() instanceof Compare) {
            if (hasContainer.getKey().equals(T.id.getAccessor())) {
                if (schemaTableTree.isHasIDPrimaryKey()) {
                    result += prefix + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID");
                    result += compareToSql((Compare) p.getBiPredicate());
                } else {
                    int i = 1;
                    for (String identifier : schemaTableTree.getIdentifiers()) {
                        result += prefix + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier);
                        result += compareToSql((Compare) p.getBiPredicate());
                        if (i++ < schemaTableTree.getIdentifiers().size()) {
                            result += " AND ";
                        }
                    }
                }
            } else {
                result += prefix + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
                result += compareToSql((Compare) p.getBiPredicate());
            }
            return result;
        } else if ((!sqlgGraph.getSqlDialect().supportsBulkWithinOut() || (!SqlgUtil.isBulkWithinAndOut(sqlgGraph, hasContainer))) && p.getBiPredicate() instanceof Contains) {
            if (hasContainer.getKey().equals(T.id.getAccessor())) {
                result += prefix + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID");
            } else {
                result += prefix + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            }
            result += containsToSql((Contains) p.getBiPredicate(), ((Collection<?>) p.getValue()).size());
            return result;
        } else if (sqlgGraph.getSqlDialect().supportsBulkWithinOut() && p.getBiPredicate() instanceof Contains) {
            result += " tmp" + (schemaTableTree.rootSchemaTableTree().getTmpTableAliasCounter() - 1);
            result += ".without IS NULL";
            return result;
        } else if (p instanceof AndP) {
            AndP<?> andP = (AndP<?>) p;
            Preconditions.checkState(andP.getPredicates().size() == 2, "Only handling AndP with 2 predicates!");
            P<?> p1 = andP.getPredicates().get(0);
            String key;
            if (hasContainer.getKey().equals(T.id.getAccessor())) {
                key = result + sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID");
            } else {
                key = result + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            }
            result += prefix + key + compareToSql((Compare) p1.getBiPredicate());
            P<?> p2 = andP.getPredicates().get(1);
            result += " and " + prefix + key + compareToSql((Compare) p2.getBiPredicate());
            return result;
        } else if (p instanceof OrP) {
            OrP<?> orP = (OrP<?>) p;
            Preconditions.checkState(orP.getPredicates().size() == 2, "Only handling OrP with 2 predicates!");
            P<?> p1 = orP.getPredicates().get(0);
            String key;
            if (hasContainer.getKey().equals(T.id.getAccessor())) {
                key = result + sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID");
            } else {
                key = result + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            }
            result += prefix + key + compareToSql((Compare) p1.getBiPredicate());
            P<?> p2 = orP.getPredicates().get(1);
            result += " or " + prefix + key + compareToSql((Compare) p2.getBiPredicate());
            return result;
        } else if (p.getBiPredicate() instanceof Text) {
            prefix += "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            result += textToSql(sqlgGraph.getSqlDialect(), prefix, (Text) p.getBiPredicate());
            return result;
        } else if (p.getBiPredicate() instanceof FullText){
        	prefix += "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
        	FullText ft=(FullText)p.getBiPredicate();
        	result += sqlgGraph.getSqlDialect().getFullTextQueryText(ft, prefix);
        	return result;
        } else if (p.getBiPredicate() instanceof Existence){
        	result += prefix + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
        	result += " "+p.getBiPredicate().toString();
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

    private static String compareToSql(Compare compare,String column) {
        switch (compare) {
            case eq:
                return " = " + column;
            case neq:
                return " <> " + column;
            case gt:
                return " > " + column;
            case gte:
                return " >= " + column;
            case lt:
                return " < " + column;
            case lte:
                return " <= " + column;
            default:
                throw new RuntimeException("Unknown Compare " + compare.name());
        }
    }

    
    private static String containsToSql(Contains contains, int size) {
        String result;
        if (size == 1) {
            switch (contains) {
                case within:
                    result = " = ?";
                    break;
                case without:
                    result = " <> ?";
                    break;
                default:
                    throw new RuntimeException("Unknown Contains" + contains.name());
            }
        } else {
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
        }
        return result;
    }

    private static String textToSql(SqlDialect sqlDialect, String prefix, Text text) {
        String result;
        switch (text) {
            case contains:
                result = LIKE;
                break;
            case ncontains:
                result = NOT_LIKE;
                break;
            case containsCIS:
                if (!sqlDialect.supportsILike()) {
                    prefix = "lower(" + prefix + ")";
                }
                if (sqlDialect.supportsILike()) {
                    result = " ilike ?";
                } else {
                    result = " like lower(?)";
                }
                break;
            case ncontainsCIS:
                if (!sqlDialect.supportsILike()) {
                    prefix = "lower(" + prefix + ")";
                }
                if (sqlDialect.supportsILike()) {
                    result = " not ilike ?";
                } else {
                    result = " not like lower(?)";
                }
                break;
            case startsWith:
                result = LIKE;
                break;
            case nstartsWith:
                result = NOT_LIKE;
                break;
            case endsWith:
                result = LIKE;
                break;
            case nendsWith:
                result = NOT_LIKE;
                break;
            default:
                throw new RuntimeException("Unknown Contains " + text.name());
        }
        return prefix + result;
    }

    public void putKeyValueMap(HasContainer hasContainer, Multimap<String, Object> keyValueMap, SchemaTableTree schemaTableTree) {
        if (p instanceof OrP) {
            OrP<?> orP = (OrP<?>) p;
            Preconditions.checkState(orP.getPredicates().size() == 2, "Only handling OrP with 2 predicates!");
            P<?> p1 = orP.getPredicates().get(0);
            P<?> p2 = orP.getPredicates().get(1);
            keyValueMap.put(hasContainer.getKey(), p1.getValue());
            keyValueMap.put(hasContainer.getKey(), p2.getValue());
        } else if (p instanceof AndP) {
            AndP<?> andP = (AndP<?>) p;
            Preconditions.checkState(andP.getPredicates().size() == 2, "Only handling AndP with 2 predicates!");
            P<?> p1 = andP.getPredicates().get(0);
            P<?> p2 = andP.getPredicates().get(1);
            keyValueMap.put(hasContainer.getKey(), p1.getValue());
            keyValueMap.put(hasContainer.getKey(), p2.getValue());
        } else if (p.getBiPredicate() == Contains.within || p.getBiPredicate() == Contains.without) {
            Collection<?> values = (Collection<?>) hasContainer.getValue();
            for (Object value : values) {
                if (hasContainer.getKey().equals(T.id.getAccessor())) {
                    keyValueMap.put("ID", value);
                } else {
                    keyValueMap.put(hasContainer.getKey(), value);
                }
            }
        } else if (p.getBiPredicate() == Text.contains || p.getBiPredicate() == Text.ncontains ||
                p.getBiPredicate() == Text.containsCIS || p.getBiPredicate() == Text.ncontainsCIS) {
            keyValueMap.put(hasContainer.getKey(), "%" + hasContainer.getValue() + "%");
        } else if (p.getBiPredicate() == Text.startsWith || p.getBiPredicate() == Text.nstartsWith) {
            keyValueMap.put(hasContainer.getKey(), hasContainer.getValue() + "%");
        } else if (p.getBiPredicate() == Text.endsWith || p.getBiPredicate() == Text.nendsWith) {
            keyValueMap.put(hasContainer.getKey(), "%" + hasContainer.getValue());
        } else if (p.getBiPredicate() instanceof Existence){
        	// no value
        } else if (hasContainer.getKey().equals(T.id.getAccessor()) &&
                hasContainer.getValue() instanceof RecordId &&
                !((RecordId)hasContainer.getValue()).hasId()) {

            RecordId recordId = (RecordId)hasContainer.getValue();
            int i = 0;
            for (Object identifier : recordId.getIdentifiers()) {
                keyValueMap.put(schemaTableTree.getIdentifiers().get(i++), identifier);
            }
        } else {
            keyValueMap.put(hasContainer.getKey(), hasContainer.getValue());
        }
    }
}
