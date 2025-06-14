package org.umlg.sqlg.sql.parse;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.T;
import org.umlg.sqlg.predicate.*;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.Preconditions;
import org.umlg.sqlg.util.SqlgUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Created by pieter on 2015/08/03.
 */
@SuppressWarnings("UnnecessaryDefault")
public class WhereClause {

    private static final String LIKE = " like ?";
    private static final String NOT_LIKE = " not like ?";
    private final P<?> p;

    private WhereClause(P<?> p) {
        this.p = p;
    }

    public static WhereClause from(P<?> p) {
        return new WhereClause(p);
    }

    public String toSql(SqlgGraph sqlgGraph, SchemaTableTree schemaTableTree, HasContainer hasContainer) {
        return toSql(sqlgGraph, schemaTableTree, hasContainer, false);
    }

    public String toSql(SqlgGraph sqlgGraph, SchemaTableTree schemaTableTree, HasContainer hasContainer, boolean isInAndOrHsContainer) {
        String prefix = sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTableTree.getSchemaTable().getSchema());
        prefix += ".";
        prefix += sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTableTree.getSchemaTable().getTable());
        return toSql(sqlgGraph, schemaTableTree, hasContainer, prefix, isInAndOrHsContainer);
    }

    public String toSql(SqlgGraph sqlgGraph, SchemaTableTree schemaTableTree, HasContainer hasContainer, String prefix) {
        return toSql(sqlgGraph, schemaTableTree, hasContainer, prefix, false);
    }

    public String toSql(SqlgGraph sqlgGraph, SchemaTableTree schemaTableTree, HasContainer hasContainer, String prefix, boolean isInAndOrHsContainer) {
        StringBuilder result = new StringBuilder();

        if (p.getValue() instanceof PropertyReference && p.getBiPredicate() instanceof Compare) {
            result.append(prefix).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey()));
            String column = prefix + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(((PropertyReference) p.getValue()).getColumnName());
            result.append(compareToSql((Compare) p.getBiPredicate(), column));
            return result.toString();
        } else if (p.getBiPredicate() instanceof Compare) {
            if (hasContainer.getKey().equals(T.id.getAccessor())) {
                if (schemaTableTree.isHasIDPrimaryKey()) {
                    result.append(prefix).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                    result.append(compareToSql((Compare) p.getBiPredicate()));
                } else {
                    int i = 1;
                    for (String identifier : schemaTableTree.getIdentifiers()) {
                        result.append(prefix).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                        result.append(compareToSql((Compare) p.getBiPredicate()));
                        if (i++ < schemaTableTree.getIdentifiers().size()) {
                            result.append(" AND ");
                        }
                    }
                }
            } else {
                result.append(prefix).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey()));
                result.append(compareToSql((Compare) p.getBiPredicate()));
            }
            return result.toString();
        } else if ((!sqlgGraph.getSqlDialect().supportsBulkWithinOut() || (!SqlgUtil.isBulkWithinAndOut(sqlgGraph, hasContainer)) || isInAndOrHsContainer) &&
                p.getBiPredicate() instanceof Contains) {

            if (hasContainer.getKey().equals(T.id.getAccessor())) {
                if (schemaTableTree.isHasIDPrimaryKey()) {
                    result.append(prefix).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                    result.append(containsToSql((Contains) p.getBiPredicate(), ((Collection<?>) p.getValue()).size()));
                } else {
                    int i = 1;
                    Collection<?> recordIds = ((Collection<?>) p.getValue());
                    for (Object ignore : recordIds) {
                        int j = 1;
                        result.append("(");
                        for (String identifier : schemaTableTree.getIdentifiers()) {
                            result.append(prefix).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                            result.append(containsToSql((Contains) p.getBiPredicate(), 1));
                            if (j++ < schemaTableTree.getIdentifiers().size()) {
                                result.append(" AND ");
                            }
                        }
                        result.append(")");
                        if (i++ < recordIds.size()) {
                            result.append(" OR\n\t");
                        }
                    }
//                    for (String identifier : schemaTableTree.getIdentifiers()) {
//                        result.append(prefix).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
//                        result.append(containsToSql((Contains) p.getBiPredicate(), ((Collection<?>) p.getValue()).size()));
//                        if (i++ < schemaTableTree.getIdentifiers().size()) {
//                            result.append(" AND ");
////                            result.append(" OR ");
//                        }
//                    }
                }
            } else {
                result.append(prefix).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey()));
                result.append(containsToSql((Contains) p.getBiPredicate(), ((Collection<?>) p.getValue()).size()));
            }
            return result.toString();
        } else if (sqlgGraph.getSqlDialect().supportsBulkWithinOut() && p.getBiPredicate() instanceof Contains) {
            result.append(" tmp").append(schemaTableTree.rootSchemaTableTree().getTmpTableAliasCounter() - 1);
            result.append(".without IS NULL");
            return result.toString();
        } else if (p instanceof AndP<?> andP) {
            Preconditions.checkState(andP.getPredicates().size() == 2, "Only handling AndP with 2 predicates!");
            P<?> p1 = andP.getPredicates().get(0);
            String key;
            if (hasContainer.getKey().equals(T.id.getAccessor())) {
                key = result + sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID");
            } else {
                key = result + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            }
            result.append(prefix).append(key).append(compareToSql((Compare) p1.getBiPredicate()));
            P<?> p2 = andP.getPredicates().get(1);
            result.append(" and ").append(prefix).append(key).append(compareToSql((Compare) p2.getBiPredicate()));
            return result.toString();
        } else if (p instanceof OrP<?> orP) {
            Preconditions.checkState(orP.getPredicates().size() == 2, "Only handling OrP with 2 predicates!");
            P<?> p1 = orP.getPredicates().get(0);
            String key;
            if (hasContainer.getKey().equals(T.id.getAccessor())) {
                key = result + sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID");
            } else {
                key = result + "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            }
            result.append(prefix).append(key).append(compareToSql((Compare) p1.getBiPredicate()));
            P<?> p2 = orP.getPredicates().get(1);
            result.append(" or ").append(prefix).append(key).append(compareToSql((Compare) p2.getBiPredicate()));
            return result.toString();
        } else if (p.getBiPredicate() instanceof Text) {
            prefix += "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            result.append(textToSql(sqlgGraph.getSqlDialect(), prefix, (Text) p.getBiPredicate()));
            return result.toString();
        } else if (p.getBiPredicate() instanceof FullText ft) {
            prefix += "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            result.append(sqlgGraph.getSqlDialect().getFullTextQueryText(ft, prefix));
            return result.toString();
        } else if (p.getBiPredicate() instanceof Lquery) {
            prefix += "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            result.append(lqueryToSql(prefix, (Lquery) p.getBiPredicate()));
            return result.toString();
        } else if (p.getBiPredicate() instanceof LqueryArray) {
            prefix += "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            result.append(lqueryToSql(prefix, (LqueryArray) p.getBiPredicate()));
            return result.toString();
        } else if (p.getBiPredicate() instanceof Existence) {
            result.append(prefix).append(".").append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey()));
            result.append(" ").append(p.getBiPredicate().toString());
            return result.toString();
        } else if (p.getBiPredicate() instanceof ArrayContains) {
            prefix += "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            result.append(sqlgGraph.getSqlDialect().getArrayContainsQueryText(prefix));
            return result.toString();
        } else if (p.getBiPredicate() instanceof ArrayOverlaps) {
            prefix += "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            result.append(sqlgGraph.getSqlDialect().getArrayOverlapsQueryText(prefix));
            return result.toString();
        } else if (p.getBiPredicate() instanceof PGVectorPredicate pgVectorPredicate) {
            prefix += "." + sqlgGraph.getSqlDialect().maybeWrapInQoutes(hasContainer.getKey());
            switch (pgVectorPredicate.queryType()) {
                case L2_DISTANCE, L1_DISTACNE -> {
                    result.append(prefix)
                            .append(" ")
                            .append(pgVectorPredicate.operator())
                            .append(" ")
                            .append(sqlgGraph.getSqlDialect().toRDBSStringLiteral(pgVectorPredicate.vector()))
                            .append(" < ?");
                }
                case INNER_PRODUCT -> {
                    result.append("((")
                            .append(prefix)
                            .append(" ")
                            .append(pgVectorPredicate.operator())
                            .append(" ")
                            .append(sqlgGraph.getSqlDialect().toRDBSStringLiteral(pgVectorPredicate.vector()))
                            .append(") * -1) < ?");
                }
                case COSINE_DISTANCE -> {
                    result.append("(1 - (")
                            .append(prefix)
                            .append(" ")
                            .append(pgVectorPredicate.operator())
                            .append(" ")
                            .append(sqlgGraph.getSqlDialect().toRDBSStringLiteral(pgVectorPredicate.vector()))
                            .append(")) < ?");
                }
                case HAMMING_DISTANCE, JACCARD_DISTANCE -> {
                    StringBuilder targetVectorAsString = new StringBuilder();
                    for (boolean b : pgVectorPredicate.bitVector()) {
                        targetVectorAsString.append(b ? "1" : "0");
                    }
                    result.append(prefix)
                            .append(" ")
                            .append(pgVectorPredicate.operator())
                            .append(" '")
                            .append(targetVectorAsString)
                            .append("' < ?");
                }
            }
            return result.toString();
        }
        throw new IllegalStateException("Unhandled BiPredicate " + p.getBiPredicate().toString());
    }

    private static String compareToSql(Compare compare) {
        return switch (compare) {
            case eq -> " = ?";
            case neq -> " <> ?";
            case gt -> " > ?";
            case gte -> " >= ?";
            case lt -> " < ?";
            case lte -> " <= ?";
            default -> throw new RuntimeException("Unknown Compare " + compare.name());
        };
    }

    private static String compareToSql(Compare compare, String column) {
        return switch (compare) {
            case eq -> " = " + column;
            case neq -> " <> " + column;
            case gt -> " > " + column;
            case gte -> " >= " + column;
            case lt -> " < " + column;
            case lte -> " <= " + column;
            default -> throw new RuntimeException("Unknown Compare " + compare.name());
        };
    }


    private static String containsToSql(Contains contains, int size) {
        StringBuilder result;
        if (size == 1) {
            result = new StringBuilder(switch (contains) {
                case within -> " = ?";
                case without -> " <> ?";
                default -> throw new RuntimeException("Unknown Contains" + contains.name());
            });
        } else {
            result = new StringBuilder(switch (contains) {
                case within -> " in (";
                case without -> " not in (";
                default -> throw new RuntimeException("Unknown Contains" + contains.name());
            });
            for (int i = 0; i < size; i++) {
                result.append("?");
//                if (i < size - 1 && size > 1) {
                if (i < size - 1) {
                    result.append(", ");

                }
            }
            result.append(")");
        }
        return result.toString();
    }

    private static String lqueryToSql(String prefix, Lquery lquery) {
        return prefix + " " + lquery.getOperator() + " ?";
    }

    private static String lqueryToSql(String prefix, LqueryArray lquery) {
        return prefix + " " + lquery.getOperator() + " ?";
    }

    private static String textToSql(SqlDialect sqlDialect, String prefix, Text text) {
        String result;
        switch (text) {
            case contains -> result = LIKE;
            case ncontains -> result = NOT_LIKE;
            case containsCIS -> {
                if (!sqlDialect.supportsILike()) {
                    prefix = "lower(" + prefix + ")";
                }
                if (sqlDialect.supportsILike()) {
                    result = " ilike ?";
                } else {
                    result = " like lower(?)";
                }
            }
            case ncontainsCIS -> {
                if (!sqlDialect.supportsILike()) {
                    prefix = "lower(" + prefix + ")";
                }
                if (sqlDialect.supportsILike()) {
                    result = " not ilike ?";
                } else {
                    result = " not like lower(?)";
                }
            }
            case startsWith -> result = LIKE;
            case nstartsWith -> result = NOT_LIKE;
            case endsWith -> result = LIKE;
            case nendsWith -> result = NOT_LIKE;
            default -> throw new RuntimeException("Unknown Contains " + text.name());
        }
        return prefix + result;
    }

    public List<Pair<PropertyDefinition, Object>> putKeyValueMap(
            HasContainer hasContainer,
            SchemaTableTree schemaTableTree) {

        List<Pair<PropertyDefinition, Object>> pairs = new ArrayList<>();
        if (hasContainer.getValue() instanceof PropertyReference) {
            return pairs;
        }
        if (this.p instanceof OrP<?> orP) {
            Preconditions.checkState(orP.getPredicates().size() == 2, "Only handling OrP with 2 predicates!");
            P<?> p1 = orP.getPredicates().get(0);
            P<?> p2 = orP.getPredicates().get(1);
            PropertyDefinition propertyDefinition = schemaTableTree.getPropertyDefinitions().get(hasContainer.getKey());
            if (propertyDefinition == null) {
                propertyDefinition = PropertyDefinition.of(PropertyType.from(hasContainer.getValue()));
            }

            pairs.add(Pair.of(propertyDefinition, p1.getValue()));
            pairs.add(Pair.of(propertyDefinition, p2.getValue()));

        } else if (this.p instanceof AndP<?> andP) {
            Preconditions.checkState(andP.getPredicates().size() == 2, "Only handling AndP with 2 predicates!");
            P<?> p1 = andP.getPredicates().get(0);
            P<?> p2 = andP.getPredicates().get(1);
            PropertyDefinition propertyDefinition = schemaTableTree.getPropertyDefinitions().get(hasContainer.getKey());
            if (propertyDefinition == null) {
                propertyDefinition = PropertyDefinition.of(PropertyType.from(hasContainer.getValue()));
            }

            pairs.add(Pair.of(propertyDefinition, p1.getValue()));
            pairs.add(Pair.of(propertyDefinition, p2.getValue()));

        } else if (this.p.getBiPredicate() == Contains.within || this.p.getBiPredicate() == Contains.without) {
            Collection<?> values = (Collection<?>) hasContainer.getValue();
            if (schemaTableTree.isHasIDPrimaryKey()) {
                for (Object value : values) {
                    if (hasContainer.getKey().equals(T.id.getAccessor())) {
                        RecordId recordId;
                        if (!(value instanceof RecordId)) {
                            recordId = RecordId.from(value);
                        } else {
                            recordId = (RecordId) value;
                        }
                        pairs.add(Pair.of(PropertyDefinition.of(PropertyType.LONG), recordId.sequenceId()));

                    } else {
                        PropertyDefinition propertyDefinition = schemaTableTree.getPropertyDefinitions().get(hasContainer.getKey());
                        if (propertyDefinition == null) {
                            propertyDefinition = PropertyDefinition.of(PropertyType.from(hasContainer.getValue()));
                        }
                        pairs.add(Pair.of(propertyDefinition, value));

                    }
                }
            } else {
                for (Object value : values) {
                    if (hasContainer.getKey().equals(T.id.getAccessor())) {
                        int i = 0;
                        for (String identifier : schemaTableTree.getIdentifiers()) {
                            Comparable<?> comparable = ((RecordId) value).getIdentifiers().get(i++);
                            PropertyDefinition propertyDefinition = schemaTableTree.getPropertyDefinitions().get(identifier);
                            Objects.requireNonNull(propertyDefinition, "PropertyDefinition not found for " + identifier);
                            pairs.add(Pair.of(propertyDefinition, comparable));

                        }
                    } else {
                        PropertyDefinition propertyDefinition = schemaTableTree.getPropertyDefinitions().get(hasContainer.getKey());
                        if (propertyDefinition == null) {
                            propertyDefinition = PropertyDefinition.of(PropertyType.from(hasContainer.getValue()));
                        }
                        pairs.add(Pair.of(propertyDefinition, value));
                    }
                }
            }
        } else if (this.p.getBiPredicate() == Text.contains || this.p.getBiPredicate() == Text.ncontains ||
                this.p.getBiPredicate() == Text.containsCIS || this.p.getBiPredicate() == Text.ncontainsCIS) {
            PropertyDefinition propertyDefinition = schemaTableTree.getPropertyDefinitions().get(hasContainer.getKey());
            if (propertyDefinition == null) {
                propertyDefinition = PropertyDefinition.of(PropertyType.from(hasContainer.getValue()));
            }
            pairs.add(Pair.of(propertyDefinition, "%" + hasContainer.getValue() + "%"));

        } else if (this.p.getBiPredicate() == Text.startsWith || this.p.getBiPredicate() == Text.nstartsWith) {
            PropertyDefinition propertyDefinition = schemaTableTree.getPropertyDefinitions().get(hasContainer.getKey());
            if (propertyDefinition == null) {
                propertyDefinition = PropertyDefinition.of(PropertyType.from(hasContainer.getValue()));
            }
            pairs.add(Pair.of(propertyDefinition, hasContainer.getValue() + "%"));

        } else if (this.p.getBiPredicate() == Text.endsWith || this.p.getBiPredicate() == Text.nendsWith) {
            PropertyDefinition propertyDefinition = schemaTableTree.getPropertyDefinitions().get(hasContainer.getKey());
            if (propertyDefinition == null) {
                propertyDefinition = PropertyDefinition.of(PropertyType.from(hasContainer.getValue()));
            }
            pairs.add(Pair.of(propertyDefinition, "%" + hasContainer.getValue()));

        } else if (this.p.getBiPredicate() instanceof Lquery) {
            PropertyDefinition propertyDefinition = schemaTableTree.getPropertyDefinitions().get(hasContainer.getKey());
            Objects.requireNonNull(propertyDefinition, "PropertyDefinition not found for " + hasContainer.getKey());
            pairs.add(Pair.of(propertyDefinition, hasContainer.getValue()));

        } else if (this.p.getBiPredicate() instanceof LqueryArray) {
            PropertyDefinition propertyDefinition = schemaTableTree.getPropertyDefinitions().get(hasContainer.getKey());
            Objects.requireNonNull(propertyDefinition, "PropertyDefinition not found for " + hasContainer.getKey());
            pairs.add(Pair.of(propertyDefinition, hasContainer.getValue()));

        } else {
            if (this.p.getBiPredicate() instanceof Existence) {
                // no value
            } else if (hasContainer.getKey().equals(T.id.getAccessor()) &&
                    hasContainer.getValue() instanceof RecordId recordId &&
                    !recordId.hasSequenceId()) {

                int i = 0;
                for (Object identifier : recordId.getIdentifiers()) {
                    String schemaTableTreeIdentifier = schemaTableTree.getIdentifiers().get(i++);
                    PropertyDefinition propertyDefinition = schemaTableTree.getPropertyDefinitions().get(schemaTableTreeIdentifier);
                    Objects.requireNonNull(propertyDefinition, "PropertyDefinition not found for " + schemaTableTreeIdentifier);
                    pairs.add(Pair.of(propertyDefinition, identifier));

                }
            } else {
                if (hasContainer.getKey().equals(T.id.getAccessor())) {
                    Object value = hasContainer.getValue();
                    RecordId recordId;
                    if (!(value instanceof RecordId)) {
                        recordId = RecordId.from(value);
                    } else {
                        recordId = (RecordId) value;
                    }
                    pairs.add(Pair.of(PropertyDefinition.of(PropertyType.LONG), recordId.sequenceId()));

                } else {
                    PropertyDefinition propertyDefinition = schemaTableTree.getPropertyDefinitions().get(hasContainer.getKey());
                    if (propertyDefinition == null) {
                        propertyDefinition = PropertyDefinition.of(PropertyType.from(hasContainer.getValue()));
                    }
                    pairs.add(Pair.of(propertyDefinition, hasContainer.getValue()));

                }
            }
        }
        return pairs;
    }
}
