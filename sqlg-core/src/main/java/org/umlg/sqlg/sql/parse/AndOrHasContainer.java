package org.umlg.sqlg.sql.parse;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.umlg.sqlg.predicate.Existence;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/11/02
 */
public class AndOrHasContainer {

    public static final String DEFAULT  = "default";

    //AND and OR means its represents a nested and/or traversal.
    //NONE means it represents an HasContainer.
    public enum TYPE {
        AND,
        OR,
        NONE;

        public static TYPE from(ConnectiveStep<?> connectiveStep) {
            if (connectiveStep instanceof AndStep) {
                return AND;
            } else if (connectiveStep instanceof OrStep) {
                return OR;
            } else {
                return NONE;
            }
        }
    }

    private final TYPE type;
    //This represents the type's traversals.
    //i.e. g.V().or(__.traversal1, __.traversal2)
    private final List<AndOrHasContainer> andOrHasContainers = new ArrayList<>();
    private final Map<String, List<HasContainer>> hasContainers = new HashMap<>();
    private LoopsStepIsStepContainer loopsStepIsStepContainer;

    public AndOrHasContainer(TYPE type) {
        this.type = type;
    }

    public void addHasContainer(String selectLabel, HasContainer hasContainer) {
        this.hasContainers.computeIfAbsent(selectLabel, k -> new ArrayList<>()).add(hasContainer);
    }

    public void addAndOrHasContainer(AndOrHasContainer andOrHasContainer) {
        this.andOrHasContainers.add(andOrHasContainer);
    }

    public LoopsStepIsStepContainer getLoopsStepIsStepContainer() {
        return loopsStepIsStepContainer;
    }

    public void setLoopsStepIsStepContainer(LoopsStepIsStepContainer loopsStepIsStepContainer) {
        this.loopsStepIsStepContainer = loopsStepIsStepContainer;
    }

    public TYPE getType() {
        return type;
    }

    void toSql(SqlgGraph sqlgGraph, SchemaTableTree schemaTableTree, StringBuilder result) {
        toSql(sqlgGraph, schemaTableTree, result, 0);
    }

    private void toSql(SqlgGraph sqlgGraph, SchemaTableTree schemaTableTree, StringBuilder result, int depth) {
        if (!this.hasContainers.isEmpty()) {
            boolean first = true;
            for (String selectLabel : this.hasContainers.keySet()) {

                //get the schemaTableTree for the label
                SchemaTableTree schemaTableTreeForLabel = selectLabel.equals(DEFAULT) ? schemaTableTree : schemaTableTree.schemaTableTreeForLabel(selectLabel);

                List<HasContainer> _hasContainers = this.hasContainers.get(selectLabel);
                for (HasContainer h : _hasContainers) {
                    if (first) {
                        first = false;
                        result.append("(");
                    } else {
                        result.append(" AND ");
                    }
                    String k = h.getKey();
                    WhereClause whereClause = WhereClause.from(h.getPredicate());

                    // check if property exists
                    String bool = null;
                    if (!k.equals(T.id.getAccessor())) {
                        Map<String, PropertyDefinition> pts = sqlgGraph.getTopology().getTableFor(schemaTableTreeForLabel.getSchemaTable());
                        if (pts != null && !pts.containsKey(k)) {
                            // verify if we have a value
                            List<Pair<PropertyDefinition, Object>> pairs = whereClause.putKeyValueMap(h, schemaTableTreeForLabel);
                            // we do
                            if (!pairs.isEmpty()) {
                                bool = "? is null";
                            } else {
                                if (Existence.NULL.equals(h.getBiPredicate())) {
                                    bool = "1=1";
                                } else {
                                    bool = "1=0";
                                }
                            }
                        }
                    }
                    if (bool != null) {
                        result.append(bool);
                    } else {
                        result.append(whereClause.toSql(sqlgGraph, schemaTableTreeForLabel, h, true));
                    }
                }
            }
            result.append(")");
        } else if (this.loopsStepIsStepContainer != null) {
            WhereClause whereClause = WhereClause.from(this.loopsStepIsStepContainer.isStep().getPredicate());
            result.append(whereClause.toSql(sqlgGraph, schemaTableTree, new HasContainer("depth", this.loopsStepIsStepContainer.isStep().getPredicate()), true));
//            result.append("(depth < ").append("2").append(")");
        }
        int count = 1;
        if (!this.andOrHasContainers.isEmpty()) {
            result.append("\n");
            result.append("\t".repeat(Math.max(0, depth)));
            if (this.hasContainers.isEmpty()) {
                result.append("(");
            } else {
                result.append(" AND (");
            }
        }
        for (AndOrHasContainer andOrHasContainer : this.andOrHasContainers) {
            andOrHasContainer.toSql(sqlgGraph, schemaTableTree, result, depth + 1);
            if (count++ < this.andOrHasContainers.size()) {
                switch (this.type) {
                    case AND -> result.append(" AND ");
                    case OR -> result.append(" OR ");
                    case NONE -> {
                    }
                }
            }
        }
        if (!this.andOrHasContainers.isEmpty()) {
            result.append("\n");
            result.append("\t".repeat(Math.max(0, depth - 1)));
            result.append(")");
        }
    }

    public List<Pair<PropertyDefinition, Object>> setParameterOnStatement(SchemaTableTree schemaTableTree) {
        List<Pair<PropertyDefinition, Object>> results = new ArrayList<>();
        for (String selectLabel : this.hasContainers.keySet()) {
            List<HasContainer> _hasContainers = this.hasContainers.get(selectLabel);
            for (HasContainer hasContainer : _hasContainers) {
                WhereClause whereClause = WhereClause.from(hasContainer.getPredicate());
                results.addAll(whereClause.putKeyValueMap(hasContainer, schemaTableTree));
            }
        }
        if (this.loopsStepIsStepContainer != null) {
            HasContainer hasContainer = new HasContainer("depth", this.loopsStepIsStepContainer.isStep().getPredicate());
            WhereClause whereClause = WhereClause.from(hasContainer.getPredicate());
            results.addAll(whereClause.putKeyValueMap(hasContainer, schemaTableTree));
        }
        for (AndOrHasContainer andOrHasContainer : this.andOrHasContainers) {
            results.addAll(andOrHasContainer.setParameterOnStatement(schemaTableTree));
        }
        return results;
    }

    //This is to check
    public boolean hasHasContainers() {
        if (!this.hasContainers.isEmpty() || this.loopsStepIsStepContainer != null) {
            return true;
        } else {
            for (AndOrHasContainer andOrHasContainer : this.andOrHasContainers) {
                if (andOrHasContainer.hasHasContainers()) {
                    return true;
                }
            }
        }
        return false;
    }
}
