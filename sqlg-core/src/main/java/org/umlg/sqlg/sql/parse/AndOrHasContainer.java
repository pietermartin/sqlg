package org.umlg.sqlg.sql.parse;

import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/11/02
 */
public class AndOrHasContainer {


    //AND and OR means its represents a nested and/or traversal.
    //NONE means it represents an HasContainer.
    public enum TYPE {
        AND,
        OR,
        NONE;

        public static TYPE from(ConnectiveStep connectiveStep) {
            if (connectiveStep instanceof AndStep) {
                return AND;
            } else if (connectiveStep instanceof OrStep) {
                return OR;
            } else {
                return NONE;
            }
        }
    }

    private TYPE type;
    //This represents the type's traversals.
    //i.e. g.V().or(__.traversal1, __.traversal2)
    private List<AndOrHasContainer> andOrHasContainers = new ArrayList<>();
    private List<HasContainer> hasContainers = new ArrayList<>();

    public AndOrHasContainer(TYPE type) {
        this.type = type;
    }

    public void addHasContainer(HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    public void addAndOrHasContainer(AndOrHasContainer andOrHasContainer) {
        this.andOrHasContainers.add(andOrHasContainer);
    }

    public TYPE getType() {
        return type;
    }

    public void toSql(SqlgGraph sqlgGraph, SchemaTableTree schemaTableTree, StringBuilder result) {
        toSql(sqlgGraph, schemaTableTree, result, 0);
    }

    public void toSql(SqlgGraph sqlgGraph, SchemaTableTree schemaTableTree, StringBuilder result, int depth) {
        if (!this.hasContainers.isEmpty()) {
            result.append("(");
        }
        for (HasContainer h : this.hasContainers) {
            WhereClause whereClause = WhereClause.from(h.getPredicate());
            result.append(whereClause.toSql(sqlgGraph, schemaTableTree, h));
        }
        if (!this.hasContainers.isEmpty()) {
            result.append(")");
        }
        int count = 1;
        if (!this.andOrHasContainers.isEmpty()) {
            result.append("\n");
            for (int i = 0; i < depth; i++) {
                result.append("\t");
            }
            result.append("(");
        }
        for (AndOrHasContainer andOrHasContainer : this.andOrHasContainers) {
            andOrHasContainer.toSql(sqlgGraph, schemaTableTree, result, depth + 1);
            if (count++ < this.andOrHasContainers.size()) {
                switch (this.type) {
                    case AND:
                        result.append(" AND ");
                        break;
                    case OR:
                        result.append(" OR ");
                        break;
                    case NONE:
                        break;
                }
            }
        }
        if (!this.andOrHasContainers.isEmpty()) {
            result.append("\n");
            for (int i = 0; i < depth - 1; i++) {
                result.append("\t");
            }
            result.append(")");
        }
    }

    public void setParameterOnStatement(Multimap<String, Object> keyValueMap) {
        for (HasContainer hasContainer : this.hasContainers) {
            WhereClause whereClause = WhereClause.from(hasContainer.getPredicate());
            whereClause.putKeyValueMap(hasContainer, keyValueMap);
        }
        for (AndOrHasContainer andOrHasContainer : this.andOrHasContainers) {
            andOrHasContainer.setParameterOnStatement(keyValueMap);
        }
    }
}
