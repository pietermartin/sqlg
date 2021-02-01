package org.umlg.sqlg.sql.parse;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.umlg.sqlg.predicate.Existence;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/11/02
 */
public class AndOrHasContainer {


    private Set<String> connectiveStepLabels;

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
    private final List<HasContainer> hasContainers = new ArrayList<>();

    public AndOrHasContainer(TYPE type) {
        this.type = type;
    }

    public Set<String> getConnectiveStepLabels() {
        return connectiveStepLabels;
    }

    public void setConnectiveStepLabels(Set<String> connectiveStepLabels) {
        this.connectiveStepLabels = connectiveStepLabels;
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

    public List<HasContainer> getAllHasContainers() {
        List<HasContainer> result = new ArrayList<>(this.hasContainers);
        internalAllHasContainers(result);
        return result;
    }

    private void internalAllHasContainers(List<HasContainer> result) {
        result.addAll(this.hasContainers);
        for (AndOrHasContainer andOrHasContainer : this.andOrHasContainers) {
            andOrHasContainer.internalAllHasContainers(result);
        }
    }

    void toSql(SqlgGraph sqlgGraph, SchemaTableTree schemaTableTree, StringBuilder result) {
        toSql(sqlgGraph, schemaTableTree, result, 0);
    }

    private void toSql(SqlgGraph sqlgGraph, SchemaTableTree schemaTableTree, StringBuilder result, int depth) {
        if (!this.hasContainers.isEmpty()) {
            boolean first = true;
            for (HasContainer h : this.hasContainers) {
                if (!SqlgUtil.isBulkWithin(sqlgGraph, h)) {
                    if (first) {
                        first = false;
                        result.append("(");
                    } else {
                    	result.append(" AND ");
                    }
                    String k=h.getKey();
                    WhereClause whereClause = WhereClause.from(h.getPredicate());
                    
                    // check if property exists
                    String bool=null;
                    if (!k.equals(T.id.getAccessor())){
                    	Map<String,PropertyType> pts=sqlgGraph.getTopology().getTableFor(schemaTableTree.getSchemaTable());
                        if (pts!=null && !pts.containsKey(k)){
                        	// verify if we have a value
                        	Multimap<String, Object> keyValueMap=LinkedListMultimap.create();
                        	whereClause.putKeyValueMap(h, keyValueMap, schemaTableTree);
                        	// we do
                        	if (keyValueMap.size()>0){
                        		bool="? is null";
                        	} else {
                        		 if (Existence.NULL.equals(h.getBiPredicate())) {
                        			 bool="1=1";
                        		 } else {
                        			 bool="1=0";
                        		 }
                        	}
                        }
                    }
                    if (bool!=null){
                    	result.append(bool);
                    } else {
                    	result.append(whereClause.toSql(sqlgGraph, schemaTableTree, h));
                    }
                }
            }
            if (!first) {
                result.append(")");
            }
        }
        int count = 1;
        if (!this.andOrHasContainers.isEmpty()) {
            result.append("\n");
            for (int i = 0; i < depth; i++) {
                result.append("\t");
            }
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

    public void setParameterOnStatement(Multimap<String, Object> keyValueMap, SchemaTableTree schemaTableTree) {
        for (HasContainer hasContainer : this.hasContainers) {
            WhereClause whereClause = WhereClause.from(hasContainer.getPredicate());
            whereClause.putKeyValueMap(hasContainer, keyValueMap, schemaTableTree);
        }
        for (AndOrHasContainer andOrHasContainer : this.andOrHasContainers) {
            andOrHasContainer.setParameterOnStatement(keyValueMap, schemaTableTree);
        }
    }
}
