package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.*;
import java.util.*;

import static org.umlg.sqlg.structure.Topology.*;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class EdgeLabel extends AbstractLabel {

    private Logger logger = LoggerFactory.getLogger(EdgeLabel.class.getName());
    //This just won't stick in my brain.
    //hand (out) ----<label>---- finger (in)
    Set<VertexLabel> outVertexLabels = new HashSet<>();
    Set<VertexLabel> inVertexLabels = new HashSet<>();
    private Set<VertexLabel> uncommittedOutVertexLabels = new HashSet<>();
    private Set<VertexLabel> uncommittedInVertexLabels = new HashSet<>();
    private Set<VertexLabel> uncommittedRemovedInVertexLabels = new HashSet<>();
    private Set<VertexLabel> uncommittedRemovedOutVertexLabels = new HashSet<>();
    
    private Topology topology;

    static EdgeLabel loadSqlgSchemaEdgeLabel(String edgeLabelName, VertexLabel outVertexLabel, VertexLabel inVertexLabel, Map<String, PropertyType> properties) {
        //edges are created in the out vertex's schema.
        EdgeLabel edgeLabel = new EdgeLabel(true, edgeLabelName, outVertexLabel, inVertexLabel, properties);
        return edgeLabel;
    }

    static EdgeLabel createEdgeLabel(String edgeLabelName, VertexLabel outVertexLabel, VertexLabel inVertexLabel, Map<String, PropertyType> properties) {
        //edges are created in the out vertex's schema.
        EdgeLabel edgeLabel = new EdgeLabel(false, edgeLabelName, outVertexLabel, inVertexLabel, properties);
        if (!inVertexLabel.getSchema().isSqlgSchema()) {
            edgeLabel.createEdgeTable(outVertexLabel, inVertexLabel, properties);
        }
        edgeLabel.committed = false;
        return edgeLabel;
    }

    static EdgeLabel loadFromDb(Topology topology, String edgeLabelName) {
        return new EdgeLabel(topology, edgeLabelName);
    }

    private EdgeLabel(boolean forSqlgSchema, String edgeLabelName, VertexLabel outVertexLabel, VertexLabel inVertexLabel, Map<String, PropertyType> properties) {
        super(outVertexLabel.getSchema().getSqlgGraph(), edgeLabelName, properties);
        if (forSqlgSchema) {
            this.outVertexLabels.add(outVertexLabel);
            this.inVertexLabels.add(inVertexLabel);
        } else {
            this.uncommittedOutVertexLabels.add(outVertexLabel);
            this.uncommittedInVertexLabels.add(inVertexLabel);
        }
        this.topology = outVertexLabel.getSchema().getTopology();
    }

    EdgeLabel(Topology topology, String edgeLabelName) {
        super(topology.getSqlgGraph(), edgeLabelName, Collections.emptyMap());
        this.topology = topology;
    }

    @Override
    public Schema getSchema() {
        if (!this.outVertexLabels.isEmpty()) {
            VertexLabel vertexLabel = this.outVertexLabels.iterator().next();
            return vertexLabel.getSchema();
        } else if (this.topology.isWriteLockHeldByCurrentThread() && !this.uncommittedOutVertexLabels.isEmpty()) {
            VertexLabel vertexLabel = this.uncommittedOutVertexLabels.iterator().next();
            return vertexLabel.getSchema();
        } else {
            throw new IllegalStateException("BUG: no outVertexLabels present when getSchema() is called");
        }
    }

    public void ensurePropertiesExist(Map<String, PropertyType> columns) {
        for (Map.Entry<String, PropertyType> column : columns.entrySet()) {
            if (!this.properties.containsKey(column.getKey())) {
                if (!this.uncommittedProperties.containsKey(column.getKey())) {
                    this.getSchema().getTopology().lock();
                    if (!this.uncommittedProperties.containsKey(column.getKey())) {
                        TopologyManager.addEdgeColumn(this.sqlgGraph, this.getSchema().getName(), EDGE_PREFIX + getLabel(), column);
                        addColumn(this.getSchema().getName(), EDGE_PREFIX + getLabel(), ImmutablePair.of(column.getKey(), column.getValue()));
                        PropertyColumn propertyColumn = new PropertyColumn(this, column.getKey(), column.getValue());
                        propertyColumn.setCommitted(false);
                        this.uncommittedProperties.put(column.getKey(), propertyColumn);
                        this.getSchema().getTopology().fire(propertyColumn, "", TopologyChangeAction.CREATE);
                    }
                }
            }
        }
    }

    private void createEdgeTable(VertexLabel outVertexLabel, VertexLabel inVertexLabel, Map<String, PropertyType> columns) {

        String schema = outVertexLabel.getSchema().getName();
        String tableName = EDGE_PREFIX + getLabel();

        SqlDialect sqlDialect = this.sqlgGraph.getSqlDialect();
        sqlDialect.assertTableName(tableName);
        StringBuilder sql = new StringBuilder(sqlDialect.createTableStatement());
        sql.append(sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(sqlDialect.maybeWrapInQoutes(tableName));
        sql.append("(");
        sql.append(sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        buildColumns(this.sqlgGraph, columns, sql);
        sql.append(", ");
        sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END));
        sql.append(" ");
        sql.append(sqlDialect.getForeignKeyTypeDefinition());
        sql.append(", ");
        sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END));
        sql.append(" ");
        sql.append(sqlDialect.getForeignKeyTypeDefinition());

        //foreign key definition start
        if (this.sqlgGraph.isImplementForeignKeys()) {
            sql.append(", ");
            sql.append("FOREIGN KEY (");
            sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getSchema().getName() + "." + inVertexLabel.getLabel() + Topology.IN_VERTEX_COLUMN_END));
            sql.append(") REFERENCES ");
            sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getSchema().getName()));
            sql.append(".");
            sql.append(sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + inVertexLabel.getLabel()));
            sql.append(" (");
            sql.append(sqlDialect.maybeWrapInQoutes("ID"));
            sql.append("), ");
            sql.append(" FOREIGN KEY (");
            sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getSchema().getName() + "." + outVertexLabel.getLabel() + Topology.OUT_VERTEX_COLUMN_END));
            sql.append(") REFERENCES ");
            sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getSchema().getName()));
            sql.append(".");
            sql.append(sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + outVertexLabel.getLabel()));
            sql.append(" (");
            sql.append(sqlDialect.maybeWrapInQoutes("ID"));
            sql.append(")");
        }
        //foreign key definition end

        sql.append(")");
        if (sqlDialect.needsSemicolon()) {
            sql.append(";");
        }

        if (sqlDialect.needForeignKeyIndex()) {
            sql.append("\nCREATE INDEX ON ");
            sql.append(sqlDialect.maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(sqlDialect.maybeWrapInQoutes(tableName));
            sql.append(" (");
            sql.append(sqlDialect.maybeWrapInQoutes(inVertexLabel.getSchema().getName() + "." + inVertexLabel.getLabel() + Topology.IN_VERTEX_COLUMN_END));
            sql.append(");");

            sql.append("\nCREATE INDEX ON ");
            sql.append(sqlDialect.maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(sqlDialect.maybeWrapInQoutes(tableName));
            sql.append(" (");
            sql.append(sqlDialect.maybeWrapInQoutes(outVertexLabel.getSchema().getName() + "." + outVertexLabel.getLabel() + Topology.OUT_VERTEX_COLUMN_END));
            sql.append(");");
        }

        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void afterCommit() {
        Preconditions.checkState(this.getSchema().getTopology().isWriteLockHeldByCurrentThread(), "EdgeLabel.afterCommit must hold the write lock");
        super.afterCommit();
        for (Iterator<VertexLabel> it = this.uncommittedInVertexLabels.iterator(); it.hasNext(); ) {
            VertexLabel vertexLabel = it.next();
            this.inVertexLabels.add(vertexLabel);
            it.remove();
        }
        for(Iterator<VertexLabel> it = this.uncommittedRemovedInVertexLabels.iterator(); it.hasNext(); ){
        	VertexLabel vertexLabel = it.next();
        	this.inVertexLabels.remove(vertexLabel);
        	it.remove();
        }
        for (Iterator<VertexLabel> it = this.uncommittedOutVertexLabels.iterator(); it.hasNext(); ) {
            VertexLabel vertexLabel = it.next();
            this.outVertexLabels.add(vertexLabel);
            it.remove();
        }
        for(Iterator<VertexLabel> it = this.uncommittedRemovedOutVertexLabels.iterator(); it.hasNext(); ){
        	VertexLabel vertexLabel = it.next();
        	this.outVertexLabels.remove(vertexLabel);
        	it.remove();
        }
    }

    void afterRollbackInEdges(VertexLabel vertexLabel) {
        Preconditions.checkState(this.getSchema().getTopology().isWriteLockHeldByCurrentThread(), "EdgeLabel.afterRollback must hold the write lock");
        super.afterRollback();
        this.uncommittedInVertexLabels.remove(vertexLabel);
    }

    void afterRollbackOutEdges(VertexLabel vertexLabel) {
        Preconditions.checkState(this.getSchema().getTopology().isWriteLockHeldByCurrentThread(), "EdgeLabel.afterRollback must hold the write lock");
        super.afterRollback();
        this.uncommittedOutVertexLabels.remove(vertexLabel);
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    private boolean foreignKeysContains(Direction direction, VertexLabel vertexLabel) {
        switch (direction) {
            case OUT:
                if (this.outVertexLabels.contains(vertexLabel)) {
                    return true;
                }
                break;
            case IN:
                if (this.inVertexLabels.contains(vertexLabel)) {
                    return true;
                }
                break;
            case BOTH:
                throw new IllegalStateException("foreignKeysContains may not be called for Direction.BOTH");
        }
        if (this.topology.isWriteLockHeldByCurrentThread()) {
            switch (direction) {
                case OUT:
                    if (this.uncommittedOutVertexLabels.contains(vertexLabel)) {
                        return true;
                    }
                    break;
                case IN:
                    if (this.uncommittedInVertexLabels.contains(vertexLabel)) {
                        return true;
                    }
                    break;
                case BOTH:
                    throw new IllegalStateException("foreignKeysContains may not be called for Direction.BOTH");
            }

        }
        return false;
    }

    Set<String> getAllEdgeForeignKeys() {
        Set<String> result = new HashSet<>();
        for (VertexLabel vertexLabel : this.getInVertexLabels()) {
        	if (!this.getSchema().getTopology().isWriteLockHeldByCurrentThread() || !this.uncommittedRemovedInVertexLabels.contains(vertexLabel)){
        		result.add(vertexLabel.getSchema().getName() + "." + vertexLabel.getLabel() + Topology.IN_VERTEX_COLUMN_END);
        	}
        }
        for (VertexLabel vertexLabel : this.getOutVertexLabels()) {
        	if (!this.getSchema().getTopology().isWriteLockHeldByCurrentThread() || !this.uncommittedRemovedOutVertexLabels.contains(vertexLabel)){
        		result.add(vertexLabel.getSchema().getName() + "." + vertexLabel.getLabel() + Topology.OUT_VERTEX_COLUMN_END);
        	}
        }
        return result;
    }

    Set<String> getUncommittedEdgeForeignKeys() {
        Set<String> result = new HashSet<>();
        //noinspection Duplicates
        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            for (VertexLabel vertexLabel : this.uncommittedInVertexLabels) {
            	if (!this.uncommittedRemovedInVertexLabels.contains(vertexLabel)){
            		result.add(vertexLabel.getFullName() + Topology.IN_VERTEX_COLUMN_END);
            	}
            }
            for (VertexLabel vertexLabel : this.uncommittedOutVertexLabels) {
            	if (!this.uncommittedRemovedOutVertexLabels.contains(vertexLabel)){
            		result.add(vertexLabel.getFullName() + Topology.OUT_VERTEX_COLUMN_END);
            	}
            }
        }
        return result;
    }

    boolean isValid(){
    	return this.outVertexLabels.size()>0 || this.uncommittedOutVertexLabels.size()>0;
    }
    
    public Set<VertexLabel> getOutVertexLabels() {
        Set<VertexLabel> result = new HashSet<>();
        result.addAll(this.outVertexLabels);
        if (isValid() && this.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            result.addAll(this.uncommittedOutVertexLabels);
           	result.removeAll(this.uncommittedRemovedOutVertexLabels);
        }
        return Collections.unmodifiableSet(result);
    }

    public Set<VertexLabel> getInVertexLabels() {
        Set<VertexLabel> result = new HashSet<>();
        result.addAll(this.inVertexLabels);
        if (isValid() && this.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            result.addAll(this.uncommittedInVertexLabels);
        	result.removeAll(this.uncommittedRemovedInVertexLabels);
        }
        return Collections.unmodifiableSet(result);
    }
    
    public Set<EdgeRole> getOutEdgeRoles() {
        Set<EdgeRole> result = new HashSet<>();
        for (VertexLabel lbl:this.outVertexLabels){
        	if (!this.uncommittedOutVertexLabels.contains(lbl)){
        		result.add(new EdgeRole(lbl,this,Direction.OUT,true));
        	}
        }
        
        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            for (VertexLabel lbl:this.uncommittedOutVertexLabels){
            	if (!this.uncommittedOutVertexLabels.contains(lbl)){
            		result.add(new EdgeRole(lbl,this,Direction.OUT,false));
            	}
            }
        }
        return Collections.unmodifiableSet(result);
    }

    public Set<EdgeRole> getInEdgeRoles() {
        Set<EdgeRole> result = new HashSet<>();
        for (VertexLabel lbl:this.inVertexLabels){
        	if (!this.uncommittedInVertexLabels.contains(lbl)){
        		result.add(new EdgeRole(lbl,this,Direction.IN,true));
        	}
        }
        
        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            for (VertexLabel lbl:this.uncommittedInVertexLabels){
            	if (!this.uncommittedInVertexLabels.contains(lbl)){
            		result.add(new EdgeRole(lbl,this,Direction.IN,false));
            	}
            }
        }
        return Collections.unmodifiableSet(result);
    } 

    public void ensureEdgeVertexLabelExist(Direction direction, VertexLabel vertexLabel) {
        //if the direction is OUT then the vertexLabel must be in the same schema as the edgeLabel (this)
        if (direction == Direction.OUT) {
            Preconditions.checkState(vertexLabel.getSchema().equals(getSchema()), "For Direction.OUT the VertexLabel must be in the same schema as the edge. Found %s and %s", vertexLabel.getSchema().getName(), getSchema().getName());
        }
        SchemaTable foreignKey = SchemaTable.of(vertexLabel.getSchema().getName(), vertexLabel.getLabel() + (direction == Direction.IN ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END));
        if (!foreignKeysContains(direction, vertexLabel)) {
            //Make sure the current thread/transaction owns the lock
            Schema schema = this.getSchema();
            schema.getTopology().lock();
            if (!foreignKeysContains(direction, vertexLabel)) {
                TopologyManager.addLabelToEdge(this.sqlgGraph, this.getSchema().getName(), EDGE_PREFIX + getLabel(), direction == Direction.IN, foreignKey);
                if (direction == Direction.IN) {
                    this.uncommittedInVertexLabels.add(vertexLabel);
                    vertexLabel.addToUncommittedInEdgeLabels(schema, this);
                } else {
                    this.uncommittedOutVertexLabels.add(vertexLabel);
                    vertexLabel.addToUncommittedOutEdgeLabels(schema, this);
                }
                SchemaTable vertexSchemaTable = SchemaTable.of(vertexLabel.getSchema().getName(), vertexLabel.getLabel());
                addEdgeForeignKey(schema.getName(), EDGE_PREFIX + getLabel(), foreignKey, vertexSchemaTable);
                this.getSchema().getTopology().fire(this, vertexSchemaTable.toString(), TopologyChangeAction.ADD_IN_VERTEX_LABELTO_EDGE);
            }
        }
    }

    private void addEdgeForeignKey(String schema, String table, SchemaTable foreignKey, SchemaTable otherVertex) {
        Preconditions.checkState(!this.getSchema().isSqlgSchema(), "BUG: ensureEdgeVertexLabelExist may not be called for %s", SQLG_SCHEMA);
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
        sql.append(" ADD COLUMN ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKey.getSchema() + "." + foreignKey.getTable()));
        sql.append(" ");
        sql.append(this.sqlgGraph.getSqlDialect().getForeignKeyTypeDefinition());
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql.setLength(0);
        //foreign key definition start
        if (this.sqlgGraph.isImplementForeignKeys()) {
            sql.append(" ALTER TABLE ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
            sql.append(" ADD CONSTRAINT ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(table + "_" + foreignKey.getSchema() + "." + foreignKey.getTable() + "_fkey"));
            sql.append(" FOREIGN KEY (");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKey.getSchema() + "." + foreignKey.getTable()));
            sql.append(") REFERENCES ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(otherVertex.getSchema()));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + otherVertex.getTable()));
            sql.append(" (");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
            sql.append(")");
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        sql.setLength(0);
        if (this.sqlgGraph.getSqlDialect().needForeignKeyIndex()) {
            sql.append("\nCREATE INDEX ON ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
            sql.append(" (");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKey.getSchema() + "." + foreignKey.getTable()));
            sql.append(")");
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    void addToOutVertexLabel(VertexLabel vertexLabel) {
        this.outVertexLabels.add(vertexLabel);
    }

    void addToInVertexLabel(VertexLabel vertexLabel) {
        this.inVertexLabels.add(vertexLabel);
    }

    @Override
    public int hashCode() {
        //Edges are unique per out vertex schemas.
        //En edge must have at least one out vertex so take it to get the schema.
        VertexLabel vertexLabel;
        if (!this.outVertexLabels.isEmpty()) {
            vertexLabel = this.outVertexLabels.iterator().next();
        } else {
            vertexLabel = this.uncommittedOutVertexLabels.iterator().next();
        }
        return (vertexLabel.getSchema().getName() + this.getLabel()).hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) {
            return false;
        }
        if (!(other instanceof EdgeLabel)) {
            return false;
        }
        EdgeLabel otherEdgeLabel = (EdgeLabel) other;
        if (isValid()){
	        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread() && !this.uncommittedInVertexLabels.isEmpty()) {
	            VertexLabel vertexLabel = this.uncommittedOutVertexLabels.iterator().next();
	            VertexLabel otherVertexLabel = otherEdgeLabel.uncommittedOutVertexLabels.iterator().next();
	            return vertexLabel.getSchema().equals(otherVertexLabel.getSchema()) && otherEdgeLabel.getLabel().equals(this.getLabel());
	        } else {
	            VertexLabel vertexLabel = this.outVertexLabels.iterator().next();
	            VertexLabel otherVertexLabel = otherEdgeLabel.outVertexLabels.iterator().next();
	            return vertexLabel.getSchema().equals(otherVertexLabel.getSchema()) && otherEdgeLabel.getLabel().equals(this.getLabel());
	        }
        }
        return otherEdgeLabel.getLabel().equals(this.getLabel());
    }

    boolean deepEquals(EdgeLabel otherEdgeLabel) {
        Preconditions.checkState(this.equals(otherEdgeLabel), "equals must have passed before calling deepEquals");

        //check every out and in edge
        for (VertexLabel outVertexLabel : this.outVertexLabels) {
            boolean ok = false;
            for (VertexLabel otherOutVertexLabel : otherEdgeLabel.outVertexLabels) {
                if (outVertexLabel.equals(otherOutVertexLabel)) {
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                return false;
            }
        }
        for (VertexLabel inVertexLabel : this.inVertexLabels) {
            boolean ok = false;
            for (VertexLabel otherInVertexLabel : otherEdgeLabel.inVertexLabels) {
                if (inVertexLabel.equals(otherInVertexLabel)) {
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected JsonNode toJson() {
        ObjectNode edgeLabelNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        if (isValid()){
        	edgeLabelNode.put("schema", getSchema().getName());
        }
        edgeLabelNode.put("label", getLabel());
        edgeLabelNode.set("properties", super.toJson());

        ArrayNode outVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (VertexLabel outVertexLabel : this.outVertexLabels) {
            ObjectNode outVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
            outVertexLabelObjectNode.put("label", outVertexLabel.getLabel());
            outVertexLabelArrayNode.add(outVertexLabelObjectNode);
        }
        edgeLabelNode.set("outVertexLabels", outVertexLabelArrayNode);

        ArrayNode inVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (VertexLabel inVertexLabel : this.inVertexLabels) {
            ObjectNode inVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
            inVertexLabelObjectNode.put("label", inVertexLabel.getLabel());
            inVertexLabelArrayNode.add(inVertexLabelObjectNode);
        }
        edgeLabelNode.set("inVertexLabels", inVertexLabelArrayNode);

        if (isValid() && this.getSchema().getTopology().isWriteLockHeldByCurrentThread()) {
            outVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel outVertexLabel : this.uncommittedOutVertexLabels) {
                ObjectNode outVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                outVertexLabelObjectNode.put("label", outVertexLabel.getLabel());
                outVertexLabelArrayNode.add(outVertexLabelObjectNode);
            }
            edgeLabelNode.set("uncommittedOutVertexLabels", outVertexLabelArrayNode);

            inVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel inVertexLabel : this.uncommittedInVertexLabels) {
                ObjectNode inVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                inVertexLabelObjectNode.put("label", inVertexLabel.getLabel());
                inVertexLabelArrayNode.add(inVertexLabelObjectNode);
            }
            edgeLabelNode.set("uncommittedInVertexLabels", inVertexLabelArrayNode);
        }

        return edgeLabelNode;
    }

    @Override
    protected Optional<JsonNode> toNotifyJson() {

        boolean foundSomething = false;
        ObjectNode edgeLabelNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        edgeLabelNode.put("schema", getSchema().getName());
        edgeLabelNode.put("label", getLabel());

        Optional<JsonNode> abstractLabelNode = super.toNotifyJson();
        if (abstractLabelNode.isPresent()) {
            foundSomething = true;
            edgeLabelNode.set("uncommittedProperties", abstractLabelNode.get().get("uncommittedProperties"));
            edgeLabelNode.set("uncommittedIndexes", abstractLabelNode.get().get("uncommittedIndexes"));
            edgeLabelNode.set("uncommittedRemovedProperties", abstractLabelNode.get().get("uncommittedRemovedProperties"));
            edgeLabelNode.set("uncommittedRemovedIndexes", abstractLabelNode.get().get("uncommittedRemovedIndexes"));
          }

        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread() && !this.uncommittedOutVertexLabels.isEmpty()) {
            foundSomething = true;
            ArrayNode outVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel outVertexLabel : this.uncommittedOutVertexLabels) {
                ObjectNode outVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                outVertexLabelObjectNode.put("label", outVertexLabel.getLabel());
                outVertexLabelArrayNode.add(outVertexLabelObjectNode);
            }
            edgeLabelNode.set("uncommittedOutVertexLabels", outVertexLabelArrayNode);
        }
        
        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread() && !this.uncommittedRemovedOutVertexLabels.isEmpty()) {
            foundSomething = true;
            ArrayNode outVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel outVertexLabel : this.uncommittedRemovedOutVertexLabels) {
                ObjectNode outVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                outVertexLabelObjectNode.put("label", outVertexLabel.getLabel());
                outVertexLabelArrayNode.add(outVertexLabelObjectNode);
            }
            edgeLabelNode.set("uncommittedRemovedOutVertexLabels", outVertexLabelArrayNode);
        }

        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread() && !this.uncommittedInVertexLabels.isEmpty()) {
            foundSomething = true;
            ArrayNode inVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel inVertexLabel : this.uncommittedInVertexLabels) {
                ObjectNode inVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                inVertexLabelObjectNode.put("label", inVertexLabel.getLabel());
                inVertexLabelArrayNode.add(inVertexLabelObjectNode);
            }
            edgeLabelNode.set("uncommittedInVertexLabels", inVertexLabelArrayNode);
        }
        
        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread() && !this.uncommittedRemovedInVertexLabels.isEmpty()) {
            foundSomething = true;
            ArrayNode inVertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel inVertexLabel : this.uncommittedRemovedInVertexLabels) {
                ObjectNode inVertexLabelObjectNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                inVertexLabelObjectNode.put("label", inVertexLabel.getLabel());
                inVertexLabelArrayNode.add(inVertexLabelObjectNode);
            }
            edgeLabelNode.set("uncommittedRemovedInVertexLabels", inVertexLabelArrayNode);
        }

        if (foundSomething) {
            return Optional.of(edgeLabelNode);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<Topology.TopologyValidationError> validateTopology(DatabaseMetaData metadata) throws SQLException {
        List<Topology.TopologyValidationError> validationErrors = new ArrayList<>();
        for (PropertyColumn propertyColumn : getProperties().values()) {
            try (ResultSet propertyRs = metadata.getColumns(null, this.getSchema().getName(), "E_" + this.getLabel(), propertyColumn.getName())) {
                if (!propertyRs.next()) {
                    validationErrors.add(new Topology.TopologyValidationError(propertyColumn));
                }
            }
        }
        return validationErrors;
    }

    @Override
    protected String getPrefix() {
        return EDGE_PREFIX;
    }
    
    @Override
    void removeProperty(PropertyColumn propertyColumn,boolean preserveData){
    	this.getSchema().getTopology().lock();
    	if (!uncommittedRemovedProperties.contains(propertyColumn.getName())){
    		uncommittedRemovedProperties.add(propertyColumn.getName());
    		TopologyManager.removeEdgeColumn(this.sqlgGraph, this.getSchema().getName(), EDGE_PREFIX + getLabel(), propertyColumn.getName());
    		if (!preserveData){
    			removeColumn(this.getSchema().getName(), EDGE_PREFIX + getLabel(), propertyColumn.getName());
    		}
    		this.getSchema().getTopology().fire(propertyColumn, "", TopologyChangeAction.DELETE);
    	}
    }
    
    @Override
    public void remove(boolean preserveData) {
    	getSchema().removeEdgeLabel(this,preserveData);
    }
    
    /**
     * delete the table
     */
    void delete(){
    	 String schema = getSchema().getName();
         String tableName = EDGE_PREFIX + getLabel();

         SqlDialect sqlDialect = this.sqlgGraph.getSqlDialect();
         sqlDialect.assertTableName(tableName);
         StringBuilder sql = new StringBuilder("DROP TABLE IF EXISTS ");
         sql.append(sqlDialect.maybeWrapInQoutes(schema));
         sql.append(".");
         sql.append(sqlDialect.maybeWrapInQoutes(tableName));
         if (sqlDialect.supportsCascade()){
        	 sql.append(" CASCADE");
         }
         if (logger.isDebugEnabled()) {
             logger.debug(sql.toString());
         }
         if (sqlDialect.needsSemicolon()) {
             sql.append(";");
         }
         Connection conn = sqlgGraph.tx().getConnection();
         try (Statement stmt = conn.createStatement()) {
             stmt.execute(sql.toString());
         } catch (SQLException e) {
             throw new RuntimeException(e);
         }
    }
    
    /**
     * delete a given column from the table
     * @param column
     */
    void deleteColumn(String column){
    	removeColumn(getSchema().getName(),  EDGE_PREFIX + getLabel(), column);
    }
    
    /**
     * remove a vertex label from the out collection
     * @param lbl the vertex label
     * @param preserveData should we keep the sql data?
     */
    void removeOutVertexLabel(VertexLabel lbl,boolean preserveData){
    	this.uncommittedRemovedOutVertexLabels.add(lbl);
    	if (!preserveData){
    		deleteColumn(lbl.getFullName() + Topology.OUT_VERTEX_COLUMN_END);
    	}
    }
    
    /**
     * remove a vertex label from the in collection
     * @param lbl the vertex label
     * @param preserveData should we keep the sql data?
     */
    void removeInVertexLabel(VertexLabel lbl,boolean preserveData){
    	this.uncommittedRemovedInVertexLabels.add(lbl);
    	if (!preserveData){
    		deleteColumn(lbl.getFullName() + Topology.IN_VERTEX_COLUMN_END);
    	}
    }
}
