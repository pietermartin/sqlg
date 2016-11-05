package org.umlg.sqlg.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.umlg.sqlg.structure.SchemaManager.*;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class VertexLabel extends AbstractElement {

    private Logger logger = LoggerFactory.getLogger(VertexLabel.class.getName());
    private Schema schema;

    //hand (out) ----<label>---- finger (in)
    private Set<EdgeLabel> inEdgeLabels = new HashSet<>();
    private Set<EdgeLabel> outEdgeLabels = new HashSet<>();
    private Set<EdgeLabel> uncommittedInEdgeLabels = new HashSet<>();
    private Set<EdgeLabel> uncommittedOutEdgeLabels = new HashSet<>();

    static VertexLabel createSqlgSchemaVertexLabel(Schema schema, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(schema.isSqlgSchema(), "createSqlgSchemaVertexLabel may only be called for \"%s\"", SQLG_SCHEMA);
        VertexLabel vertexLabel = new VertexLabel(schema, label);
        //Add the properties directly. As they are pre-created do not add them to uncommittedProperties.
        for (Map.Entry<String, PropertyType> propertyEntry : columns.entrySet()) {
            Property property = new Property(vertexLabel, propertyEntry.getKey(), propertyEntry.getValue());
            vertexLabel.properties.put(propertyEntry.getKey(), property);
        }
        return vertexLabel;
    }

    static VertexLabel createVertexLabel(SqlgGraph sqlgGraph, Schema schema, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!schema.isSqlgSchema(), "createVertexLabel may not be called for \"%s\"", SQLG_SCHEMA);
        VertexLabel vertexLabel = new VertexLabel(schema, label, columns);
        vertexLabel.createVertexLabelOnDb(sqlgGraph, columns);
        TopologyManager.addVertexLabel(sqlgGraph, schema.getName(), label, columns);
        return vertexLabel;
    }

    VertexLabel(Schema schema, String label) {
        super(label);
        this.schema = schema;
    }

    private VertexLabel(Schema schema, String label, Map<String, PropertyType> columns) {
        super(label, columns);
        this.schema = schema;
    }

    @Override
    protected Schema getSchema() {
        return this.schema;
    }

    Set<EdgeLabel> getInEdgeLabels() {
        Set<EdgeLabel> result = new HashSet<>();
        result.addAll(this.inEdgeLabels);
        if (this.schema.getTopology().isLockHeldByCurrentThread()) {
            result.addAll(this.uncommittedInEdgeLabels);
        }
        return result;
    }

    Set<EdgeLabel> getOutEdgeLabels() {
        Set<EdgeLabel> result = new HashSet<>();
        result.addAll(this.outEdgeLabels);
        if (this.schema.getTopology().isLockHeldByCurrentThread()) {
            result.addAll(this.uncommittedOutEdgeLabels);
        }
        return result;
    }

    public void addToUncommittedInEdgeLabels(EdgeLabel edgeLabel) {
        this.uncommittedInEdgeLabels.add(edgeLabel);
    }

    public void addToUncommittedOutEdgeLabels(EdgeLabel edgeLabel) {
        this.uncommittedOutEdgeLabels.add(edgeLabel);
    }

    EdgeLabel loadSqlgSchemaEdgeLabel(String edgeLabelName, VertexLabel inVertexLabel, Map<String, PropertyType> properties) {
        Preconditions.checkState(this.schema.isSqlgSchema(), "loadSqlgSchemaEdgeLabel must be called for \"%s\" found \"%s\"", SQLG_SCHEMA, this.schema.getName());
        EdgeLabel edgeLabel = EdgeLabel.loadSqlgSchemaEdgeLabel(edgeLabelName, this, inVertexLabel, properties);
        this.outEdgeLabels.add(edgeLabel);
        inVertexLabel.inEdgeLabels.add(edgeLabel);
        return edgeLabel;
    }

    EdgeLabel addEdgeLabel(SqlgGraph sqlgGraph, String edgeLabelName, VertexLabel inVertexLabel, Map<String, PropertyType> properties) {
        EdgeLabel edgeLabel = EdgeLabel.createEdgeLabel(sqlgGraph, edgeLabelName, this, inVertexLabel, properties);
        if (this.schema.isSqlgSchema()) {
            this.outEdgeLabels.add(edgeLabel);
            inVertexLabel.inEdgeLabels.add(edgeLabel);
        } else {
            this.uncommittedOutEdgeLabels.add(edgeLabel);
            inVertexLabel.uncommittedInEdgeLabels.add(edgeLabel);
        }
        return edgeLabel;
    }

    public void ensureColumnsExist(SqlgGraph sqlgGraph, Map<String, PropertyType> columns) {
        for (Map.Entry<String, PropertyType> column : columns.entrySet()) {
            if (!this.properties.containsKey(column.getKey())) {
                Preconditions.checkState(!this.schema.isSqlgSchema(), "schema may not be %s", SQLG_SCHEMA);
                if (!this.uncommittedProperties.containsKey(column.getKey())) {
                    this.schema.getTopology().lock();
                    if (!this.uncommittedProperties.containsKey(column.getKey())) {
                        TopologyManager.addVertexColumn(sqlgGraph, this.schema.getName(), VERTEX_PREFIX + getLabel(), column);
                        addColumn(sqlgGraph, this.schema.getName(), VERTEX_PREFIX + getLabel(), ImmutablePair.of(column.getKey(), column.getValue()));
                        this.uncommittedProperties.put(column.getKey(), new Property(this, column.getKey(), column.getValue()));
                    }
                }
            }
        }
    }


    //TODO refactor out columns as its already in the object as this.properties.
    private void createVertexLabelOnDb(SqlgGraph sqlgGraph, Map<String, PropertyType> columns) {
        StringBuilder sql = new StringBuilder(sqlgGraph.getSqlDialect().createTableStatement());
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema.getName()));
        sql.append(".");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + getLabel()));
        sql.append(" (");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(sqlgGraph.getSqlDialect().getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        buildColumns(sqlgGraph, columns, sql);
        sql.append(")");
        if (sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public Pair<Set<SchemaTable>, Set<SchemaTable>> getTableLabels() {
        Set<SchemaTable> inSchemaTables = new HashSet<>();
        Set<SchemaTable> outSchemaTables = new HashSet<>();
        for (EdgeLabel inEdgeLabel : inEdgeLabels) {
            inSchemaTables.add(SchemaTable.of(inEdgeLabel.getSchema().getName(), EDGE_PREFIX + inEdgeLabel.getLabel()));
        }
        for (EdgeLabel outEdgeLabel : outEdgeLabels) {
            outSchemaTables.add(SchemaTable.of(outEdgeLabel.getSchema().getName(), EDGE_PREFIX + outEdgeLabel.getLabel()));
        }
        if (this.schema.getTopology().isLockHeldByCurrentThread()) {
            for (EdgeLabel inEdgeLabel : uncommittedInEdgeLabels) {
                inSchemaTables.add(SchemaTable.of(inEdgeLabel.getSchema().getName(), EDGE_PREFIX + inEdgeLabel.getLabel()));
            }
            for (EdgeLabel outEdgeLabel : uncommittedOutEdgeLabels) {
                outSchemaTables.add(SchemaTable.of(outEdgeLabel.getSchema().getName(), EDGE_PREFIX + outEdgeLabel.getLabel()));
            }
        }
        return Pair.of(inSchemaTables, outSchemaTables);
    }

    void afterCommit() {
        super.afterCommit();
        if (this.getSchema().getTopology().isLockHeldByCurrentThread()) {
            for (Iterator<EdgeLabel> it = this.uncommittedOutEdgeLabels.iterator(); it.hasNext(); ) {
                EdgeLabel edgeLabel = it.next();
                this.outEdgeLabels.add(edgeLabel);
                edgeLabel.afterCommit();
                it.remove();
            }
            for (Iterator<EdgeLabel> it = this.uncommittedInEdgeLabels.iterator(); it.hasNext(); ) {
                EdgeLabel edgeLabel = it.next();
                this.inEdgeLabels.add(edgeLabel);
                edgeLabel.afterCommit();
                it.remove();
            }
        }
        for (Iterator<EdgeLabel> it = this.outEdgeLabels.iterator(); it.hasNext(); ) {
            EdgeLabel edgeLabel = it.next();
            edgeLabel.afterCommit();
        }
        for (Iterator<EdgeLabel> it = this.inEdgeLabels.iterator(); it.hasNext(); ) {
            EdgeLabel edgeLabel = it.next();
            edgeLabel.afterCommit();
        }
    }

    void afterRollbackForInEdges() {
        super.afterRollback();
        if (this.getSchema().getTopology().isLockHeldByCurrentThread()) {
            for (Iterator<EdgeLabel> it = this.uncommittedInEdgeLabels.iterator(); it.hasNext(); ) {
                EdgeLabel edgeLabel = it.next();
                edgeLabel.afterRollbackInEdges();
                it.remove();
            }
        }
    }

    void afterRollbackForOutEdges() {
        super.afterRollback();
        if (this.getSchema().getTopology().isLockHeldByCurrentThread()) {
            for (Iterator<EdgeLabel> it = this.uncommittedOutEdgeLabels.iterator(); it.hasNext(); ) {
                EdgeLabel edgeLabel = it.next();
                it.remove();
                //It is important to first remove the EdgeLabel from the iterator as the EdgeLabel's outVertex is still
                // present and its needed for the hashCode method which is invoked during the it.remove()
                edgeLabel.afterRollbackOutEdges();
            }
        }
        //Only need to go though the outEdgeLabels. All edgeLabels will be touched
        for (Iterator<EdgeLabel> it = this.outEdgeLabels.iterator(); it.hasNext(); ) {
            EdgeLabel edgeLabel = it.next();
            edgeLabel.afterRollbackOutEdges();
        }
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    void addToOutEdgeLabels(EdgeLabel edgeLabel) {
        edgeLabel.addToOutVertexLabel(this);
        this.outEdgeLabels.add(edgeLabel);
    }

    void addToInEdgeLabels(EdgeLabel edgeLabel) {
        edgeLabel.addToInVertexLabel(this);
        this.inEdgeLabels.add(edgeLabel);
    }

    @Override
    public int hashCode() {
        return (this.schema.getName() + this.getLabel()).hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (!(other instanceof VertexLabel)) {
            return false;
        }
        VertexLabel otherVertexLabel = (VertexLabel) other;
        return this.schema.equals(otherVertexLabel.getSchema()) && otherVertexLabel.getLabel().equals(this.getLabel());
    }

    public JsonNode toJson() {
        ObjectNode vertexLabelNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        vertexLabelNode.put("label", getLabel());

        ArrayNode outEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (EdgeLabel edgeLabel : this.outEdgeLabels) {
            outEdgeLabelsArrayNode.add(edgeLabel.toJson());
        }
        vertexLabelNode.set("outEdgeLabels", outEdgeLabelsArrayNode);

        ArrayNode inEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (EdgeLabel edgeLabel : this.inEdgeLabels) {
            inEdgeLabelsArrayNode.add(edgeLabel.toJson());
        }
        vertexLabelNode.set("inEdgeLabels", inEdgeLabelsArrayNode);

        if (this.schema.getTopology().isLockHeldByCurrentThread()) {
            outEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (EdgeLabel edgeLabel : this.uncommittedOutEdgeLabels) {
                outEdgeLabelsArrayNode.add(edgeLabel.toJson());
            }
            vertexLabelNode.set("uncommittedOutEdgeLabels", outEdgeLabelsArrayNode);

            inEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (EdgeLabel edgeLabel : this.uncommittedInEdgeLabels) {
                inEdgeLabelsArrayNode.add(edgeLabel.toJson());
            }
            vertexLabelNode.set("uncommittedInEdgeLabels", inEdgeLabelsArrayNode);
        }
        return vertexLabelNode;
    }

    protected Optional<JsonNode> toNotifyJson() {
        ObjectNode vertexLabelNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        vertexLabelNode.put("label", getLabel());

        Optional<JsonNode> propertyNode = super.toNotifyJson();
        if (propertyNode.isPresent()) {
            vertexLabelNode.set("uncommittedProperties", propertyNode.get());
        }

        if (this.getSchema().getTopology().isLockHeldByCurrentThread() && !this.uncommittedOutEdgeLabels.isEmpty()) {
            ArrayNode outEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (EdgeLabel edgeLabel : this.uncommittedOutEdgeLabels) {
                Optional<JsonNode> jsonNodeOptional = edgeLabel.toNotifyJson();
                Preconditions.checkState(jsonNodeOptional.isPresent(), "There must be data to notify as the edgeLabel itself is uncommitted");
                //noinspection OptionalGetWithoutIsPresent
                outEdgeLabelsArrayNode.add(jsonNodeOptional.get());
            }
            vertexLabelNode.set("uncommittedOutEdgeLabels", outEdgeLabelsArrayNode);
        }

        if (this.uncommittedInEdgeLabels.size() > 0) {
            ArrayNode inEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (EdgeLabel edgeLabel : this.uncommittedInEdgeLabels) {
                Optional<JsonNode> jsonNodeOptional = edgeLabel.toNotifyJson();
                Preconditions.checkState(jsonNodeOptional.isPresent(), "There must be data to notify as the edgeLabel itself is uncommitted");
                //noinspection OptionalGetWithoutIsPresent
                inEdgeLabelsArrayNode.add(jsonNodeOptional.get());
            }
            vertexLabelNode.set("uncommittedInEdgeLabels", inEdgeLabelsArrayNode);
        }

        //check for uncommittedProperties in existing edges
        ArrayNode outEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        boolean foundOutEdgeLabel = false;
        for (EdgeLabel edgeLabel : this.outEdgeLabels) {
            Optional<JsonNode> jsonNodeOptional = edgeLabel.toNotifyJson();
            if (jsonNodeOptional.isPresent()) {
                foundOutEdgeLabel = true;
                outEdgeLabelsArrayNode.add(jsonNodeOptional.get());
            }
        }
        if (foundOutEdgeLabel) {
            vertexLabelNode.set("outEdgeLabels", outEdgeLabelsArrayNode);
        }

        ArrayNode inEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        boolean foundInEdgeLabels = false;
        for (EdgeLabel edgeLabel : this.inEdgeLabels) {
            Optional<JsonNode> jsonNodeOptional = edgeLabel.toNotifyJson();
            if (jsonNodeOptional.isPresent()) {
                foundInEdgeLabels = true;
                inEdgeLabelsArrayNode.add(jsonNodeOptional.get());
            }
        }
        if (foundInEdgeLabels) {
            vertexLabelNode.set("inEdgeLabels", inEdgeLabelsArrayNode);
        }
        return Optional.of(vertexLabelNode);
    }

    public void fromNotifyJson(JsonNode vertexLabelJson) {

        super.fromPropertyNotifyJson(vertexLabelJson);

        for (String s : Arrays.asList("uncommittedOutEdgeLabels", "outEdgeLabels")) {
            ArrayNode unCommittedOutEdgeLabels = (ArrayNode) vertexLabelJson.get(s);
            if (unCommittedOutEdgeLabels != null) {
                for (JsonNode unCommittedOutEdgeLabel : unCommittedOutEdgeLabels) {
                    String edgeLabelName = unCommittedOutEdgeLabel.get("label").asText();
                    Optional<EdgeLabel> edgeLabelOptional = this.schema.getEdgeLabel(edgeLabelName);
                    EdgeLabel edgeLabel;
                    if (!edgeLabelOptional.isPresent()) {
                        edgeLabel = new EdgeLabel(this.getSchema().getTopology(), edgeLabelName);
                    } else {
                        edgeLabel = edgeLabelOptional.get();
                    }
                    edgeLabel.addToOutVertexLabel(this);
                    this.outEdgeLabels.add(edgeLabel);
                    edgeLabel.fromPropertyNotifyJson(unCommittedOutEdgeLabel);
                }
            }
        }

        for (String s : Arrays.asList("uncommittedInEdgeLabels", "inEdgeLabels")) {
            ArrayNode unCommittedInEdgeLabels = (ArrayNode) vertexLabelJson.get(s);
            if (unCommittedInEdgeLabels != null) {
                for (JsonNode unCommittedInEdgeLabel : unCommittedInEdgeLabels) {
                    String edgeLabelName = unCommittedInEdgeLabel.get("label").asText();
                    Optional<EdgeLabel> edgeLabelOptional = this.schema.getEdgeLabel(edgeLabelName);
                    EdgeLabel edgeLabel;
                    if (!edgeLabelOptional.isPresent()) {
                        edgeLabel = new EdgeLabel(this.getSchema().getTopology(), edgeLabelName);
                    } else {
                        edgeLabel = edgeLabelOptional.get();
                    }
                    edgeLabel.addToInVertexLabel(this);
                    this.inEdgeLabels.add(edgeLabel);
                    edgeLabel.fromPropertyNotifyJson(unCommittedInEdgeLabel);
                }
            }
        }

    }
}
