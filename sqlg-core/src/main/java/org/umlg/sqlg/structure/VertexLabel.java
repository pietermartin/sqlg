package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static org.umlg.sqlg.structure.SchemaManager.EDGE_PREFIX;
import static org.umlg.sqlg.structure.SchemaManager.VERTEX_PREFIX;
import static org.umlg.sqlg.structure.Topology.SQLG_SCHEMA;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class VertexLabel extends AbstractLabel {

    private Logger logger = LoggerFactory.getLogger(VertexLabel.class.getName());
    private Schema schema;

    //hand (out) ----<label>---- finger (in)
    //The key of the map must include the schema.
    //This is A.A --ab-->A.B
    //        B.B --ab-->A.B
    //In this case 2 EdgeLabels will exist A.ab and B.ab
    //A.B's inEdgeLabels will contain both EdgeLabels
    private Map<String, EdgeLabel> inEdgeLabels = new HashMap<>();
    private Map<String, EdgeLabel> outEdgeLabels = new HashMap<>();
    private Map<String, EdgeLabel> uncommittedInEdgeLabels = new HashMap<>();
    private Map<String, EdgeLabel> uncommittedOutEdgeLabels = new HashMap<>();

    static VertexLabel createSqlgSchemaVertexLabel(Schema schema, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(schema.isSqlgSchema(), "createSqlgSchemaVertexLabel may only be called for \"%s\"", SQLG_SCHEMA);
        VertexLabel vertexLabel = new VertexLabel(schema, label);
        //Add the properties directly. As they are pre-created do not add them to uncommittedProperties.
        for (Map.Entry<String, PropertyType> propertyEntry : columns.entrySet()) {
            PropertyColumn property = new PropertyColumn(vertexLabel, propertyEntry.getKey(), propertyEntry.getValue());
            vertexLabel.properties.put(propertyEntry.getKey(), property);
        }
        return vertexLabel;
    }

    static VertexLabel createVertexLabel(SqlgGraph sqlgGraph, Schema schema, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!schema.isSqlgSchema(), "createVertexLabel may not be called for \"%s\"", SQLG_SCHEMA);
        VertexLabel vertexLabel = new VertexLabel(schema, label, columns);
        vertexLabel.createVertexLabelOnDb(columns);
        TopologyManager.addVertexLabel(sqlgGraph, schema.getName(), label, columns);
        vertexLabel.committed = false;
        return vertexLabel;
    }

    VertexLabel(Schema schema, String label) {
        super(schema.getSqlgGraph(), label);
        this.schema = schema;
    }

    /**
     * Only called for a new label being added.
     *
     * @param schema     The schema.
     * @param label      The vertex's label.
     * @param properties The vertex's properties.
     */
    private VertexLabel(Schema schema, String label, Map<String, PropertyType> properties) {
        super(schema.getSqlgGraph(), label, properties);
        this.schema = schema;
    }

    @Override
    protected Schema getSchema() {
        return this.schema;
    }

    Map<String, EdgeLabel> getInEdgeLabels() {
        Map<String, EdgeLabel> result = new HashMap<>();
        result.putAll(this.inEdgeLabels);
        if (this.schema.getTopology().isWriteLockHeldByCurrentThread()) {
            result.putAll(this.uncommittedInEdgeLabels);
        }
        return result;
    }

    public Map<String, EdgeLabel> getOutEdgeLabels() {
        Map<String, EdgeLabel> result = new HashMap<>();
        result.putAll(this.outEdgeLabels);
        if (this.schema.getTopology().isWriteLockHeldByCurrentThread()) {
            result.putAll(this.uncommittedOutEdgeLabels);
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Out EdgeLabels are always in the same schema as the this VertexLabel' schema.
     * So the edgeLabelName must not contain the schema prefix
     *
     * @param edgeLabelName
     * @return
     */
    public Optional<EdgeLabel> getOutEdgeLabel(String edgeLabelName) {
        EdgeLabel edgeLabel = getOutEdgeLabels().get(this.schema.getName() + "." + edgeLabelName);
        if (edgeLabel != null) {
            return Optional.of(edgeLabel);
        }
        return Optional.empty();
    }

    /**
     * A getter for a map of all uncommitted {@link EdgeLabel}s. This may include {@link EdgeLabel}s that are already
     * committed but that contain uncommitted properties.
     * This will only return something if the current thread has the write lock.
     *
     * @return A map of uncommitted EdgeLabels. The map key is the EdgeLabels label.
     */
    Map<String, EdgeLabel> getUncommittedOutEdgeLabels() {
        if (this.schema.getTopology().isWriteLockHeldByCurrentThread()) {
            Map<String, EdgeLabel> result = new HashMap<>();
            result.putAll(this.uncommittedOutEdgeLabels);
            for (EdgeLabel outEdgeLabel : this.outEdgeLabels.values()) {
                Map<String, PropertyColumn> propertyMap = outEdgeLabel.getUncommittedPropertyTypeMap();
                if (!propertyMap.isEmpty()) {
                    result.put(outEdgeLabel.getLabel(), outEdgeLabel);
                }
            }
            return result;
        } else {
            return Collections.emptyMap();
        }
    }

    Optional<EdgeLabel> getUncommittedOutEdgeLabel(String edgeLabelName) {
        if (this.schema.getTopology().isWriteLockHeldByCurrentThread()) {
            EdgeLabel edgeLabel = this.getUncommittedOutEdgeLabels().get(edgeLabelName);
            if (edgeLabel != null) {
                return Optional.of(edgeLabel);
            }
        }
        return Optional.empty();
    }

    void addToUncommittedInEdgeLabels(Schema schema, EdgeLabel edgeLabel) {
        this.uncommittedInEdgeLabels.put(schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
    }

    void addToUncommittedOutEdgeLabels(Schema schema, EdgeLabel edgeLabel) {
        this.uncommittedOutEdgeLabels.put(schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
    }

    /**
     * Called from {@link Topology#Topology(SqlgGraph)}s constructor to preload the sqlg_schema.
     * This can only be called for sqlg_schema.
     *
     * @param edgeLabelName The edge's label.
     * @param inVertexLabel The edge's in {@link VertexLabel}. 'this' is the out {@link VertexLabel}.
     * @param properties    A map of the edge's properties.
     * @return The {@link EdgeLabel} that been loaded.
     */
    EdgeLabel loadSqlgSchemaEdgeLabel(String edgeLabelName, VertexLabel inVertexLabel, Map<String, PropertyType> properties) {
        Preconditions.checkState(this.schema.isSqlgSchema(), "loadSqlgSchemaEdgeLabel must be called for \"%s\" found \"%s\"", SQLG_SCHEMA, this.schema.getName());
        EdgeLabel edgeLabel = EdgeLabel.loadSqlgSchemaEdgeLabel(edgeLabelName, this, inVertexLabel, properties);
        this.outEdgeLabels.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
        inVertexLabel.inEdgeLabels.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
        return edgeLabel;
    }

    /**
     * Ensures that the {@link EdgeLabel} exists. It will be created if it does not exists.
     * "this" is the out {@link VertexLabel} and inVertexLabel is the inVertexLabel
     * This method is equivalent to {@link Schema#ensureEdgeLabelExist(String, VertexLabel, VertexLabel, Map)}
     *
     * @param edgeLabelName The EdgeLabel's label's name.
     * @param inVertexLabel The edge's in VertexLabel.
     * @return The {@link EdgeLabel}.
     */
    public EdgeLabel ensureEdgeLabelExist(final String edgeLabelName, final VertexLabel inVertexLabel) {
        return this.getSchema().ensureEdgeLabelExist(edgeLabelName, this, inVertexLabel, Collections.emptyMap());
    }

    /**
     * Ensures that the {@link EdgeLabel} exists. It will be created if it does not exists.
     * "this" is the out {@link VertexLabel} and inVertexLabel is the inVertexLabel
     * This method is equivalent to {@link Schema#ensureEdgeLabelExist(String, VertexLabel, VertexLabel, Map)}
     *
     * @param edgeLabelName The EdgeLabel's label's name.
     * @param inVertexLabel The edge's in VertexLabel.
     * @param properties    The EdgeLabel's properties
     * @return
     */
    public EdgeLabel ensureEdgeLabelExist(final String edgeLabelName, final VertexLabel inVertexLabel, Map<String, PropertyType> properties) {
        return this.getSchema().ensureEdgeLabelExist(edgeLabelName, this, inVertexLabel, properties);
    }

    /**
     * Called via {@link Schema#ensureEdgeLabelExist(String, VertexLabel, VertexLabel, Map)}
     * This is called when the {@link EdgeLabel} does not exist and needs to be created.
     *
     * @param edgeLabelName
     * @param inVertexLabel
     * @param properties
     * @return
     */
    EdgeLabel addEdgeLabel(String edgeLabelName, VertexLabel inVertexLabel, Map<String, PropertyType> properties) {
        EdgeLabel edgeLabel = EdgeLabel.createEdgeLabel(edgeLabelName, this, inVertexLabel, properties);
        if (this.schema.isSqlgSchema()) {
            this.outEdgeLabels.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
            inVertexLabel.inEdgeLabels.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
        } else {
            this.uncommittedOutEdgeLabels.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
            inVertexLabel.uncommittedInEdgeLabels.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
        }
        return edgeLabel;
    }

    public void ensurePropertiesExist(Map<String, PropertyType> columns) {
        for (Map.Entry<String, PropertyType> column : columns.entrySet()) {
            if (!this.properties.containsKey(column.getKey())) {
                Preconditions.checkState(!this.schema.isSqlgSchema(), "schema may not be %s", SQLG_SCHEMA);
                if (!this.uncommittedProperties.containsKey(column.getKey())) {
                    this.schema.getTopology().lock();
                    if (!this.uncommittedProperties.containsKey(column.getKey())) {
                        TopologyManager.addVertexColumn(this.sqlgGraph, this.schema.getName(), VERTEX_PREFIX + getLabel(), column);
                        addColumn(this.schema.getName(), VERTEX_PREFIX + getLabel(), ImmutablePair.of(column.getKey(), column.getValue()));
                        PropertyColumn propertyColumn = new PropertyColumn(this, column.getKey(), column.getValue());
                        propertyColumn.setCommitted(false);
                        this.uncommittedProperties.put(column.getKey(), propertyColumn);
                        this.getSchema().getTopology().fire(propertyColumn, "", TopologyChangeAction.CREATE);
                    }
                }
            }
        }
    }

    //TODO refactor out columns as its already in the object as this.properties.
    private void createVertexLabelOnDb(Map<String, PropertyType> columns) {
        StringBuilder sql = new StringBuilder(this.sqlgGraph.getSqlDialect().createTableStatement());
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema.getName()));
        sql.append(".");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + getLabel()));
        sql.append(" (");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlgGraph.getSqlDialect().getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        buildColumns(this.sqlgGraph, columns, sql);
        sql.append(")");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
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


    //TODO refactor to not use SchemaTable at this level
    Pair<Set<SchemaTable>, Set<SchemaTable>> getTableLabels() {
        Set<SchemaTable> inSchemaTables = new HashSet<>();
        Set<SchemaTable> outSchemaTables = new HashSet<>();
        for (EdgeLabel inEdgeLabel : this.inEdgeLabels.values()) {
            inSchemaTables.add(SchemaTable.of(inEdgeLabel.getSchema().getName(), EDGE_PREFIX + inEdgeLabel.getLabel()));
        }
        for (EdgeLabel outEdgeLabel : this.outEdgeLabels.values()) {
            outSchemaTables.add(SchemaTable.of(outEdgeLabel.getSchema().getName(), EDGE_PREFIX + outEdgeLabel.getLabel()));
        }
        if (this.schema.getTopology().isWriteLockHeldByCurrentThread()) {
            for (EdgeLabel inEdgeLabel : this.uncommittedInEdgeLabels.values()) {
                inSchemaTables.add(SchemaTable.of(inEdgeLabel.getSchema().getName(), EDGE_PREFIX + inEdgeLabel.getLabel()));
            }
            for (EdgeLabel outEdgeLabel : this.uncommittedOutEdgeLabels.values()) {
                outSchemaTables.add(SchemaTable.of(outEdgeLabel.getSchema().getName(), EDGE_PREFIX + outEdgeLabel.getLabel()));
            }
        }
        return Pair.of(inSchemaTables, outSchemaTables);
    }

    void afterCommit() {
        Preconditions.checkState(this.schema.getTopology().isWriteLockHeldByCurrentThread(), "VertexLabel.afterCommit must hold the write lock");
        super.afterCommit();
        Iterator<Map.Entry<String, EdgeLabel>> edgeLabelEntryIter = this.uncommittedOutEdgeLabels.entrySet().iterator();
        while (edgeLabelEntryIter.hasNext()) {
            Map.Entry<String, EdgeLabel> edgeLabelEntry = edgeLabelEntryIter.next();
            String edgeLabelName = edgeLabelEntry.getKey();
            EdgeLabel edgeLabel = edgeLabelEntry.getValue();
            this.outEdgeLabels.put(edgeLabelName, edgeLabel);
            edgeLabel.afterCommit();
            this.getSchema().addToAllEdgeCache(edgeLabel);
            edgeLabelEntryIter.remove();
        }
        edgeLabelEntryIter = this.uncommittedInEdgeLabels.entrySet().iterator();
        while (edgeLabelEntryIter.hasNext()) {
            Map.Entry<String, EdgeLabel> edgeLabelEntry = edgeLabelEntryIter.next();
            String edgeLabelName = edgeLabelEntry.getKey();
            EdgeLabel edgeLabel = edgeLabelEntry.getValue();
            this.inEdgeLabels.put(edgeLabelName, edgeLabel);
            edgeLabel.afterCommit();
            edgeLabelEntryIter.remove();

        }
        for (EdgeLabel edgeLabel : this.outEdgeLabels.values()) {
            edgeLabel.afterCommit();
        }
        for (EdgeLabel edgeLabel : this.inEdgeLabels.values()) {
            edgeLabel.afterCommit();
        }
    }

    void afterRollbackForInEdges() {
        Preconditions.checkState(this.schema.getTopology().isWriteLockHeldByCurrentThread(), "VertexLabel.afterRollback must hold the write lock");
        super.afterRollback();
        for (Iterator<EdgeLabel> it = this.uncommittedInEdgeLabels.values().iterator(); it.hasNext(); ) {
            EdgeLabel edgeLabel = it.next();
            edgeLabel.afterRollbackInEdges(this);
            it.remove();
        }
    }

    void afterRollbackForOutEdges() {
        Preconditions.checkState(this.schema.getTopology().isWriteLockHeldByCurrentThread(), "VertexLabel.afterRollback must hold the write lock");
        super.afterRollback();
        for (Iterator<EdgeLabel> it = this.uncommittedOutEdgeLabels.values().iterator(); it.hasNext(); ) {
            EdgeLabel edgeLabel = it.next();
            it.remove();
            //It is important to first remove the EdgeLabel from the iterator as the EdgeLabel's outVertex is still
            // present and its needed for the hashCode method which is invoked during the it.remove()
            edgeLabel.afterRollbackOutEdges(this);
        }
        //Only need to go though the outEdgeLabels. All edgeLabels will be touched
        for (EdgeLabel edgeLabel : this.outEdgeLabels.values()) {
            edgeLabel.afterRollbackOutEdges(this);
        }
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    void addToOutEdgeLabels(String schema, EdgeLabel edgeLabel) {
        edgeLabel.addToOutVertexLabel(this);
        this.outEdgeLabels.put(schema + "." + edgeLabel.getLabel(), edgeLabel);
    }

    void addToInEdgeLabels(EdgeLabel edgeLabel) {
        edgeLabel.addToInVertexLabel(this);
        this.inEdgeLabels.put(edgeLabel.getSchema().getName() + "." + edgeLabel.getLabel(), edgeLabel);
    }

    @Override
    protected JsonNode toJson() {
        ObjectNode vertexLabelNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        vertexLabelNode.put("schema", getSchema().getName());
        vertexLabelNode.put("label", getLabel());
        vertexLabelNode.set("properties", super.toJson());

        ArrayNode outEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (EdgeLabel edgeLabel : this.outEdgeLabels.values()) {
            outEdgeLabelsArrayNode.add(edgeLabel.toJson());
        }
        vertexLabelNode.set("outEdgeLabels", outEdgeLabelsArrayNode);

        ArrayNode inEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (EdgeLabel edgeLabel : this.inEdgeLabels.values()) {
            inEdgeLabelsArrayNode.add(edgeLabel.toJson());
        }
        vertexLabelNode.set("inEdgeLabels", inEdgeLabelsArrayNode);

        if (this.schema.getTopology().isWriteLockHeldByCurrentThread()) {
            outEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (EdgeLabel edgeLabel : this.uncommittedOutEdgeLabels.values()) {
                outEdgeLabelsArrayNode.add(edgeLabel.toJson());
            }
            vertexLabelNode.set("uncommittedOutEdgeLabels", outEdgeLabelsArrayNode);

            inEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (EdgeLabel edgeLabel : this.uncommittedInEdgeLabels.values()) {
                inEdgeLabelsArrayNode.add(edgeLabel.toJson());
            }
            vertexLabelNode.set("uncommittedInEdgeLabels", inEdgeLabelsArrayNode);
        }
        return vertexLabelNode;
    }

    protected Optional<JsonNode> toNotifyJson() {
        ObjectNode vertexLabelNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        vertexLabelNode.put("label", getLabel());

        Optional<JsonNode> abstractLabelNode = super.toNotifyJson();
        if (abstractLabelNode.isPresent()) {
            vertexLabelNode.set("uncommittedProperties", abstractLabelNode.get().get("uncommittedProperties"));
            vertexLabelNode.set("uncommittedIndexes", abstractLabelNode.get().get("uncommittedIndexes"));
        }

        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread() && !this.uncommittedOutEdgeLabels.isEmpty()) {
            ArrayNode outEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (EdgeLabel edgeLabel : this.uncommittedOutEdgeLabels.values()) {
                Optional<JsonNode> jsonNodeOptional = edgeLabel.toNotifyJson();
                Preconditions.checkState(jsonNodeOptional.isPresent(), "There must be data to notify as the edgeLabel itself is uncommitted");
                //noinspection OptionalGetWithoutIsPresent
                outEdgeLabelsArrayNode.add(jsonNodeOptional.get());
            }
            vertexLabelNode.set("uncommittedOutEdgeLabels", outEdgeLabelsArrayNode);
        }

        if (this.getSchema().getTopology().isWriteLockHeldByCurrentThread() && !this.uncommittedInEdgeLabels.isEmpty()) {
            ArrayNode inEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (EdgeLabel edgeLabel : this.uncommittedInEdgeLabels.values()) {
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
        for (EdgeLabel edgeLabel : this.outEdgeLabels.values()) {
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
        for (EdgeLabel edgeLabel : this.inEdgeLabels.values()) {
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

    void fromNotifyJsonOutEdge(JsonNode vertexLabelJson) {
        super.fromPropertyNotifyJson(vertexLabelJson);
        for (String s : Arrays.asList("uncommittedOutEdgeLabels", "outEdgeLabels")) {
            ArrayNode uncommittedOutEdgeLabels = (ArrayNode) vertexLabelJson.get(s);
            if (uncommittedOutEdgeLabels != null) {
                for (JsonNode uncommittedOutEdgeLabel : uncommittedOutEdgeLabels) {
                    String schemaName = uncommittedOutEdgeLabel.get("schema").asText();
                    Preconditions.checkState(schemaName.equals(getSchema().getName()), "out edges must be for the same schema that the edge specifies");
                    String edgeLabelName = uncommittedOutEdgeLabel.get("label").asText();
                    Optional<EdgeLabel> edgeLabelOptional = this.schema.getEdgeLabel(edgeLabelName);
                    EdgeLabel edgeLabel;
                    if (!edgeLabelOptional.isPresent()) {
                        edgeLabel = new EdgeLabel(this.getSchema().getTopology(), edgeLabelName);
                    } else {
                        edgeLabel = edgeLabelOptional.get();
                    }
                    edgeLabel.addToOutVertexLabel(this);
                    this.outEdgeLabels.put(schemaName + "." + edgeLabel.getLabel(), edgeLabel);
                    edgeLabel.fromPropertyNotifyJson(uncommittedOutEdgeLabel);
                    //Babysit the cache
                    this.getSchema().getTopology().addToAllTables(getSchema().getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel.getPropertyTypeMap());
                    this.getSchema().addToAllEdgeCache(edgeLabel);
                    this.getSchema().getTopology().addOutForeignKeysToVertexLabel(this, edgeLabel);
                    this.getSchema().getTopology().addToEdgeForeignKeyCache(
                            this.getSchema().getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(),
                            this.getSchema().getName() + "." + this.getLabel() + SchemaManager.OUT_VERTEX_COLUMN_END);
                }
            }
        }
    }

    void fromNotifyJsonInEdge(JsonNode vertexLabelJson) {
        for (String s : Arrays.asList("uncommittedInEdgeLabels", "inEdgeLabels")) {
            ArrayNode uncommittedInEdgeLabels = (ArrayNode) vertexLabelJson.get(s);
            if (uncommittedInEdgeLabels != null) {
                for (JsonNode uncommittedInEdgeLabel : uncommittedInEdgeLabels) {
                    String schemaName = uncommittedInEdgeLabel.get("schema").asText();
                    String edgeLabelName = uncommittedInEdgeLabel.get("label").asText();
                    Optional<Schema> schemaOptional = getSchema().getTopology().getSchema(schemaName);
                    Preconditions.checkState(schemaOptional.isPresent(), "Schema %s must be present", schemaName);
                    @SuppressWarnings("OptionalGetWithoutIsPresent")
                    Optional<EdgeLabel> edgeLabelOptional = schemaOptional.get().getEdgeLabel(edgeLabelName);
                    Preconditions.checkState(edgeLabelOptional.isPresent(), "edge label must be present as the in can not be there without the out");
                    @SuppressWarnings("OptionalGetWithoutIsPresent")
                    EdgeLabel edgeLabel = edgeLabelOptional.get();
                    edgeLabel.addToInVertexLabel(this);
                    this.inEdgeLabels.put(schemaName + "." + edgeLabel.getLabel(), edgeLabel);
                    edgeLabel.fromPropertyNotifyJson(uncommittedInEdgeLabel);
                    this.getSchema().getTopology().addInForeignKeysToVertexLabel(this, edgeLabel);
                    this.getSchema().getTopology().addToEdgeForeignKeyCache(
                            edgeLabel.getSchema().getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(),
                            this.getSchema().getName() + "." + this.getLabel() + SchemaManager.IN_VERTEX_COLUMN_END);
                }
            }
        }
    }

    @Override
    public int hashCode() {
        return (this.schema.getName() + this.getLabel()).hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) {
            return false;
        }
        if (!(other instanceof VertexLabel)) {
            return false;
        }
        VertexLabel otherVertexLabel = (VertexLabel) other;
        return this.schema.equals(otherVertexLabel.getSchema()) && super.equals(otherVertexLabel);
    }

    boolean deepEquals(VertexLabel other) {
        Preconditions.checkState(this.equals(other), "deepEquals is only called after equals has succeeded");
        if (!this.outEdgeLabels.equals(other.outEdgeLabels)) {
            return false;
        } else {
            if (this.outEdgeLabels.size() != other.outEdgeLabels.size()) {
                return false;
            } else {
                for (EdgeLabel outEdgeLabel : this.outEdgeLabels.values()) {
                    for (EdgeLabel otherOutEdgeLabel : other.outEdgeLabels.values()) {
                        if (outEdgeLabel.equals(otherOutEdgeLabel)) {
                            if (!outEdgeLabel.deepEquals(otherOutEdgeLabel)) {
                                return false;
                            }
                        }
                    }
                }
            }
            return true;
        }
    }

    @Override
    public List<Topology.TopologyValidationError> validateTopology(DatabaseMetaData metadata) throws SQLException {
        List<Topology.TopologyValidationError> validationErrors = new ArrayList<>();
        for (PropertyColumn propertyColumn : getProperties().values()) {
            try (ResultSet propertyRs = metadata.getColumns(null, this.getSchema().getName(), "V_" + this.getLabel(), propertyColumn.getName())) {
                if (!propertyRs.next()) {
                    validationErrors.add(new Topology.TopologyValidationError(propertyColumn));
                }
            }
        }
        for (Index index : getIndexes().values()) {
            validationErrors.addAll(index.validateTopology(metadata));
        }
        return validationErrors;
    }

    @Override
    protected String getPrefix() {
        return SchemaManager.VERTEX_PREFIX;
    }

    Pair<Set<SchemaTable>, Set<SchemaTable>> getUncommittedSchemaTableForeignKeys() {
        Pair<Set<SchemaTable>, Set<SchemaTable>> result = Pair.of(new HashSet<>(), new HashSet<>());
        for (Map.Entry<String, EdgeLabel> uncommittedEdgeLabelEntry : this.uncommittedOutEdgeLabels.entrySet()) {
            result.getRight().add(SchemaTable.of(this.getSchema().getName(), SchemaManager.EDGE_PREFIX + uncommittedEdgeLabelEntry.getValue().getLabel()));
        }
        for (Map.Entry<String, EdgeLabel> uncommittedEdgeLabelEntry : this.uncommittedInEdgeLabels.entrySet()) {
            result.getLeft().add(SchemaTable.of(uncommittedEdgeLabelEntry.getValue().getSchema().getName(), SchemaManager.EDGE_PREFIX + uncommittedEdgeLabelEntry.getValue().getLabel()));
        }
        return result;
    }
}
