package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyChangeAction;
import org.umlg.sqlg.util.ThreadLocalMap;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class VertexLabel extends AbstractLabel {

    private static final Logger LOGGER = LoggerFactory.getLogger(VertexLabel.class);
    private final Schema schema;

    //hand (out) ----<label>---- finger (in)
    //The key of the map must include the schema.
    //This is A.A --ab-->A.B
    //        B.B --ab-->A.B
    //In this case 2 EdgeLabels will exist A.ab and B.ab
    //A.B's inEdgeLabels will contain both EdgeLabels
    final Map<String, EdgeLabel> inEdgeLabels = new ConcurrentHashMap<>();
    final Map<String, EdgeLabel> outEdgeLabels = new ConcurrentHashMap<>();
    private final Map<String, EdgeLabel> uncommittedInEdgeLabels = new ThreadLocalMap<>();
    private final Map<String, EdgeLabel> uncommittedOutEdgeLabels = new ThreadLocalMap<>();
    private final Map<String, Pair<EdgeLabel, EdgeRemoveType>> uncommittedRemovedInEdgeLabels = new ThreadLocalMap<>();
    private final Map<String, Pair<EdgeLabel, EdgeRemoveType>> uncommittedRemovedOutEdgeLabels = new ThreadLocalMap<>();

    private enum EdgeRemoveType {
        EDGE_LABEL,
        EDGE_ROLE
    }

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

    static VertexLabel createVertexLabel(SqlgGraph sqlgGraph, Schema schema, String label, Map<String, PropertyType> columns, ListOrderedSet<String> identifiers) {
        Preconditions.checkArgument(!schema.isSqlgSchema(), "createVertexLabel may not be called for \"%s\"", SQLG_SCHEMA);
        VertexLabel vertexLabel = new VertexLabel(schema, label, columns, identifiers);
        vertexLabel.createVertexLabelOnDb(columns, identifiers);
        TopologyManager.addVertexLabel(sqlgGraph, schema.getName(), label, columns, identifiers);
        vertexLabel.committed = false;
        return vertexLabel;
    }

    static VertexLabel renameVertexLabel(SqlgGraph sqlgGraph, Schema schema, String oldLabel, String newLabel, Map<String, PropertyType> columns, ListOrderedSet<String> identifiers) {
        Preconditions.checkArgument(!schema.isSqlgSchema(), "renameVertexLabel may not be called for \"%s\"", SQLG_SCHEMA);
        VertexLabel vertexLabel = new VertexLabel(schema, newLabel, columns, identifiers);
        vertexLabel.renameVertexLabelOnDb(oldLabel, newLabel);
        TopologyManager.renameVertexLabel(sqlgGraph, schema.getName(), VERTEX_PREFIX + oldLabel, VERTEX_PREFIX + newLabel);
        vertexLabel.committed = false;
        return vertexLabel;
    }

    static VertexLabel createPartitionedVertexLabel(
            SqlgGraph sqlgGraph,
            Schema schema,
            String label,
            Map<String, PropertyType> columns,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression,
            boolean addPrimaryKeyConstraint) {

        Preconditions.checkArgument(!schema.isSqlgSchema(), "createVertexLabel may not be called for \"%s\"", SQLG_SCHEMA);
        Preconditions.checkArgument(partitionType != PartitionType.NONE, "PartitionType must be RANGE or LIST. Found NONE.");
        Preconditions.checkArgument(!StringUtils.isEmpty(partitionExpression), "partitionExpression may not be null or empty when creating a partitioned vertex label.");
        Preconditions.checkArgument(!identifiers.isEmpty(), "Partitioned label must have at least one identifier.");
        VertexLabel vertexLabel = new VertexLabel(schema, label, columns, identifiers, partitionType, partitionExpression);
        vertexLabel.createPartitionedVertexLabelOnDb(columns, identifiers, addPrimaryKeyConstraint);
        TopologyManager.addVertexLabel(sqlgGraph, schema.getName(), label, columns, identifiers, partitionType, partitionExpression);
        vertexLabel.committed = false;
        return vertexLabel;
    }

    VertexLabel(Schema schema, String label) {
        super(schema.getSqlgGraph(), label);
        this.schema = schema;
    }

    VertexLabel(Schema schema, String label, boolean isForeignVertexLabel) {
        super(schema.getSqlgGraph(), label, isForeignVertexLabel);
        this.schema = schema;
    }

    /**
     * Only called for a new label being added.
     *
     * @param schema     The schema.
     * @param label      The vertex's label.
     * @param properties The vertex's properties.
     */
    private VertexLabel(Schema schema, String label, Map<String, PropertyType> properties, ListOrderedSet<String> identifiers) {
        super(schema.getSqlgGraph(), label, properties, identifiers);
        this.schema = schema;
    }

    /**
     * Called for a partitioned vertex label loaded on startup.
     *
     * @param schema        The schema.
     * @param label         The vertex's label.
     * @param partitionType The vertex's partition strategy. i.e. RANGE or LIST.
     */
    VertexLabel(Schema schema, String label, PartitionType partitionType, String partitionExpression) {
        super(schema.getSqlgGraph(), label, partitionType, partitionExpression);
        this.schema = schema;
    }

    /**
     * Called for a new partitioned vertex label being added and when partitioned vertices are loaded on startup.
     *
     * @param schema        The schema.
     * @param label         The vertex's label.
     * @param properties    The vertex's properties.
     * @param identifiers   The vertex's identifiers.
     * @param partitionType The vertex's partition strategy. i.e. RANGE or LIST.
     */
    VertexLabel(Schema schema, String label, Map<String, PropertyType> properties, ListOrderedSet<String> identifiers, PartitionType partitionType, String partitionExpression) {
        super(schema.getSqlgGraph(), label, properties, identifiers, partitionType, partitionExpression);
        this.schema = schema;
    }

    @Override
    public Schema getSchema() {
        return this.schema;
    }

    @Override
    public Topology getTopology() {
        return this.schema.getTopology();
    }

    public Map<String, EdgeLabel> getInEdgeLabels() {
        Map<String, EdgeLabel> result = new HashMap<>(this.inEdgeLabels);
        if (this.schema.getTopology().isSchemaChanged()) {
            result.putAll(this.uncommittedInEdgeLabels);
            for (String e : this.uncommittedRemovedInEdgeLabels.keySet()) {
                result.remove(e);
            }
        }
        return result;
    }

    public Map<String, EdgeLabel> getOutEdgeLabels() {
        Map<String, EdgeLabel> result = new HashMap<>(this.outEdgeLabels);
        if (this.schema.getTopology().isSchemaChanged()) {
            result.putAll(this.uncommittedOutEdgeLabels);
            for (String e : this.uncommittedRemovedOutEdgeLabels.keySet()) {
                result.remove(e);
            }
        }
        return Collections.unmodifiableMap(result);
    }


    public Map<String, EdgeRole> getInEdgeRoles() {
        Map<String, EdgeRole> result = new HashMap<>();
        for (String k : this.inEdgeLabels.keySet()) {
            result.put(k, new EdgeRole(this, this.inEdgeLabels.get(k), Direction.IN, true));
        }
        if (this.schema.getTopology().isSchemaChanged()) {
            for (String k : this.uncommittedInEdgeLabels.keySet()) {
                result.put(k, new EdgeRole(this, this.uncommittedInEdgeLabels.get(k), Direction.IN, false));
            }
            for (String e : this.uncommittedRemovedInEdgeLabels.keySet()) {
                result.remove(e);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<String, EdgeRole> getOutEdgeRoles() {
        Map<String, EdgeRole> result = new HashMap<>();
        for (String k : this.outEdgeLabels.keySet()) {
            result.put(k, new EdgeRole(this, this.outEdgeLabels.get(k), Direction.OUT, true));
        }
        if (this.schema.getTopology().isSchemaChanged()) {
            for (String k : this.uncommittedOutEdgeLabels.keySet()) {
                result.put(k, new EdgeRole(this, this.uncommittedOutEdgeLabels.get(k), Direction.OUT, false));
            }
            for (String e : this.uncommittedRemovedOutEdgeLabels.keySet()) {
                result.remove(e);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * Out EdgeLabels are always in the same schema as the 'this' VertexLabel' schema.
     * So the edgeLabelName must not contain the schema prefix
     *
     * @param edgeLabelName The edge label's name
     * @return Optionally the EdgeLabel
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
        if (this.schema.getTopology().isSchemaChanged()) {
            Map<String, EdgeLabel> result = new HashMap<>(this.uncommittedOutEdgeLabels);
            for (EdgeLabel outEdgeLabel : this.outEdgeLabels.values()) {
                Map<String, PropertyColumn> propertyMap = outEdgeLabel.getUncommittedPropertyTypeMap();
                if (!propertyMap.isEmpty() || !outEdgeLabel.getUncommittedRemovedProperties().isEmpty()) {
                    result.put(outEdgeLabel.getLabel(), outEdgeLabel);
                }
            }
            return Collections.unmodifiableMap(result);
        } else {
            return Collections.emptyMap();
        }
    }

    Optional<EdgeLabel> getUncommittedOutEdgeLabel(String edgeLabelName) {
        if (this.schema.getTopology().isSchemaChanged()) {
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
     * @return The EdgeLabel
     */
    public EdgeLabel ensureEdgeLabelExist(final String edgeLabelName, final VertexLabel inVertexLabel, Map<String, PropertyType> properties) {
        return this.getSchema().ensureEdgeLabelExist(edgeLabelName, this, inVertexLabel, properties);
    }

    /**
     * Ensures that the {@link EdgeLabel} exists. It will be created if it does not exists.
     * "this" is the out {@link VertexLabel} and inVertexLabel is the inVertexLabel
     * This method is equivalent to {@link Schema#ensureEdgeLabelExist(String, VertexLabel, VertexLabel, Map, ListOrderedSet)}
     *
     * @param edgeLabelName The EdgeLabel's label's name.
     * @param inVertexLabel The edge's in VertexLabel.
     * @param properties    The EdgeLabel's properties.
     * @param identifiers   The EdgeLabel's identifiers.
     * @return The EdgeLabel
     */
    public EdgeLabel ensureEdgeLabelExist(final String edgeLabelName, final VertexLabel inVertexLabel, Map<String, PropertyType> properties, ListOrderedSet<String> identifiers) {
        return this.getSchema().ensureEdgeLabelExist(edgeLabelName, this, inVertexLabel, properties, identifiers);
    }

    public EdgeLabel ensurePartitionedEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel inVertexLabel,
            Map<String, PropertyType> properties,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression,
            boolean isForeignKeyPartition) {

        return this.getSchema().ensurePartitionedEdgeLabelExist(
                edgeLabelName,
                this,
                inVertexLabel,
                properties,
                identifiers,
                partitionType,
                partitionExpression,
                isForeignKeyPartition
        );
    }

    public EdgeLabel ensurePartitionedEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel inVertexLabel,
            Map<String, PropertyType> properties,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression) {

        return this.getSchema().ensurePartitionedEdgeLabelExist(
                edgeLabelName,
                this,
                inVertexLabel,
                properties,
                identifiers,
                partitionType,
                partitionExpression);
    }

    EdgeLabel addPartitionedEdgeLabel(
            final String edgeLabelName,
            final VertexLabel inVertexLabel,
            final Map<String, PropertyType> properties,
            final ListOrderedSet<String> identifiers,
            final PartitionType partitionType,
            final String partitionExpression,
            boolean isForeignKeyPartition) {

        EdgeLabel edgeLabel = EdgeLabel.createPartitionedEdgeLabel(
                edgeLabelName,
                this,
                inVertexLabel,
                properties,
                identifiers,
                partitionType,
                partitionExpression,
                isForeignKeyPartition);
        if (this.schema.isSqlgSchema()) {
            this.outEdgeLabels.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
            inVertexLabel.inEdgeLabels.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
        } else {
            this.uncommittedOutEdgeLabels.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
            inVertexLabel.uncommittedInEdgeLabels.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
        }
        return edgeLabel;
    }

    /**
     * Called via {@link Schema#ensureEdgeLabelExist(String, VertexLabel, VertexLabel, Map)}
     * This is called when the {@link EdgeLabel} does not exist and needs to be created.
     *
     * @param edgeLabelName The edge's label.
     * @param inVertexLabel The edge's in vertex.
     * @param properties    The edge's properties.
     * @return The new EdgeLabel.
     */
    EdgeLabel addEdgeLabel(
            String edgeLabelName,
            VertexLabel inVertexLabel,
            Map<String, PropertyType> properties) {
        return addEdgeLabel(edgeLabelName, inVertexLabel, properties, new ListOrderedSet<>());
    }

    /**
     * Called via {@link Schema#ensureEdgeLabelExist(String, VertexLabel, VertexLabel, Map)}
     * This is called when the {@link EdgeLabel} does not exist and needs to be created.
     *
     * @param edgeLabelName The edge's label.
     * @param inVertexLabel The edge's in vertex.
     * @param properties    The edge's properties.
     * @param identifiers   The edge's user defined identifiers.
     * @return The new EdgeLabel.
     */
    EdgeLabel addEdgeLabel(
            String edgeLabelName,
            VertexLabel inVertexLabel,
            Map<String, PropertyType> properties,
            ListOrderedSet<String> identifiers) {

        EdgeLabel edgeLabel = EdgeLabel.createEdgeLabel(edgeLabelName, this, inVertexLabel, properties, identifiers);
        if (this.schema.isSqlgSchema()) {
            this.outEdgeLabels.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
            inVertexLabel.inEdgeLabels.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
        } else {
            this.uncommittedOutEdgeLabels.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
            inVertexLabel.uncommittedInEdgeLabels.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeLabel);
        }
        return edgeLabel;
    }

    //    @Override
    public void ensurePropertiesExist(Map<String, PropertyType> columns) {
        Preconditions.checkState(!this.isForeignAbstractLabel, "'%s' is a read only foreign VertexLabel!", this.label);
        for (Map.Entry<String, PropertyType> column : columns.entrySet()) {
            if (!this.properties.containsKey(column.getKey())) {
                Preconditions.checkState(!this.schema.isSqlgSchema(), "schema may not be %s", SQLG_SCHEMA);
                this.sqlgGraph.getSqlDialect().validateColumnName(column.getKey());
                if (!this.uncommittedProperties.containsKey(column.getKey())) {
                    this.schema.getTopology().lock();
                    if (getProperty(column.getKey()).isEmpty()) {
                        TopologyManager.addVertexColumn(this.sqlgGraph, this.schema.getName(), VERTEX_PREFIX + getLabel(), column);
                        addColumn(this.schema.getName(), VERTEX_PREFIX + getLabel(), ImmutablePair.of(column.getKey(), column.getValue()));
                        PropertyColumn propertyColumn = new PropertyColumn(this, column.getKey(), column.getValue());
                        propertyColumn.setCommitted(false);
                        this.uncommittedProperties.put(column.getKey(), propertyColumn);
                        this.getSchema().getTopology().fire(propertyColumn, null, TopologyChangeAction.CREATE);
                    }
                }
            }
        }
    }

    public void ensureDistributed(int shardCount, PropertyColumn distributionPropertyColumn) {
        ensureDistributed(shardCount, distributionPropertyColumn, null);
    }


    public void ensureDistributed(PropertyColumn distributionPropertyColumn, AbstractLabel colocate) {
        ensureDistributed(-1, distributionPropertyColumn, colocate);
    }

    private void createVertexLabelOnDb(Map<String, PropertyType> columns, ListOrderedSet<String> identifiers) {
        StringBuilder sql = new StringBuilder(this.sqlgGraph.getSqlDialect().createTableStatement());
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema.getName()));
        sql.append(".");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + getLabel()));
        sql.append(" (");

        if (identifiers.isEmpty()) {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
            sql.append(" ");
            sql.append(this.sqlgGraph.getSqlDialect().getAutoIncrementPrimaryKeyConstruct());
            if (columns.size() > 0) {
                sql.append(", ");
            }
        }
        buildColumns(this.sqlgGraph, identifiers, columns, sql);
        if (!identifiers.isEmpty()) {
            sql.append(", PRIMARY KEY(");
            int count = 1;
            for (String identifier : identifiers) {
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                if (count++ < identifiers.size()) {
                    sql.append(", ");
                }
            }
            sql.append(")");
        }
        sql.append(")");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void renameVertexLabelOnDb(String oldLabel, String newLabel) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema.getName()));
        sql.append(".");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + oldLabel));
        sql.append(" RENAME TO ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + newLabel));
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createPartitionedVertexLabelOnDb(Map<String, PropertyType> columns, ListOrderedSet<String> identifiers, boolean addPrimaryKeyConstraint) {
        Preconditions.checkState(!identifiers.isEmpty(), "Partitioned table must have identifiers.");
        StringBuilder sql = new StringBuilder(this.sqlgGraph.getSqlDialect().createTableStatement());
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema.getName()));
        sql.append(".");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + getLabel()));
        sql.append(" (");
        buildColumns(this.sqlgGraph, new ListOrderedSet<>(), columns, sql);

        //below is needed for #408, to create the primary key.
        //It however clashes with existing partitioned table definitions.
        if (addPrimaryKeyConstraint) {
            sql.append(", PRIMARY KEY(");
            int count = 1;
            for (String identifier : identifiers) {
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                if (count++ < identifiers.size()) {
                    sql.append(", ");
                }
            }
            sql.append(")");
        }

        //nothing to do with the identifiers as partitioned tables do not have primary keys.
        sql.append(") PARTITION BY ");
        sql.append(this.partitionType.name());
        sql.append(" (");
        sql.append(this.partitionExpression);
        sql.append(")");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql.toString());
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
        if (this.schema.getTopology().isSchemaChanged()) {
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
        Preconditions.checkState(this.schema.getTopology().isSchemaChanged(), "VertexLabel.afterCommit must have schemaChanged = true");
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
        for (Iterator<String> it = this.uncommittedRemovedOutEdgeLabels.keySet().iterator(); it.hasNext(); ) {
            String s = it.next();
            EdgeLabel lbl = this.outEdgeLabels.remove(s);
            if (lbl != null) {
                ForeignKey foreignKey;
                if (hasIDPrimaryKey()) {
                    foreignKey = ForeignKey.of(this.getFullName() + Topology.OUT_VERTEX_COLUMN_END);
                } else {
                    foreignKey = new ForeignKey();
                    for (String identifier : this.getIdentifiers()) {
                        foreignKey.add(this.getFullName(), identifier, Topology.OUT_VERTEX_COLUMN_END);
                    }
                }
                this.getSchema().getTopology().removeFromEdgeForeignKeyCache(
                        lbl.getSchema().getName() + "." + EDGE_PREFIX + lbl.getLabel(),
                        foreignKey);
                this.getSchema().getTopology().removeOutForeignKeysFromVertexLabel(this, lbl);

            }
            it.remove();
        }

        for (Iterator<String> it = this.uncommittedRemovedInEdgeLabels.keySet().iterator(); it.hasNext(); ) {
            String s = it.next();
            EdgeLabel lbl = this.inEdgeLabels.remove(s);
            if (lbl != null) {
                ForeignKey foreignKey;
                if (this.hasIDPrimaryKey()) {
                    foreignKey = ForeignKey.of(this.getFullName() + Topology.IN_VERTEX_COLUMN_END);
                } else {
                    foreignKey = new ForeignKey();
                    for (String identifier : this.getIdentifiers()) {
                        foreignKey.add(this.getFullName(), identifier, Topology.IN_VERTEX_COLUMN_END);
                    }
                }
                this.getSchema().getTopology().removeFromEdgeForeignKeyCache(
                        lbl.getSchema().getName() + "." + EDGE_PREFIX + lbl.getLabel(),
                        foreignKey);
                this.getSchema().getTopology().removeInForeignKeysFromVertexLabel(this, lbl);
            }
            it.remove();
        }

        for (EdgeLabel edgeLabel : this.outEdgeLabels.values()) {
            edgeLabel.afterCommit();
        }
        for (EdgeLabel edgeLabel : this.inEdgeLabels.values()) {
            edgeLabel.afterCommit();
        }
    }

    void afterRollbackForInEdges() {
        Preconditions.checkState(this.schema.getTopology().isSchemaChanged(), "VertexLabel.afterRollback must have schemaChanged = true");
        super.afterRollback();
        for (Iterator<EdgeLabel> it = this.uncommittedInEdgeLabels.values().iterator(); it.hasNext(); ) {
            EdgeLabel edgeLabel = it.next();
            edgeLabel.afterRollbackInEdges(this);
            it.remove();
        }
        for (EdgeLabel edgeLabel : this.inEdgeLabels.values()) {
            edgeLabel.afterRollbackInEdges(this);
            this.uncommittedRemovedInEdgeLabels.remove(edgeLabel.getFullName());
        }
    }

    void afterRollbackForOutEdges() {
        Preconditions.checkState(this.schema.getTopology().isSchemaChanged(), "VertexLabel.afterRollback must have schemaChanged = true");
        super.afterRollback();
        for (Iterator<EdgeLabel> it = this.uncommittedOutEdgeLabels.values().iterator(); it.hasNext(); ) {
            EdgeLabel edgeLabel = it.next();
            it.remove();
            //It is important to first remove the EdgeLabel from the iterator as the EdgeLabel's outVertex is still
            // present and its needed for the hashCode method which is invoked during the it.remove()
            edgeLabel.afterRollbackOutEdges(this);
        }
        for (EdgeLabel edgeLabel : this.outEdgeLabels.values()) {
            edgeLabel.afterRollbackOutEdges(this);
            this.uncommittedRemovedOutEdgeLabels.remove(edgeLabel.getFullName());
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
        List<EdgeLabel> edgeLabels = new ArrayList<>(this.outEdgeLabels.values());
        edgeLabels.sort(Comparator.comparing(EdgeLabel::getName));
        for (EdgeLabel edgeLabel : edgeLabels) {
            outEdgeLabelsArrayNode.add(edgeLabel.toJson());
        }
        vertexLabelNode.set("outEdgeLabels", outEdgeLabelsArrayNode);

        ArrayNode inEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        edgeLabels = new ArrayList<>(this.inEdgeLabels.values());
        edgeLabels.sort(Comparator.comparing(EdgeLabel::getName));
        for (EdgeLabel edgeLabel : edgeLabels) {
            inEdgeLabelsArrayNode.add(edgeLabel.toJson());
        }
        vertexLabelNode.set("inEdgeLabels", inEdgeLabelsArrayNode);

        ArrayNode partitionArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
        for (Partition partition : this.getPartitions().values()) {
            Optional<ObjectNode> json = partition.toJson();
            json.ifPresent(partitionArrayNode::add);
        }
        vertexLabelNode.set("partitions", partitionArrayNode);

        if (this.schema.getTopology().isSchemaChanged()) {
            outEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            edgeLabels = new ArrayList<>(this.uncommittedOutEdgeLabels.values());
            edgeLabels.sort(Comparator.comparing(EdgeLabel::getName));
            for (EdgeLabel edgeLabel : edgeLabels) {
                outEdgeLabelsArrayNode.add(edgeLabel.toJson());
            }
            vertexLabelNode.set("uncommittedOutEdgeLabels", outEdgeLabelsArrayNode);

            inEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            edgeLabels = new ArrayList<>(this.uncommittedInEdgeLabels.values());
            edgeLabels.sort(Comparator.comparing(EdgeLabel::getName));
            for (EdgeLabel edgeLabel : edgeLabels) {
                inEdgeLabelsArrayNode.add(edgeLabel.toJson());
            }
            vertexLabelNode.set("uncommittedInEdgeLabels", inEdgeLabelsArrayNode);
        }
        return vertexLabelNode;
    }

    protected Optional<JsonNode> toNotifyJson() {
        ObjectNode vertexLabelNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        vertexLabelNode.put("label", getLabel());
        vertexLabelNode.put("partitionType", this.partitionType.name());
        vertexLabelNode.put("partitionExpression", this.partitionExpression);

        Optional<JsonNode> abstractLabelNode = super.toNotifyJson();
        if (abstractLabelNode.isPresent()) {
            vertexLabelNode.set("uncommittedProperties", abstractLabelNode.get().get("uncommittedProperties"));
            vertexLabelNode.set("uncommittedIdentifiers", abstractLabelNode.get().get("uncommittedIdentifiers"));
            vertexLabelNode.set("renamedIdentifiers", abstractLabelNode.get().get("renamedIdentifiers"));
            vertexLabelNode.set("uncommittedPartitions", abstractLabelNode.get().get("uncommittedPartitions"));
            vertexLabelNode.set("uncommittedPartitions", abstractLabelNode.get().get("uncommittedPartitions"));
            if (abstractLabelNode.get().get("uncommittedDistributionPropertyColumn") != null) {
                vertexLabelNode.set("uncommittedDistributionPropertyColumn", abstractLabelNode.get().get("uncommittedDistributionPropertyColumn"));
            }
            if (abstractLabelNode.get().get("uncommittedShardCount") != null) {
                vertexLabelNode.set("uncommittedShardCount", abstractLabelNode.get().get("uncommittedShardCount"));
            }
            if (abstractLabelNode.get().get("uncommittedDistributionColocateAbstractLabel") != null) {
                vertexLabelNode.set("uncommittedDistributionColocateAbstractLabel", abstractLabelNode.get().get("uncommittedDistributionColocateAbstractLabel"));
            }
            vertexLabelNode.set("partitions", abstractLabelNode.get().get("partitions"));
            vertexLabelNode.set("uncommittedIndexes", abstractLabelNode.get().get("uncommittedIndexes"));
            vertexLabelNode.set("uncommittedRemovedProperties", abstractLabelNode.get().get("uncommittedRemovedProperties"));
            vertexLabelNode.set("uncommittedRemovedPartitions", abstractLabelNode.get().get("uncommittedRemovedPartitions"));
            vertexLabelNode.set("uncommittedRemovedIndexes", abstractLabelNode.get().get("uncommittedRemovedIndexes"));
        }

        if (this.schema.getTopology().isSchemaChanged() && !this.uncommittedOutEdgeLabels.isEmpty()) {
            ArrayNode outEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (EdgeLabel edgeLabel : this.uncommittedOutEdgeLabels.values()) {
                Optional<JsonNode> jsonNodeOptional = edgeLabel.toNotifyJson();
                Preconditions.checkState(jsonNodeOptional.isPresent(), "There must be data to notify as the edgeLabel itself is uncommitted");
                outEdgeLabelsArrayNode.add(jsonNodeOptional.get());
            }
            vertexLabelNode.set("uncommittedOutEdgeLabels", outEdgeLabelsArrayNode);
        }

        if (this.schema.getTopology().isSchemaChanged() && !this.uncommittedRemovedOutEdgeLabels.isEmpty()) {
            ArrayNode outEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (String edgeLabel : this.uncommittedRemovedOutEdgeLabels.keySet()) {
                ObjectNode edgeRemove = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                edgeRemove.put("label", edgeLabel);
                edgeRemove.put("type", this.uncommittedRemovedOutEdgeLabels.get(edgeLabel).getRight().name());
                outEdgeLabelsArrayNode.add(edgeRemove);
            }
            vertexLabelNode.set("uncommittedRemovedOutEdgeLabels", outEdgeLabelsArrayNode);
        }


        if (this.schema.getTopology().isSchemaChanged() && !this.uncommittedInEdgeLabels.isEmpty()) {
            ArrayNode inEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (EdgeLabel edgeLabel : this.uncommittedInEdgeLabels.values()) {
                Optional<JsonNode> jsonNodeOptional = edgeLabel.toNotifyJson();
                Preconditions.checkState(jsonNodeOptional.isPresent(), "There must be data to notify as the edgeLabel itself is uncommitted");
                inEdgeLabelsArrayNode.add(jsonNodeOptional.get());
            }
            vertexLabelNode.set("uncommittedInEdgeLabels", inEdgeLabelsArrayNode);
        }

        if (this.schema.getTopology().isSchemaChanged() && !this.uncommittedRemovedInEdgeLabels.isEmpty()) {
            ArrayNode outEdgeLabelsArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (String edgeLabel : this.uncommittedRemovedInEdgeLabels.keySet()) {
                ObjectNode edgeRemove = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
                edgeRemove.put("label", edgeLabel);
                edgeRemove.put("type", this.uncommittedRemovedInEdgeLabels.get(edgeLabel).getRight().name());
                outEdgeLabelsArrayNode.add(edgeRemove);
            }
            vertexLabelNode.set("uncommittedRemovedInEdgeLabels", outEdgeLabelsArrayNode);
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
            if (edgeLabel.isValid()) {
                Optional<JsonNode> jsonNodeOptional = edgeLabel.toNotifyJson();
                if (jsonNodeOptional.isPresent()) {
                    foundInEdgeLabels = true;
                    inEdgeLabelsArrayNode.add(jsonNodeOptional.get());
                }
            }
        }
        if (foundInEdgeLabels) {
            vertexLabelNode.set("inEdgeLabels", inEdgeLabelsArrayNode);
        }
        return Optional.of(vertexLabelNode);
    }

    /**
     * @param vertexLabelJson The VertexLabel's notification json
     * @param fire            should we fire topology events
     */
    void fromNotifyJsonOutEdge(JsonNode vertexLabelJson, boolean fire) {
        super.fromPropertyNotifyJson(vertexLabelJson, fire);
        for (String s : Arrays.asList("uncommittedOutEdgeLabels", "outEdgeLabels")) {
            ArrayNode uncommittedOutEdgeLabels = (ArrayNode) vertexLabelJson.get(s);
            if (uncommittedOutEdgeLabels != null) {
                for (JsonNode uncommittedOutEdgeLabel : uncommittedOutEdgeLabels) {
                    String schemaName = uncommittedOutEdgeLabel.get("schema").asText();
                    Preconditions.checkState(schemaName.equals(getSchema().getName()), "out edges must be for the same schema that the edge specifies");
                    String edgeLabelName = uncommittedOutEdgeLabel.get("label").asText();
                    Optional<EdgeLabel> edgeLabelOptional = this.schema.getEdgeLabel(edgeLabelName);
                    EdgeLabel edgeLabel;
                    if (edgeLabelOptional.isEmpty()) {
                        PartitionType partitionType = PartitionType.valueOf(uncommittedOutEdgeLabel.get("partitionType").asText());
                        if (partitionType.isNone()) {
                            edgeLabel = new EdgeLabel(this.getSchema().getTopology(), edgeLabelName);
                        } else {
                            String partitionExpression = uncommittedOutEdgeLabel.get("partitionExpression").asText();
                            edgeLabel = new EdgeLabel(this.getSchema().getTopology(), edgeLabelName, partitionType, partitionExpression);
                        }
                    } else {
                        edgeLabel = edgeLabelOptional.get();
                    }
                    edgeLabel.addToOutVertexLabel(this);
                    this.outEdgeLabels.put(schemaName + "." + edgeLabel.getLabel(), edgeLabel);
                    // fire if we didn't create the edge label
                    edgeLabel.fromPropertyNotifyJson(uncommittedOutEdgeLabel, edgeLabelOptional.isPresent());
                    //Babysit the cache
                    this.getSchema().getTopology().addToAllTables(getSchema().getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel.getPropertyTypeMap());
                    this.getSchema().addToAllEdgeCache(edgeLabel);
                    this.getSchema().getTopology().addOutForeignKeysToVertexLabel(this, edgeLabel);
                    ForeignKey foreignKey;
                    if (this.hasIDPrimaryKey()) {
                        foreignKey = ForeignKey.of(this.getFullName() + Topology.OUT_VERTEX_COLUMN_END);
                    } else {
                        foreignKey = new ForeignKey();
                        for (String identifier : this.getIdentifiers()) {
                            if (!isDistributed() || !getDistributionPropertyColumn().getName().equals(identifier)) {
                                //The distribution column needs to be ignored as its a regular property and not a __I or __O property
                                foreignKey.add(this.getFullName(), identifier, Topology.OUT_VERTEX_COLUMN_END);
                            }
                        }
                    }
                    this.getSchema().getTopology().addToEdgeForeignKeyCache(
                            this.getSchema().getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(),
                            foreignKey
                    );
                    // fire only applies to top level, fire for new edges
                    if (edgeLabelOptional.isEmpty()) {
                        this.getSchema().getTopology().fire(edgeLabel, null, TopologyChangeAction.CREATE);
                    }
                }
            }
        }
        ArrayNode uncommittedRemoveOutEdgeLabels = (ArrayNode) vertexLabelJson.get("uncommittedRemovedOutEdgeLabels");
        if (uncommittedRemoveOutEdgeLabels != null) {
            for (JsonNode n : uncommittedRemoveOutEdgeLabels) {
                EdgeLabel lbl = this.outEdgeLabels.remove(n.get("label").asText());
                if (lbl != null) {
                    EdgeRemoveType ert = EdgeRemoveType.valueOf(n.get("type").asText());
                    ForeignKey foreignKey;
                    if (this.hasIDPrimaryKey()) {
                        foreignKey = ForeignKey.of(this.getFullName() + Topology.OUT_VERTEX_COLUMN_END);
                    } else {
                        foreignKey = new ForeignKey();
                        for (String identifier : this.getIdentifiers()) {
                            if (!isDistributed() || !getDistributionPropertyColumn().getName().equals(identifier)) {
                                //The distribution column needs to be ignored as it's a regular property and not a __I or __O property
                                foreignKey.add(this.getFullName(), identifier, Topology.OUT_VERTEX_COLUMN_END);
                            }
                        }
                    }
                    this.getSchema().getTopology().removeFromEdgeForeignKeyCache(
                            lbl.getSchema().getName() + "." + EDGE_PREFIX + lbl.getLabel(),
                            foreignKey
                    );
                    this.getSchema().getTopology().removeOutForeignKeysFromVertexLabel(this, lbl);
                    lbl.outVertexLabels.remove(this);

                    switch (ert) {
                        case EDGE_LABEL:
                            this.getSchema().getTopology().fire(lbl, lbl, TopologyChangeAction.DELETE);
                            break;
                        case EDGE_ROLE:
                            EdgeRole topologyInf = new EdgeRole(this, lbl, Direction.OUT, true);
                            this.getSchema().getTopology().fire(topologyInf, topologyInf, TopologyChangeAction.DELETE);
                            break;
                        default:
                            break;
                    }

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
                    Optional<EdgeLabel> edgeLabelOptional = schemaOptional.get().getEdgeLabel(edgeLabelName);
                    //The edgeLabel could have been deleted
                    if (edgeLabelOptional.isPresent()) {
                        EdgeLabel edgeLabel = edgeLabelOptional.get();
                        edgeLabel.addToInVertexLabel(this);
                        this.inEdgeLabels.put(schemaName + "." + edgeLabel.getLabel(), edgeLabel);
                        edgeLabel.fromPropertyNotifyJson(uncommittedInEdgeLabel, false);
                        this.getSchema().getTopology().addInForeignKeysToVertexLabel(this, edgeLabel);
                        ForeignKey foreignKey;
                        if (this.hasIDPrimaryKey()) {
                            foreignKey = ForeignKey.of(this.getFullName() + Topology.IN_VERTEX_COLUMN_END);
                        } else {
                            foreignKey = new ForeignKey();
                            for (String identifier : this.getIdentifiers()) {
                                if (!isDistributed() || !getDistributionPropertyColumn().getName().equals(identifier)) {
                                    //The distribution column needs to be ignored as its a regular property and not a __I or __O property
                                    foreignKey.add(this.getFullName(), identifier, Topology.IN_VERTEX_COLUMN_END);
                                }
                            }
                        }
                        this.getSchema().getTopology().addToEdgeForeignKeyCache(
                                edgeLabel.getSchema().getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(),
                                foreignKey
                        );

                    }
                }
            }
        }
        ArrayNode uncommittedRemoveInEdgeLabels = (ArrayNode) vertexLabelJson.get("uncommittedRemovedInEdgeLabels");
        if (uncommittedRemoveInEdgeLabels != null) {
            for (JsonNode n : uncommittedRemoveInEdgeLabels) {
                EdgeLabel lbl = this.inEdgeLabels.remove(n.get("label").asText());
                if (lbl != null) {
                    EdgeRemoveType ert = EdgeRemoveType.valueOf(n.get("type").asText());
                    if (lbl.isValid()) {
                        ForeignKey foreignKey;
                        if (this.hasIDPrimaryKey()) {
                            foreignKey = ForeignKey.of(this.getFullName() + Topology.IN_VERTEX_COLUMN_END);
                        } else {
                            foreignKey = new ForeignKey();
                            for (String identifier : this.getIdentifiers()) {
                                foreignKey.add(this.getFullName(), identifier, Topology.IN_VERTEX_COLUMN_END);
                            }
                        }
                        this.getSchema().getTopology().removeFromEdgeForeignKeyCache(
                                lbl.getSchema().getName() + "." + EDGE_PREFIX + lbl.getLabel(),
                                foreignKey
                        );
                        this.getSchema().getTopology().removeInForeignKeysFromVertexLabel(this, lbl);
                    }
                    lbl.inVertexLabels.remove(this);

                    switch (ert) {
                        case EDGE_LABEL:
                            this.getSchema().getTopology().fire(lbl, lbl, TopologyChangeAction.DELETE);
                            break;
                        case EDGE_ROLE:
                            EdgeRole topologyInf = new EdgeRole(this, lbl, Direction.IN, true);
                            this.getSchema().getTopology().fire(topologyInf, topologyInf, TopologyChangeAction.DELETE);
                            break;
                    }

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
            for (EdgeLabel outEdgeLabel : this.outEdgeLabels.values()) {
                for (EdgeLabel otherOutEdgeLabel : other.outEdgeLabels.values()) {
                    if (outEdgeLabel.equals(otherOutEdgeLabel)) {
                        if (!outEdgeLabel.deepEquals(otherOutEdgeLabel)) {
                            return false;
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
            List<Triple<String, Integer, String>> columns = this.sqlgGraph.getSqlDialect().getTableColumns(metadata, null, this.getSchema().getName(), "V_" + this.getLabel(), propertyColumn.getName());
            if (columns.isEmpty()) {
                validationErrors.add(new Topology.TopologyValidationError(propertyColumn));
            }
//            try (ResultSet propertyRs = metadata.getColumns(null, this.getSchema().getName(), "V_" + this.getLabel(), propertyColumn.getName())) {
//                if (!propertyRs.next()) {
//                    validationErrors.add(new Topology.TopologyValidationError(propertyColumn));
//                }
//            }
        }
        for (Index index : getIndexes().values()) {
            validationErrors.addAll(index.validateTopology(metadata));
        }
        return validationErrors;
    }

    @Override
    public String getPrefix() {
        return VERTEX_PREFIX;
    }

    Pair<Set<SchemaTable>, Set<SchemaTable>> getUncommittedSchemaTableForeignKeys() {
        Pair<Set<SchemaTable>, Set<SchemaTable>> result = Pair.of(new HashSet<>(), new HashSet<>());
        for (Map.Entry<String, EdgeLabel> uncommittedEdgeLabelEntry : this.uncommittedOutEdgeLabels.entrySet()) {
            String key = uncommittedEdgeLabelEntry.getKey();
            EdgeLabel edgeLabel = uncommittedEdgeLabelEntry.getValue();
            if (!this.uncommittedRemovedOutEdgeLabels.containsKey(key)) {
                result.getRight().add(SchemaTable.of(this.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()));
            }
        }
        for (Map.Entry<String, EdgeLabel> uncommittedEdgeLabelEntry : this.uncommittedInEdgeLabels.entrySet()) {
            String key = uncommittedEdgeLabelEntry.getKey();
            EdgeLabel edgeLabel = uncommittedEdgeLabelEntry.getValue();
            if (!this.uncommittedRemovedInEdgeLabels.containsKey(key)) {
                result.getLeft().add(SchemaTable.of(edgeLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()));
            }
        }
        return result;
    }

    Pair<Set<SchemaTable>, Set<SchemaTable>> getUncommittedRemovedSchemaTableForeignKeys() {
        Pair<Set<SchemaTable>, Set<SchemaTable>> result = Pair.of(new HashSet<>(), new HashSet<>());
        for (Map.Entry<String, Pair<EdgeLabel, EdgeRemoveType>> uncommittedEdgeLabelEntry : this.uncommittedRemovedOutEdgeLabels.entrySet()) {
            Pair<EdgeLabel, EdgeRemoveType> edgeLabelPair = uncommittedEdgeLabelEntry.getValue();
            EdgeLabel edgeLabel = edgeLabelPair.getKey();
            result.getRight().add(SchemaTable.of(this.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()));
        }
        for (Map.Entry<String, Pair<EdgeLabel, EdgeRemoveType>> uncommittedEdgeLabelEntry : this.uncommittedRemovedInEdgeLabels.entrySet()) {
            Pair<EdgeLabel, EdgeRemoveType> edgeLabelPair = uncommittedEdgeLabelEntry.getValue();
            EdgeLabel edgeLabel = edgeLabelPair.getKey();
            result.getLeft().add(SchemaTable.of(edgeLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()));
        }
        return result;
    }

    @Override
    void removeProperty(PropertyColumn propertyColumn, boolean preserveData) {
        this.getSchema().getTopology().lock();
        if (!this.uncommittedRemovedProperties.contains(propertyColumn.getName())) {
            this.uncommittedRemovedProperties.add(propertyColumn.getName());
            for (Index index : getIndexes().values()) {
                for (PropertyColumn property : index.getProperties()) {
                    if (property.getName().equals(propertyColumn.getName())) {
                        index.remove(preserveData);
                        break;
                    }
                }
            }
            TopologyManager.removeVertexColumn(this.sqlgGraph, this.schema.getName(), VERTEX_PREFIX + getLabel(), propertyColumn.getName());
            if (!preserveData) {
                removeColumn(this.schema.getName(), VERTEX_PREFIX + getLabel(), propertyColumn.getName());
            }
            this.getSchema().getTopology().fire(propertyColumn, propertyColumn, TopologyChangeAction.DELETE);
        }
    }

    @Override
    void renameProperty(String name, PropertyColumn propertyColumn) {
        this.getSchema().getTopology().lock();
        String oldName = propertyColumn.getName();
        Pair<String, String> namePair = Pair.of(oldName, name);
        if (!this.uncommittedRemovedProperties.contains(name)) {
            this.uncommittedRemovedProperties.add(oldName);
            PropertyColumn copy = new PropertyColumn(this, name, propertyColumn.getPropertyType());
            this.uncommittedProperties.put(name, copy);
            TopologyManager.renamePropertyColumn(this.sqlgGraph, this.schema.getName(), VERTEX_PREFIX + getLabel(), oldName, name);
            renameColumn(this.schema.getName(), VERTEX_PREFIX + getLabel(), oldName, name);
            if (this.getIdentifiers().contains(oldName)) {
                Preconditions.checkState(!this.renamedIdentifiers.contains(namePair), "BUG! renamedIdentifiers may not yet contain '%s'", oldName);
                this.renamedIdentifiers.add(namePair);
            }
            this.getSchema().getTopology().fire(copy, propertyColumn, TopologyChangeAction.UPDATE);
        }
    }

    void removeOutEdge(EdgeLabel lbl) {
        this.uncommittedRemovedOutEdgeLabels.put(lbl.getFullName(), Pair.of(lbl, EdgeRemoveType.EDGE_LABEL));
    }

    void removeInEdge(EdgeLabel lbl) {
        this.uncommittedRemovedInEdgeLabels.put(lbl.getFullName(), Pair.of(lbl, EdgeRemoveType.EDGE_LABEL));
    }

    /**
     * remove a given edge role
     *
     * @param er           the edge role
     * @param preserveData should we keep the SQL data
     */
    void removeEdgeRole(EdgeRole er, boolean dropEdges, boolean preserveData) {
        if (er.getVertexLabel() != this) {
            throw new IllegalStateException("Trying to remove a EdgeRole from a non owner VertexLabel");
        }
        Collection<VertexLabel> ers;
        switch (er.getDirection()) {
            // we don't support both
            case BOTH:
                throw new IllegalStateException("BOTH is not a supported direction");
            case IN:
                ers = er.getEdgeLabel().getInVertexLabels();
                break;
            case OUT:
                ers = er.getEdgeLabel().getOutVertexLabels();
                break;
            default:
                throw new IllegalStateException("Unknown direction!");
        }
        if (!ers.contains(this)) {
            throw new IllegalStateException("Trying to remove a EdgeRole from a non owner VertexLabel");
        }
        // the edge had only this vertex on that direction, remove the edge
        if (ers.size() == 1) {
            er.getEdgeLabel().remove(preserveData);
        } else {
            getSchema().getTopology().lock();
            EdgeLabel edgeLabel = er.getEdgeLabel();
            switch (er.getDirection()) {
                // we don't support both
                case BOTH:
                    throw new IllegalStateException("BOTH is not a supported direction");
                case IN:
                    er.getEdgeLabel().removeInVertexLabel(this, dropEdges, preserveData);
                    this.uncommittedRemovedInEdgeLabels.put(edgeLabel.getFullName(), Pair.of(edgeLabel, EdgeRemoveType.EDGE_ROLE));
                    break;
                case OUT:
                    er.getEdgeLabel().removeOutVertexLabel(this, dropEdges, preserveData);
                    this.uncommittedRemovedOutEdgeLabels.put(edgeLabel.getFullName(), Pair.of(edgeLabel, EdgeRemoveType.EDGE_ROLE));
                    break;
            }

            this.getSchema().getTopology().fire(er, er, TopologyChangeAction.DELETE);
        }
    }

    @Override
    public void remove(boolean preserveData) {
        this.getSchema().removeVertexLabel(this, preserveData);
    }

    /**
     * delete the table
     */
    void delete() {
        String schema = getSchema().getName();
        String tableName = VERTEX_PREFIX + getLabel();

        SqlDialect sqlDialect = this.sqlgGraph.getSqlDialect();
        sqlDialect.assertTableName(tableName);
        StringBuilder sql = new StringBuilder("DROP TABLE IF EXISTS ");
        sql.append(sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(sqlDialect.maybeWrapInQoutes(tableName));
        if (sqlDialect.supportsCascade()) {
            sql.append(" CASCADE");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql.toString());
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

    VertexLabel readOnlyCopy(Schema schema) {
        VertexLabel copy = new VertexLabel(schema, this.label, true);
        for (String property : this.properties.keySet()) {
            copy.properties.put(property, this.properties.get(property).readOnlyCopy(copy));
        }
        return copy;
    }

    @Override
    public void rename(String label) {
        Objects.requireNonNull(label, "Given label must not be null");
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not be prefixed with \"%s\"", VERTEX_PREFIX);
        Preconditions.checkState(!this.isForeignAbstractLabel, "'%s' is a read only foreign table!", label);
        this.getSchema().getTopology().lock();
        VertexLabel renamedVertexLabel = this.getSchema().renameVertexLabel(this, label);
        Map<String, EdgeLabel> outEdgeLabels = getOutEdgeLabels();
        for (String outEdgeLabel : outEdgeLabels.keySet()) {
            EdgeLabel edgeLabel = outEdgeLabels.get(outEdgeLabel);
            edgeLabel.renameOutVertexLabel(renamedVertexLabel, this);
        }
        Map<String, EdgeLabel> inEdgeLabels = getInEdgeLabels();
        for (String inEdgeLabel : inEdgeLabels.keySet()) {
            EdgeLabel edgeLabel = inEdgeLabels.get(inEdgeLabel);
            edgeLabel.renameInVertexLabel(renamedVertexLabel, this);
        }
    }
}
