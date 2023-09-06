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
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.util.SqlgUtil;
import org.umlg.sqlg.util.ThreadLocalMap;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
    final Map<String, EdgeRole> inEdgeRoles = new ConcurrentHashMap<>();
    final Map<String, EdgeRole> outEdgeRoles = new ConcurrentHashMap<>();
    private final Map<String, EdgeRole> uncommittedInEdgeRoles = new ThreadLocalMap<>();
    private final Map<String, EdgeRole> uncommittedOutEdgeRoles = new ThreadLocalMap<>();
    private final Map<String, Pair<EdgeRole, EdgeRemoveType>> uncommittedRemovedInEdgeRoles = new ThreadLocalMap<>();
    private final Map<String, Pair<EdgeRole, EdgeRemoveType>> uncommittedRemovedOutEdgeRoles = new ThreadLocalMap<>();

    private enum EdgeRemoveType {
        EDGE_LABEL,
        EDGE_ROLE
    }

    static VertexLabel createSqlgSchemaVertexLabel(Schema schema, String label, Map<String, PropertyDefinition> columns) {
        Preconditions.checkArgument(schema.isSqlgSchema(), "createSqlgSchemaVertexLabel may only be called for \"%s\"", SQLG_SCHEMA);
        VertexLabel vertexLabel = new VertexLabel(schema, label);
        //Add the properties directly. As they are pre-created do not add them to uncommittedProperties.
        for (Map.Entry<String, PropertyDefinition> propertyEntry : columns.entrySet()) {
            PropertyColumn property = new PropertyColumn(vertexLabel, propertyEntry.getKey(), propertyEntry.getValue());
            vertexLabel.properties.put(propertyEntry.getKey(), property);
        }
        return vertexLabel;
    }

    static VertexLabel createVertexLabel(SqlgGraph sqlgGraph, Schema schema, String label, Map<String, PropertyDefinition> columns, ListOrderedSet<String> identifiers) {
        Preconditions.checkArgument(!schema.isSqlgSchema(), "createVertexLabel may not be called for \"%s\"", SQLG_SCHEMA);
        VertexLabel vertexLabel = new VertexLabel(schema, label, columns, identifiers);
        vertexLabel.createVertexLabelOnDb(columns, identifiers);
        TopologyManager.addVertexLabel(sqlgGraph, schema.getName(), label, columns, identifiers);
        vertexLabel.committed = false;
        return vertexLabel;
    }

    static VertexLabel renameVertexLabel(
            SqlgGraph sqlgGraph,
            Schema schema,
            String oldLabel,
            String newLabel,
            Map<String, PropertyDefinition> columns,
            ListOrderedSet<String> identifiers) {

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
            Map<String, PropertyDefinition> columns,
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
    private VertexLabel(Schema schema, String label, Map<String, PropertyDefinition> properties, ListOrderedSet<String> identifiers) {
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
    VertexLabel(Schema schema, String label, Map<String, PropertyDefinition> properties, ListOrderedSet<String> identifiers, PartitionType partitionType, String partitionExpression) {
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
        Map<String, EdgeLabel> result = new HashMap<>(this.inEdgeRoles.size());
        for (Map.Entry<String, EdgeRole> stringEdgeRoleEntry : getInEdgeRoles().entrySet()) {
            result.put(stringEdgeRoleEntry.getKey(), stringEdgeRoleEntry.getValue().getEdgeLabel());
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<String, EdgeRole> getInEdgeRoles() {
        if (this.schema.getTopology().isSchemaChanged()) {
            Map<String, EdgeRole> result = new HashMap<>(this.inEdgeRoles);
            result.putAll(this.uncommittedInEdgeRoles);
            for (String e : this.uncommittedRemovedInEdgeRoles.keySet()) {
                result.remove(e);
            }
            return Collections.unmodifiableMap(result);
        } else {
            return Collections.unmodifiableMap(this.inEdgeRoles);
        }
    }

    public Map<String, EdgeLabel> getOutEdgeLabels() {
        Map<String, EdgeLabel> result = new HashMap<>(this.outEdgeRoles.size());
        for (Map.Entry<String, EdgeRole> stringEdgeRoleEntry : getOutEdgeRoles().entrySet()) {
            result.put(stringEdgeRoleEntry.getKey(), stringEdgeRoleEntry.getValue().getEdgeLabel());
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<String, EdgeRole> getOutEdgeRoles() {
        Map<String, EdgeRole> result = new HashMap<>(this.outEdgeRoles);
        if (this.schema.getTopology().isSchemaChanged()) {
            result.putAll(this.uncommittedOutEdgeRoles);
            for (String e : this.uncommittedRemovedOutEdgeRoles.keySet()) {
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
        Map<String, EdgeLabel> result = new HashMap<>();
        for (Map.Entry<String, EdgeRole> stringEdgeRoleEntry : getUncommittedOutEdgeRoles().entrySet()) {
            String key = stringEdgeRoleEntry.getKey();
            EdgeRole edgeRole = stringEdgeRoleEntry.getValue();
            EdgeLabel edgeLabel = edgeRole.getEdgeLabel();
            result.put(key, edgeLabel);
        }
//        for (EdgeRole outEdgeRole : this.outEdgeRoles.values()) {
//            Map<String, PropertyColumn> propertyMap = outEdgeRole.getEdgeLabel().getUncommittedPropertyTypeMap();
//            if (!propertyMap.isEmpty() || !outEdgeRole.getEdgeLabel().getUncommittedRemovedProperties().isEmpty()) {
//                result.put(outEdgeRole.getEdgeLabel().getLabel(), outEdgeRole.getEdgeLabel());
//            }
//        }
        return Collections.unmodifiableMap(result);
    }

    Map<String, EdgeRole> getUncommittedOutEdgeRoles() {
        if (this.schema.getTopology().isSchemaChanged()) {
            Map<String, EdgeRole> result = new HashMap<>(this.uncommittedOutEdgeRoles);
            return Collections.unmodifiableMap(result);
        } else {
            return Collections.emptyMap();
        }
    }

    void addToUncommittedInEdgeRoles(Schema schema, EdgeRole edgeRole) {
        this.uncommittedInEdgeRoles.put(schema.getName() + "." + edgeRole.getEdgeLabel().getLabel(), edgeRole);
    }

    void addToUncommittedOutEdgeRoles(Schema schema, EdgeRole edgeRole) {
        this.uncommittedOutEdgeRoles.put(schema.getName() + "." + edgeRole.getEdgeLabel().getLabel(), edgeRole);
    }

    /**
     * Called from {@link Topology#Topology(SqlgGraph)}s constructor to preload the sqlg_schema.
     * This can only be called for sqlg_schema.
     *
     * @param edgeLabelName The edge's label.
     * @param inVertexLabel The edge's in {@link VertexLabel}. 'this' is the out {@link VertexLabel}.
     * @param properties    A map of the edge's properties.
     */
    void loadSqlgSchemaEdgeLabel(String edgeLabelName, VertexLabel inVertexLabel, Map<String, PropertyDefinition> properties) {
        Preconditions.checkState(this.schema.isSqlgSchema(), "loadSqlgSchemaEdgeLabel must be called for \"%s\" found \"%s\"", SQLG_SCHEMA, this.schema.getName());
        EdgeLabel edgeLabel = EdgeLabel.loadSqlgSchemaEdgeLabel(edgeLabelName, this, inVertexLabel, properties);
        this.outEdgeRoles.put(this.schema.getName() + "." + edgeLabel.getLabel(), new EdgeRole(this, edgeLabel, Direction.OUT, true, Multiplicity.of(0, -1)));
        inVertexLabel.inEdgeRoles.put(this.schema.getName() + "." + edgeLabel.getLabel(), new EdgeRole(this, edgeLabel, Direction.IN, true, Multiplicity.of(0, -1)));
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

    public EdgeLabel ensureEdgeLabelExist(final String edgeLabelName, final VertexLabel inVertexLabel, EdgeDefinition edgeDefinition) {
        return this.getSchema().ensureEdgeLabelExist(edgeLabelName, this, inVertexLabel, Collections.emptyMap(), edgeDefinition);
    }

    public EdgeLabel ensureEdgeLabelExist(final String edgeLabelName, final VertexLabel inVertexLabel, EdgeDefinition edgeDefinition, Map<String, PropertyDefinition> properties) {
        return this.getSchema().ensureEdgeLabelExist(edgeLabelName, this, inVertexLabel, properties, edgeDefinition);
    }

    /**
     * Ensures that the {@link EdgeLabel} exists. It will be created if it does not exist.
     * "this" is the out {@link VertexLabel} and inVertexLabel is the inVertexLabel
     * This method is equivalent to {@link Schema#ensureEdgeLabelExist(String, VertexLabel, VertexLabel, Map)}
     *
     * @param edgeLabelName The EdgeLabel's label's name.
     * @param inVertexLabel The edge's in VertexLabel.
     * @param properties    The EdgeLabel's properties
     * @return The EdgeLabel
     */
    public EdgeLabel ensureEdgeLabelExist(final String edgeLabelName, final VertexLabel inVertexLabel, Map<String, PropertyDefinition> properties) {
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
    public EdgeLabel ensureEdgeLabelExist(final String edgeLabelName, final VertexLabel inVertexLabel, Map<String, PropertyDefinition> properties, ListOrderedSet<String> identifiers) {
        return this.getSchema().ensureEdgeLabelExist(edgeLabelName, this, inVertexLabel, properties, identifiers);
    }

    public EdgeLabel ensurePartitionedEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> properties,
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
            final Map<String, PropertyDefinition> properties,
            final ListOrderedSet<String> identifiers,
            final PartitionType partitionType,
            final String partitionExpression,
            boolean isForeignKeyPartition,
            EdgeDefinition edgeDefinition) {

        EdgeLabel edgeLabel = EdgeLabel.createPartitionedEdgeLabel(
                edgeLabelName,
                this,
                inVertexLabel,
                properties,
                identifiers,
                partitionType,
                partitionExpression,
                isForeignKeyPartition,
                edgeDefinition);
        if (this.schema.isSqlgSchema()) {
            this.outEdgeRoles.put(
                    this.schema.getName() + "." + edgeLabel.getLabel(),
                    new EdgeRole(this, edgeLabel, Direction.OUT, true, edgeDefinition.outMultiplicity())
            );
            inVertexLabel.inEdgeRoles.put(
                    this.schema.getName() + "." + edgeLabel.getLabel(),
                    new EdgeRole(this, edgeLabel, Direction.IN, true, edgeDefinition.inMultiplicity())
            );
        } else {
            this.uncommittedOutEdgeRoles.put(
                    this.schema.getName() + "." + edgeLabel.getLabel(),
                    new EdgeRole(this, edgeLabel, Direction.OUT, true, edgeDefinition.outMultiplicity())
            );
            inVertexLabel.uncommittedInEdgeRoles.put(
                    this.schema.getName() + "." + edgeLabel.getLabel(),
                    new EdgeRole(this, edgeLabel, Direction.IN, true, edgeDefinition.inMultiplicity())
            );
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
     * @param identifiers   The edge's user defined identifiers.
     * @return The new EdgeLabel.
     */
    EdgeLabel addEdgeLabel(
            String edgeLabelName,
            VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> properties,
            ListOrderedSet<String> identifiers,
            EdgeDefinition edgeDefinition) {

        EdgeLabel edgeLabel = EdgeLabel.createEdgeLabel(edgeLabelName, this, inVertexLabel, properties, identifiers, edgeDefinition);
        if (this.schema.isSqlgSchema()) {
            EdgeRole outEdgeRole = new EdgeRole(this, edgeLabel, Direction.OUT, true, edgeDefinition.outMultiplicity());
            this.outEdgeRoles.put(
                    this.schema.getName() + "." + edgeLabel.getLabel(),
                    outEdgeRole
            );
            EdgeRole inEdgeRole = new EdgeRole(inVertexLabel, edgeLabel, Direction.IN, true, edgeDefinition.inMultiplicity());
            inVertexLabel.inEdgeRoles.put(
                    this.schema.getName() + "." + edgeLabel.getLabel(),
                    inEdgeRole
            );
        } else {
            EdgeRole outEdgeRole = new EdgeRole(this, edgeLabel, Direction.OUT, false, edgeDefinition.outMultiplicity());
            this.uncommittedOutEdgeRoles.put(
                    this.schema.getName() + "." + edgeLabel.getLabel(),
                    outEdgeRole
            );
            EdgeRole inEdgeRole = new EdgeRole(inVertexLabel, edgeLabel, Direction.IN, false, edgeDefinition.inMultiplicity());
            inVertexLabel.uncommittedInEdgeRoles.put(
                    this.schema.getName() + "." + edgeLabel.getLabel(),
                    inEdgeRole
            );
        }
        return edgeLabel;
    }

    public void ensurePropertiesExist(Map<String, PropertyDefinition> columns) {
        for (Map.Entry<String, PropertyDefinition> column : columns.entrySet()) {
            PropertyDefinition incomingPropertyDefinition = column.getValue();
            PropertyColumn propertyColumn = this.properties.get(column.getKey());
            if (propertyColumn == null) {
                Preconditions.checkState(!this.isForeignAbstractLabel, "'%s' is a read only foreign VertexLabel!", this.label);
                Preconditions.checkState(!this.schema.isSqlgSchema(), "schema may not be %s", SQLG_SCHEMA);
                this.sqlgGraph.getSqlDialect().validateColumnName(column.getKey());
                propertyColumn = this.uncommittedProperties.get(column.getKey());
                if (propertyColumn == null) {
                    this.schema.getTopology().startSchemaChange(
                            String.format("VertexLabel '%s' ensurePropertiesExist with '%s'", getFullName(), columns.keySet().stream().reduce((a, b) -> a + "," + b).orElse(""))
                    );
                    if (getProperty(column.getKey()).isEmpty()) {
                        TopologyManager.addVertexColumn(this.sqlgGraph, this.schema.getName(), VERTEX_PREFIX + getLabel(), column);
                        addColumn(this.schema.getName(), VERTEX_PREFIX + getLabel(), ImmutablePair.of(column.getKey(), column.getValue()));
                        propertyColumn = new PropertyColumn(this, column.getKey(), column.getValue());
                        propertyColumn.setCommitted(false);
                        this.uncommittedProperties.put(column.getKey(), propertyColumn);
                        this.getSchema().getTopology().fire(propertyColumn, null, TopologyChangeAction.CREATE, true);
                    }
                } else {
                    SqlgUtil.validateIncomingPropertyType(
                            getFullName() + "." + column.getKey(),
                            incomingPropertyDefinition,
                            getFullName() + "." + propertyColumn.getName(),
                            propertyColumn.getPropertyDefinition()
                    );
                    //Set the proper definition in the map;
                    columns.put(column.getKey(), propertyColumn.getPropertyDefinition());
                }
            } else {
                if (!sqlgGraph.tx().isInStreamingBatchMode()) {
                    //Postgres will check if the types.
                    //For streaming the data may come csv in which case the csv must specify the correct literal.
                    SqlgUtil.validateIncomingPropertyType(
                            getFullName() + "." + column.getKey(),
                            incomingPropertyDefinition,
                            getFullName() + "." + propertyColumn.getName(),
                            propertyColumn.getPropertyDefinition()
                    );
                    //Set the proper definition in the map;
                    columns.put(column.getKey(), propertyColumn.getPropertyDefinition());
                }
            }
        }
    }

    public void ensureDistributed(int shardCount, PropertyColumn distributionPropertyColumn) {
        ensureDistributed(shardCount, distributionPropertyColumn, null);
    }

    private void createVertexLabelOnDb(Map<String, PropertyDefinition> columns, ListOrderedSet<String> identifiers) {
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
        String sql = this.sqlgGraph.getSqlDialect().renameTable(
                this.schema.getName(),
                VERTEX_PREFIX + oldLabel,
                VERTEX_PREFIX + newLabel
        );
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql);
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createPartitionedVertexLabelOnDb(Map<String, PropertyDefinition> columns, ListOrderedSet<String> identifiers, boolean addPrimaryKeyConstraint) {
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
        for (EdgeRole inEdgeRole : this.inEdgeRoles.values()) {
            inSchemaTables.add(SchemaTable.of(inEdgeRole.getEdgeLabel().getSchema().getName(), EDGE_PREFIX + inEdgeRole.getEdgeLabel().getLabel()));
        }
        for (EdgeRole outEdgeRole : this.outEdgeRoles.values()) {
            outSchemaTables.add(SchemaTable.of(outEdgeRole.getEdgeLabel().getSchema().getName(), EDGE_PREFIX + outEdgeRole.getEdgeLabel().getLabel()));
        }
        if (this.schema.getTopology().isSchemaChanged()) {
            for (EdgeRole inEdgeRole : this.uncommittedInEdgeRoles.values()) {
                inSchemaTables.add(SchemaTable.of(inEdgeRole.getEdgeLabel().getSchema().getName(), EDGE_PREFIX + inEdgeRole.getEdgeLabel().getLabel()));
            }
            for (EdgeRole outEdgeRole : this.uncommittedOutEdgeRoles.values()) {
                outSchemaTables.add(SchemaTable.of(outEdgeRole.getEdgeLabel().getSchema().getName(), EDGE_PREFIX + outEdgeRole.getEdgeLabel().getLabel()));
            }
        }
        return Pair.of(inSchemaTables, outSchemaTables);
    }

    void afterCommit() {
        Preconditions.checkState(this.schema.getTopology().isSchemaChanged(), "VertexLabel.afterCommit must have schemaChanged = true");
        super.afterCommit();
        Iterator<Map.Entry<String, EdgeRole>> edgeRoleEntryIter = this.uncommittedOutEdgeRoles.entrySet().iterator();
        while (edgeRoleEntryIter.hasNext()) {
            Map.Entry<String, EdgeRole> edgeRoleEntry = edgeRoleEntryIter.next();
            String edgeLabelName = edgeRoleEntry.getKey();
            EdgeRole edgeRole = edgeRoleEntry.getValue();
            this.outEdgeRoles.put(edgeLabelName, edgeRole);
            edgeRole.getEdgeLabel().afterCommit();
            this.getSchema().addToAllEdgeCache(edgeRole.getEdgeLabel());
            edgeRoleEntryIter.remove();
        }
        edgeRoleEntryIter = this.uncommittedInEdgeRoles.entrySet().iterator();
        while (edgeRoleEntryIter.hasNext()) {
            Map.Entry<String, EdgeRole> edgeRoleEntry = edgeRoleEntryIter.next();
            String edgeLabelName = edgeRoleEntry.getKey();
            EdgeRole edgeRole = edgeRoleEntry.getValue();
            this.inEdgeRoles.put(edgeLabelName, edgeRole);
            edgeRole.getEdgeLabel().afterCommit();
            edgeRoleEntryIter.remove();

        }
        for (Iterator<String> it = this.uncommittedRemovedOutEdgeRoles.keySet().iterator(); it.hasNext(); ) {
            String s = it.next();
            EdgeRole edgeRole = this.outEdgeRoles.remove(s);
            if (edgeRole != null) {
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
                        edgeRole.getEdgeLabel().getSchema().getName() + "." + EDGE_PREFIX + edgeRole.getEdgeLabel().getLabel(),
                        foreignKey);
                this.getSchema().getTopology().removeOutForeignKeysFromVertexLabel(this, edgeRole.getEdgeLabel());

            }
            it.remove();
        }

        for (Iterator<String> it = this.uncommittedRemovedInEdgeRoles.keySet().iterator(); it.hasNext(); ) {
            String s = it.next();
            EdgeRole edgeRole = this.inEdgeRoles.remove(s);
            if (edgeRole != null) {
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
                        edgeRole.getEdgeLabel().getSchema().getName() + "." + EDGE_PREFIX + edgeRole.getEdgeLabel().getLabel(),
                        foreignKey);
                this.getSchema().getTopology().removeInForeignKeysFromVertexLabel(this, edgeRole.getEdgeLabel());
            }
            it.remove();
        }

        for (EdgeRole edgeRole : this.outEdgeRoles.values()) {
            edgeRole.getEdgeLabel().afterCommit();
        }
        for (EdgeRole edgeRole : this.inEdgeRoles.values()) {
            edgeRole.getEdgeLabel().afterCommit();
        }
    }

    void afterRollbackForInEdges() {
        Preconditions.checkState(this.schema.getTopology().isSchemaChanged(), "VertexLabel.afterRollback must have schemaChanged = true");
        super.afterRollback();
        for (Iterator<EdgeRole> it = this.uncommittedInEdgeRoles.values().iterator(); it.hasNext(); ) {
            EdgeRole edgeRole = it.next();
            edgeRole.getEdgeLabel().afterRollbackInEdges(this);
            it.remove();
        }
        for (EdgeRole edgeRole : this.inEdgeRoles.values()) {
            edgeRole.getEdgeLabel().afterRollbackInEdges(this);
            this.uncommittedRemovedInEdgeRoles.remove(edgeRole.getEdgeLabel().getFullName());
        }
    }

    void afterRollbackForOutEdges() {
        Preconditions.checkState(this.schema.getTopology().isSchemaChanged(), "VertexLabel.afterRollback must have schemaChanged = true");
        super.afterRollback();
        for (Iterator<EdgeRole> it = this.uncommittedOutEdgeRoles.values().iterator(); it.hasNext(); ) {
            EdgeRole edgeRole = it.next();
            it.remove();
            //It is important to first remove the EdgeLabel from the iterator as the EdgeLabel's outVertex is still
            // present and its needed for the hashCode method which is invoked during the it.remove()
            edgeRole.getEdgeLabel().afterRollbackOutEdges(this);
        }
        for (EdgeRole edgeRole : this.outEdgeRoles.values()) {
            edgeRole.getEdgeLabel().afterRollbackOutEdges(this);
            this.uncommittedRemovedOutEdgeRoles.remove(edgeRole.getEdgeLabel().getFullName());
        }
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    void addToOutEdgeRoles(String schema, EdgeRole edgeRole) {
        edgeRole.getEdgeLabel().addToOutEdgeRole(edgeRole);
        this.outEdgeRoles.put(schema + "." + edgeRole.getEdgeLabel().getLabel(), edgeRole);
    }

    void addToInEdgeRoles(EdgeRole edgeRole) {
        edgeRole.getEdgeLabel().addToInEdgeRole(edgeRole);
        this.inEdgeRoles.put(edgeRole.getEdgeLabel().getSchema().getName() + "." + edgeRole.getEdgeLabel().getLabel(), edgeRole);
    }

    @Override
    protected JsonNode toJson() {
        ObjectNode vertexLabelNode = Topology.OBJECT_MAPPER.createObjectNode();
        vertexLabelNode.put("schema", getSchema().getName());
        vertexLabelNode.put("label", getLabel());
        vertexLabelNode.set("properties", super.toJson());

        ArrayNode outEdgeLabelsArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
        List<EdgeRole> edgeRoles = new ArrayList<>(this.outEdgeRoles.values());
        edgeRoles.sort(Comparator.comparing(o -> o.getEdgeLabel().getName()));
        for (EdgeRole edgeRole : edgeRoles) {
            outEdgeLabelsArrayNode.add(edgeRole.getEdgeLabel().toJson());
        }
        vertexLabelNode.set("outEdgeLabels", outEdgeLabelsArrayNode);

        ArrayNode inEdgeLabelsArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
        edgeRoles = new ArrayList<>(this.inEdgeRoles.values());
        edgeRoles.sort(Comparator.comparing(o -> o.getEdgeLabel().getName()));
        for (EdgeRole edgeRole : edgeRoles) {
            inEdgeLabelsArrayNode.add(edgeRole.getEdgeLabel().toJson());
        }
        vertexLabelNode.set("inEdgeLabels", inEdgeLabelsArrayNode);

        ArrayNode partitionArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
        for (Partition partition : this.getPartitions().values()) {
            Optional<ObjectNode> json = partition.toJson();
            json.ifPresent(partitionArrayNode::add);
        }
        vertexLabelNode.set("partitions", partitionArrayNode);

        if (this.schema.getTopology().isSchemaChanged()) {
            ArrayNode uncommittedOutEdgeRolesArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            edgeRoles = new ArrayList<>(this.uncommittedOutEdgeRoles.values());
            edgeRoles.sort(Comparator.comparing(o -> o.getEdgeLabel().getName()));
            for (EdgeRole edgeRole : edgeRoles) {
                uncommittedOutEdgeRolesArrayNode.add(edgeRole.getEdgeLabel().toJson());
            }
            vertexLabelNode.set("uncommittedOutEdgeRoles", uncommittedOutEdgeRolesArrayNode);

            ArrayNode uncommittedInEdgeLabelsArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            edgeRoles = new ArrayList<>(this.uncommittedInEdgeRoles.values());
            edgeRoles.sort(Comparator.comparing(o -> o.getEdgeLabel().getName()));
            for (EdgeRole edgeRole : edgeRoles) {
                uncommittedInEdgeLabelsArrayNode.add(edgeRole.getEdgeLabel().toJson());
            }
            vertexLabelNode.set("uncommittedInEdgeRoles", uncommittedInEdgeLabelsArrayNode);
        }
        return vertexLabelNode;
    }

    protected Optional<JsonNode> toNotifyJson() {
        ObjectNode vertexLabelNode = Topology.OBJECT_MAPPER.createObjectNode();
        vertexLabelNode.put("label", getLabel());
        vertexLabelNode.put("partitionType", this.partitionType.name());
        vertexLabelNode.put("partitionExpression", this.partitionExpression);

        Optional<JsonNode> abstractLabelNode = super.toNotifyJson();
        if (abstractLabelNode.isPresent()) {
            vertexLabelNode.set("uncommittedProperties", abstractLabelNode.get().get("uncommittedProperties"));
            vertexLabelNode.set("uncommittedUpdatedProperties", abstractLabelNode.get().get("uncommittedUpdatedProperties"));
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

        if (this.schema.getTopology().isSchemaChanged() && !this.uncommittedOutEdgeRoles.isEmpty()) {
            ArrayNode uncommittedOutEdgeRolesArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            for (EdgeRole edgeRole : this.uncommittedOutEdgeRoles.values()) {
                Optional<JsonNode> edgeRoleJsonOptional = edgeRole.toNotifyJson();
                Preconditions.checkState(edgeRoleJsonOptional.isPresent(), "There must be data to notify as the edgeLabel itself is uncommitted");
                uncommittedOutEdgeRolesArrayNode.add(edgeRoleJsonOptional.get());
            }
            vertexLabelNode.set("uncommittedOutEdgeRoles", uncommittedOutEdgeRolesArrayNode);
        }

        if (this.schema.getTopology().isSchemaChanged() && !this.uncommittedRemovedOutEdgeRoles.isEmpty()) {
            ArrayNode uncommittedOutEdgeRolesArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            for (String edgeRole : this.uncommittedRemovedOutEdgeRoles.keySet()) {
                ObjectNode edgeRemove = Topology.OBJECT_MAPPER.createObjectNode();
                edgeRemove.put("label", edgeRole);
                edgeRemove.put("type", this.uncommittedRemovedOutEdgeRoles.get(edgeRole).getRight().name());
                uncommittedOutEdgeRolesArrayNode.add(edgeRemove);
            }
            vertexLabelNode.set("uncommittedRemovedOutEdgeRoles", uncommittedOutEdgeRolesArrayNode);
        }

        if (this.schema.getTopology().isSchemaChanged() && !this.uncommittedInEdgeRoles.isEmpty()) {
            ArrayNode uncommittedInEdgeRolesArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            for (EdgeRole edgeRole : this.uncommittedInEdgeRoles.values()) {
                Optional<JsonNode> edgeRoleJsonNodeOptional = edgeRole.toNotifyJson();
                Preconditions.checkState(edgeRoleJsonNodeOptional.isPresent(), "There must be data to notify as the edgeLabel itself is uncommitted");
                uncommittedInEdgeRolesArrayNode.add(edgeRoleJsonNodeOptional.get());
            }
            vertexLabelNode.set("uncommittedInEdgeRoles", uncommittedInEdgeRolesArrayNode);
        }

        if (this.schema.getTopology().isSchemaChanged() && !this.uncommittedRemovedInEdgeRoles.isEmpty()) {
            ArrayNode uncommittedInEdgeRolesArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            for (String edgeRole : this.uncommittedRemovedInEdgeRoles.keySet()) {
                ObjectNode edgeRemove = Topology.OBJECT_MAPPER.createObjectNode();
                edgeRemove.put("label", edgeRole);
                edgeRemove.put("type", this.uncommittedRemovedInEdgeRoles.get(edgeRole).getRight().name());
                uncommittedInEdgeRolesArrayNode.add(edgeRemove);
            }
            vertexLabelNode.set("uncommittedRemovedInEdgeRoles", uncommittedInEdgeRolesArrayNode);
        }

        return Optional.of(vertexLabelNode);
    }

    /**
     * @param vertexLabelJson The VertexLabel's notification json
     * @param fire            should we fire topology events
     */
    void fromNotifyJsonOutEdge(JsonNode vertexLabelJson, boolean fire) {
        super.fromPropertyNotifyJson(vertexLabelJson, fire);
        ArrayNode uncommittedRemoveOutEdgeRoles = (ArrayNode) vertexLabelJson.get("uncommittedRemovedOutEdgeRoles");
        if (uncommittedRemoveOutEdgeRoles != null) {
            for (JsonNode n : uncommittedRemoveOutEdgeRoles) {
                EdgeRole edgeRole = this.outEdgeRoles.remove(n.get("label").asText());
                if (edgeRole != null) {
                    EdgeRemoveType edgeRemoveType = EdgeRemoveType.valueOf(n.get("type").asText());
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
                            edgeRole.getEdgeLabel().getSchema().getName() + "." + EDGE_PREFIX + edgeRole.getEdgeLabel().getLabel(),
                            foreignKey
                    );
                    this.getSchema().getTopology().removeOutForeignKeysFromVertexLabel(this, edgeRole.getEdgeLabel());

                    switch (edgeRemoveType) {
                        case EDGE_LABEL -> {
                            //noop, uncommittedRemovedEdgeLabels section will take care of the EdgeLabel
                        }
                        case EDGE_ROLE -> {
                            edgeRole.getEdgeLabel().outEdgeRoles.remove(edgeRole);
                            this.getSchema().getTopology().fire(edgeRole, edgeRole, TopologyChangeAction.DELETE, false);
                        }
                    }
                }
            }
        }
        ArrayNode uncommittedOutEdgeRoles = (ArrayNode) vertexLabelJson.get("uncommittedOutEdgeRoles");
        if (uncommittedOutEdgeRoles != null) {
            for (JsonNode uncommittedOutEdgeRole : uncommittedOutEdgeRoles) {

                String edgeLabelName = uncommittedOutEdgeRole.get("edgeLabelName").asText();
                Direction direction = Direction.valueOf(uncommittedOutEdgeRole.get("direction").asText());
                Preconditions.checkState(direction == Direction.OUT);
                Multiplicity multiplicity = Multiplicity.fromNotifyJson(uncommittedOutEdgeRole.get("multiplicity"));

                Optional<EdgeLabel> edgeLabelOptional = this.schema.getEdgeLabel(edgeLabelName);
                Preconditions.checkState(edgeLabelOptional.isPresent(), "Failed to find EdgeLabel '%s'", edgeLabelName);
                EdgeLabel edgeLabel = edgeLabelOptional.get();
                EdgeRole edgeRole = new EdgeRole(this, edgeLabel, Direction.OUT, true, multiplicity);
                edgeLabel.addToOutEdgeRole(edgeRole);
                //TODO get EdgeDefinition from json
                this.outEdgeRoles.put(this.schema.getName() + "." + edgeLabel.getLabel(), edgeRole);
                //Babysit the cache
                this.getSchema().getTopology().addToAllTables(getSchema().getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel.getPropertyDefinitionMap());
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
                this.getSchema().getTopology().fire(edgeRole, null, TopologyChangeAction.CREATE, false);
            }
        }
    }

    void fromNotifyJsonInEdge(JsonNode vertexLabelJson) {
        for (String s : Arrays.asList("uncommittedInEdgeRoles", "inEdgeLabels")) {
            ArrayNode uncommittedInEdgeRoles = (ArrayNode) vertexLabelJson.get(s);
            if (uncommittedInEdgeRoles != null) {
                for (JsonNode uncommittedInEdgeRole : uncommittedInEdgeRoles) {

                    Direction direction = Direction.valueOf(uncommittedInEdgeRole.get("direction").asText());
                    Preconditions.checkState(direction == Direction.IN);
                    Multiplicity multiplicity = Multiplicity.fromNotifyJson(uncommittedInEdgeRole.get("multiplicity"));
                    String schemaName = uncommittedInEdgeRole.get("schemaName").asText();
                    String edgeLabelName = uncommittedInEdgeRole.get("edgeLabelName").asText();

                    Optional<Schema> schemaOptional = getSchema().getTopology().getSchema(schemaName);
                    Preconditions.checkState(schemaOptional.isPresent(), "Schema %s must be present", schemaName);
                    Optional<EdgeLabel> edgeLabelOptional = schemaOptional.get().getEdgeLabel(edgeLabelName);
                    //The edgeLabel could have been deleted
                    if (edgeLabelOptional.isPresent()) {
                        EdgeLabel edgeLabel = edgeLabelOptional.get();
                        EdgeRole edgeRole = new EdgeRole(this, edgeLabel, Direction.IN, true, multiplicity);
                        edgeLabel.addToInEdgeRole(edgeRole);
                        this.inEdgeRoles.put(schemaName + "." + edgeLabel.getLabel(), edgeRole);
//                        edgeLabel.fromPropertyNotifyJson(uncommittedInEdgeLabel, false);
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
        ArrayNode uncommittedRemoveInEdgeRoles = (ArrayNode) vertexLabelJson.get("uncommittedRemovedInEdgeRoles");
        if (uncommittedRemoveInEdgeRoles != null) {
            for (JsonNode n : uncommittedRemoveInEdgeRoles) {
                EdgeRole edgeRole = this.inEdgeRoles.remove(n.get("label").asText());
                if (edgeRole != null) {
                    EdgeRemoveType edgeRemoveType = EdgeRemoveType.valueOf(n.get("type").asText());
                    if (edgeRole.getEdgeLabel().isValid()) {
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
                                edgeRole.getEdgeLabel().getSchema().getName() + "." + EDGE_PREFIX + edgeRole.getEdgeLabel().getLabel(),
                                foreignKey
                        );
                        this.getSchema().getTopology().removeInForeignKeysFromVertexLabel(this, edgeRole.getEdgeLabel());
                    }

                    switch (edgeRemoveType) {
                        case EDGE_LABEL -> {
                            //noop, uncommittedRemovedEdgeLabels section will take care of the EdgeLabel
                        }
                        case EDGE_ROLE -> {
                            edgeRole.getEdgeLabel().inEdgeRoles.remove(edgeRole);
                            this.getSchema().getTopology().fire(edgeRole, edgeRole, TopologyChangeAction.DELETE, false);
                        }
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
        if (!(other instanceof VertexLabel otherVertexLabel)) {
            return false;
        }
        return this.schema.equals(otherVertexLabel.getSchema()) && super.equals(otherVertexLabel);
    }

//    boolean deepEquals(VertexLabel other) {
//        Preconditions.checkState(this.equals(other), "deepEquals is only called after equals has succeeded");
//        if (!this.outEdgeRoles.equals(other.outEdgeRoles)) {
//            return false;
//        } else {
//            for (EdgeLabel outEdgeLabel : this.outEdgeRoles.values()) {
//                for (EdgeLabel otherOutEdgeLabel : other.outEdgeRoles.values()) {
//                    if (outEdgeLabel.equals(otherOutEdgeLabel)) {
//                        if (!outEdgeLabel.deepEquals(otherOutEdgeLabel)) {
//                            return false;
//                        }
//                    }
//                }
//            }
//            return true;
//        }
//    }

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
        for (Map.Entry<String, EdgeRole> uncommittedEdgeRoleEntry : this.uncommittedOutEdgeRoles.entrySet()) {
            String key = uncommittedEdgeRoleEntry.getKey();
            EdgeRole edgeRole = uncommittedEdgeRoleEntry.getValue();
            if (!this.uncommittedRemovedOutEdgeRoles.containsKey(key)) {
                result.getRight().add(SchemaTable.of(this.getSchema().getName(), EDGE_PREFIX + edgeRole.getEdgeLabel().getLabel()));
            }
        }
        for (Map.Entry<String, EdgeRole> uncommittedEdgeRoleEntry : this.uncommittedInEdgeRoles.entrySet()) {
            String key = uncommittedEdgeRoleEntry.getKey();
            EdgeRole edgeRole = uncommittedEdgeRoleEntry.getValue();
            if (!this.uncommittedRemovedInEdgeRoles.containsKey(key)) {
                result.getLeft().add(SchemaTable.of(edgeRole.getEdgeLabel().getSchema().getName(), EDGE_PREFIX + edgeRole.getEdgeLabel().getLabel()));
            }
        }
        return result;
    }

    Pair<Set<SchemaTable>, Set<SchemaTable>> getUncommittedRemovedSchemaTableForeignKeys() {
        Pair<Set<SchemaTable>, Set<SchemaTable>> result = Pair.of(new HashSet<>(), new HashSet<>());
        for (Map.Entry<String, Pair<EdgeRole, EdgeRemoveType>> uncommittedEdgeRoleEntry : this.uncommittedRemovedOutEdgeRoles.entrySet()) {
            Pair<EdgeRole, EdgeRemoveType> edgeRolePair = uncommittedEdgeRoleEntry.getValue();
            EdgeRole edgeRole = edgeRolePair.getKey();
            result.getRight().add(SchemaTable.of(this.getSchema().getName(), EDGE_PREFIX + edgeRole.getEdgeLabel().getLabel()));
        }
        for (Map.Entry<String, Pair<EdgeRole, EdgeRemoveType>> uncommittedEdgeRoleEntry : this.uncommittedRemovedInEdgeRoles.entrySet()) {
            Pair<EdgeRole, EdgeRemoveType> edgeRolePair = uncommittedEdgeRoleEntry.getValue();
            EdgeRole edgeRole = edgeRolePair.getKey();
            result.getLeft().add(SchemaTable.of(edgeRole.getEdgeLabel().getSchema().getName(), EDGE_PREFIX + edgeRole.getEdgeLabel().getLabel()));
        }
        return result;
    }

    @Override
    void removeProperty(PropertyColumn propertyColumn, boolean preserveData) {
        this.getSchema().getTopology().startSchemaChange(
                String.format("VertexLabel '%s' removeProperty with '%s'", getFullName(), propertyColumn.getName())
        );
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
            this.getSchema().getTopology().fire(propertyColumn, propertyColumn, TopologyChangeAction.DELETE, true);
        }
    }

    @Override
    void updatePropertyDefinition(PropertyColumn propertyColumn, PropertyDefinition propertyDefinition) {
        Preconditions.checkState(!sqlgGraph.getSqlDialect().isMariaDb(), "updatePropertyDefinition is not supported for mariadb. Dropping constraints is complicated so will only do if if requested.");
        PropertyDefinition currentPropertyDefinition = propertyColumn.getPropertyDefinition();
        Preconditions.checkState(currentPropertyDefinition.propertyType().equals(propertyDefinition.propertyType()),
                "PropertyType must be the same for updatePropertyDefinition. Original: '%s', Updated: '%s' '%s'", currentPropertyDefinition.propertyType(), propertyDefinition.propertyType()
        );
        this.getSchema().getTopology().startSchemaChange(
                String.format("VertexLabel '%s' updatePropertyDefinition with '%s' '%s'", getFullName(), propertyColumn.getName(), propertyDefinition)
        );
        String name = propertyColumn.getName();
        if (!this.uncommittedUpdatedProperties.containsKey(name)) {
            PropertyColumn copy = new PropertyColumn(this, name, propertyDefinition);
            this.uncommittedUpdatedProperties.put(name, copy);
            TopologyManager.updateVertexLabelPropertyColumn(
                    this.sqlgGraph,
                    getSchema().getName(),
                    VERTEX_PREFIX + getLabel(),
                    name,
                    propertyDefinition
            );
            internalUpdatePropertyDefinition(propertyColumn, propertyDefinition, currentPropertyDefinition, name, copy);
        }
    }

    @Override
    void renameProperty(String name, PropertyColumn propertyColumn) {
        this.getSchema().getTopology().startSchemaChange(
                String.format("VertexLabel '%s' renameProperty with '%s' '%s'", getFullName(), name, propertyColumn.getName())
        );
        String oldName = propertyColumn.getName();
        Pair<String, String> namePair = Pair.of(oldName, name);
        if (!this.uncommittedRemovedProperties.contains(name)) {
            this.uncommittedRemovedProperties.add(oldName);
            PropertyColumn copy = new PropertyColumn(this, name, propertyColumn.getPropertyDefinition());
            this.uncommittedProperties.put(name, copy);
            TopologyManager.renameVertexLabelPropertyColumn(this.sqlgGraph, this.schema.getName(), VERTEX_PREFIX + getLabel(), oldName, name);
            renameColumn(this.schema.getName(), VERTEX_PREFIX + getLabel(), oldName, name);
            if (this.getIdentifiers().contains(oldName)) {
                Preconditions.checkState(!this.renamedIdentifiers.contains(namePair), "BUG! renamedIdentifiers may not yet contain '%s'", oldName);
                this.renamedIdentifiers.add(namePair);

                Map<String, EdgeLabel> outEdgeLabels = getOutEdgeLabels();
                for (String outEdgeLabel : outEdgeLabels.keySet()) {
                    EdgeLabel edgeLabel = outEdgeLabels.get(outEdgeLabel);
                    edgeLabel.renameOutForeignKeyIdentifier(oldName, name, this);
                }
                Map<String, EdgeLabel> inEdgeLabels = getInEdgeLabels();
                for (String inEdgeLabel : inEdgeLabels.keySet()) {
                    EdgeLabel edgeLabel = inEdgeLabels.get(inEdgeLabel);
                    edgeLabel.renameInForeignKeyIdentifier(oldName, name, this);
                }

            }
            this.getSchema().getTopology().fire(propertyColumn, propertyColumn, TopologyChangeAction.DELETE, true);
            this.getSchema().getTopology().fire(copy, propertyColumn, TopologyChangeAction.CREATE, true);
        }
    }

    void removeOutEdge(EdgeLabel edgeLabel) {
        Set<EdgeRole> outEdgeRoles = edgeLabel.getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().equals(this)).collect(Collectors.toSet());
        for (EdgeRole outEdgeRole : outEdgeRoles) {
            this.uncommittedRemovedOutEdgeRoles.put(edgeLabel.getFullName(), Pair.of(outEdgeRole, EdgeRemoveType.EDGE_LABEL));
        }
    }

    void removeInEdge(EdgeLabel edgeLabel) {
        Set<EdgeRole> inEdgeRoles = edgeLabel.getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().equals(this)).collect(Collectors.toSet());
        for (EdgeRole inEdgeRole : inEdgeRoles) {
            this.uncommittedRemovedInEdgeRoles.put(edgeLabel.getFullName(), Pair.of(inEdgeRole, EdgeRemoveType.EDGE_LABEL));
        }
    }

    /**
     * remove a given edge role
     *
     * @param edgeRole     the edge role
     * @param preserveData should we keep the SQL data
     */
    void removeEdgeRole(EdgeRole edgeRole, boolean dropEdges, boolean preserveData) {
        if (edgeRole.getVertexLabel() != this) {
            throw new IllegalStateException("Trying to remove a EdgeRole from a non owner VertexLabel");
        }
        Collection<VertexLabel> inOutVertexLabels = switch (edgeRole.getDirection()) {
            // we don't support both
            case BOTH -> throw new IllegalStateException("BOTH is not a supported direction");
            case IN -> edgeRole.getEdgeLabel().getInVertexLabels();
            case OUT -> edgeRole.getEdgeLabel().getOutVertexLabels();
        };
        if (!inOutVertexLabels.contains(this)) {
            throw new IllegalStateException("Trying to remove a EdgeRole from a non owner VertexLabel");
        }
        // the edge had only this vertex on that direction, remove the edge
        if (inOutVertexLabels.size() == 1) {
            edgeRole.getEdgeLabel().remove(preserveData);
        } else {
            getSchema().getTopology().startSchemaChange(
                    String.format("VertexLabel '%s' removeEdgeRole with '%s'", getFullName(), edgeRole.getName())
            );
            EdgeLabel edgeLabel = edgeRole.getEdgeLabel();
            switch (edgeRole.getDirection()) {
                // we don't support both
                case BOTH -> throw new IllegalStateException("BOTH is not a supported direction");
                case IN -> {
                    edgeRole.getEdgeLabel().removeInVertexLabel(this, dropEdges, preserveData);
                    this.uncommittedRemovedInEdgeRoles.put(edgeLabel.getFullName(), Pair.of(edgeRole, EdgeRemoveType.EDGE_ROLE));
                }
                case OUT -> {
                    edgeRole.getEdgeLabel().removeOutVertexLabel(this, dropEdges, preserveData);
                    this.uncommittedRemovedOutEdgeRoles.put(edgeLabel.getFullName(), Pair.of(edgeRole, EdgeRemoveType.EDGE_ROLE));
                }
            }

            this.getSchema().getTopology().fire(edgeRole, edgeRole, TopologyChangeAction.DELETE, true);
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
        copy.identifiers.addAll(this.identifiers);
        return copy;
    }

    @Override
    public void rename(String label) {
        Objects.requireNonNull(label, "Given label must not be null");
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not be prefixed with \"%s\"", VERTEX_PREFIX);
        Preconditions.checkState(!this.isForeignAbstractLabel, "'%s' is a read only foreign table!", label);
        this.getSchema().getTopology().startSchemaChange(
                String.format("VertexLabel '%s' rename with '%s'", getFullName(), label)
        );

        //fire deletion events first
        for (String s : getOutEdgeRoles().keySet()) {
            EdgeRole edgeRole = getOutEdgeRoles().get(s);
            this.getTopology().fire(edgeRole, edgeRole, TopologyChangeAction.DELETE, true);
        }
        for (String s : getInEdgeRoles().keySet()) {
            EdgeRole edgeRole = getInEdgeRoles().get(s);
            this.getTopology().fire(edgeRole, edgeRole, TopologyChangeAction.DELETE, true);
        }
        this.getTopology().fire(this, this, TopologyChangeAction.DELETE, true);

        VertexLabel renamedVertexLabel = this.getSchema().renameVertexLabel(this, label);

        this.getTopology().fire(renamedVertexLabel, this, TopologyChangeAction.CREATE, true);
        for (String s : getOutEdgeRoles().keySet()) {
            EdgeRole edgeRole = getOutEdgeRoles().get(s);
            EdgeLabel edgeLabel = edgeRole.getEdgeLabel();
            edgeLabel.renameOutVertexLabel(renamedVertexLabel, this);
            EdgeRole renamedEdgeRole = edgeLabel.getOutEdgeRoles(renamedVertexLabel);
            this.getTopology().fire(renamedEdgeRole, null, TopologyChangeAction.CREATE, true);
        }
        for (String s : getInEdgeRoles().keySet()) {
            EdgeRole edgeRole = getInEdgeRoles().get(s);
            EdgeLabel edgeLabel = edgeRole.getEdgeLabel();
            edgeLabel.renameInVertexLabel(renamedVertexLabel, this);
            EdgeRole renamedEdgeRole = edgeLabel.getInEdgeRoles(renamedVertexLabel);
            this.getTopology().fire(renamedEdgeRole, null, TopologyChangeAction.CREATE, true);
        }
    }
}
