package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.util.ThreadLocalMap;
import org.umlg.sqlg.util.ThreadLocalSet;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class Schema implements TopologyInf {

    private static final Logger LOGGER = LoggerFactory.getLogger(Schema.class);
    private final SqlgGraph sqlgGraph;
    private final Topology topology;
    private final String name;
    private boolean committed = true;
    private boolean isForeignSchema;

    //The key is schema + "." + VERTEX_PREFIX + vertex label. i.e. "A.V_A"
    private final Map<String, VertexLabel> vertexLabels = new ConcurrentHashMap<>();
    private final Map<String, VertexLabel> uncommittedVertexLabels = new ThreadLocalMap<>();
    public final Set<String> uncommittedRemovedVertexLabels = new ThreadLocalSet<>();

    private final Map<String, EdgeLabel> outEdgeLabels = new ConcurrentHashMap<>();
    private final Map<String, EdgeLabel> uncommittedOutEdgeLabels = new ThreadLocalMap<>();
    final Set<String> uncommittedRemovedEdgeLabels = new ThreadLocalSet<>();

    public static final String SQLG_SCHEMA = "sqlg_schema";

    //temporary table map. it is in a thread local as temporary tables are only valid per session/connection.
    private final ThreadLocal<Map<String, Map<String, PropertyDefinition>>> threadLocalTemporaryTables = ThreadLocal.withInitial(HashMap::new);

    /**
     * Creates the SqlgSchema. The sqlg_schema always exist and is created via sql in {@link SqlDialect#sqlgTopologyCreationScripts()}
     *
     * @param topology A reference to the {@link Topology} that contains the sqlg_schema schema.
     * @return The Schema that represents 'sqlg_schema'
     */
    static Schema instantiateSqlgSchema(Topology topology) {
        return new Schema(topology, SQLG_SCHEMA);
    }

    /**
     * Creates the 'public' schema that always already exist and is preloaded in {@link Topology()} @see {@link Topology#cacheTopology()}
     *
     * @param publicSchemaName The 'public' schema's name. Sometimes its upper case (Hsqldb) sometimes lower (Postgresql)
     * @param topology         The {@link Topology} that contains the public schema.
     * @return The Schema that represents 'public'
     */
    static Schema createPublicSchema(SqlgGraph sqlgGraph, Topology topology, String publicSchemaName) {
        Schema schema = new Schema(topology, publicSchemaName);
        if (!existPublicSchema(sqlgGraph)) {
            schema.createSchemaOnDb();
        }
        schema.committed = false;
        return schema;
    }

    private static boolean existPublicSchema(SqlgGraph sqlgGraph) {
        Connection conn = sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            return sqlgGraph.getSqlDialect().schemaExists(metadata, sqlgGraph.getSqlDialect().getPublicSchema());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static Schema createSchema(SqlgGraph sqlgGraph, Topology topology, String name) {
        Schema schema = new Schema(topology, name);
        Preconditions.checkArgument(!name.equals(SQLG_SCHEMA) && !sqlgGraph.getSqlDialect().getPublicSchema().equals(name), "createSchema may not be called for 'sqlg_schema' or 'public'");
        schema.createSchemaOnDb();
        TopologyManager.addSchema(sqlgGraph, name);
        schema.committed = false;
        return schema;
    }

    /**
     * Only called from {@link Topology#fromNotifyJson(int, LocalDateTime)}
     *
     * @param topology   The {@link Topology}
     * @param schemaName The schema's name
     * @return The Schema that has already been created by another graph.
     */
    static Schema instantiateSchema(Topology topology, String schemaName) {
        return new Schema(topology, schemaName);
    }

    private Schema(Topology topology, String name) {
        this(topology.getSqlgGraph(), topology, name);
    }

    private Schema(SqlgGraph sqlgGraph, Topology topology, String name) {
        this.topology = topology;
        this.name = name;
        this.sqlgGraph = sqlgGraph;
    }

    private Schema(SqlgGraph sqlgGraph, Topology topology, String name, boolean isForeignSchema) {
        Preconditions.checkState(isForeignSchema);
        this.topology = topology;
        this.name = name;
        this.sqlgGraph = sqlgGraph;
        this.isForeignSchema = true;
    }

    SqlgGraph getSqlgGraph() {
        return this.sqlgGraph;
    }

    @Override
    public boolean isCommitted() {
        return this.committed;
    }

    public boolean isForeignSchema() {
        return isForeignSchema;
    }

    public VertexLabel ensureVertexLabelExist(final String label) {
        return ensureVertexLabelExist(label, Collections.emptyMap());
    }

    void ensureTemporaryVertexTableExist(final String label, final Map<String, PropertyDefinition> columns) {
        Objects.requireNonNull(label, "Given table must not be null");
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not be prefixed with %s", VERTEX_PREFIX);

        final String prefixedTable = VERTEX_PREFIX + label;
        if (!this.threadLocalTemporaryTables.get().containsKey(prefixedTable)) {
            this.topology.startSchemaChange(
                    String.format("Schema '%s' ensureTemporaryVertexTableExist with '%s'", getName(), label)
            );
            if (!this.threadLocalTemporaryTables.get().containsKey(prefixedTable)) {
                this.threadLocalTemporaryTables.get().put(prefixedTable, columns);
                createTempTable(prefixedTable, columns);
            }
        }
    }

    public VertexLabel ensureVertexLabelExist(final String label, Map<String, PropertyDefinition> columns) {
        return ensureVertexLabelExist(label, columns, new ListOrderedSet<>());
    }

    public VertexLabel ensureVertexLabelExist(final String label, final Map<String, PropertyDefinition> columns, ListOrderedSet<String> identifiers) {
        Objects.requireNonNull(label, "Given table must not be null");
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not be prefixed with \"%s\"", VERTEX_PREFIX);
        for (String identifier : identifiers) {
            Preconditions.checkState(columns.containsKey(identifier), "The identifiers must be in the specified columns. \"%s\" not found", identifier);
        }

        Optional<VertexLabel> vertexLabelOptional = this.getVertexLabel(label);
        if (vertexLabelOptional.isEmpty()) {
            Preconditions.checkState(!this.isForeignSchema, "'%s' is a read only foreign schema!", this.name);
            this.topology.startSchemaChange(
                    String.format("Schema '%s' ensureVertexLabelExist with '%s'", getName(), label)
            );
            vertexLabelOptional = this.getVertexLabel(label);
            return vertexLabelOptional.orElseGet(() -> this.createVertexLabel(label, columns, identifiers));
        } else {
            VertexLabel vertexLabel = vertexLabelOptional.get();
            //check if all the columns are there.
            vertexLabel.ensurePropertiesExist(columns);
            return vertexLabel;
        }
    }

    VertexLabel renameVertexLabel(VertexLabel vertexLabel, String label) {
        Optional<VertexLabel> vertexLabelOptional = this.getVertexLabel(label);
        Preconditions.checkState(vertexLabelOptional.isEmpty(), "'%s' already exists", label);
        Preconditions.checkState(!this.isSqlgSchema(), "renameVertexLabel may not be called for \"%s\"", SQLG_SCHEMA);
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "vertex label may not start with " + VERTEX_PREFIX);
        this.sqlgGraph.getSqlDialect().validateTableName(label);
        this.uncommittedRemovedVertexLabels.add(this.name + "." + VERTEX_PREFIX + vertexLabel.label);
        VertexLabel renamedVertexLabel = VertexLabel.renameVertexLabel(
                this.sqlgGraph,
                this,
                vertexLabel.label,
                label,
                vertexLabel.getPropertyDefinitionMap(),
                vertexLabel.getIdentifiers()
        );
        this.uncommittedVertexLabels.put(this.name + "." + VERTEX_PREFIX + label, renamedVertexLabel);
        return renamedVertexLabel;
    }

    @SuppressWarnings("UnusedReturnValue")
    void renameEdgeLabel(EdgeLabel edgeLabel, String label) {
        Optional<EdgeLabel> edgeLabelOptional = this.getEdgeLabel(label);
        Preconditions.checkState(edgeLabelOptional.isEmpty(), "'%s' already exists", label);
        Preconditions.checkState(!this.isSqlgSchema(), "renameEdgeLabel may not be called for \"%s\"", SQLG_SCHEMA);
        Preconditions.checkArgument(!label.startsWith(EDGE_PREFIX), "edge label may not start with " + EDGE_PREFIX);
        this.sqlgGraph.getSqlDialect().validateTableName(label);

//        this.getTopology().fire(edgeLabel, edgeLabel, TopologyChangeAction.DELETE, true);
        this.uncommittedRemovedEdgeLabels.add(this.name + "." + EDGE_PREFIX + edgeLabel.label);

        Set<VertexLabel> outVertexLabels = edgeLabel.getOutVertexLabels();
        Set<VertexLabel> inVertexLabels = edgeLabel.getInVertexLabels();

        EdgeLabel renamedEdgeLabel = EdgeLabel.renameEdgeLabel(
                this.sqlgGraph,
                this,
                edgeLabel,
                label,
                outVertexLabels.stream().flatMap(vertexLabel -> vertexLabel.getOutEdgeRoles().values().stream()).collect(Collectors.toSet()),
                inVertexLabels.stream().flatMap(vertexLabel -> vertexLabel.getInEdgeRoles().values().stream()).collect(Collectors.toSet()),
                edgeLabel.getPropertyDefinitionMap(),
                edgeLabel.getIdentifiers()
        );
        this.uncommittedOutEdgeLabels.put(this.name + "." + EDGE_PREFIX + label, renamedEdgeLabel);
    }

    public VertexLabel ensurePartitionedVertexLabelExist(
            final String label,
            final Map<String, PropertyDefinition> columns,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression) {

        return ensurePartitionedVertexLabelExist(label, columns, identifiers, partitionType, partitionExpression, true);
    }

    public VertexLabel ensurePartitionedVertexLabelExist(
            final String label,
            final Map<String, PropertyDefinition> columns,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression,
            boolean addPrimaryKeyConstraint) {

        Preconditions.checkState(this.sqlgGraph.getSqlDialect().supportsPartitioning());
        Objects.requireNonNull(label, "Given table must not be null");
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not be prefixed with %s", VERTEX_PREFIX);

        Optional<VertexLabel> vertexLabelOptional = this.getVertexLabel(label);
        if (vertexLabelOptional.isEmpty()) {
            this.topology.startSchemaChange(
                    String.format("Schema '%s' ensurePartitionedVertexLabelExist with '%s'", getName(), label)
            );
            vertexLabelOptional = this.getVertexLabel(label);
            return vertexLabelOptional.orElseGet(
                    () -> this.createPartitionedVertexLabel(label, columns, identifiers, partitionType, partitionExpression, addPrimaryKeyConstraint)
            );
        } else {
            VertexLabel vertexLabel = vertexLabelOptional.get();
            //check if all the columns are there.
            vertexLabel.ensurePropertiesExist(columns);
            return vertexLabel;
        }
    }

    public EdgeLabel ensureEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> columns,
            EdgeDefinition edgeDefinition) {
        return ensureEdgeLabelExist(edgeLabelName, outVertexLabel, inVertexLabel, columns, new ListOrderedSet<>(), edgeDefinition);
    }

    public EdgeLabel ensureEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> columns) {
        return ensureEdgeLabelExist(edgeLabelName, outVertexLabel, inVertexLabel, columns, new ListOrderedSet<>());
    }

    public EdgeLabel ensureEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> columns,
            ListOrderedSet<String> identifiers) {

        return ensureEdgeLabelExist(edgeLabelName, outVertexLabel, inVertexLabel, columns, identifiers, null);
    }

    public EdgeLabel ensureEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> columns,
            ListOrderedSet<String> identifiers,
            EdgeDefinition edgeDefinition) {

        Objects.requireNonNull(edgeLabelName, "Given edgeLabelName may not be null");
        Objects.requireNonNull(outVertexLabel, "Given outVertexLabel may not be null");
        Objects.requireNonNull(inVertexLabel, "Given inVertexLabel may not be null");
        Objects.requireNonNull(identifiers, "Given identifiers may not be null");

        this.sqlgGraph.getSqlDialect().validateTableName(edgeLabelName);
        for (String columnName : columns.keySet()) {
            this.sqlgGraph.getSqlDialect().validateColumnName(columnName);
        }

        for (String identifier : identifiers) {
            Preconditions.checkState(columns.containsKey(identifier), "The identifiers must be in the specified columns. \"%s\" not found", identifier);
        }

        Preconditions.checkState((outVertexLabel.isDistributed() && inVertexLabel.isDistributed()) || (!outVertexLabel.isDistributed() && !inVertexLabel.isDistributed()), "The in and out vertex labels must both either be distributed or not.");
        if (outVertexLabel.isDistributed()) {
            Preconditions.checkState(outVertexLabel.getDistributionPropertyColumn().getName().equals(inVertexLabel.getDistributionPropertyColumn().getName()), "The in and out vertex label's distribution columns must have the same name.");
        }

        EdgeLabel edgeLabel;
        Optional<EdgeLabel> edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
        if (edgeLabelOptional.isEmpty()) {
            Preconditions.checkState(!this.isForeignSchema, "'A' is a read only foreign schema!");
            this.topology.startSchemaChange(
                    String.format("Schema '%s' ensureEdgeLabelExist with '%s'", getName(), edgeLabelName)
            );
            edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
            if (edgeLabelOptional.isEmpty()) {
                edgeLabel = this.createEdgeLabel(
                        edgeLabelName,
                        outVertexLabel,
                        inVertexLabel,
                        columns,
                        identifiers,
                        edgeDefinition != null ? edgeDefinition : EdgeDefinition.of()
                );
                this.uncommittedRemovedEdgeLabels.remove(this.name + "." + EDGE_PREFIX + edgeLabelName);
                this.uncommittedOutEdgeLabels.put(this.name + "." + EDGE_PREFIX + edgeLabelName, edgeLabel);
                this.getTopology().fire(edgeLabel, null, TopologyChangeAction.CREATE, true);
                //nothing more to do as the edge did not exist and will have been created with the correct foreign keys.
            } else {
                EdgeRole outEdgeRole = edgeLabelOptional.get().getOutEdgeRoles(outVertexLabel);
                Multiplicity outMultiplicity;
                if (outEdgeRole != null) {
                    outMultiplicity = outEdgeRole.getMultiplicity();
                } else {
                    outMultiplicity = Multiplicity.of(0, -1);
                }
                EdgeRole inEdgeRole = edgeLabelOptional.get().getInEdgeRoles(inVertexLabel);
                Multiplicity inMultiplicity;
                if (inEdgeRole != null) {
                    inMultiplicity = inEdgeRole.getMultiplicity();
                } else {
                    inMultiplicity = Multiplicity.of(0, -1);
                }
                edgeLabel = edgeLabelOptional.get();
                internalEnsureEdgeTableExists(edgeLabel, outVertexLabel, inVertexLabel, columns, EdgeDefinition.of(outMultiplicity, inMultiplicity));
            }
        } else {
            edgeLabel = edgeLabelOptional.get();
            EdgeRole outEdgeRole = edgeLabel.getOutEdgeRoles(outVertexLabel);
            Multiplicity outMultiplicity;
            if (outEdgeRole != null) {
                outMultiplicity = outEdgeRole.getMultiplicity();
            } else {
                if (edgeDefinition != null) {
                    outMultiplicity = edgeDefinition.outMultiplicity();
                } else {
                    outMultiplicity = Multiplicity.of(0, -1);
                }
            }
            EdgeRole inEdgeRole = edgeLabelOptional.get().getInEdgeRoles(inVertexLabel);
            Multiplicity inMultiplicity;
            if (inEdgeRole != null) {
                inMultiplicity = inEdgeRole.getMultiplicity();
            } else {
                if (edgeDefinition != null) {
                    inMultiplicity = edgeDefinition.inMultiplicity();
                } else {
                    inMultiplicity = Multiplicity.of(0, -1);
                }
            }
            internalEnsureEdgeTableExists(edgeLabel, outVertexLabel, inVertexLabel, columns, EdgeDefinition.of(outMultiplicity, inMultiplicity));
        }
        return edgeLabel;
    }

    public EdgeLabel ensureEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> columns,
            ListOrderedSet<String> identifiers,
            int shardCount,
            String distributionColumn,
            AbstractLabel colocate) {

        return ensureEdgeLabelExist(
                edgeLabelName,
                outVertexLabel,
                inVertexLabel,
                columns,
                identifiers,
                shardCount,
                distributionColumn,
                colocate,
                EdgeDefinition.of(Multiplicity.of(0, -1), Multiplicity.of(0, -1))
        );
    }

    public EdgeLabel ensureEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> columns,
            ListOrderedSet<String> identifiers,
            int shardCount,
            String distributionColumn,
            AbstractLabel colocate,
            EdgeDefinition edgeDefinition) {

        Objects.requireNonNull(edgeLabelName, "Given edgeLabelName may not be null");
        Objects.requireNonNull(outVertexLabel, "Given outVertexLabel may not be null");
        Objects.requireNonNull(inVertexLabel, "Given inVertexLabel may not be null");
        Objects.requireNonNull(identifiers, "Given identifiers may not be null");
        Preconditions.checkArgument(shardCount > 0, "Given shardCount must be bigger than 0");
        Objects.requireNonNull(distributionColumn, "Given distributionColumn may not be null");
        Objects.requireNonNull(colocate, "Given colocate may not be null");

        for (String identifier : identifiers) {
            Preconditions.checkState(columns.containsKey(identifier), "The identifiers must be in the specified columns. \"%s\" not found", identifier);
        }
        Preconditions.checkArgument(identifiers.contains(distributionColumn), "The distribution column must be an identifier.");

        EdgeLabel edgeLabel;
        Optional<EdgeLabel> edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
        if (edgeLabelOptional.isEmpty()) {
            this.topology.startSchemaChange(
                    String.format("Schema '%s' ensureEdgeLabelExist with '%s'", getName(), edgeLabelName)
            );
            edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
            if (edgeLabelOptional.isEmpty()) {
                edgeLabel = this.createEdgeLabel(edgeLabelName, outVertexLabel, inVertexLabel, columns, identifiers, edgeDefinition);
                this.uncommittedRemovedEdgeLabels.remove(this.name + "." + EDGE_PREFIX + edgeLabelName);
                this.uncommittedOutEdgeLabels.put(this.name + "." + EDGE_PREFIX + edgeLabelName, edgeLabel);
                this.getTopology().fire(edgeLabel, null, TopologyChangeAction.CREATE, true);
                //nothing more to do as the edge did not exist and will have been created with the correct foreign keys.
            } else {
                edgeLabel = edgeLabelOptional.get();
                internalEnsureEdgeTableExists(edgeLabel, outVertexLabel, inVertexLabel, columns, edgeDefinition);
            }
        } else {
            edgeLabel = edgeLabelOptional.get();
            internalEnsureEdgeTableExists(edgeLabel, outVertexLabel, inVertexLabel, columns, edgeDefinition);
        }
        return edgeLabel;
    }

    public EdgeLabel ensurePartitionedEdgeLabelExistOnInOrOutVertexLabel(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> columns,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            VertexLabel foreignKeyVertexLabel) {

        Preconditions.checkState(foreignKeyVertexLabel.equals(outVertexLabel) || foreignKeyVertexLabel.equals(inVertexLabel),
                "foreignKeyVertexLabel must be either the outVertexLabel or inVertexLabel!");

        StringBuilder partitionExpression = new StringBuilder();
        Direction direction;
        if (foreignKeyVertexLabel.equals(outVertexLabel)) {
            direction = Direction.OUT;
        } else {
            direction = Direction.IN;
        }
        if (foreignKeyVertexLabel.hasIDPrimaryKey()) {
            partitionExpression.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKeyVertexLabel.getSchema().getName() + "." + foreignKeyVertexLabel.getLabel() + (direction == Direction.OUT ? OUT_VERTEX_COLUMN_END : IN_VERTEX_COLUMN_END)));
        } else {
            int countIdentifier = 1;
            for (String identifier : foreignKeyVertexLabel.getIdentifiers()) {
                PropertyColumn propertyColumn = foreignKeyVertexLabel.getProperty(identifier).orElseThrow(
                        () -> new IllegalStateException(String.format("identifier %s column must be a property", identifier))
                );
                PropertyType propertyType = propertyColumn.getPropertyType();
                String[] propertyTypeToSqlDefinition = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                int count = 1;
                for (String ignored : propertyTypeToSqlDefinition) {
                    if (count > 1) {
                        partitionExpression.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                foreignKeyVertexLabel.getFullName() + "." + identifier + propertyType.getPostFixes()[count - 2] + (direction == Direction.OUT ? OUT_VERTEX_COLUMN_END : IN_VERTEX_COLUMN_END)
                        ));
                    } else {
                        //The first column existVertexLabel no postfix
                        partitionExpression.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                                foreignKeyVertexLabel.getFullName() + "." + identifier + (direction == Direction.OUT ? OUT_VERTEX_COLUMN_END : IN_VERTEX_COLUMN_END)
                        ));
                    }
                    count++;
                }
                if (countIdentifier++ < foreignKeyVertexLabel.getIdentifiers().size()) {
                    partitionExpression.append(", ");
                }
            }
        }
        return ensurePartitionedEdgeLabelExist(
                edgeLabelName,
                outVertexLabel,
                inVertexLabel,
                columns,
                identifiers,
                partitionType,
                partitionExpression.toString(),
                true,
                EdgeDefinition.of(Multiplicity.of(0, -1), Multiplicity.of(0, -1))
        );
    }

    public EdgeLabel ensurePartitionedEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> columns,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression) {

        return ensurePartitionedEdgeLabelExist(
                edgeLabelName,
                outVertexLabel,
                inVertexLabel,
                columns,
                identifiers,
                partitionType,
                partitionExpression,
                false,
                EdgeDefinition.of(Multiplicity.of(0, -1), Multiplicity.of(0, -1))
        );
    }

    public EdgeLabel ensurePartitionedEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> columns,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression,
            boolean isForeignKeyPartition,
            EdgeDefinition edgeDefinition) {

        Preconditions.checkState(this.sqlgGraph.getSqlDialect().supportsPartitioning());
        Objects.requireNonNull(edgeLabelName, "Given edgeLabelName may not be null");
        Objects.requireNonNull(outVertexLabel, "Given outVertexLabel may not be null");
        Objects.requireNonNull(inVertexLabel, "Given inVertexLabel may not be null");

        EdgeLabel edgeLabel;
        Optional<EdgeLabel> edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
        if (edgeLabelOptional.isEmpty()) {
            this.topology.startSchemaChange(
                    String.format("Schema '%s' ensurePartitionedEdgeLabelExist with '%s'", getName(), edgeLabelName)
            );
            edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
            if (edgeLabelOptional.isEmpty()) {
                edgeLabel = this.createPartitionedEdgeLabel(
                        edgeLabelName,
                        outVertexLabel,
                        inVertexLabel,
                        columns,
                        identifiers,
                        partitionType,
                        partitionExpression,
                        isForeignKeyPartition,
                        edgeDefinition);
                this.uncommittedRemovedEdgeLabels.remove(this.name + "." + EDGE_PREFIX + edgeLabelName);
                this.uncommittedOutEdgeLabels.put(this.name + "." + EDGE_PREFIX + edgeLabelName, edgeLabel);
                this.getTopology().fire(edgeLabel, null, TopologyChangeAction.CREATE, true);
                //nothing more to do as the edge did not exist and will have been created with the correct foreign keys.
            } else {
                edgeLabel = edgeLabelOptional.get();
                internalEnsureEdgeTableExists(edgeLabel, outVertexLabel, inVertexLabel, columns, edgeDefinition);
            }
        } else {
            edgeLabel = edgeLabelOptional.get();
            internalEnsureEdgeTableExists(edgeLabel, outVertexLabel, inVertexLabel, columns, edgeDefinition);
        }
        return edgeLabel;
    }

    private void internalEnsureEdgeTableExists(
            EdgeLabel edgeLabel,
            VertexLabel outVertexLabel,
            VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> columns,
            EdgeDefinition edgeDefinition) {

        Preconditions.checkNotNull(edgeLabel);
        edgeLabel.ensureEdgeVertexLabelExist(Direction.OUT, outVertexLabel, edgeDefinition);
        edgeLabel.ensureEdgeVertexLabelExist(Direction.IN, inVertexLabel, edgeDefinition);
        edgeLabel.ensurePropertiesExist(columns);
    }

    private EdgeLabel createPartitionedEdgeLabel(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            final Map<String, PropertyDefinition> columns,
            final ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression,
            boolean isForeignKeyPartition,
            EdgeDefinition edgeDefinition) {

        Preconditions.checkArgument(this.topology.isSchemaChanged(), "Schema.createPartitionedEdgeLabel must have schemaChanged = true");
        Preconditions.checkArgument(!edgeLabelName.startsWith(EDGE_PREFIX), "edgeLabelName may not start with " + EDGE_PREFIX);
        Preconditions.checkState(!this.isSqlgSchema(), "createPartitionedEdgeLabel may not be called for \"%s\"", SQLG_SCHEMA);

        Schema inVertexSchema = inVertexLabel.getSchema();

        //Edge may not already exist.
        Preconditions.checkState(getEdgeLabel(edgeLabelName).isEmpty(), "BUG: Edge \"%s\" already exists!", edgeLabelName);

        SchemaTable foreignKeyOut = SchemaTable.of(this.name, outVertexLabel.getLabel());
        SchemaTable foreignKeyIn = SchemaTable.of(inVertexSchema.name, inVertexLabel.getLabel());

        TopologyManager.addEdgeLabel(
                this.sqlgGraph,
                this.getName(),
                EDGE_PREFIX + edgeLabelName,
                foreignKeyOut,
                foreignKeyIn,
                columns,
                identifiers,
                partitionType,
                partitionExpression,
                edgeDefinition
        );
        if (this.sqlgGraph.getSqlDialect().needsSchemaCreationPrecommit()) {
            try {
                this.sqlgGraph.tx().getConnection().commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return outVertexLabel.addPartitionedEdgeLabel(
                edgeLabelName,
                inVertexLabel,
                columns,
                identifiers,
                partitionType,
                partitionExpression,
                isForeignKeyPartition,
                edgeDefinition
        );

    }

    private EdgeLabel createEdgeLabel(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            final Map<String, PropertyDefinition> columns,
            final ListOrderedSet<String> identifiers,
            final EdgeDefinition edgeDefinition) {

        Preconditions.checkArgument(this.topology.isSchemaChanged(), "Schema.createEdgeLabel must have schemaChanged = true");
        Preconditions.checkArgument(!edgeLabelName.startsWith(EDGE_PREFIX), "edgeLabelName may not start with " + EDGE_PREFIX);
        Preconditions.checkState(!this.isSqlgSchema(), "createEdgeLabel may not be called for \"%s\"", SQLG_SCHEMA);

        //remove temp on PropertyDefinition
        for (String col : columns.keySet()) {
            PropertyDefinition original = columns.get(col);
            PropertyDefinition copy = new PropertyDefinition(
                    original.propertyType(),
                    original.multiplicity(),
                    original.defaultLiteral(),
                    original.checkConstraint(),
                    false
            );
            columns.put(col, copy);
        }

        Schema inVertexSchema = inVertexLabel.getSchema();

        //Edge may not already exist.
        Preconditions.checkState(getEdgeLabel(edgeLabelName).isEmpty(), "BUG: Edge \"%s\" already exists!", edgeLabelName);

        SchemaTable foreignKeyOut = SchemaTable.of(this.name, outVertexLabel.getLabel());
        SchemaTable foreignKeyIn = SchemaTable.of(inVertexSchema.name, inVertexLabel.getLabel());

        TopologyManager.addEdgeLabel(
                this.sqlgGraph,
                this.getName(),
                EDGE_PREFIX + edgeLabelName,
                foreignKeyOut,
                foreignKeyIn,
                columns,
                identifiers,
                edgeDefinition);
        if (this.sqlgGraph.getSqlDialect().needsSchemaCreationPrecommit()) {
            try {
                this.sqlgGraph.tx().getConnection().commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return outVertexLabel.addEdgeLabel(edgeLabelName, inVertexLabel, columns, identifiers, edgeDefinition);
    }

    VertexLabel createSqlgSchemaVertexLabel(String vertexLabelName, Map<String, PropertyDefinition> columns) {
        Preconditions.checkState(this.isSqlgSchema(), "createSqlgSchemaVertexLabel may only be called for \"%s\"", SQLG_SCHEMA);
        Preconditions.checkArgument(!vertexLabelName.startsWith(VERTEX_PREFIX), "vertex label may not start with " + VERTEX_PREFIX);
        VertexLabel vertexLabel = VertexLabel.createSqlgSchemaVertexLabel(this, vertexLabelName, columns);
        this.vertexLabels.put(this.name + "." + VERTEX_PREFIX + vertexLabelName, vertexLabel);
        return vertexLabel;
    }

    private VertexLabel createVertexLabel(String vertexLabelName, Map<String, PropertyDefinition> columns, ListOrderedSet<String> identifiers) {
        Preconditions.checkState(!this.isSqlgSchema(), "createVertexLabel may not be called for \"%s\"", SQLG_SCHEMA);
        Preconditions.checkArgument(!vertexLabelName.startsWith(VERTEX_PREFIX), "vertex label may not start with " + VERTEX_PREFIX);

        //remove temp on PropertyDefinition
        for (String s : columns.keySet()) {
            PropertyDefinition original = columns.get(s);
            PropertyDefinition copy = new PropertyDefinition(original.propertyType(), original.multiplicity(), original.defaultLiteral(), original.checkConstraint(), false);
            columns.put(s, copy);
        }

        this.sqlgGraph.getSqlDialect().validateTableName(VERTEX_PREFIX + vertexLabelName);
        for (String columnName : columns.keySet()) {
            this.sqlgGraph.getSqlDialect().validateColumnName(columnName);
        }

        this.uncommittedRemovedVertexLabels.remove(this.name + "." + VERTEX_PREFIX + vertexLabelName);
        VertexLabel vertexLabel = VertexLabel.createVertexLabel(this.sqlgGraph, this, vertexLabelName, columns, identifiers);
        this.uncommittedVertexLabels.put(this.name + "." + VERTEX_PREFIX + vertexLabelName, vertexLabel);
        this.getTopology().fire(vertexLabel, null, TopologyChangeAction.CREATE, true);
        return vertexLabel;
    }

    private VertexLabel createPartitionedVertexLabel(
            String vertexLabelName,
            Map<String, PropertyDefinition> columns,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression,
            boolean addPrimaryKeyConstraint) {

        Preconditions.checkState(!this.isSqlgSchema(), "createVertexLabel may not be called for \"%s\"", SQLG_SCHEMA);
        Preconditions.checkArgument(!vertexLabelName.startsWith(VERTEX_PREFIX), "vertex label may not start with " + VERTEX_PREFIX);
        this.sqlgGraph.getSqlDialect().validateTableName(vertexLabelName);
        for (String columnName : columns.keySet()) {
            this.sqlgGraph.getSqlDialect().validateColumnName(columnName);
        }

        this.uncommittedRemovedVertexLabels.remove(this.name + "." + VERTEX_PREFIX + vertexLabelName);
        VertexLabel vertexLabel = VertexLabel.createPartitionedVertexLabel(
                this.sqlgGraph,
                this,
                vertexLabelName,
                columns,
                identifiers,
                partitionType,
                partitionExpression,
                addPrimaryKeyConstraint
        );
        this.uncommittedVertexLabels.put(this.name + "." + VERTEX_PREFIX + vertexLabelName, vertexLabel);
        this.getTopology().fire(vertexLabel, null, TopologyChangeAction.CREATE, true);
        return vertexLabel;
    }

    void ensureVertexColumnsExist(String label, Map<String, PropertyDefinition> columns) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not start with \"%s\"", VERTEX_PREFIX);
        Preconditions.checkState(!isSqlgSchema(), "Schema.ensureVertexLabelPropertiesExist may not be called for \"%s\"", SQLG_SCHEMA);

        Optional<VertexLabel> vertexLabel = getVertexLabel(label);
        Preconditions.checkState(vertexLabel.isPresent(), "BUG: vertexLabel \"%s\" must exist", label);
        vertexLabel.get().ensurePropertiesExist(columns);
    }

    void ensureEdgeColumnsExist(String label, Map<String, PropertyDefinition> columns) {
        Preconditions.checkArgument(!label.startsWith(EDGE_PREFIX), "label may not start with \"%s\"", EDGE_PREFIX);
        Preconditions.checkState(!isSqlgSchema(), "Schema.ensureEdgePropertiesExist may not be called for \"%s\"", SQLG_SCHEMA);

        Optional<EdgeLabel> edgeLabel = getEdgeLabel(label);
        Preconditions.checkState(edgeLabel.isPresent(), "BUG: edgeLabel \"%s\" must exist", label);
        edgeLabel.get().ensurePropertiesExist(columns);
    }

    /**
     * Creates a new schema on the database. i.e. 'CREATE SCHEMA...' sql statement.
     */
    private void createSchemaOnDb() {
        StringBuilder sql = new StringBuilder();
        sql.append(topology.getSqlgGraph().getSqlDialect().createSchemaStatement(this.name));
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

    /**
     * Loads the existing schema from the topology.
     *
     * @param topology   The {@link Topology} that contains this schema.
     * @param schemaName The schema's name.
     * @return The loaded Schema.
     */
    static Schema loadUserSchema(Topology topology, String schemaName) {
        return new Schema(topology, schemaName);
    }


    public String getName() {
        return name;
    }

    Topology getTopology() {
        return topology;
    }

    private Map<String, VertexLabel> getUncommittedVertexLabels() {
        return this.uncommittedVertexLabels;
    }

    public Optional<VertexLabel> getVertexLabel(String vertexLabelName) {
        Preconditions.checkArgument(!vertexLabelName.startsWith(VERTEX_PREFIX), "vertex label may not start with \"%s\"", Topology.VERTEX_PREFIX);
        if (this.topology.isSchemaChanged() && this.uncommittedRemovedVertexLabels.contains(this.name + "." + VERTEX_PREFIX + vertexLabelName)) {
            return Optional.empty();
        }
        VertexLabel result = null;
        if (this.topology.isSchemaChanged()) {
            result = this.uncommittedVertexLabels.get(this.name + "." + VERTEX_PREFIX + vertexLabelName);
        }
        if (result == null) {
            result = this.vertexLabels.get(this.name + "." + VERTEX_PREFIX + vertexLabelName);
        }
        return Optional.ofNullable(result);
    }

    public Map<String, EdgeLabel> getEdgeLabels() {
        Map<String, EdgeLabel> result = new HashMap<>(this.outEdgeLabels);
        if (this.topology.isSchemaChanged()) {
            result.putAll(this.uncommittedOutEdgeLabels);
            for (String e : uncommittedRemovedEdgeLabels) {
                result.remove(e);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private Map<String, EdgeLabel> getUncommittedOutEdgeLabels() {
        Map<String, EdgeLabel> result = new HashMap<>();
        if (this.topology.isSchemaChanged()) {
            result.putAll(this.uncommittedOutEdgeLabels);
            for (EdgeLabel edgeLabel : this.outEdgeLabels.values()) {
                Map<String, PropertyColumn> propertyMap = edgeLabel.getUncommittedPropertyTypeMap();
                if (!propertyMap.isEmpty() || !edgeLabel.getUncommittedRemovedProperties().isEmpty()) {
                    result.put(edgeLabel.getLabel(), edgeLabel);
                }

            }
        }
        return result;
    }

    public Optional<EdgeLabel> getEdgeLabel(String edgeLabelName) {
        Preconditions.checkArgument(!edgeLabelName.startsWith(Topology.EDGE_PREFIX), "edge label may not start with \"%s\"", Topology.EDGE_PREFIX);
        if (this.topology.isSchemaChanged() && this.uncommittedRemovedEdgeLabels.contains(this.name + "." + EDGE_PREFIX + edgeLabelName)) {
            return Optional.empty();
        }
        EdgeLabel edgeLabel = this.outEdgeLabels.get(this.name + "." + EDGE_PREFIX + edgeLabelName);
        if (edgeLabel != null) {
            return Optional.of(edgeLabel);
        }
        if (this.topology.isSchemaChanged()) {
            edgeLabel = this.uncommittedOutEdgeLabels.get(this.name + "." + EDGE_PREFIX + edgeLabelName);
            if (edgeLabel != null) {
                return Optional.of(edgeLabel);
            }
        }
        return Optional.empty();
    }

    //remove in favour of PropertyColumn
    Map<String, Map<String, PropertyDefinition>> getAllTables() {
        Map<String, Map<String, PropertyDefinition>> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {
            String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
            result.put(vertexQualifiedName, vertexLabelEntry.getValue().getPropertyDefinitionMap());
        }
        if (this.topology.isSchemaChanged()) {
            for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {
                String vertexQualifiedName = vertexLabelEntry.getKey();
                VertexLabel vertexLabel = vertexLabelEntry.getValue();
                result.put(vertexQualifiedName, vertexLabel.getPropertyDefinitionMap());
            }
        }
        for (EdgeLabel edgeLabel : this.getEdgeLabels().values()) {
            String edgeQualifiedName = this.name + "." + EDGE_PREFIX + edgeLabel.getLabel();
            result.put(edgeQualifiedName, edgeLabel.getPropertyDefinitionMap());
        }
        return result;
    }

    public Map<String, VertexLabel> getVertexLabels() {
        Map<String, VertexLabel> result;
        if (this.topology.isSchemaChanged()) {
            result = new HashMap<>(this.vertexLabels);
            result.putAll(this.uncommittedVertexLabels);
            for (String e : uncommittedRemovedVertexLabels) {
                result.remove(e);
            }
        } else {
            result = this.vertexLabels;
        }
        return Collections.unmodifiableMap(result);
    }

    Map<String, AbstractLabel> getUncommittedLabels() {
        Preconditions.checkState(this.topology.isSchemaChanged(), "Schema.getUncommittedAllTables must have schemaChanged = true");
        Map<String, AbstractLabel> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {
            String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
            Map<String, PropertyColumn> uncommittedPropertyColumnMap = vertexLabelEntry.getValue().getUncommittedPropertyTypeMap();
            Set<String> uncommittedRemovedProperties = vertexLabelEntry.getValue().getUncommittedRemovedProperties();
            if (!uncommittedPropertyColumnMap.isEmpty() || !uncommittedRemovedProperties.isEmpty()) {
                result.put(vertexQualifiedName, vertexLabelEntry.getValue());
            }
        }
        for (Map.Entry<String, VertexLabel> stringVertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {
            String vertexQualifiedLabel = stringVertexLabelEntry.getKey();
            VertexLabel vertexLabel = stringVertexLabelEntry.getValue();
            result.put(vertexQualifiedLabel, vertexLabel);
        }
        for (EdgeLabel edgeLabel : this.getUncommittedOutEdgeLabels().values()) {
            result.put(this.name + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel);
        }
        return result;
    }

    Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getUncommittedSchemaTableForeignKeys() {
        Preconditions.checkState(getTopology().isSchemaChanged(), "Schema.getUncommittedSchemaTableForeignKeys must have schemaChanged = true");
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {
            String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
            SchemaTable schemaTable = SchemaTable.from(this.sqlgGraph, vertexQualifiedName);
            Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedSchemaTableForeignKeys = vertexLabelEntry.getValue().getUncommittedSchemaTableForeignKeys();
            if (!uncommittedSchemaTableForeignKeys.getLeft().isEmpty() || !uncommittedSchemaTableForeignKeys.getRight().isEmpty()) {
                result.put(schemaTable, uncommittedSchemaTableForeignKeys);
            }
        }
        for (Map.Entry<String, VertexLabel> uncommittedVertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {
            String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + uncommittedVertexLabelEntry.getValue().getLabel();
            SchemaTable schemaTable = SchemaTable.from(this.sqlgGraph, vertexQualifiedName);
            Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedSchemaTableForeignKeys = uncommittedVertexLabelEntry.getValue().getUncommittedSchemaTableForeignKeys();
            result.put(schemaTable, uncommittedSchemaTableForeignKeys);
        }
        return result;
    }

    Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getUncommittedRemovedSchemaTableForeignKeys() {
        Preconditions.checkState(getTopology().isSchemaChanged(), "Schema.getUncommittedRemovedSchemaTableForeignKeys must have schemaChanged = true");
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {
            String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
            SchemaTable schemaTable = SchemaTable.from(this.sqlgGraph, vertexQualifiedName);
            Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedSchemaTableForeignKeys = vertexLabelEntry.getValue().getUncommittedRemovedSchemaTableForeignKeys();
            if (!uncommittedSchemaTableForeignKeys.getLeft().isEmpty() || !uncommittedSchemaTableForeignKeys.getRight().isEmpty()) {
                result.put(schemaTable, uncommittedSchemaTableForeignKeys);
            }
        }
        for (Map.Entry<String, VertexLabel> uncommittedVertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {
            String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + uncommittedVertexLabelEntry.getValue().getLabel();
            SchemaTable schemaTable = SchemaTable.from(this.sqlgGraph, vertexQualifiedName);
            Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedSchemaTableForeignKeys = uncommittedVertexLabelEntry.getValue().getUncommittedRemovedSchemaTableForeignKeys();
            result.put(schemaTable, uncommittedSchemaTableForeignKeys);
        }
        return result;
    }

    Map<String, Set<ForeignKey>> getUncommittedEdgeForeignKeys() {
        Map<String, Set<ForeignKey>> result = new HashMap<>();
        for (EdgeLabel outEdgeLabel : this.outEdgeLabels.values()) {
            result.put(this.getName() + "." + Topology.EDGE_PREFIX + outEdgeLabel.getLabel(), outEdgeLabel.getUncommittedEdgeForeignKeys());
        }
        for (EdgeLabel outEdgeLabel : this.uncommittedOutEdgeLabels.values()) {
            result.put(this.getName() + "." + Topology.EDGE_PREFIX + outEdgeLabel.getLabel(), outEdgeLabel.getUncommittedEdgeForeignKeys());
        }
        for (String e : this.uncommittedRemovedEdgeLabels) {
            result.remove(e);
        }
        return result;
    }

    Map<String, Set<ForeignKey>> getUncommittedRemovedEdgeForeignKeys() {
        Map<String, Set<ForeignKey>> result = new HashMap<>();
        for (EdgeLabel outEdgeLabel : this.outEdgeLabels.values()) {
            result.put(this.getName() + "." + Topology.EDGE_PREFIX + outEdgeLabel.getLabel(), outEdgeLabel.getUncommittedRemovedEdgeForeignKeys());
        }
        for (EdgeLabel outEdgeLabel : this.uncommittedOutEdgeLabels.values()) {
            result.put(this.getName() + "." + Topology.EDGE_PREFIX + outEdgeLabel.getLabel(), outEdgeLabel.getUncommittedRemovedEdgeForeignKeys());
        }
        return result;
    }

    Map<String, PropertyColumn> getPropertiesFor(SchemaTable schemaTable) {
        Preconditions.checkArgument(schemaTable.getTable().startsWith(VERTEX_PREFIX) || schemaTable.getTable().startsWith(EDGE_PREFIX), "label must start with \"%s\" or \"%s\"", Topology.VERTEX_PREFIX, Topology.EDGE_PREFIX);
        if (schemaTable.isVertexTable()) {
            Optional<VertexLabel> vertexLabelOptional = getVertexLabel(schemaTable.withOutPrefix().getTable());
            if (vertexLabelOptional.isPresent()) {
                return vertexLabelOptional.get().getProperties();
            }
        } else {
            Optional<EdgeLabel> edgeLabelOptional = getEdgeLabel(schemaTable.withOutPrefix().getTable());
            if (edgeLabelOptional.isPresent()) {
                return edgeLabelOptional.get().getProperties();
            }
        }
        return Collections.emptyMap();
    }

    Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getTableLabels() {
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = new HashMap<>(this.vertexLabels.size());
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {
            Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with " + VERTEX_PREFIX);
            String prefixedVertexName = VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
            SchemaTable schemaTable = SchemaTable.of(this.getName(), prefixedVertexName);
            result.put(schemaTable, vertexLabelEntry.getValue().getTableLabels());
        }
        if (this.topology.isSchemaChanged()) {
            Preconditions.checkState(this.uncommittedVertexLabels.isEmpty());
        }
        return result;
    }

    Map<String, Set<ForeignKey>> getAllEdgeForeignKeys() {
        Map<String, Set<ForeignKey>> result = new HashMap<>();
        for (Map.Entry<String, EdgeLabel> stringEdgeLabelEntry : getEdgeLabels().entrySet()) {
            String edgeSchemaAndLabel = stringEdgeLabelEntry.getKey();
            EdgeLabel edgeLabel = stringEdgeLabelEntry.getValue();
            result.put(edgeSchemaAndLabel, edgeLabel.getAllEdgeForeignKeys());
        }
        return result;
    }

    void afterCommit() {
        Preconditions.checkState(this.topology.isSchemaChanged(), "Schema.afterCommit must have schemaChanged = true");
        for (Iterator<Map.Entry<String, VertexLabel>> it = this.uncommittedVertexLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, VertexLabel> entry = it.next();
            this.vertexLabels.put(entry.getKey(), entry.getValue());
            it.remove();
        }
        for (Iterator<String> it = uncommittedRemovedVertexLabels.iterator(); it.hasNext(); ) {
            String s = it.next();
            VertexLabel lbl = this.vertexLabels.remove(s);
            if (lbl != null) {
                this.getTopology().removeVertexLabel(lbl);
            }
            it.remove();
        }
        for (VertexLabel vertexLabel : this.vertexLabels.values()) {
            vertexLabel.afterCommit();
        }
        for (Iterator<String> it = uncommittedRemovedEdgeLabels.iterator(); it.hasNext(); ) {
            String s = it.next();
            getTopology().removeEdgeLabel(s);
            this.outEdgeLabels.remove(s);
            it.remove();
        }

        this.uncommittedOutEdgeLabels.clear();
        this.committed = true;
    }

    void afterRollback() {
        Preconditions.checkState(this.topology.isSchemaChanged(), "Schema.afterRollback must have schemaChanged = true");
        for (Map.Entry<String, VertexLabel> entry : this.uncommittedVertexLabels.entrySet()) {
            entry.getValue().afterRollbackForInEdges();
        }
        for (Iterator<Map.Entry<String, VertexLabel>> it = this.uncommittedVertexLabels.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, VertexLabel> entry = it.next();
            entry.getValue().afterRollbackForOutEdges();
            it.remove();
        }
        for (VertexLabel vertexLabel : this.vertexLabels.values()) {
            vertexLabel.afterRollbackForInEdges();
        }
        for (VertexLabel vertexLabel : this.vertexLabels.values()) {
            vertexLabel.afterRollbackForOutEdges();
        }
        this.uncommittedOutEdgeLabels.clear();
        this.uncommittedRemovedEdgeLabels.clear();
        this.uncommittedRemovedVertexLabels.clear();
    }

    boolean isSqlgSchema() {
        return this.name.equals(SQLG_SCHEMA);
    }

    public void addToOutEdgeLabels(String schemaName, String edgeLabelName, EdgeLabel edgeLabel) {
        this.outEdgeLabels.put(schemaName + "." + EDGE_PREFIX + edgeLabelName, edgeLabel);
    }

    VertexLabel cacheTopologyAddToVertexLabels(String tableName, PartitionType partitionType, String partitionExpression, Integer shardCount) {
        VertexLabel vertexLabel;
        if (!partitionType.isNone()) {
            vertexLabel = new VertexLabel(this, tableName, partitionType, partitionExpression);
        } else {
            vertexLabel = new VertexLabel(this, tableName);
        }
        if (shardCount != null) {
            vertexLabel.setShardCount(shardCount);
        }
        this.vertexLabels.put(this.name + "." + VERTEX_PREFIX + tableName, vertexLabel);
        return vertexLabel;
    }

    /**
     * load indices for (out) edges on all vertices of schema
     */
    void loadEdgeIndices(String tableName, String edgeName, String indexName, String index_type, String propertyName) {
        String schemaName = getName();
        VertexLabel vertexLabel = this.vertexLabels.get(schemaName + "." + VERTEX_PREFIX + tableName);
        Preconditions.checkNotNull(vertexLabel, "VertexLabel %s not found in %s", tableName, schemaName);
        Optional<EdgeLabel> oel = vertexLabel.getOutEdgeLabel(edgeName);
        Preconditions.checkState(oel.isPresent(), "Failed to find %s in %s");
        EdgeLabel edgeLabel = oel.get();
        Optional<Index> optionalIndex = edgeLabel.getIndex(indexName);
        Index idx;
        if (optionalIndex.isPresent()) {
            idx = optionalIndex.get();
        } else {
            idx = new Index(indexName, IndexType.fromString(index_type), edgeLabel);
            edgeLabel.addIndex(idx);
        }
        edgeLabel.getProperty(propertyName).ifPresent(idx::addProperty);
    }

    void loadInEdgeLabels(
            String vertexLabelName,
            String outEdgeLabelName,
            String inVertexLabelName,
            String inSchemaVertexLabelName,
            long lowerMultiplicityProperty,
            long upperMultiplicityProperty,
            boolean uniqueMultiplicityProperty) {

        String schemaName = getName();
        VertexLabel vertexLabel = this.vertexLabels.get(schemaName + "." + VERTEX_PREFIX + vertexLabelName);
        Preconditions.checkState(vertexLabel != null, "vertexLabel must be present when loading inEdges. Not found for %s", schemaName + "." + VERTEX_PREFIX + vertexLabelName);
        if (outEdgeLabelName != null) {
            //inVertex and inSchema must be present.
            Preconditions.checkState(inVertexLabelName != null, "BUG: In vertex not found edge for \"%s\"", outEdgeLabelName);
            Preconditions.checkState(inSchemaVertexLabelName != null, "BUG: In schema vertex not found for edge \"%s\"", outEdgeLabelName);

            Optional<EdgeLabel> outEdgeLabelOptional = this.topology.getEdgeLabel(getName(), outEdgeLabelName);
            Preconditions.checkState(outEdgeLabelOptional.isPresent(), "BUG: EdgeLabel for \"%s\" should already be loaded", getName() + "." + outEdgeLabelName);
            EdgeLabel outEdgeLabel = outEdgeLabelOptional.get();

            Optional<VertexLabel> vertexLabelOptional = this.topology.getVertexLabel(inSchemaVertexLabelName, inVertexLabelName);
            Preconditions.checkState(vertexLabelOptional.isPresent(), "BUG: VertexLabel not found for schema %s and label %s", inSchemaVertexLabelName, inVertexLabelName);
            VertexLabel inVertexLabel = vertexLabelOptional.get();

            Multiplicity multiplicity = Multiplicity.of(lowerMultiplicityProperty, upperMultiplicityProperty, uniqueMultiplicityProperty);
            inVertexLabel.addToInEdgeRoles(new EdgeRole(inVertexLabel, outEdgeLabel, Direction.IN, true, multiplicity));
        }

    }

    public JsonNode toJson() {
        ObjectNode schemaNode = Topology.OBJECT_MAPPER.createObjectNode();
        schemaNode.put("name", this.getName());
        ArrayNode vertexLabelArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
        List<VertexLabel> vertexLabels = new ArrayList<>(this.getVertexLabels().values());
        vertexLabels.sort(Comparator.comparing(VertexLabel::getName));
        for (VertexLabel vertexLabel : vertexLabels) {
            vertexLabelArrayNode.add(vertexLabel.toJson());
        }
        schemaNode.set("vertexLabels", vertexLabelArrayNode);
        return schemaNode;
    }

    Optional<JsonNode> toNotifyJson() {
        boolean foundVertexLabels = false;
        boolean foundOutEdgeLabel = false;
        ObjectNode schemaNode = Topology.OBJECT_MAPPER.createObjectNode();
        schemaNode.put("name", this.getName());

        //do EdgeLabels
        if (this.topology.isSchemaChanged()) {
            ArrayNode uncommittedEdgeLabelsArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            if (!this.uncommittedOutEdgeLabels.isEmpty()) {
                for (String edgeLabelKey : this.uncommittedOutEdgeLabels.keySet()) {
                    if (!this.uncommittedRemovedEdgeLabels.contains(edgeLabelKey)) {
                        EdgeLabel edgeLabel = this.uncommittedOutEdgeLabels.get(edgeLabelKey);
                        Optional<JsonNode> edgeLabelJsonNodeOptional = edgeLabel.toNotifyJson();
                        if (edgeLabelJsonNodeOptional.isPresent()) {
                            foundOutEdgeLabel = true;
                            uncommittedEdgeLabelsArrayNode.add(edgeLabelJsonNodeOptional.get());
                        }
                    }
                }
            }
            if (foundOutEdgeLabel) {
                schemaNode.set("uncommittedEdgeLabels", uncommittedEdgeLabelsArrayNode);
            }

            foundOutEdgeLabel = false;
            ArrayNode edgeLabelsArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            if (!this.outEdgeLabels.isEmpty()) {
                for (String edgeLabelKey : this.outEdgeLabels.keySet()) {
                    if (!this.uncommittedRemovedEdgeLabels.contains(edgeLabelKey)) {
                        EdgeLabel edgeLabel = this.outEdgeLabels.get(edgeLabelKey);
                        Optional<JsonNode> edgeLabelJsonNodeOptional = edgeLabel.toNotifyJson();
                        if (edgeLabelJsonNodeOptional.isPresent()) {
                            foundOutEdgeLabel = true;
                            edgeLabelsArrayNode.add(edgeLabelJsonNodeOptional.get());
                        }
                    }
                }
            }
            if (foundOutEdgeLabel) {
                schemaNode.set("edgeLabels", edgeLabelsArrayNode);
            }
        }

        //do VertexLabels
        if (this.topology.isSchemaChanged() && !this.getUncommittedVertexLabels().isEmpty()) {
            ArrayNode vertexLabelArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            for (VertexLabel vertexLabel : this.getUncommittedVertexLabels().values()) {
                //VertexLabel toNotifyJson always returns something even though it's an Optional.
                //This is because it extends AbstractElement's toNotifyJson that does not always return something.
                //Do not send uncommitted vertex labels that have also been removed
                if (!this.uncommittedRemovedVertexLabels.contains(this.name + "." + VERTEX_PREFIX + vertexLabel.getLabel())) {
                    @SuppressWarnings("OptionalGetWithoutIsPresent")
                    JsonNode jsonNode = vertexLabel.toNotifyJson().get();
                    vertexLabelArrayNode.add(jsonNode);
                }
            }
            schemaNode.set("uncommittedVertexLabels", vertexLabelArrayNode);
            foundVertexLabels = true;
        }
        if (this.topology.isSchemaChanged() && !this.uncommittedRemovedVertexLabels.isEmpty()) {
            ArrayNode vertexLabelArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            for (String s : this.uncommittedRemovedVertexLabels) {
                //Do not send uncommitted removed if it has also been uncommitted added.
                if (!this.getUncommittedVertexLabels().containsKey(s)) {
                    vertexLabelArrayNode.add(s);
                }
            }
            schemaNode.set("uncommittedRemovedVertexLabels", vertexLabelArrayNode);
            foundVertexLabels = true;
        }
        if (this.topology.isSchemaChanged() && !this.uncommittedRemovedEdgeLabels.isEmpty()) {
            ArrayNode edgeLabelArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            for (String s : this.uncommittedRemovedEdgeLabels) {
                edgeLabelArrayNode.add(s);
            }
            schemaNode.set("uncommittedRemovedEdgeLabels", edgeLabelArrayNode);
            foundVertexLabels = true;
        }

        if (!this.vertexLabels.isEmpty()) {
            ArrayNode vertexLabelArrayNode = Topology.OBJECT_MAPPER.createArrayNode();
            for (String s : this.vertexLabels.keySet()) {
                VertexLabel vertexLabel = this.vertexLabels.get(s);
                if (!this.uncommittedRemovedVertexLabels.contains(s)) {
                    Optional<JsonNode> notifyJsonOptional = vertexLabel.toNotifyJson();
                    if (notifyJsonOptional.isPresent()) {
                        JsonNode notifyJson = notifyJsonOptional.get();
                        if (notifyJson.get("uncommittedProperties") != null ||
                                notifyJson.get("uncommittedUpdatedProperties") != null ||
                                notifyJson.get("uncommittedOutEdgeRoles") != null ||
                                notifyJson.get("uncommittedInEdgeRoles") != null ||
                                notifyJson.get("outEdgeLabels") != null ||
                                notifyJson.get("inEdgeLabels") != null ||
                                notifyJson.get("uncommittedRemovedOutEdgeRoles") != null ||
                                notifyJson.get("uncommittedRemovedInEdgeRoles") != null
                        ) {

                            vertexLabelArrayNode.add(notifyJsonOptional.get());
                            foundVertexLabels = true;
                        }
                    }
                }
            }
            if (!vertexLabelArrayNode.isEmpty()) {
                schemaNode.set("vertexLabels", vertexLabelArrayNode);
            }
        }

        if (foundVertexLabels || foundOutEdgeLabel) {
            return Optional.of(schemaNode);
        } else {
            return Optional.empty();
        }
    }

    void fromNotifyJsonOutEdges(JsonNode jsonSchema) {

        ArrayNode uncommittedEdgeLabels = (ArrayNode) jsonSchema.get("uncommittedEdgeLabels");
        if (uncommittedEdgeLabels != null) {
            for (JsonNode uncommittedOutEdgeLabel : uncommittedEdgeLabels) {
                String edgeLabelName = uncommittedOutEdgeLabel.get("label").asText();
                Optional<EdgeLabel> edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
                EdgeLabel edgeLabel;
                if (edgeLabelOptional.isEmpty()) {
                    PartitionType partitionType = PartitionType.valueOf(uncommittedOutEdgeLabel.get("partitionType").asText());
                    if (partitionType.isNone()) {
                        edgeLabel = new EdgeLabel(this.getTopology(), edgeLabelName);
                    } else {
                        String partitionExpression = uncommittedOutEdgeLabel.get("partitionExpression").asText();
                        edgeLabel = new EdgeLabel(this.getTopology(), edgeLabelName, partitionType, partitionExpression);
                    }
                } else {
                    edgeLabel = edgeLabelOptional.get();
                }
                //Do not load the properties here as the edgeLabel is inconsistent.
                //The EdgeLabel needs its EdgeRole to be loaded before it is consistent and can be used in firing events.
                this.addToAllEdgeCache(edgeLabel);
            }
        }

        ArrayNode edgeLabels = (ArrayNode) jsonSchema.get("edgeLabels");
        if (edgeLabels != null) {
            for (JsonNode edgeLabelJson : edgeLabels) {
                String edgeLabelName = edgeLabelJson.get("label").asText();
                Optional<EdgeLabel> edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
                Preconditions.checkState(edgeLabelOptional.isPresent(), "Failed to find EdgeLabel '%s'", edgeLabelName);
                EdgeLabel edgeLabel = edgeLabelOptional.get();
                edgeLabel.fromPropertyNotifyJson(edgeLabelJson, true);
                this.addToAllEdgeCache(edgeLabel);
                this.getTopology().addToAllTables(this.getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel.getPropertyDefinitionMap());
            }
        }

        JsonNode rem = jsonSchema.get("uncommittedRemovedVertexLabels");
        if (rem != null && rem.isArray()) {
            ArrayNode an = (ArrayNode) rem;
            for (int a = 0; a < an.size(); a++) {
                String s = an.get(a).asText();
                VertexLabel lbl = this.vertexLabels.remove(s);
                if (lbl != null) {
                    this.getTopology().removeVertexLabel(lbl);
                    for (EdgeRole edgeRole : lbl.getOutEdgeRoles().values()) {
                        this.getTopology().fire(edgeRole, edgeRole, TopologyChangeAction.DELETE, false);
                        edgeRole.getEdgeLabel().outEdgeRoles.remove(edgeRole);
                    }
                    for (EdgeRole edgeRole : lbl.getInEdgeRoles().values()) {
                        this.getTopology().fire(edgeRole, edgeRole, TopologyChangeAction.DELETE, false);
                        edgeRole.getEdgeLabel().inEdgeRoles.remove(edgeRole);
                    }
                    this.getTopology().fire(lbl, lbl, TopologyChangeAction.DELETE, false);
                }
            }
        }

        rem = jsonSchema.get("uncommittedRemovedEdgeLabels");
        if (rem != null && rem.isArray()) {
            ArrayNode an = (ArrayNode) rem;
            for (int a = 0; a < an.size(); a++) {
                String s = an.get(a).asText();
                EdgeLabel edgeLabel = this.outEdgeLabels.remove(s);
                if (edgeLabel != null) {
                    for (VertexLabel lbl : edgeLabel.getOutVertexLabels()) {
                        if (edgeLabel.isValid()) {
                            EdgeRole edgeRole = lbl.outEdgeRoles.get(edgeLabel.getFullName());
                            this.getTopology().fire(edgeRole, edgeRole, TopologyChangeAction.DELETE, false);
                        }
                    }
                    for (VertexLabel lbl : edgeLabel.getInVertexLabels()) {
                        if (edgeLabel.isValid()) {
                            EdgeRole edgeRole = lbl.inEdgeRoles.get(edgeLabel.getFullName());
                            this.getTopology().fire(edgeRole, edgeRole, TopologyChangeAction.DELETE, false);
                        }
                    }
                    this.getTopology().fire(edgeLabel, edgeLabel, TopologyChangeAction.DELETE, false);
                }
            }
        }

        for (String s : Arrays.asList("vertexLabels", "uncommittedVertexLabels")) {
            JsonNode vertexLabels = jsonSchema.get(s);
            if (vertexLabels != null) {
                for (JsonNode vertexLabelJson : vertexLabels) {
                    String vertexLabelName = vertexLabelJson.get("label").asText();
                    Optional<VertexLabel> vertexLabelOptional = getVertexLabel(vertexLabelName);
                    VertexLabel vertexLabel;
                    if (vertexLabelOptional.isPresent()) {
                        vertexLabel = vertexLabelOptional.get();
                    } else {
                        PartitionType partitionType = PartitionType.valueOf(vertexLabelJson.get("partitionType").asText());
                        if (partitionType.isNone()) {
                            vertexLabel = new VertexLabel(this, vertexLabelName);
                        } else {
                            String partitionExpression = vertexLabelJson.get("partitionExpression").asText();
                            vertexLabel = new VertexLabel(this, vertexLabelName, partitionType, partitionExpression);
                        }
                        this.vertexLabels.put(this.name + "." + VERTEX_PREFIX + vertexLabelName, vertexLabel);
                        this.getTopology().fire(vertexLabel, null, TopologyChangeAction.CREATE, false);
                    }
                    //The order of the next two statements matter.
                    //fromNotifyJsonOutEdge needs to happen first to ensure the properties are on the VertexLabel
                    // fire only if we didn't create the vertex label
                    vertexLabel.fromNotifyJsonOutEdge(vertexLabelJson, vertexLabelOptional.isPresent());
                    this.getTopology().addToAllTables(this.getName() + "." + VERTEX_PREFIX + vertexLabelName, vertexLabel.getPropertyDefinitionMap());
                }
            }
        }

        //loop through all uncommittedEdgeLabels, this time loading the properties and firing the events as the EdgeLabel is now consistent.
        uncommittedEdgeLabels = (ArrayNode) jsonSchema.get("uncommittedEdgeLabels");
        if (uncommittedEdgeLabels != null) {
            for (JsonNode uncommittedOutEdgeLabel : uncommittedEdgeLabels) {
                String edgeLabelName = uncommittedOutEdgeLabel.get("label").asText();
                Optional<EdgeLabel> edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
                Preconditions.checkState(edgeLabelOptional.isPresent(), "Failed to find EdgeLabel '%s'", edgeLabelName);
                EdgeLabel edgeLabel = edgeLabelOptional.get();
                this.getTopology().fire(edgeLabel, null, TopologyChangeAction.CREATE, false);
                edgeLabel.fromPropertyNotifyJson(uncommittedOutEdgeLabel, true);
                this.getTopology().addToAllTables(this.getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel.getPropertyDefinitionMap());
            }
        }

    }

    void fromNotifyJsonInEdges(JsonNode jsonSchema) {
        JsonNode rem = jsonSchema.get("uncommittedRemovedVertexLabels");
        Set<String> removed = new HashSet<>();
        if (rem != null && rem.isArray()) {
            ArrayNode an = (ArrayNode) rem;
            for (int a = 0; a < an.size(); a++) {
                String s = an.get(a).asText();
                removed.add(s);
            }
        }
        for (String s : Arrays.asList("vertexLabels", "uncommittedVertexLabels")) {
            JsonNode vertexLabels = jsonSchema.get(s);
            if (vertexLabels != null) {
                for (JsonNode vertexLabelJson : vertexLabels) {
                    String vertexLabelName = vertexLabelJson.get("label").asText();
                    if (!removed.contains(this.name + "." + VERTEX_PREFIX + vertexLabelName)) {
                        Optional<VertexLabel> vertexLabelOptional = getVertexLabel(vertexLabelName);
                        Preconditions.checkState(vertexLabelOptional.isPresent(), "VertexLabel " + vertexLabelName + " must be present");
                        VertexLabel vertexLabel = vertexLabelOptional.get();
                        vertexLabel.fromNotifyJsonInEdge(vertexLabelJson);
                    }
                }
            }
        }
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Schema other)) {
            return false;
        }
        //only equals on the name of the schema. it is assumed that the VertexLabels (tables) are the same.
        return this.name.equals(other.name);
    }

    @Override
    public String toString() {
        return "schema: " + this.name;
    }

    void cacheEdgeLabels() {
        for (Map.Entry<String, VertexLabel> entry : this.vertexLabels.entrySet()) {
            for (EdgeLabel edgeLabel : entry.getValue().getOutEdgeLabels().values()) {
                this.outEdgeLabels.put(this.name + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel);
            }
        }
    }

    void addToAllEdgeCache(EdgeLabel edgeLabel) {
        this.outEdgeLabels.put(this.name + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel);
    }

    List<Topology.TopologyValidationError> validateTopology(DatabaseMetaData metadata) throws SQLException {
        List<Topology.TopologyValidationError> validationErrors = new ArrayList<>();
        for (VertexLabel vertexLabel : getVertexLabels().values()) {
            try (ResultSet tableRs = metadata.getTables(null, this.getName(), "V_" + vertexLabel.getLabel(), null)) {
                if (!tableRs.next()) {
                    validationErrors.add(new Topology.TopologyValidationError(vertexLabel));
                } else {
                    validationErrors.addAll(vertexLabel.validateTopology(metadata));
                    //validate edges
                    for (EdgeLabel edgeLabel : vertexLabel.getOutEdgeLabels().values()) {
                        try (ResultSet edgeRs = metadata.getTables(null, this.getName(), "E_" + edgeLabel.getLabel(), null)) {
                            if (!edgeRs.next()) {
                                validationErrors.add(new Topology.TopologyValidationError(edgeLabel));
                            } else {
                                validationErrors.addAll(edgeLabel.validateTopology(metadata));
                            }
                        }
                    }
                }
            }
        }
        return validationErrors;
    }


    @Override
    public void remove(boolean preserveData) {
        if (this.getName().equals(sqlgGraph.getSqlDialect().getPublicSchema()) && !preserveData) {
            throw new IllegalArgumentException(String.format("Public schema ('%s') cannot be deleted", sqlgGraph.getSqlDialect().getPublicSchema()));
        }
        getTopology().removeSchema(this, preserveData);
    }

    /**
     * remove a given edge label
     *
     * @param edgeLabel    the edge label
     * @param preserveData should we keep the SQL data
     */
    void removeEdgeLabel(EdgeLabel edgeLabel, boolean preserveData) {
        getTopology().startSchemaChange(
                String.format("Schema '%s' removeEdgeLabel with '%s'", getName(), edgeLabel.getName())
        );
        String fn = this.name + "." + EDGE_PREFIX + edgeLabel.getName();

        if (!this.uncommittedRemovedEdgeLabels.contains(fn)) {
            this.uncommittedRemovedEdgeLabels.add(fn);
            TopologyManager.removeEdgeLabel(this.sqlgGraph, edgeLabel);
            for (VertexLabel lbl : edgeLabel.getOutVertexLabels()) {
                lbl.removeOutEdge(edgeLabel);
            }
            for (VertexLabel lbl : edgeLabel.getInVertexLabels()) {
                lbl.removeInEdge(edgeLabel);
            }

            if (!preserveData) {
                edgeLabel.delete();
            }
            getTopology().fire(edgeLabel, edgeLabel, TopologyChangeAction.DELETE, true);
        }
    }

    /**
     * remove a given vertex label
     *
     * @param vertexLabel  the vertex label
     * @param preserveData should we keep the SQL data
     */
    void removeVertexLabel(VertexLabel vertexLabel, boolean preserveData) {
        getTopology().startSchemaChange(
                String.format("Schema '%s' removeVertexLabel with '%s'", getName(), vertexLabel.getName())
        );
        String fn = this.name + "." + VERTEX_PREFIX + vertexLabel.getName();
        if (!this.uncommittedRemovedVertexLabels.contains(fn)) {
            this.sqlgGraph.traversal().V().hasLabel(this.name + "." + vertexLabel.getLabel()).drop().iterate();
            this.uncommittedRemovedVertexLabels.add(fn);
            for (EdgeRole er : vertexLabel.getOutEdgeRoles().values()) {
                er.removeViaVertexLabelRemove(preserveData);
            }
            for (EdgeRole er : vertexLabel.getInEdgeRoles().values()) {
                er.removeViaVertexLabelRemove(preserveData);
            }
            TopologyManager.removeVertexLabel(this.sqlgGraph, vertexLabel);
            if (!preserveData) {
                vertexLabel.delete();
            }
            getTopology().fire(vertexLabel, vertexLabel, TopologyChangeAction.DELETE, true);
        }

    }

    /**
     * delete schema in DB
     */
    void delete() {
        StringBuilder sql = new StringBuilder();
        sql.append("DROP SCHEMA ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.name));
        if (this.sqlgGraph.getSqlDialect().supportsCascade()) {
            sql.append(" CASCADE");
        }
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
            LOGGER.error("schema deletion failed {}", this.sqlgGraph, e);
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("SqlSourceToSinkFlow")
    public void createTempTable(String tableName, Map<String, PropertyDefinition> columns) {
        this.sqlgGraph.getSqlDialect().assertTableName(tableName);
        StringBuilder sql = new StringBuilder(this.sqlgGraph.getSqlDialect().createTemporaryTableStatement());
        if (this.sqlgGraph.getSqlDialect().needsTemporaryTablePrefix()) {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                    this.sqlgGraph.getSqlDialect().temporaryTablePrefix() +
                            tableName));
        } else {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(tableName));
        }
        sql.append("(");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlgGraph.getSqlDialect().getAutoIncrementPrimaryKeyConstruct());
        if (!columns.isEmpty()) {
            sql.append(", ");
        }
        AbstractLabel.buildColumns(this.sqlgGraph, new ListOrderedSet<>(), columns, sql);
        sql.append(") ");
        sql.append(this.sqlgGraph.getSqlDialect().afterCreateTemporaryTableStatement());
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

    void removeTemporaryTables() {
        if (!this.sqlgGraph.getSqlDialect().supportsTemporaryTableOnCommitDrop()) {
            for (Map.Entry<String, Map<String, PropertyDefinition>> temporaryTableEntry : this.threadLocalTemporaryTables.get().entrySet()) {
                String tableName = temporaryTableEntry.getKey();
                Connection conn = this.sqlgGraph.tx().getConnection();
                try (Statement stmt = conn.createStatement()) {
                    String sql = "DROP TEMPORARY TABLE " +
                            this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()) + "." +
                            this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(tableName);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(sql);
                    }
                    stmt.execute(sql);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            }
        }
        this.threadLocalTemporaryTables.remove();
    }

    public Map<String, PropertyDefinition> getTemporaryTable(String tableName) {
        return this.threadLocalTemporaryTables.get().get(tableName);
    }

    Schema readOnlyCopyVertexLabels(SqlgGraph sqlgGraph, Topology topology) {
        Preconditions.checkState(!getTopology().isSchemaChanged(), "To make a schema copy the topology must not have any pending changes!");
        Schema foreignSchema = new Schema(sqlgGraph, topology, this.name, true);
        for (String label : this.vertexLabels.keySet()) {
            VertexLabel vertexLabel = this.vertexLabels.get(label);
            VertexLabel foreignVertexLabel = vertexLabel.readOnlyCopy(foreignSchema);
            foreignSchema.vertexLabels.put(label, foreignVertexLabel);
        }
        return foreignSchema;
    }

    void readOnlyCopyEdgeLabels(Topology topology, Schema foreignSchema, Set<Schema> foreignSchemas) {
        Preconditions.checkState(!getTopology().isSchemaChanged(), "To make a schema copy the topology must not have any pending changes!");
        for (String label : this.outEdgeLabels.keySet()) {
            EdgeLabel edgeLabel = this.outEdgeLabels.get(label);
            EdgeLabel foreignEdgeLabel = edgeLabel.readOnlyCopy(topology, foreignSchema, foreignSchemas);
            foreignSchema.addToAllEdgeCache(foreignEdgeLabel);
            foreignSchema.outEdgeLabels.put(label, foreignEdgeLabel);
        }
    }

    void importForeignVertexAndEdgeLabels(Set<VertexLabel> vertexLabels, Set<EdgeLabel> edgeLabels) {
        Preconditions.checkState(!getTopology().isSchemaChanged(), "To import a foreign VertexLabel there must not be any pending changes!");

        Set<String> fullVertexLabels = vertexLabels.stream().map(AbstractLabel::getFullName).collect(Collectors.toSet());
        Set<String> fullEdgeLabels = edgeLabels.stream().map(AbstractLabel::getFullName).collect(Collectors.toSet());

        for (VertexLabel vertexLabel : vertexLabels) {
            for (EdgeRole inEdgeRole : vertexLabel.inEdgeRoles.values()) {
                Preconditions.checkState(fullEdgeLabels.contains(inEdgeRole.getEdgeLabel().getFullName()), "'%s' is not present in the foreign EdgeLabels", this.name + "." + EDGE_PREFIX + inEdgeRole.getEdgeLabel().getFullName());
            }
            for (EdgeRole outEdgeRole : vertexLabel.outEdgeRoles.values()) {
                Preconditions.checkState(fullEdgeLabels.contains(outEdgeRole.getEdgeLabel().getFullName()), "'%s' is not present in the foreign EdgeLabels", this.name + "." + EDGE_PREFIX + outEdgeRole.getEdgeLabel().getFullName());
            }
        }
        for (EdgeLabel edgeLabel : edgeLabels) {
            for (EdgeRole inEdgeRole : edgeLabel.inEdgeRoles) {
                Preconditions.checkState(fullVertexLabels.contains(inEdgeRole.getVertexLabel().getFullName()), "'%s' is not present in the foreign VertexLabels", this.name + "." + VERTEX_PREFIX + inEdgeRole.getVertexLabel().getFullName());
            }
        }

        for (VertexLabel vertexLabel : vertexLabels) {
            VertexLabel foreignVertexLabel = vertexLabel.readOnlyCopy(this);
            this.vertexLabels.put(this.name + "." + VERTEX_PREFIX + vertexLabel.getLabel(), foreignVertexLabel);
        }
        for (EdgeLabel edgeLabel : edgeLabels) {
            EdgeLabel foreignEdgeLabel = edgeLabel.readOnlyCopy(topology, this, Set.of(this));
            Set<EdgeRole> outEdgeRoles = foreignEdgeLabel.getOutEdgeRoles();
            for (EdgeRole outEdgeRole : outEdgeRoles) {
                String l = this.getName() + "." + VERTEX_PREFIX + outEdgeRole.getVertexLabel().getLabel();
                Preconditions.checkState(this.vertexLabels.containsKey(l), "VertexLabel '%s' not found in schema '%s'.", l, this.getName());
                List<EdgeRole> vertexEdgeRoles = this.vertexLabels.get(l).getOutEdgeRoles().values().stream().filter(edgeRole -> edgeRole.getEdgeLabel().equals(edgeLabel)).toList();
                Preconditions.checkState(vertexEdgeRoles.size() == 1);
                foreignEdgeLabel.outEdgeRoles.add(vertexEdgeRoles.get(0));
            }
            Set<VertexLabel> inVertexLabels = edgeLabel.getInVertexLabels();
            for (VertexLabel inVertexLabel : inVertexLabels) {
                String l = this.getName() + "." + VERTEX_PREFIX + inVertexLabel.getLabel();
                Preconditions.checkState(this.vertexLabels.containsKey(l), "VertexLabel '%s' not found in schema '%s'.", l, this.getName());
                List<EdgeRole> vertexEdgeRoles = this.vertexLabels.get(l).getInEdgeRoles().values().stream().filter(edgeRole -> edgeRole.getEdgeLabel().equals(edgeLabel)).toList();
                Preconditions.checkState(vertexEdgeRoles.size() == 1);
                foreignEdgeLabel.inEdgeRoles.add(vertexEdgeRoles.get(0));
            }
            this.addToAllEdgeCache(foreignEdgeLabel);
            this.outEdgeLabels.put(this.name + "." + EDGE_PREFIX + edgeLabel.getLabel(), foreignEdgeLabel);
        }
    }

    static void validateImportingVertexLabels(Set<Schema> originalSchemas) {
        for (Schema foreignSchema : originalSchemas) {
            for (String label : foreignSchema.vertexLabels.keySet()) {
                VertexLabel vertexLabel = foreignSchema.vertexLabels.get(label);
                for (EdgeRole edgeRole : vertexLabel.outEdgeRoles.values()) {
                    Schema edgeSchema = edgeRole.getEdgeLabel().getSchema();
                    if (originalSchemas.stream().noneMatch(s -> s.getName().equals(edgeSchema.getName()))) {
                        throw new IllegalStateException(String.format(
                                "VertexLabel '%s' has an outEdgeLabel '%s' that is not present in a foreign schema",
                                label, edgeRole.getEdgeLabel().getLabel()));
                    }
                }
                for (EdgeRole edgeRole : vertexLabel.inEdgeRoles.values()) {
                    Schema edgeSchema = edgeRole.getEdgeLabel().getSchema();
                    if (originalSchemas.stream().noneMatch(s -> s.getName().equals(edgeSchema.getName()))) {
                        throw new IllegalStateException(String.format(
                                "VertexLabel '%s' has an inEdgeLabel '%s' that is not present in a foreign schema",
                                label, edgeRole.getEdgeLabel().getLabel()));
                    }
                }
            }
        }
    }

    static void validateImportingEdgeLabels(Set<Schema> originalSchemas) {
        for (Schema foreignSchema : originalSchemas) {
            for (String label : foreignSchema.outEdgeLabels.keySet()) {
                EdgeLabel edgeLabel = foreignSchema.outEdgeLabels.get(label);
                for (EdgeRole inEdgeRole : edgeLabel.inEdgeRoles) {
                    //Is the inVertexLabel in the current schema being imported or in an already imported schema
                    if (originalSchemas.stream().noneMatch(s -> s.getVertexLabel(inEdgeRole.getVertexLabel().getLabel()).isPresent())) {
                        throw new IllegalStateException(String.format("EdgeLabel '%s' has a inVertexLabel '%s' that is not present in a foreign schema", label, inEdgeRole.getVertexLabel().getLabel()));
                    }
                }
                for (EdgeRole outEdgeRole : edgeLabel.outEdgeRoles) {
                    //Is the outVertexLabel in the current schema being imported or in an already imported schema
                    if (originalSchemas.stream().noneMatch(s -> s.getVertexLabel(outEdgeRole.getVertexLabel().getLabel()).isPresent())) {
                        throw new IllegalStateException(String.format("EdgeLabel '%s' has a outVertexLabel '%s' that is not present in a foreign schema", label, outEdgeRole.getVertexLabel().getLabel()));
                    }
                }
            }
        }
    }

    Pair<Set<Pair<String, String>>, Set<Pair<String, String>>> clearForeignAbstractLabels() {
        Pair<Set<Pair<String, String>>, Set<Pair<String, String>>> result = Pair.of(new HashSet<>(), new HashSet<>());
        Set<String> toRemoveEdges = new HashSet<>();
        for (Map.Entry<String, EdgeLabel> edgeLabelEntry : this.getEdgeLabels().entrySet()) {
            String key = edgeLabelEntry.getKey();
            EdgeLabel edgeLabel = edgeLabelEntry.getValue();
            if (edgeLabel.isForeign()) {
                result.getRight().add(Pair.of(key, edgeLabel.getLabel()));
                toRemoveEdges.add(key);
            }
        }
        for (String toRemoveEdge : toRemoveEdges) {
            this.outEdgeLabels.remove(toRemoveEdge);
        }
        Set<String> toRemoveVertices = new HashSet<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.getVertexLabels().entrySet()) {
            String key = vertexLabelEntry.getKey();
            VertexLabel vertexLabel = vertexLabelEntry.getValue();
            if (vertexLabel.isForeign()) {
                result.getLeft().add(Pair.of(key, vertexLabel.getLabel()));
                toRemoveVertices.add(key);
            }
        }
        for (String toRemoveVertex : toRemoveVertices) {
            this.vertexLabels.remove(toRemoveVertex);
        }
        return result;
    }

}
