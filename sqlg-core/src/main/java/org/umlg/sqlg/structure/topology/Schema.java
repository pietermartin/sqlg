package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.strategy.BaseStrategy;
import org.umlg.sqlg.structure.PropertyType;
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

    private static final Logger logger = LoggerFactory.getLogger(Schema.class);
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
    private static final String MARKER = "~gremlin.incidentToAdjacent";

    //temporary table map. it is in a thread local as temporary tables are only valid per session/connection.
    private final ThreadLocal<Map<String, Map<String, PropertyType>>> threadLocalTemporaryTables = ThreadLocal.withInitial(HashMap::new);

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
     * Creates the 'public' schema that always already exist and is pre-loaded in {@link Topology()} @see {@link Topology#cacheTopology()}
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

    void ensureTemporaryVertexTableExist(final String label, final Map<String, PropertyType> columns) {
        Objects.requireNonNull(label, "Given table must not be null");
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not be prefixed with %s", VERTEX_PREFIX);

        final String prefixedTable = VERTEX_PREFIX + label;
        if (!this.threadLocalTemporaryTables.get().containsKey(prefixedTable)) {
            this.topology.startSchemaChange();
            if (!this.threadLocalTemporaryTables.get().containsKey(prefixedTable)) {
                this.threadLocalTemporaryTables.get().put(prefixedTable, columns);
                createTempTable(prefixedTable, columns);
            }
        }
    }

    public VertexLabel ensureVertexLabelExist(final String label, final Map<String, PropertyType> columns) {
        return ensureVertexLabelExist(label, columns, new ListOrderedSet<>());
    }

    public VertexLabel ensureVertexLabelExist(final String label, final Map<String, PropertyType> columns, ListOrderedSet<String> identifiers) {
        Objects.requireNonNull(label, "Given table must not be null");
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not be prefixed with \"%s\"", VERTEX_PREFIX);
        for (String identifier : identifiers) {
            Preconditions.checkState(columns.containsKey(identifier), "The identifiers must be in the specified columns. \"%s\" not found", identifier);
        }

        Optional<VertexLabel> vertexLabelOptional = this.getVertexLabel(label);
        if (vertexLabelOptional.isEmpty()) {
            Preconditions.checkState(!this.isForeignSchema, "'%s' is a read only foreign schema!", this.name);
            this.topology.startSchemaChange();
            vertexLabelOptional = this.getVertexLabel(label);
            if (vertexLabelOptional.isEmpty()) {
                return this.createVertexLabel(label, columns, identifiers);
            } else {
                return vertexLabelOptional.get();
            }
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
                vertexLabel.getPropertyTypeMap(),
                vertexLabel.getIdentifiers()
        );
        this.uncommittedVertexLabels.put(this.name + "." + VERTEX_PREFIX + label, renamedVertexLabel);
        this.getTopology().fire(renamedVertexLabel, vertexLabel, TopologyChangeAction.UPDATE);
        return renamedVertexLabel;
    }

    @SuppressWarnings("UnusedReturnValue")
    void renameEdgeLabel(EdgeLabel edgeLabel, String label) {
        Optional<EdgeLabel> edgeLabelOptional = this.getEdgeLabel(label);
        Preconditions.checkState(edgeLabelOptional.isEmpty(), "'%s' already exists", label);
        Preconditions.checkState(!this.isSqlgSchema(), "renameEdgeLabel may not be called for \"%s\"", SQLG_SCHEMA);
        Preconditions.checkArgument(!label.startsWith(EDGE_PREFIX), "edge label may not start with " + EDGE_PREFIX);
        this.sqlgGraph.getSqlDialect().validateTableName(label);
        this.uncommittedRemovedEdgeLabels.add(this.name + "." + EDGE_PREFIX + edgeLabel.label);

        Set<VertexLabel> outVertexLabels = edgeLabel.getOutVertexLabels();
        Set<VertexLabel> inVertexLabels = edgeLabel.getInVertexLabels();

        EdgeLabel renamedEdgeLabel = EdgeLabel.renameEdgeLabel(
                this.sqlgGraph,
                this,
                edgeLabel,
                label,
                outVertexLabels,
                inVertexLabels,
                edgeLabel.getPropertyTypeMap(),
                edgeLabel.getIdentifiers()
        );
        this.uncommittedOutEdgeLabels.put(this.name + "." + EDGE_PREFIX + label, renamedEdgeLabel);
        this.getTopology().fire(renamedEdgeLabel, edgeLabel, TopologyChangeAction.UPDATE);
    }

    public VertexLabel ensurePartitionedVertexLabelExist(
            final String label,
            final Map<String, PropertyType> columns,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression) {

        return ensurePartitionedVertexLabelExist(label, columns, identifiers, partitionType, partitionExpression, true);
    }

    public VertexLabel ensurePartitionedVertexLabelExist(
            final String label,
            final Map<String, PropertyType> columns,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression,
            boolean addPrimaryKeyConstraint) {

        Preconditions.checkState(this.sqlgGraph.getSqlDialect().supportsPartitioning());
        Objects.requireNonNull(label, "Given table must not be null");
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not be prefixed with %s", VERTEX_PREFIX);

        Optional<VertexLabel> vertexLabelOptional = this.getVertexLabel(label);
        if (vertexLabelOptional.isEmpty()) {
            this.topology.startSchemaChange();
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
            Map<String, PropertyType> columns) {
        return ensureEdgeLabelExist(edgeLabelName, outVertexLabel, inVertexLabel, columns, new ListOrderedSet<>());
    }

    public EdgeLabel ensureEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyType> columns,
            ListOrderedSet<String> identifiers) {

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
            this.topology.startSchemaChange();
            edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
            if (edgeLabelOptional.isEmpty()) {
                edgeLabel = this.createEdgeLabel(edgeLabelName, outVertexLabel, inVertexLabel, columns, identifiers);
                this.uncommittedRemovedEdgeLabels.remove(this.name + "." + EDGE_PREFIX + edgeLabelName);
                this.uncommittedOutEdgeLabels.put(this.name + "." + EDGE_PREFIX + edgeLabelName, edgeLabel);
                this.getTopology().fire(edgeLabel, null, TopologyChangeAction.CREATE);
                //nothing more to do as the edge did not exist and will have been created with the correct foreign keys.
            } else {
                edgeLabel = internalEnsureEdgeTableExists(edgeLabelOptional.get(), outVertexLabel, inVertexLabel, columns);
            }
        } else {
            edgeLabel = internalEnsureEdgeTableExists(edgeLabelOptional.get(), outVertexLabel, inVertexLabel, columns);
        }
        return edgeLabel;
    }

    public EdgeLabel ensureEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyType> columns,
            ListOrderedSet<String> identifiers,
            int shardCount,
            String distributionColumn,
            AbstractLabel colocate) {

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
            this.topology.startSchemaChange();
            edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
            if (edgeLabelOptional.isEmpty()) {
                edgeLabel = this.createEdgeLabel(edgeLabelName, outVertexLabel, inVertexLabel, columns, identifiers);
                this.uncommittedRemovedEdgeLabels.remove(this.name + "." + EDGE_PREFIX + edgeLabelName);
                this.uncommittedOutEdgeLabels.put(this.name + "." + EDGE_PREFIX + edgeLabelName, edgeLabel);
                this.getTopology().fire(edgeLabel, null, TopologyChangeAction.CREATE);
                //nothing more to do as the edge did not exist and will have been created with the correct foreign keys.
            } else {
                edgeLabel = internalEnsureEdgeTableExists(edgeLabelOptional.get(), outVertexLabel, inVertexLabel, columns);
            }
        } else {
            edgeLabel = internalEnsureEdgeTableExists(edgeLabelOptional.get(), outVertexLabel, inVertexLabel, columns);
        }
        return edgeLabel;
    }

    public EdgeLabel ensurePartitionedEdgeLabelExistOnInOrOutVertexLabel(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyType> columns,
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
                true
        );
    }

    public EdgeLabel ensurePartitionedEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyType> columns,
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
                false);
    }

    public EdgeLabel ensurePartitionedEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyType> columns,
            ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression,
            boolean isForeignKeyPartition) {

        Preconditions.checkState(this.sqlgGraph.getSqlDialect().supportsPartitioning());
        Objects.requireNonNull(edgeLabelName, "Given edgeLabelName may not be null");
        Objects.requireNonNull(outVertexLabel, "Given outVertexLabel may not be null");
        Objects.requireNonNull(inVertexLabel, "Given inVertexLabel may not be null");

        EdgeLabel edgeLabel;
        Optional<EdgeLabel> edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
        if (edgeLabelOptional.isEmpty()) {
            this.topology.startSchemaChange();
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
                        isForeignKeyPartition);
                this.uncommittedRemovedEdgeLabels.remove(this.name + "." + EDGE_PREFIX + edgeLabelName);
                this.uncommittedOutEdgeLabels.put(this.name + "." + EDGE_PREFIX + edgeLabelName, edgeLabel);
                this.getTopology().fire(edgeLabel, null, TopologyChangeAction.CREATE);
                //nothing more to do as the edge did not exist and will have been created with the correct foreign keys.
            } else {
                edgeLabel = internalEnsureEdgeTableExists(edgeLabelOptional.get(), outVertexLabel, inVertexLabel, columns);
            }
        } else {
            edgeLabel = internalEnsureEdgeTableExists(edgeLabelOptional.get(), outVertexLabel, inVertexLabel, columns);
        }
        return edgeLabel;
    }

    private EdgeLabel internalEnsureEdgeTableExists(EdgeLabel edgeLabel, VertexLabel outVertexLabel, VertexLabel inVertexLabel, Map<String, PropertyType> columns) {
        edgeLabel.ensureEdgeVertexLabelExist(Direction.OUT, outVertexLabel);
        edgeLabel.ensureEdgeVertexLabelExist(Direction.IN, inVertexLabel);
        edgeLabel.ensurePropertiesExist(columns);
        return edgeLabel;
    }

    private EdgeLabel createPartitionedEdgeLabel(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            final Map<String, PropertyType> columns,
            final ListOrderedSet<String> identifiers,
            PartitionType partitionType,
            String partitionExpression,
            boolean isForeignKeyPartition) {

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
                partitionExpression);
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
                isForeignKeyPartition
        );

    }

    private EdgeLabel createEdgeLabel(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            final Map<String, PropertyType> columns,
            final ListOrderedSet<String> identifiers) {

        Preconditions.checkArgument(this.topology.isSchemaChanged(), "Schema.createEdgeLabel must have schemaChanged = true");
        Preconditions.checkArgument(!edgeLabelName.startsWith(EDGE_PREFIX), "edgeLabelName may not start with " + EDGE_PREFIX);
        Preconditions.checkState(!this.isSqlgSchema(), "createEdgeLabel may not be called for \"%s\"", SQLG_SCHEMA);

        Schema inVertexSchema = inVertexLabel.getSchema();

        //Edge may not already exist.
        Preconditions.checkState(getEdgeLabel(edgeLabelName).isEmpty(), "BUG: Edge \"%s\" already exists!", edgeLabelName);

        SchemaTable foreignKeyOut = SchemaTable.of(this.name, outVertexLabel.getLabel());
        SchemaTable foreignKeyIn = SchemaTable.of(inVertexSchema.name, inVertexLabel.getLabel());

        TopologyManager.addEdgeLabel(this.sqlgGraph, this.getName(), EDGE_PREFIX + edgeLabelName, foreignKeyOut, foreignKeyIn, columns, identifiers);
        if (this.sqlgGraph.getSqlDialect().needsSchemaCreationPrecommit()) {
            try {
                this.sqlgGraph.tx().getConnection().commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return outVertexLabel.addEdgeLabel(edgeLabelName, inVertexLabel, columns, identifiers);
    }

    VertexLabel createSqlgSchemaVertexLabel(String vertexLabelName, Map<String, PropertyType> columns) {
        Preconditions.checkState(this.isSqlgSchema(), "createSqlgSchemaVertexLabel may only be called for \"%s\"", SQLG_SCHEMA);
        Preconditions.checkArgument(!vertexLabelName.startsWith(VERTEX_PREFIX), "vertex label may not start with " + VERTEX_PREFIX);
        VertexLabel vertexLabel = VertexLabel.createSqlgSchemaVertexLabel(this, vertexLabelName, columns);
        this.vertexLabels.put(this.name + "." + VERTEX_PREFIX + vertexLabelName, vertexLabel);
        return vertexLabel;
    }

    private VertexLabel createVertexLabel(String vertexLabelName, Map<String, PropertyType> columns, ListOrderedSet<String> identifiers) {
        Preconditions.checkState(!this.isSqlgSchema(), "createVertexLabel may not be called for \"%s\"", SQLG_SCHEMA);
        Preconditions.checkArgument(!vertexLabelName.startsWith(VERTEX_PREFIX), "vertex label may not start with " + VERTEX_PREFIX);
        this.sqlgGraph.getSqlDialect().validateTableName(vertexLabelName);
        for (String columnName : columns.keySet()) {
            this.sqlgGraph.getSqlDialect().validateColumnName(columnName);
        }

        this.uncommittedRemovedVertexLabels.remove(this.name + "." + VERTEX_PREFIX + vertexLabelName);
        VertexLabel vertexLabel = VertexLabel.createVertexLabel(this.sqlgGraph, this, vertexLabelName, columns, identifiers);
        this.uncommittedVertexLabels.put(this.name + "." + VERTEX_PREFIX + vertexLabelName, vertexLabel);
        this.getTopology().fire(vertexLabel, null, TopologyChangeAction.CREATE);
        return vertexLabel;
    }

    private VertexLabel createPartitionedVertexLabel(
            String vertexLabelName,
            Map<String, PropertyType> columns,
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
        this.getTopology().fire(vertexLabel, null, TopologyChangeAction.CREATE);
        return vertexLabel;
    }

    void ensureVertexColumnsExist(String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not start with \"%s\"", VERTEX_PREFIX);
        Preconditions.checkState(!isSqlgSchema(), "Schema.ensureVertexLabelPropertiesExist may not be called for \"%s\"", SQLG_SCHEMA);

        Optional<VertexLabel> vertexLabel = getVertexLabel(label);
        Preconditions.checkState(vertexLabel.isPresent(), "BUG: vertexLabel \"%s\" must exist", label);
        vertexLabel.get().ensurePropertiesExist(columns);
    }

    void ensureEdgeColumnsExist(String label, Map<String, PropertyType> columns) {
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
            for (VertexLabel vertexLabel : this.vertexLabels.values()) {
                result.putAll(vertexLabel.getUncommittedOutEdgeLabels());
            }
            for (VertexLabel vertexLabel : this.uncommittedVertexLabels.values()) {
                result.putAll(vertexLabel.getUncommittedOutEdgeLabels());
            }
            for (String e : uncommittedRemovedEdgeLabels) {
                result.remove(e);
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
    Map<String, Map<String, PropertyType>> getAllTables() {
        Map<String, Map<String, PropertyType>> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {
            String vertexQualifiedName = this.name + "." + VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
            result.put(vertexQualifiedName, vertexLabelEntry.getValue().getPropertyTypeMap());
        }
        if (this.topology.isSchemaChanged()) {
            for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {
                String vertexQualifiedName = vertexLabelEntry.getKey();
                VertexLabel vertexLabel = vertexLabelEntry.getValue();
                result.put(vertexQualifiedName, vertexLabel.getPropertyTypeMap());
            }
        }
        for (EdgeLabel edgeLabel : this.getEdgeLabels().values()) {
            String edgeQualifiedName = this.name + "." + EDGE_PREFIX + edgeLabel.getLabel();
            result.put(edgeQualifiedName, edgeLabel.getPropertyTypeMap());
        }
        return result;
    }

    public Map<String, VertexLabel> getVertexLabels() {
        Map<String, VertexLabel> result = new HashMap<>(this.vertexLabels);
        if (this.topology.isSchemaChanged()) {
            result.putAll(this.uncommittedVertexLabels);
            for (String e : uncommittedRemovedVertexLabels) {
                result.remove(e);
            }
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

    Map<String, PropertyType> getTableFor(SchemaTable schemaTable) {
        Preconditions.checkArgument(schemaTable.getTable().startsWith(VERTEX_PREFIX) || schemaTable.getTable().startsWith(EDGE_PREFIX), "label must start with \"%s\" or \"%s\"", VERTEX_PREFIX, EDGE_PREFIX);
        if (schemaTable.isVertexTable()) {
            Optional<VertexLabel> vertexLabelOptional = getVertexLabel(schemaTable.withOutPrefix().getTable());
            if (vertexLabelOptional.isPresent()) {
                return vertexLabelOptional.get().getPropertyTypeMap();
            }
        } else {
            Optional<EdgeLabel> edgeLabelOptional = getEdgeLabel(schemaTable.withOutPrefix().getTable());
            if (edgeLabelOptional.isPresent()) {
                return edgeLabelOptional.get().getPropertyTypeMap();
            }
        }
        return Collections.emptyMap();
    }

    Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getTableLabels() {
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = new HashMap<>();
        for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.vertexLabels.entrySet()) {
            Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with " + VERTEX_PREFIX);
            String prefixedVertexName = VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
            SchemaTable schemaTable = SchemaTable.of(this.getName(), prefixedVertexName);
            result.put(schemaTable, vertexLabelEntry.getValue().getTableLabels());
        }
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> uncommittedResult = new HashMap<>();
        if (this.topology.isSchemaChanged()) {
            for (Map.Entry<String, VertexLabel> vertexLabelEntry : this.uncommittedVertexLabels.entrySet()) {
                Preconditions.checkState(!vertexLabelEntry.getValue().getLabel().startsWith(VERTEX_PREFIX), "vertexLabel may not start with " + VERTEX_PREFIX);
                String prefixedVertexName = VERTEX_PREFIX + vertexLabelEntry.getValue().getLabel();
                SchemaTable schemaTable = SchemaTable.of(this.getName(), prefixedVertexName);
                uncommittedResult.put(schemaTable, vertexLabelEntry.getValue().getTableLabels());
            }
        }
        //need to fromNotifyJson in the uncommitted table labels in.
        for (Map.Entry<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> schemaTablePairEntry : uncommittedResult.entrySet()) {
            SchemaTable schemaTable = schemaTablePairEntry.getKey();
            Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedForeignKeys = schemaTablePairEntry.getValue();
            Pair<Set<SchemaTable>, Set<SchemaTable>> foreignKeys = result.get(schemaTable);
            if (foreignKeys != null) {
                foreignKeys.getLeft().addAll(uncommittedForeignKeys.getLeft());
                foreignKeys.getRight().addAll(uncommittedForeignKeys.getRight());
            } else {
                result.put(schemaTable, uncommittedForeignKeys);
            }
        }
        return result;
    }

    Map<String, Set<ForeignKey>> getAllEdgeForeignKeys() {
        Map<String, Set<ForeignKey>> result = new ConcurrentHashMap<>();
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

    void loadVertexOutEdgesAndProperties(GraphTraversalSource traversalSource, Vertex schemaVertex) {
        //First load the vertex and its properties
        Map<String, Partition> partitionMap = new HashMap<>();
        List<Path> vertices = traversalSource
                .V(schemaVertex)
                .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("vertex")
                //a vertex does not necessarily have properties so use optional.
                .optional(
                        __.outE(
                                        SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE,
                                        SQLG_SCHEMA_VERTEX_IDENTIFIER_EDGE,
                                        SQLG_SCHEMA_VERTEX_PARTITION_EDGE,
                                        SQLG_SCHEMA_VERTEX_DISTRIBUTION_COLUMN_EDGE,
                                        SQLG_SCHEMA_VERTEX_DISTRIBUTION_COLOCATE_EDGE
                                ).as("edgeToProperty").otherV().as("property_partition")
                                .optional(
                                        __.repeat(__.out(SQLG_SCHEMA_PARTITION_PARTITION_EDGE)).emit().as("subPartition")
                                )
                )
                .path()
                .toList();
        for (Path vertexPath : vertices) {
            Vertex vertexVertex = null;
            Vertex vertexPropertyPartitionVertex = null;
            Vertex partitionParentVertex = null;
            Element partitionParentParentElement = null;
            Vertex subPartition = null;
            Edge edgeToIdentifierOrColocate = null;
            List<Set<String>> labelsList = vertexPath.labels();
            for (Set<String> labels : labelsList) {
                for (String label : labels) {
                    switch (label) {
                        case "vertex":
                            vertexVertex = vertexPath.get("vertex");
                            break;
                        case "property_partition":
                            vertexPropertyPartitionVertex = vertexPath.get("property_partition");
                            break;
                        case BaseStrategy.SQLG_PATH_FAKE_LABEL:
                            break;
                        case "subPartition":
                            Preconditions.checkState(vertexPropertyPartitionVertex != null);
                            subPartition = vertexPath.get("subPartition");
                            partitionParentVertex = vertexPath.get(vertexPath.size() - 2);
                            partitionParentParentElement = vertexPath.get(vertexPath.size() - 3);
                            break;
                        case "edgeToProperty":
                            edgeToIdentifierOrColocate = vertexPath.get("edgeToProperty");
                            break;
                        case MARKER:
                            break;
                        case "sqlgPathTempFakeLabel":
                            break;
                        default:
                            throw new IllegalStateException(String.format("BUG: Only \"vertex\", \"property\" and \"partition\" are expected as a label. Found %s", label));
                    }
                }
            }
            Preconditions.checkState(vertexVertex != null, "BUG: Topology vertex not found.");
            String schemaName = schemaVertex.value(SQLG_SCHEMA_SCHEMA_NAME);
            String tableName = vertexVertex.value(SQLG_SCHEMA_VERTEX_LABEL_NAME);
            PartitionType partitionType = PartitionType.valueOf(vertexVertex.value(SQLG_SCHEMA_VERTEX_LABEL_PARTITION_TYPE));
            VertexProperty<String> partitionExpression = vertexVertex.property(SQLG_SCHEMA_VERTEX_LABEL_PARTITION_EXPRESSION);
            VertexProperty<Integer> shardCount = vertexVertex.property(SQLG_SCHEMA_VERTEX_LABEL_DISTRIBUTION_SHARD_COUNT);
            VertexLabel vertexLabel;
            vertexLabel = this.vertexLabels.get(schemaName + "." + VERTEX_PREFIX + tableName);
            if (vertexLabel == null) {
                if (!partitionType.isNone()) {
                    vertexLabel = new VertexLabel(this, tableName, partitionType, partitionExpression.value());
                } else {
                    vertexLabel = new VertexLabel(this, tableName);
                }
                if (shardCount.isPresent()) {
                    vertexLabel.setShardCount(shardCount.value());
                }
                this.vertexLabels.put(schemaName + "." + VERTEX_PREFIX + tableName, vertexLabel);
            }
            if (vertexPropertyPartitionVertex != null) {
                if (vertexPropertyPartitionVertex.label().equals("sqlg_schema.property")) {
                    //load the property
                    //Because there are multiple edges to the same property, identifier and distribution properties will be loaded multiple times.
                    //Its ok because of set semantics.
                    vertexLabel.addProperty(vertexPropertyPartitionVertex);
                    //Check if the property is an identifier (primary key)
                    if (edgeToIdentifierOrColocate != null) {
                        if (edgeToIdentifierOrColocate.label().equals(SQLG_SCHEMA_VERTEX_IDENTIFIER_EDGE)) {
                            vertexLabel.addIdentifier(vertexPropertyPartitionVertex.value(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME), edgeToIdentifierOrColocate.value(Topology.SQLG_SCHEMA_VERTEX_IDENTIFIER_INDEX_EDGE));
                        } else if (edgeToIdentifierOrColocate.label().equals(SQLG_SCHEMA_VERTEX_DISTRIBUTION_COLUMN_EDGE)) {
                            vertexLabel.addDistributionProperty(vertexPropertyPartitionVertex);
                        }
                    }
                } else if (edgeToIdentifierOrColocate != null && edgeToIdentifierOrColocate.label().equals("vertex_colocate")) {
                    Preconditions.checkState(vertexPropertyPartitionVertex.label().equals("sqlg_schema.vertex"));
                    vertexLabel.addDistributionColocate(vertexPropertyPartitionVertex);
                } else if (!partitionMap.containsKey(vertexPropertyPartitionVertex.<String>value(SQLG_SCHEMA_PARTITION_NAME)) && (partitionParentParentElement == null || partitionParentParentElement.label().equals("vertex_partition"))) {
                    Partition partition = vertexLabel.addPartition(vertexPropertyPartitionVertex);
                    partitionMap.put(partition.getName(), partition);
                }
            }
            if (subPartition != null) {
                Partition partition = partitionMap.get(partitionParentVertex.<String>value(SQLG_SCHEMA_PARTITION_NAME));
                Preconditions.checkState(partition != null, "Partition %s not found", partitionParentVertex.<String>value(SQLG_SCHEMA_PARTITION_NAME));
                Partition partition1 = partition.addPartition(subPartition);
                partitionMap.put(partition1.getName(), partition1);
            }
        }


        partitionMap.clear();
        //Load the out edges. This will load all edges as all edges have a out vertex.
        List<Path> outEdges = traversalSource
                .V(schemaVertex)
                .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("vertex")
                //a vertex does not necessarily have properties so use optional.
                .optional(
                        __.out(SQLG_SCHEMA_OUT_EDGES_EDGE).as("outEdgeVertex")
                                .optional(
                                        __.outE(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE,
                                                        SQLG_SCHEMA_EDGE_IDENTIFIER_EDGE,
                                                        SQLG_SCHEMA_EDGE_PARTITION_EDGE,
                                                        SQLG_SCHEMA_EDGE_DISTRIBUTION_COLUMN_EDGE,
                                                        SQLG_SCHEMA_EDGE_LABEL_DISTRIBUTION_SHARD_COUNT,
                                                        SQLG_SCHEMA_EDGE_DISTRIBUTION_COLOCATE_EDGE
                                                ).as("edge_identifier").otherV().as("property_partition")
                                                .optional(
                                                        __.repeat(__.out(SQLG_SCHEMA_PARTITION_PARTITION_EDGE)).emit().as("subPartition")
                                                )
                                )
                )
                .path()
                .toList();
        for (Path outEdgePath : outEdges) {
            List<Set<String>> labelsList = outEdgePath.labels();
            Vertex vertexVertex = null;
            Vertex outEdgeVertex = null;
            Vertex edgePropertyPartitionVertex = null;
            Vertex partitionParentVertex = null;
            Element partitionParentParentElement = null;
            Vertex subPartition = null;
            Edge edgeIdentifierEdge = null;
            for (Set<String> labels : labelsList) {
                for (String label : labels) {
                    switch (label) {
                        case "vertex":
                            vertexVertex = outEdgePath.get("vertex");
                            break;
                        case "outEdgeVertex":
                            outEdgeVertex = outEdgePath.get("outEdgeVertex");
                            break;
                        case "property_partition":
                            edgePropertyPartitionVertex = outEdgePath.get("property_partition");
                            break;
                        case "subPartition":
                            Preconditions.checkState(edgePropertyPartitionVertex != null);
                            subPartition = outEdgePath.get("subPartition");
                            partitionParentVertex = outEdgePath.get(outEdgePath.size() - 2);
                            partitionParentParentElement = outEdgePath.get(outEdgePath.size() - 3);
                            break;
                        case "edge_identifier":
                            edgeIdentifierEdge = outEdgePath.get("edge_identifier");
                            break;
                        case BaseStrategy.SQLG_PATH_FAKE_LABEL:
                        case MARKER:
                        case "sqlgPathTempFakeLabel":
                            break;
                        default:
                            throw new IllegalStateException(String.format("BUG: Only \"vertex\", \"outEdgeVertex\" and \"property\" is expected as a label. Found \"%s\"", label));
                    }
                }
            }
            Preconditions.checkState(vertexVertex != null, "BUG: Topology vertex not found.");
            String schemaName = schemaVertex.value(SQLG_SCHEMA_SCHEMA_NAME);
            String tableName = vertexVertex.value(SQLG_SCHEMA_VERTEX_LABEL_NAME);
            VertexLabel vertexLabel = this.vertexLabels.get(schemaName + "." + VERTEX_PREFIX + tableName);
            Preconditions.checkState(vertexLabel != null, "vertexLabel must be present when loading outEdges. Not found for \"%s\"", schemaName + "." + VERTEX_PREFIX + tableName);
            if (outEdgeVertex != null) {
                //load the EdgeLabel
                String edgeLabelName = outEdgeVertex.value(SQLG_SCHEMA_EDGE_LABEL_NAME);
                PartitionType partitionType = PartitionType.valueOf(outEdgeVertex.value(SQLG_SCHEMA_EDGE_LABEL_PARTITION_TYPE));
                VertexProperty<String> partitionExpression = outEdgeVertex.property(SQLG_SCHEMA_EDGE_LABEL_PARTITION_EXPRESSION);
                VertexProperty<Integer> shardCount = outEdgeVertex.property(SQLG_SCHEMA_EDGE_LABEL_DISTRIBUTION_SHARD_COUNT);
                Optional<EdgeLabel> edgeLabelOptional = this.getEdgeLabel(edgeLabelName);
                EdgeLabel edgeLabel;
                if (edgeLabelOptional.isEmpty()) {
                    if (partitionType.isNone()) {
                        edgeLabel = EdgeLabel.loadFromDb(vertexLabel.getSchema().getTopology(), edgeLabelName);
                    } else {
                        Preconditions.checkState(partitionExpression.isPresent());
                        edgeLabel = EdgeLabel.loadFromDb(vertexLabel.getSchema().getTopology(), edgeLabelName, partitionType, partitionExpression.value());
                    }
                    vertexLabel.addToOutEdgeLabels(schemaName, edgeLabel);
                } else {
                    edgeLabel = edgeLabelOptional.get();
                    vertexLabel.addToOutEdgeLabels(schemaName, edgeLabel);
                }
                if (shardCount.isPresent()) {
                    edgeLabel.setShardCount(shardCount.value());
                }
                if (edgePropertyPartitionVertex != null) {
                    if (edgePropertyPartitionVertex.label().equals("sqlg_schema.property")) {
                        //load the property
                        edgeLabel.addProperty(edgePropertyPartitionVertex);
                        //Check if the property is an identifier (primary key)
                        if (edgeIdentifierEdge != null && edgeIdentifierEdge.label().equals("edge_identifier")) {
                            edgeLabel.addIdentifier(edgePropertyPartitionVertex.value(Topology.SQLG_SCHEMA_EDGE_LABEL_NAME), edgeIdentifierEdge.value(Topology.SQLG_SCHEMA_EDGE_IDENTIFIER_INDEX_EDGE));
                        } else if (edgeIdentifierEdge != null && edgeIdentifierEdge.label().equals(SQLG_SCHEMA_EDGE_DISTRIBUTION_COLUMN_EDGE)) {
                            edgeLabel.addDistributionProperty(edgePropertyPartitionVertex);
                        }
                    } else if (edgeIdentifierEdge != null && edgeIdentifierEdge.label().equals("edge_colocate")) {
                        Preconditions.checkState(edgePropertyPartitionVertex.label().equals("sqlg_schema.vertex"));
                        edgeLabel.addDistributionColocate(edgePropertyPartitionVertex);
                    } else if (!partitionMap.containsKey(edgePropertyPartitionVertex.<String>value(SQLG_SCHEMA_PARTITION_NAME)) && (partitionParentParentElement == null || partitionParentParentElement.label().equals("edge_partition"))) {
                        Partition partition = edgeLabel.addPartition(edgePropertyPartitionVertex);
                        partitionMap.put(partition.getName(), partition);
                    }
                }
                if (subPartition != null) {
                    Partition partition = partitionMap.get(partitionParentVertex.<String>value(SQLG_SCHEMA_PARTITION_NAME));
                    Preconditions.checkState(partition != null, "Partition %s not found", partitionParentVertex.<String>value(SQLG_SCHEMA_PARTITION_NAME));
                    Partition partition1 = partition.addPartition(subPartition);
                    partitionMap.put(partition1.getName(), partition1);
                }
                this.outEdgeLabels.put(schemaName + "." + EDGE_PREFIX + edgeLabelName, edgeLabel);
            }
        }
        //We can clear all AbstractLabel.identifierMap to save some memory
        for (VertexLabel vertexLabel : this.vertexLabels.values()) {
            vertexLabel.clearIdentifiersMap();
        }
        for (EdgeLabel edgeLabel : this.outEdgeLabels.values()) {
            edgeLabel.clearIdentifiersMap();
        }
    }

    /**
     * load indices for all vertices in schema
     */
    void loadVertexIndices(GraphTraversalSource traversalSource, Vertex schemaVertex) {
        List<Path> indices = traversalSource
                .V(schemaVertex)
                .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("vertex")
                .out(SQLG_SCHEMA_VERTEX_INDEX_EDGE).as("index")
                .outE(SQLG_SCHEMA_INDEX_PROPERTY_EDGE)
                .order().by(SQLG_SCHEMA_INDEX_PROPERTY_EDGE_SEQUENCE)
                .inV().as("property")
                .path()
                .toList();
        for (Path vertexIndices : indices) {
            Vertex vertexVertex = null;
            Vertex vertexIndex = null;
            Vertex propertyIndex = null;
            List<Set<String>> labelsList = vertexIndices.labels();
            for (Set<String> labels : labelsList) {
                for (String label : labels) {
                    switch (label) {
                        case "vertex":
                            vertexVertex = vertexIndices.get("vertex");
                            break;
                        case "index":
                            vertexIndex = vertexIndices.get("index");
                            break;
                        case "property":
                            propertyIndex = vertexIndices.get("property");
                            break;
                        case BaseStrategy.SQLG_PATH_FAKE_LABEL:
                        case BaseStrategy.SQLG_PATH_ORDER_RANGE_LABEL:
                        case Schema.MARKER:
                            break;
                        default:
                            throw new IllegalStateException(String.format("BUG: Only \"vertex\",\"index\" and \"property\" is expected as a label. Found %s", label));
                    }
                }
            }
            Preconditions.checkState(vertexVertex != null, "BUG: Topology vertex not found.");
            String schemaName = schemaVertex.value(SQLG_SCHEMA_SCHEMA_NAME);
            String tableName = vertexVertex.value(SQLG_SCHEMA_VERTEX_LABEL_NAME);
            VertexLabel vertexLabel = this.vertexLabels.get(schemaName + "." + VERTEX_PREFIX + tableName);
            if (vertexLabel == null) {
                vertexLabel = new VertexLabel(this, tableName);
                this.vertexLabels.put(schemaName + "." + VERTEX_PREFIX + tableName, vertexLabel);
            }
            if (vertexIndex != null) {
                String indexName = vertexIndex.value(SQLG_SCHEMA_INDEX_NAME);
                Optional<Index> oidx = vertexLabel.getIndex(indexName);
                Index idx;
                if (oidx.isPresent()) {
                    idx = oidx.get();
                } else {
                    idx = new Index(indexName, IndexType.fromString(vertexIndex.value(SQLG_SCHEMA_INDEX_INDEX_TYPE)), vertexLabel);
                    vertexLabel.addIndex(idx);
                }
                if (propertyIndex != null) {
                    String propertyName = propertyIndex.value(SQLG_SCHEMA_PROPERTY_NAME);
                    vertexLabel.getProperty(propertyName).ifPresent(idx::addProperty);
                }

            }
        }
    }

    void loadInEdgeLabels(GraphTraversalSource traversalSource, Vertex schemaVertex) {
        //Load the in edges via the out edges. This is necessary as the out vertex is needed to know the schema the edge is in.
        //As all edges are already loaded via the out edges this will only set the in edge association.
        List<Path> inEdges = traversalSource
                .V(schemaVertex)
                .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("vertex")
                //a vertex does not necessarily have properties so use optional.
                .optional(
                        __.out(SQLG_SCHEMA_OUT_EDGES_EDGE).as("outEdgeVertex")
                                .in(SQLG_SCHEMA_IN_EDGES_EDGE).as("inVertex")
                                .in(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("inSchema")
                )
                .path()
                .toList();
        for (Path inEdgePath : inEdges) {
            List<Set<String>> labelsList = inEdgePath.labels();
            Vertex vertexVertex = null;
            Vertex outEdgeVertex = null;
            Vertex inVertex = null;
            Vertex inSchemaVertex = null;
            for (Set<String> labels : labelsList) {
                for (String label : labels) {
                    switch (label) {
                        case "vertex":
                            vertexVertex = inEdgePath.get("vertex");
                            break;
                        case "outEdgeVertex":
                            outEdgeVertex = inEdgePath.get("outEdgeVertex");
                            break;
                        case "inVertex":
                            inVertex = inEdgePath.get("inVertex");
                            break;
                        case "inSchema":
                            inSchemaVertex = inEdgePath.get("inSchema");
                            break;
                        case BaseStrategy.SQLG_PATH_FAKE_LABEL:
                        case MARKER:
                            break;
                        default:
                            throw new IllegalStateException(String.format("BUG: Only \"vertex\", \"outEdgeVertex\" and \"inVertex\" are expected as a label. Found %s", label));
                    }
                }
            }
            Preconditions.checkState(vertexVertex != null, "BUG: Topology vertex not found.");
            String schemaName = schemaVertex.value(SQLG_SCHEMA_SCHEMA_NAME);
            String tableName = vertexVertex.value(SQLG_SCHEMA_VERTEX_LABEL_NAME);
            VertexLabel vertexLabel = this.vertexLabels.get(schemaName + "." + VERTEX_PREFIX + tableName);
            Preconditions.checkState(vertexLabel != null, "vertexLabel must be present when loading inEdges. Not found for %s", schemaName + "." + VERTEX_PREFIX + tableName);
            if (outEdgeVertex != null) {
                String edgeLabelName = outEdgeVertex.value(SQLG_SCHEMA_EDGE_LABEL_NAME);

                //inVertex and inSchema must be present.
                Preconditions.checkState(inVertex != null, "BUG: In vertex not found edge for \"%s\"", edgeLabelName);
                Preconditions.checkState(inSchemaVertex != null, "BUG: In schema vertex not found for edge \"%s\"", edgeLabelName);

                Optional<EdgeLabel> outEdgeLabelOptional = this.topology.getEdgeLabel(getName(), edgeLabelName);
                Preconditions.checkState(outEdgeLabelOptional.isPresent(), "BUG: EdgeLabel for \"%s\" should already be loaded", getName() + "." + edgeLabelName);
                EdgeLabel outEdgeLabel = outEdgeLabelOptional.get();

                String inVertexLabelName = inVertex.value(SQLG_SCHEMA_VERTEX_LABEL_NAME);
                String inSchemaVertexLabelName = inSchemaVertex.value(SQLG_SCHEMA_SCHEMA_NAME);
                Optional<VertexLabel> vertexLabelOptional = this.topology.getVertexLabel(inSchemaVertexLabelName, inVertexLabelName);
                Preconditions.checkState(vertexLabelOptional.isPresent(), "BUG: VertexLabel not found for schema %s and label %s", inSchemaVertexLabelName, inVertexLabelName);
                VertexLabel inVertexLabel = vertexLabelOptional.get();

                inVertexLabel.addToInEdgeLabels(outEdgeLabel);
            }
        }
    }

    /**
     * load indices for (out) edges on all vertices of schema
     */
    void loadEdgeIndices(GraphTraversalSource traversalSource, Vertex schemaVertex) {
        List<Path> indices = traversalSource
                .V(schemaVertex)
                .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("vertex")
                .out(SQLG_SCHEMA_OUT_EDGES_EDGE).as("outEdgeVertex")
                .out(SQLG_SCHEMA_EDGE_INDEX_EDGE).as("index")
                .outE(SQLG_SCHEMA_INDEX_PROPERTY_EDGE)
                .order().by(SQLG_SCHEMA_INDEX_PROPERTY_EDGE_SEQUENCE)
                .inV().as("property")
                .path()
                .toList();
        for (Path vertexIndices : indices) {
            Vertex vertexVertex = null;
            Vertex vertexEdge = null;
            Vertex vertexIndex = null;
            Vertex propertyIndex = null;
            List<Set<String>> labelsList = vertexIndices.labels();
            for (Set<String> labels : labelsList) {
                for (String label : labels) {
                    switch (label) {
                        case "vertex":
                            vertexVertex = vertexIndices.get("vertex");
                            break;
                        case "outEdgeVertex":
                            vertexEdge = vertexIndices.get("outEdgeVertex");
                            break;
                        case "index":
                            vertexIndex = vertexIndices.get("index");
                            break;
                        case "property":
                            propertyIndex = vertexIndices.get("property");
                            break;
                        case BaseStrategy.SQLG_PATH_FAKE_LABEL:
                        case BaseStrategy.SQLG_PATH_ORDER_RANGE_LABEL:
                        case MARKER:
                            break;
                        default:
                            throw new IllegalStateException(String.format("BUG: Only \"vertex\",\"outEdgeVertex\",\"index\" and \"property\" is expected as a label. Found %s", label));
                    }
                }
            }
            Preconditions.checkState(vertexVertex != null, "BUG: Topology vertex not found.");
            String schemaName = schemaVertex.value(SQLG_SCHEMA_SCHEMA_NAME);
            String tableName = vertexVertex.value(SQLG_SCHEMA_VERTEX_LABEL_NAME);
            VertexLabel vertexLabel = this.vertexLabels.get(schemaName + "." + VERTEX_PREFIX + tableName);
            if (vertexLabel == null) {
                vertexLabel = new VertexLabel(this, tableName);
                this.vertexLabels.put(schemaName + "." + VERTEX_PREFIX + tableName, vertexLabel);
            }
            if (vertexEdge != null) {
                String edgeName = vertexEdge.value(SQLG_SCHEMA_EDGE_LABEL_NAME);
                Optional<EdgeLabel> oel = vertexLabel.getOutEdgeLabel(edgeName);
                if (oel.isPresent()) {
                    EdgeLabel edgeLabel = oel.get();
                    if (vertexIndex != null) {
                        String indexName = vertexIndex.value(SQLG_SCHEMA_INDEX_NAME);
                        Optional<Index> oidx = edgeLabel.getIndex(indexName);
                        Index idx;
                        if (oidx.isPresent()) {
                            idx = oidx.get();
                        } else {
                            idx = new Index(indexName, IndexType.fromString(vertexIndex.value(SQLG_SCHEMA_INDEX_INDEX_TYPE)), edgeLabel);
                            edgeLabel.addIndex(idx);
                        }
                        if (propertyIndex != null) {
                            String propertyName = propertyIndex.value(SQLG_SCHEMA_PROPERTY_NAME);
                            edgeLabel.getProperty(propertyName).ifPresent(idx::addProperty);
                        }

                    }
                }
            }
        }
    }

    public JsonNode toJson() {
        ObjectNode schemaNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        schemaNode.put("name", this.getName());
        ArrayNode vertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
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
        ObjectNode schemaNode = new ObjectNode(Topology.OBJECT_MAPPER.getNodeFactory());
        schemaNode.put("name", this.getName());
        if (this.topology.isSchemaChanged() && !this.getUncommittedVertexLabels().isEmpty()) {
            ArrayNode vertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel vertexLabel : this.getUncommittedVertexLabels().values()) {
                //VertexLabel toNotifyJson always returns something even though its an Optional.
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
            ArrayNode vertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
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
            ArrayNode edgeLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (String s : this.uncommittedRemovedEdgeLabels) {
                edgeLabelArrayNode.add(s);
            }
            schemaNode.set("uncommittedRemovedEdgeLabels", edgeLabelArrayNode);
            foundVertexLabels = true;
        }

        if (!this.vertexLabels.isEmpty()) {
            ArrayNode vertexLabelArrayNode = new ArrayNode(Topology.OBJECT_MAPPER.getNodeFactory());
            for (VertexLabel vertexLabel : this.vertexLabels.values()) {
                Optional<JsonNode> notifyJsonOptional = vertexLabel.toNotifyJson();
                if (notifyJsonOptional.isPresent()) {
                    JsonNode notifyJson = notifyJsonOptional.get();
                    if (notifyJson.get("uncommittedProperties") != null ||
                            notifyJson.get("uncommittedOutEdgeLabels") != null ||
                            notifyJson.get("uncommittedInEdgeLabels") != null ||
                            notifyJson.get("outEdgeLabels") != null ||
                            notifyJson.get("inEdgeLabels") != null ||
                            notifyJson.get("uncommittedRemovedOutEdgeLabels") != null ||
                            notifyJson.get("uncommittedRemovedInEdgeLabels") != null
                    ) {

                        vertexLabelArrayNode.add(notifyJsonOptional.get());
                        foundVertexLabels = true;
                    }
                }
            }
            if (vertexLabelArrayNode.size() > 0) {
                schemaNode.set("vertexLabels", vertexLabelArrayNode);
            }
        }
        if (foundVertexLabels) {
            return Optional.of(schemaNode);
        } else {
            return Optional.empty();
        }
    }

    void fromNotifyJsonOutEdges(JsonNode jsonSchema) {
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
                        this.getTopology().fire(vertexLabel, null, TopologyChangeAction.CREATE);
                    }
                    //The order of the next two statements matter.
                    //fromNotifyJsonOutEdge needs to happen first to ensure the properties are on the VertexLabel
                    // fire only if we didn't create the vertex label
                    vertexLabel.fromNotifyJsonOutEdge(vertexLabelJson, vertexLabelOptional.isPresent());
                    this.getTopology().addToAllTables(this.getName() + "." + VERTEX_PREFIX + vertexLabelName, vertexLabel.getPropertyTypeMap());
                }
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
                    for (EdgeRole er : lbl.getOutEdgeRoles().values()) {
                        er.getEdgeLabel().outVertexLabels.remove(lbl);
                    }
                    for (EdgeRole er : lbl.getInEdgeRoles().values()) {
                        er.getEdgeLabel().inVertexLabels.remove(lbl);
                    }
                    this.getTopology().fire(lbl, lbl, TopologyChangeAction.DELETE);

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
                            lbl.outEdgeLabels.remove(edgeLabel.getFullName());
                        }
                    }
                    for (VertexLabel lbl : edgeLabel.getInVertexLabels()) {
                        if (edgeLabel.isValid()) {
                            lbl.inEdgeLabels.remove(edgeLabel.getFullName());
                        }
                    }
                    this.getTopology().fire(edgeLabel, edgeLabel, TopologyChangeAction.DELETE);
                }
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
        if (!(o instanceof Schema)) {
            return false;
        }
        //only equals on the name of the schema. it is assumed that the VertexLabels (tables) are the same.
        Schema other = (Schema) o;
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
        getTopology().startSchemaChange();
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
            getTopology().fire(edgeLabel, edgeLabel, TopologyChangeAction.DELETE);
        }
    }

    /**
     * remove a given vertex label
     *
     * @param vertexLabel  the vertex label
     * @param preserveData should we keep the SQL data
     */
    void removeVertexLabel(VertexLabel vertexLabel, boolean preserveData) {
        getTopology().startSchemaChange();
        String fn = this.name + "." + VERTEX_PREFIX + vertexLabel.getName();
        if (!this.uncommittedRemovedVertexLabels.contains(fn)) {
            this.sqlgGraph.traversal().V().hasLabel(this.name + "." + vertexLabel.getLabel()).drop().iterate();
            this.uncommittedRemovedVertexLabels.add(fn);
            TopologyManager.removeVertexLabel(this.sqlgGraph, vertexLabel);
            for (EdgeRole er : vertexLabel.getOutEdgeRoles().values()) {
                er.removeViaVertexLabelRemove(preserveData);
            }
            for (EdgeRole er : vertexLabel.getInEdgeRoles().values()) {
                er.removeViaVertexLabelRemove(preserveData);
            }
            if (!preserveData) {
                vertexLabel.delete();
            }
            getTopology().fire(vertexLabel, vertexLabel, TopologyChangeAction.DELETE);
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
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            logger.error("schema deletion failed " + this.sqlgGraph, e);
            throw new RuntimeException(e);
        }
    }

    public void createTempTable(String tableName, Map<String, PropertyType> columns) {
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
        if (columns.size() > 0) {
            sql.append(", ");
        }
        AbstractLabel.buildColumns(this.sqlgGraph, new ListOrderedSet<>(), columns, sql);
        sql.append(") ");
        sql.append(this.sqlgGraph.getSqlDialect().afterCreateTemporaryTableStatement());
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

    void removeTemporaryTables() {
        if (!this.sqlgGraph.getSqlDialect().supportsTemporaryTableOnCommitDrop()) {
            for (Map.Entry<String, Map<String, PropertyType>> temporaryTableEntry : this.threadLocalTemporaryTables.get().entrySet()) {
                String tableName = temporaryTableEntry.getKey();
                Connection conn = this.sqlgGraph.tx().getConnection();
                try (Statement stmt = conn.createStatement()) {
                    String sql = "DROP TEMPORARY TABLE " +
                            this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()) + "." +
                            this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(tableName);
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql);
                    }
                    stmt.execute(sql);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            }
        }
        this.threadLocalTemporaryTables.remove();
    }

    public Map<String, PropertyType> getTemporaryTable(String tableName) {
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
            for (EdgeLabel inEdgeLabel : vertexLabel.inEdgeLabels.values()) {
                Preconditions.checkState(fullEdgeLabels.contains(inEdgeLabel.getFullName()), "'%s' is not present in the foreign EdgeLabels", this.name + "." + EDGE_PREFIX + inEdgeLabel.getFullName());
            }
            for (EdgeLabel outEdgeLabel : vertexLabel.outEdgeLabels.values()) {
                Preconditions.checkState(fullEdgeLabels.contains(outEdgeLabel.getFullName()), "'%s' is not present in the foreign EdgeLabels", this.name + "." + EDGE_PREFIX + outEdgeLabel.getFullName());
            }
        }
        for (EdgeLabel edgeLabel : edgeLabels) {
            for (VertexLabel vertexLabel : edgeLabel.inVertexLabels) {
                Preconditions.checkState(fullVertexLabels.contains(vertexLabel.getFullName()), "'%s' is not present in the foreign VertexLabels", this.name + "." + VERTEX_PREFIX + vertexLabel.getFullName());
            }
        }

        for (VertexLabel vertexLabel : vertexLabels) {
            VertexLabel foreignVertexLabel = vertexLabel.readOnlyCopy(this);
            this.vertexLabels.put(this.name + "." + VERTEX_PREFIX + vertexLabel.getLabel(), foreignVertexLabel);
        }
        for (EdgeLabel edgeLabel : edgeLabels) {
            EdgeLabel foreignEdgeLabel = edgeLabel.readOnlyCopy(topology, this, Set.of(this));
            Set<VertexLabel> outVertexLabels = edgeLabel.getOutVertexLabels();
            for (VertexLabel outVertexLabel : outVertexLabels) {
                String l = this.getName() + "." + VERTEX_PREFIX + outVertexLabel.getLabel();
                Preconditions.checkState(this.vertexLabels.containsKey(l), "VertexLabel '%s' not found in schema '%s'.", l, this.getName());
                foreignEdgeLabel.outVertexLabels.add(this.vertexLabels.get(l));
            }
            Set<VertexLabel> inVertexLabels = edgeLabel.getInVertexLabels();
            for (VertexLabel inVertexLabel : inVertexLabels) {
                String l = this.getName() + "." + VERTEX_PREFIX + inVertexLabel.getLabel();
                Preconditions.checkState(this.vertexLabels.containsKey(l), "VertexLabel '%s' not found in schema '%s'.", l, this.getName());
                foreignEdgeLabel.inVertexLabels.add(this.vertexLabels.get(l));
            }
            this.addToAllEdgeCache(foreignEdgeLabel);
            this.outEdgeLabels.put(this.name + "." + EDGE_PREFIX + edgeLabel.getLabel(), foreignEdgeLabel);
        }
    }

    static void validateImportingVertexLabels(Set<Schema> originalSchemas) {
        for (Schema foreignSchema : originalSchemas) {
            for (String label : foreignSchema.vertexLabels.keySet()) {
                VertexLabel vertexLabel = foreignSchema.vertexLabels.get(label);
                for (EdgeLabel edgeLabel : vertexLabel.outEdgeLabels.values()) {
                    Schema edgeSchema = edgeLabel.getSchema();
                    if (originalSchemas.stream().noneMatch(s -> s.getName().equals(edgeSchema.getName()))) {
                        throw new IllegalStateException(String.format(
                                "VertexLabel '%s' has an outEdgeLabel '%s' that is not present in a foreign schema",
                                label, edgeLabel.getLabel()));
                    }
                }
                for (EdgeLabel edgeLabel : vertexLabel.inEdgeLabels.values()) {
                    Schema edgeSchema = edgeLabel.getSchema();
                    if (originalSchemas.stream().noneMatch(s -> s.getName().equals(edgeSchema.getName()))) {
                        throw new IllegalStateException(String.format(
                                "VertexLabel '%s' has an inEdgeLabel '%s' that is not present in a foreign schema",
                                label, edgeLabel.getLabel()));
                    }
                }
            }
        }
    }

    static void validateImportingEdgeLabels(Set<Schema> originalSchemas) {
        for (Schema foreignSchema : originalSchemas) {
            for (String label : foreignSchema.outEdgeLabels.keySet()) {
                EdgeLabel edgeLabel = foreignSchema.outEdgeLabels.get(label);
                for (VertexLabel inVertexLabel : edgeLabel.inVertexLabels) {
                    //Is the inVertexLabel in the current schema being imported or in an already imported schema
                    if (originalSchemas.stream().noneMatch(s -> s.getVertexLabel(inVertexLabel.getLabel()).isPresent())) {
                        throw new IllegalStateException(String.format("EdgeLabel '%s' has a inVertexLabel '%s' that is not present in a foreign schema", label, inVertexLabel.getLabel()));
                    }
                }
                for (VertexLabel outVertexLabel : edgeLabel.outVertexLabels) {
                    //Is the outVertexLabel in the current schema being imported or in an already imported schema
                    if (originalSchemas.stream().noneMatch(s -> s.getVertexLabel(outVertexLabel.getLabel()).isPresent())) {
                        throw new IllegalStateException(String.format("EdgeLabel '%s' has a outVertexLabel '%s' that is not present in a foreign schema", label, outVertexLabel.getLabel()));
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
