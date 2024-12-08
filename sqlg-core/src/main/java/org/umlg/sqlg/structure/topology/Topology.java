package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.sql.dialect.SqlSchemaChangeDialect;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.util.ThreadLocalMap;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class Topology {

    private static final Logger LOGGER = LoggerFactory.getLogger(Topology.class);
    public static final String GRAPH = "graph";
    public static final String VERTEX_PREFIX = "V_";
    public static final String EDGE_PREFIX = "E_";
    public static final String VERTICES = "VERTICES";
    public static final String ID = "ID";
    public static final String VERTEX_SCHEMA = "VERTEX_SCHEMA";
    public static final String VERTEX_TABLE = "VERTEX_TABLE";
    public static final String LABEL_SEPARATOR = ":::";
    public static final String IN_VERTEX_COLUMN_END = "__I";
    public static final String OUT_VERTEX_COLUMN_END = "__O";
    public static final String ZONEID = "~~~ZONEID";
    public static final String MONTHS = "~~~MONTHS";
    public static final String DAYS = "~~~DAYS";
    public static final String DURATION_NANOS = "~~~NANOS";
    public static final String BULK_TEMP_EDGE = "BULK_TEMP_EDGE";

    private final SqlgGraph sqlgGraph;
    private boolean distributed = false;

    private final Map<String, Map<String, PropertyDefinition>> allTableCache = new ConcurrentHashMap<>();
    private final Map<String, Map<String, PropertyDefinition>> sqlgSchemaTableCache = new ConcurrentHashMap<>();
    //This cache is needed as too much time is taken building it on the fly.
    //The cache is invalidated on every topology change
    private final Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> schemaTableForeignKeyCache = new ConcurrentHashMap<>();
    private final Map<String, Set<ForeignKey>> edgeForeignKeyCache = new ConcurrentHashMap<>();
    //Map the topology. This is for regular schemas. i.e. 'public.Person', 'special.Car'
    private final Map<String, Schema> schemas = new ConcurrentHashMap<>();

    private final ThreadLocal<Boolean> schemaChanged = ThreadLocal.withInitial(() -> false);
    private boolean locked = false;
    private final ThreadLocalMap<String, Schema> uncommittedSchemas = new ThreadLocalMap<>();
    private final Set<String> uncommittedRemovedSchemas = new ConcurrentSkipListSet<>();
    private final Map<String, Schema> metaSchemas;

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final String SQLG_NOTIFICATION_CHANNEL = "SQLG_NOTIFY";

    //ownPids are the pids to ignore as it is what the graph sent a notification for.
    private final Set<Integer> ownPids = Collections.synchronizedSet(new HashSet<>());

    private final List<TopologyValidationError> validationErrors = new ArrayList<>();
    private final List<TopologyListener> topologyListeners = new CopyOnWriteArrayList<>();

    @SuppressWarnings("WeakerAccess")
    public static final String CREATED_ON = "createdOn";

    @SuppressWarnings("WeakerAccess")
    public static final String UPDATED_ON = "updatedOn";

    @SuppressWarnings("WeakerAccess")
    public static final String SCHEMA_VERTEX_DISPLAY = "schemaVertex";

    /**
     * Rdbms schema that holds sqlg topology.
     */
    public static final String SQLG_SCHEMA = "sqlg_schema";
    /**
     * Table storing the graph's graph meta data.
     */
    public static final String SQLG_SCHEMA_GRAPH = "graph";
    /**
     * graph's sqlg version.
     */
    public static final String SQLG_SCHEMA_GRAPH_VERSION = "version";
    /**
     * graph's database version. This is sourced from {@link DatabaseMetaData#getDatabaseProductVersion()}
     */
    public static final String SQLG_SCHEMA_GRAPH_DB_VERSION = "dbVersion";
    /**
     * Table storing the graph's schemas.
     */
    public static final String SQLG_SCHEMA_SCHEMA = "schema";
    /**
     * Schema's name.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_SCHEMA_NAME = "name";
    /**
     * Table storing the graphs vertex labels.
     */
    public static final String SQLG_SCHEMA_VERTEX_LABEL = "vertex";
    /**
     * VertexLabel's name property.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_VERTEX_LABEL_NAME = "name";
    /**
     * VertexLabel's partition type. {@link PartitionType}
     */
    public static final String SQLG_SCHEMA_VERTEX_LABEL_PARTITION_TYPE = "partitionType";
    /**
     * VertexLabel's partition expression.
     */
    public static final String SQLG_SCHEMA_VERTEX_LABEL_PARTITION_EXPRESSION = "partitionExpression";
    /**
     * Table storing the graphs edge labels.
     */
    public static final String SQLG_SCHEMA_EDGE_LABEL = "edge";
    /**
     * EdgeLabel's name property.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_EDGE_LABEL_NAME = "name";
    /**
     * EdgeLabel's partition type. {@link PartitionType}
     */
    public static final String SQLG_SCHEMA_EDGE_LABEL_PARTITION_TYPE = "partitionType";
    /**
     * EdgeLabel's partition expression.
     */
    public static final String SQLG_SCHEMA_EDGE_LABEL_PARTITION_EXPRESSION = "partitionExpression";

    /**
     * Table storing the partition.
     */
    public static final String SQLG_SCHEMA_PARTITION = "partition";
    /**
     * The Partition's name.
     */
    public static final String SQLG_SCHEMA_PARTITION_NAME = "name";
    public static final String SQLG_SCHEMA_PARTITION_SCHEMA_NAME = "schemaName";
    public static final String SQLG_SCHEMA_PARTITION_ABSTRACT_LABEL_NAME = "abstractLabelName";

    /**
     * The Partition's from spec.
     */
    public static final String SQLG_SCHEMA_PARTITION_FROM = "from";
    /**
     * The Partition's to spec.
     */
    public static final String SQLG_SCHEMA_PARTITION_TO = "to";
    /**
     * The Partition's in spec. i.e. CREATE TABLE "public"."TEST1" PARTITION OF "public"."V_RealWorkspaceElement" FOR VALUES IN ('TEST1');
     */
    public static final String SQLG_SCHEMA_PARTITION_IN = "in";
    /**
     * The Partition's modulus spec. i.e. CREATE TABLE "public"."TEST1" PARTITION OF "public"."V_RealWorkspaceElement" FOR VALUES (MODULUS m, REMAINDER r);
     */
    public static final String SQLG_SCHEMA_PARTITION_MODULUS = "modulus";
    /**
     * The Partition's remainder spec. i.e. CREATE TABLE "public"."TEST1" PARTITION OF "public"."V_RealWorkspaceElement" FOR VALUES (MODULUS m, REMAINDER r);
     */
    public static final String SQLG_SCHEMA_PARTITION_REMAINDER = "remainder";
    /**
     * The Partition's sub-partition's PartitionType.
     */
    public static final String SQLG_SCHEMA_PARTITION_PARTITION_TYPE = "partitionType";
    /**
     * The Partition's sub-partition's partitionExpression.
     */
    public static final String SQLG_SCHEMA_PARTITION_PARTITION_EXPRESSION = "partitionExpression";
    /**
     * Edge table for the vertex's partitions.
     */
    public static final String SQLG_SCHEMA_VERTEX_PARTITION_EDGE = "vertex_partition";
    /**
     * Edge table for the edge's partitions.
     */
    public static final String SQLG_SCHEMA_EDGE_PARTITION_EDGE = "edge_partition";
    /**
     * Partition table for the partition's partitions.
     */
    public static final String SQLG_SCHEMA_PARTITION_PARTITION_EDGE = "partition_partition";

    /**
     * Edge table for the vertex's distribution column.
     */
    public static final String SQLG_SCHEMA_VERTEX_DISTRIBUTION_COLUMN_EDGE = "vertex_distribution";
    /**
     * Edge table for the vertex's colocate label.
     */
    public static final String SQLG_SCHEMA_VERTEX_DISTRIBUTION_COLOCATE_EDGE = "vertex_colocate";
    /**
     * vertex's shard_count property.
     */
    public static final String SQLG_SCHEMA_VERTEX_LABEL_DISTRIBUTION_SHARD_COUNT = "shardCount";


    /**
     * Edge table for the edge's distribution column.
     */
    public static final String SQLG_SCHEMA_EDGE_DISTRIBUTION_COLUMN_EDGE = "edge_distribution";
    /**
     * Edge table for the edge's colocate label. The edge's colocate will always be to its incoming vertex label.
     */
    public static final String SQLG_SCHEMA_EDGE_DISTRIBUTION_COLOCATE_EDGE = "edge_colocate";
    /**
     * Edge's shard_count property.
     */
    public static final String SQLG_SCHEMA_EDGE_LABEL_DISTRIBUTION_SHARD_COUNT = "shardCount";


    /**
     * Table storing the graphs element properties.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_PROPERTY = "property";
    /**
     * Edge table for the schema to vertex edge.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_SCHEMA_VERTEX_EDGE = "schema_vertex";
    /**
     * Edge table for the vertices in edges.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_IN_EDGES_EDGE = "in_edges";
    /**
     * Edge table for the vertices out edges.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_OUT_EDGES_EDGE = "out_edges";

    public static final String SQLG_SCHEMA_IN_EDGES_LOWER_MULTIPLICITY = "lowerMultiplicity";
    public static final String SQLG_SCHEMA_IN_EDGES_UPPER_MULTIPLICITY = "upperMultiplicity";
    public static final String SQLG_SCHEMA_IN_EDGES_UNIQUE = "unique";
    public static final String SQLG_SCHEMA_IN_EDGES_ORDERED = "ordered";
    public static final String SQLG_SCHEMA_OUT_EDGES_LOWER_MULTIPLICITY = "lowerMultiplicity";
    public static final String SQLG_SCHEMA_OUT_EDGES_UPPER_MULTIPLICITY = "upperMultiplicity";
    public static final String SQLG_SCHEMA_OUT_EDGES_UNIQUE = "unique";
    public static final String SQLG_SCHEMA_OUT_EDGES_ORDERED = "ordered";


    /**
     * Edge table for the vertex's properties.
     */
    public static final String SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE = "vertex_property";
    /**
     * Edge table for the edge's properties.
     */
    public static final String SQLG_SCHEMA_EDGE_PROPERTIES_EDGE = "edge_property";
    /**
     * Edge table for the vertex's identifier properties. i.e. user defined primary key columns
     */
    public static final String SQLG_SCHEMA_VERTEX_IDENTIFIER_EDGE = "vertex_identifier";
    /**
     * Indicates the primary key order.
     */
    public static final String SQLG_SCHEMA_VERTEX_IDENTIFIER_INDEX_EDGE = "identifier_index";
    /**
     * Edge table for the edge's identifier properties. i.e. user defined primary key columns
     */
    public static final String SQLG_SCHEMA_EDGE_IDENTIFIER_EDGE = "edge_identifier";
    /**
     * Indicates the primary key order.
     */
    public static final String SQLG_SCHEMA_EDGE_IDENTIFIER_INDEX_EDGE = "identifier_index";
    /**
     * Property table's name property
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_PROPERTY_NAME = "name";

    /**
     * Table storing the graphs indexes.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_INDEX = "index";
    /**
     * Index table's name property
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_INDEX_NAME = "name";
    /**
     * Index table's index_type property
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_INDEX_INDEX_TYPE = "index_type";
    /**
     * Edge table for the VertexLabel to Index.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_VERTEX_INDEX_EDGE = "vertex_index";
    /**
     * Edge table for the EdgeLabel to Index.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_EDGE_INDEX_EDGE = "edge_index";
    /**
     * Edge table for Index to Property
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_INDEX_PROPERTY_EDGE = "index_property";

    public static final String SQLG_SCHEMA_INDEX_PROPERTY_EDGE_SEQUENCE = "sequence";

    /**
     * Table storing the logs.
     */
    public static final String SQLG_SCHEMA_LOG = "log";
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_LOG_TIMESTAMP = "timestamp";
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_LOG_LOG = "log";
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_LOG_PID = "pid";

    /**
     * Property table's type property
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_PROPERTY_TYPE = "type";

    /**
     * Lower multiplicity of the property. > 0 indicates the property is required
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER = "lowerMultiplicity";

    /**
     * Upper multiplicity of the property. -1 indicates the property has no upper limit
     */
    public static final String SQLG_SCHEMA_PROPERTY_MULTIPLICITY_UPPER = "upperMultiplicity";

    /**
     * The default value for the property. The value gets passed straight into the db without inspection.
     */
    public static final String SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL = "defaultLiteral";

    /**
     * A check constraint for the property. The value gets passed straight into the db without inspection.
     */
    public static final String SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT = "checkConstraint";

    /**
     * Topology is a singleton created when the {@link SqlgGraph} is opened.
     * As the topology, i.e. sqlg_schema is created upfront the meta topology is pre-loaded.
     *
     * @param sqlgGraph The graph.
     */
    public Topology(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
        boolean canUserCreateSchemas = sqlgGraph.getSqlDialect().canUserCreateSchemas(sqlgGraph);

        //Pre-create the meta topology.
        Schema sqlgSchema = Schema.instantiateSqlgSchema(this);
        this.metaSchemas = Map.of(SQLG_SCHEMA, sqlgSchema);

        Map<String, PropertyDefinition> columns = new HashMap<>();
        columns.put(SQLG_SCHEMA_GRAPH_VERSION, PropertyDefinition.of(PropertyType.STRING));
        columns.put(SQLG_SCHEMA_GRAPH_DB_VERSION, PropertyDefinition.of(PropertyType.STRING));
        columns.put(CREATED_ON, PropertyDefinition.of(PropertyType.LOCALDATETIME));
        columns.put(UPDATED_ON, PropertyDefinition.of(PropertyType.LOCALDATETIME));
        sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_GRAPH, columns);

        columns.clear();
        columns.put(SQLG_SCHEMA_PROPERTY_NAME, PropertyDefinition.of(PropertyType.STRING));
        columns.put(CREATED_ON, PropertyDefinition.of(PropertyType.LOCALDATETIME));
        VertexLabel schemaVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_SCHEMA, columns);

        columns.clear();
        columns.put(SQLG_SCHEMA_VERTEX_LABEL_NAME, PropertyDefinition.of(PropertyType.STRING));
        columns.put(CREATED_ON, PropertyDefinition.of(PropertyType.LOCALDATETIME));
        columns.put(SCHEMA_VERTEX_DISPLAY, PropertyDefinition.of(PropertyType.STRING));
        columns.put(SQLG_SCHEMA_VERTEX_LABEL_PARTITION_TYPE, PropertyDefinition.of(PropertyType.STRING));
        columns.put(SQLG_SCHEMA_VERTEX_LABEL_PARTITION_EXPRESSION, PropertyDefinition.of(PropertyType.STRING));
        columns.put(SQLG_SCHEMA_VERTEX_LABEL_DISTRIBUTION_SHARD_COUNT, PropertyDefinition.of(PropertyType.INTEGER));
        VertexLabel vertexVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_VERTEX_LABEL, columns);

        columns.clear();
        columns.put(SQLG_SCHEMA_PROPERTY_NAME, PropertyDefinition.of(PropertyType.STRING));
        columns.put(CREATED_ON, PropertyDefinition.of(PropertyType.LOCALDATETIME));
        columns.put(SQLG_SCHEMA_EDGE_LABEL_PARTITION_TYPE, PropertyDefinition.of(PropertyType.STRING));
        columns.put(SQLG_SCHEMA_EDGE_LABEL_PARTITION_EXPRESSION, PropertyDefinition.of(PropertyType.STRING));
        columns.put(SQLG_SCHEMA_EDGE_LABEL_DISTRIBUTION_SHARD_COUNT, PropertyDefinition.of(PropertyType.INTEGER));
        VertexLabel edgeVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_EDGE_LABEL, columns);

        VertexLabel partitionVertexLabel;
        columns.clear();
        columns.put(SQLG_SCHEMA_PARTITION_NAME, PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        columns.put(SQLG_SCHEMA_PARTITION_SCHEMA_NAME, PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        columns.put(SQLG_SCHEMA_PARTITION_ABSTRACT_LABEL_NAME, PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));

        columns.put(CREATED_ON, PropertyDefinition.of(PropertyType.LOCALDATETIME));
        columns.put(SQLG_SCHEMA_PARTITION_FROM, PropertyDefinition.of(PropertyType.STRING));
        columns.put(SQLG_SCHEMA_PARTITION_TO, PropertyDefinition.of(PropertyType.STRING));
        columns.put(SQLG_SCHEMA_PARTITION_IN, PropertyDefinition.of(PropertyType.STRING));
        columns.put(SQLG_SCHEMA_PARTITION_MODULUS, PropertyDefinition.of(PropertyType.INTEGER));
        columns.put(SQLG_SCHEMA_PARTITION_REMAINDER, PropertyDefinition.of(PropertyType.INTEGER));
        columns.put(SQLG_SCHEMA_PARTITION_PARTITION_TYPE, PropertyDefinition.of(PropertyType.STRING));
        columns.put(SQLG_SCHEMA_PARTITION_PARTITION_EXPRESSION, PropertyDefinition.of(PropertyType.STRING));
        partitionVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_PARTITION, columns);

        columns.clear();
        columns.put(SQLG_SCHEMA_PROPERTY_NAME, PropertyDefinition.of(PropertyType.STRING));
        columns.put(CREATED_ON, PropertyDefinition.of(PropertyType.LOCALDATETIME));
        columns.put(SQLG_SCHEMA_PROPERTY_TYPE, PropertyDefinition.of(PropertyType.STRING));
        columns.put(SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER, PropertyDefinition.of(PropertyType.LONG));
        columns.put(SQLG_SCHEMA_PROPERTY_MULTIPLICITY_UPPER, PropertyDefinition.of(PropertyType.LONG));
        columns.put(SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL, PropertyDefinition.of(PropertyType.STRING));
        columns.put(SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT, PropertyDefinition.of(PropertyType.STRING));
        VertexLabel propertyVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_PROPERTY, columns);

        columns.clear();
        columns.put(SQLG_SCHEMA_INDEX_NAME, PropertyDefinition.of(PropertyType.STRING));
        columns.put(SQLG_SCHEMA_INDEX_INDEX_TYPE, PropertyDefinition.of(PropertyType.STRING));
        columns.put(CREATED_ON, PropertyDefinition.of(PropertyType.LOCALDATETIME));
        VertexLabel indexVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_INDEX, columns);

        columns.clear();
        schemaVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE, vertexVertexLabel, columns);

        columns.put(SQLG_SCHEMA_IN_EDGES_LOWER_MULTIPLICITY, PropertyDefinition.of(PropertyType.LONG));
        columns.put(SQLG_SCHEMA_IN_EDGES_UPPER_MULTIPLICITY, PropertyDefinition.of(PropertyType.LONG));
        columns.put(SQLG_SCHEMA_IN_EDGES_UNIQUE, PropertyDefinition.of(PropertyType.BOOLEAN));
        columns.put(SQLG_SCHEMA_IN_EDGES_ORDERED, PropertyDefinition.of(PropertyType.BOOLEAN));
        vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_IN_EDGES_EDGE, edgeVertexLabel, columns);
        columns.clear();
        columns.put(SQLG_SCHEMA_OUT_EDGES_LOWER_MULTIPLICITY, PropertyDefinition.of(PropertyType.LONG));
        columns.put(SQLG_SCHEMA_OUT_EDGES_UPPER_MULTIPLICITY, PropertyDefinition.of(PropertyType.LONG));
        columns.put(SQLG_SCHEMA_OUT_EDGES_UNIQUE, PropertyDefinition.of(PropertyType.BOOLEAN));
        columns.put(SQLG_SCHEMA_OUT_EDGES_ORDERED, PropertyDefinition.of(PropertyType.BOOLEAN));
        vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_OUT_EDGES_EDGE, edgeVertexLabel, columns);
        columns.clear();

        vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_PARTITION_EDGE, partitionVertexLabel, columns);
        edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_PARTITION_EDGE, partitionVertexLabel, columns);
        partitionVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_PARTITION_PARTITION_EDGE, partitionVertexLabel, columns);
        vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_DISTRIBUTION_COLUMN_EDGE, propertyVertexLabel, columns);
        vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_DISTRIBUTION_COLOCATE_EDGE, vertexVertexLabel, columns);

        edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_DISTRIBUTION_COLUMN_EDGE, propertyVertexLabel, columns);
        edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_DISTRIBUTION_COLOCATE_EDGE, vertexVertexLabel, columns);

        vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE, propertyVertexLabel, columns);
        edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, propertyVertexLabel, columns);

        columns.put(SQLG_SCHEMA_VERTEX_IDENTIFIER_INDEX_EDGE, PropertyDefinition.of(PropertyType.INTEGER));
        vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_IDENTIFIER_EDGE, propertyVertexLabel, columns);
        columns.clear();

        columns.put(SQLG_SCHEMA_EDGE_IDENTIFIER_INDEX_EDGE, PropertyDefinition.of(PropertyType.INTEGER));
        edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_IDENTIFIER_EDGE, propertyVertexLabel, columns);
        columns.clear();

        vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_INDEX_EDGE, indexVertexLabel, columns);
        edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_INDEX_EDGE, indexVertexLabel, columns);
        columns.put(SQLG_SCHEMA_INDEX_PROPERTY_EDGE_SEQUENCE, PropertyDefinition.of(PropertyType.INTEGER));
        indexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_INDEX_PROPERTY_EDGE, propertyVertexLabel, columns);
        columns.clear();

        columns.put(SQLG_SCHEMA_LOG_TIMESTAMP, PropertyDefinition.of(PropertyType.LOCALDATETIME));
        columns.put(SQLG_SCHEMA_LOG_LOG, PropertyDefinition.of(PropertyType.JSON));
        columns.put(SQLG_SCHEMA_LOG_PID, PropertyDefinition.of(PropertyType.INTEGER));
        sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_LOG, columns);

        //add the public schema
        if (canUserCreateSchemas) {
            this.schemas.put(sqlgGraph.getSqlDialect().getPublicSchema(), Schema.createPublicSchema(sqlgGraph, this, sqlgGraph.getSqlDialect().getPublicSchema()));
        } else {
            Schema schema = Schema.instantiateSchema(this, sqlgGraph.getSqlDialect().getPublicSchema());
            this.schemas.put(sqlgGraph.getSqlDialect().getPublicSchema(), schema);
        }

        //populate the schema's allEdgesCache
        sqlgSchema.cacheEdgeLabels();
        //populate the allTablesCache
        sqlgSchema.getVertexLabels().values().forEach(
                (v) -> this.sqlgSchemaTableCache.put(
                        v.getSchema().getName() + "." + VERTEX_PREFIX + v.getLabel(),
                        v.getPropertyDefinitionMap()
                )
        );
        sqlgSchema.getEdgeLabels().values().forEach(
                (e) -> this.sqlgSchemaTableCache.put(
                        e.getSchema().getName() + "." + EDGE_PREFIX + e.getLabel(),
                        e.getPropertyDefinitionMap()
                )
        );

        sqlgSchema.getVertexLabels().values().forEach((v) -> {
            SchemaTable vertexLabelSchemaTable = SchemaTable.of(v.getSchema().getName(), VERTEX_PREFIX + v.getLabel());
            this.schemaTableForeignKeyCache.put(vertexLabelSchemaTable, Pair.of(new HashSet<>(), new HashSet<>()));
            v.getInEdgeLabels().forEach(
                    (edgeLabelName, edgeLabel) -> this.schemaTableForeignKeyCache.get(vertexLabelSchemaTable)
                            .getLeft()
                            .add(SchemaTable.of(edgeLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()))
            );
            v.getOutEdgeLabels().forEach(
                    (edgeLabelName, edgeLabel) -> this.schemaTableForeignKeyCache.get(vertexLabelSchemaTable)
                            .getRight()
                            .add(SchemaTable.of(v.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()))
            );
        });

        this.edgeForeignKeyCache.putAll(sqlgSchema.getAllEdgeForeignKeys());

        this.sqlgGraph.tx().beforeCommit(this::beforeCommit);
        this.sqlgGraph.tx().afterCommit(this::afterCommit);

        this.sqlgGraph.tx().afterRollback(() -> {
            if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
                afterRollback();
            } else {
                afterCommit();
            }
        });

    }

    public SqlgGraph getSqlgGraph() {
        return this.sqlgGraph;
    }

    public void close() {
        if (this.distributed) {
            ((SqlSchemaChangeDialect) this.sqlgGraph.getSqlDialect()).unregisterListener();
        }
    }

    public List<TopologyValidationError> getValidationErrors() {
        return this.validationErrors;
    }

    public boolean isImplementingForeignKeys() {
        return this.sqlgGraph.configuration().getBoolean("implement.foreign.keys", true);
    }

    public void threadWriteLock() {
        if (!this.sqlgGraph.tx().isWriteTransaction()) {
            this.sqlgGraph.tx().setWriteTransaction(true);
        }
    }

    /**
     * Global indicator to change the topology.
     */
    void startSchemaChange(String changeDescription) {
        if (this.locked && this.sqlgGraph.tx().isTopologyLocked()) {
            if (changeDescription == null) {
                throw new IllegalStateException("The topology is locked! Changes are not allowed, first unlock it. Either globally or for the transaction.");
            } else {
                throw new IllegalStateException(String.format("The topology is locked! Changes are not allowed, first unlock it. Either globally or for the transaction.\nChange description: '%s'", changeDescription));
            }
        }
        sqlgGraph.getSchemaTableTreeCache().clear();
        this.sqlgGraph.tx().readWrite();
        this.schemaChanged.set(true);
    }

    public void lock() {
        this.locked = true;
    }

    public void unlock() {
        this.locked = false;
    }

    public boolean isLocked() {
        return this.locked;
    }

    boolean isSchemaChanged() {
        return this.schemaChanged.get();
    }

    /**
     * Called from {@link Topology#afterCommit()} and {@link Topology#afterRollback()}
     * Releases the lock.
     */
    private void z_internalSqlWriteUnlock() {
        this.sqlgGraph.tx().setWriteTransaction(false);
    }

    /**
     * Ensures that the schema exists.
     *
     * @param schemaName The schema to create if it does not exist.
     */
    public Schema ensureSchemaExist(final String schemaName) {
        Objects.requireNonNull(schemaName, "schemaName can not be null!");
        Optional<Schema> schemaOptional = this.getSchema(schemaName);
        Schema schema;
        if (schemaOptional.isEmpty()) {
            this.startSchemaChange(
                    String.format("Topology ensureSchemaExist with '%s'", schemaName)
            );
            //search again after the lock is obtained.
            schemaOptional = this.getSchema(schemaName);
            if (schemaOptional.isEmpty()) {
                //create the schema and the vertex label.
                schema = Schema.createSchema(this.sqlgGraph, this, schemaName);
                this.uncommittedRemovedSchemas.remove(schemaName);
                this.uncommittedSchemas.put(schemaName, schema);
                fire(schema, null, TopologyChangeAction.CREATE, true);
                return schema;
            } else {
                return schemaOptional.get();
            }
        } else {
            return schemaOptional.get();
        }
    }

    /**
     * Import the foreign schema into the local graph's meta data.
     *
     * @param originalSchemas The foreign schemas to import.
     */
    public void importForeignSchemas(Set<Schema> originalSchemas) {
        Preconditions.checkState(!isSchemaChanged(), "To import a foreign schema there must not be any pending changes!");
        Preconditions.checkState(!this.locked, "The topology is locked, first unlock it before importing foreign schemas.");

        //validate all edge's vertices are in a foreign schema
        Schema.validateImportingEdgeLabels(originalSchemas);
        //Validate the VertexLabel's in and outEdgeLabels are in an imported schema.
        Schema.validateImportingVertexLabels(originalSchemas);

        Set<Schema> foreignSchemas = new HashSet<>();
        for (Schema originalSchema : originalSchemas) {
            Schema copy = originalSchema.readOnlyCopyVertexLabels(getSqlgGraph(), this);
            Preconditions.checkState(!this.schemas.containsKey(copy.getName()), "Schema with name '%s' exists.", copy.getName());
            foreignSchemas.add(copy);
            this.schemas.put(copy.getName(), copy);
            for (String label : copy.getVertexLabels().keySet()) {
                VertexLabel vertexLabel = copy.getVertexLabels().get(label);
                this.allTableCache.put(label, vertexLabel.getPropertyDefinitionMap());
            }
        }
        for (Schema originalSchema : originalSchemas) {
            Preconditions.checkState(this.schemas.containsKey(originalSchema.getName()), "'%s' not found in the schemas.", originalSchema.getName());
            Schema foreignSchema = this.schemas.get(originalSchema.getName());
            originalSchema.readOnlyCopyEdgeLabels(this, foreignSchema, foreignSchemas);
        }

        for (Schema originalSchema : originalSchemas) {
            Schema foreignSchema = this.schemas.get(originalSchema.getName());
            for (String label : foreignSchema.getEdgeLabels().keySet()) {
                EdgeLabel edgeLabel = foreignSchema.getEdgeLabels().get(label);
                this.allTableCache.put(label, edgeLabel.getPropertyDefinitionMap());
            }
            for (VertexLabel vertexLabel : foreignSchema.getVertexLabels().values()) {
                SchemaTable vertexLabelSchemaTable = SchemaTable.of(vertexLabel.getSchema().getName(), VERTEX_PREFIX + vertexLabel.getLabel());
                this.schemaTableForeignKeyCache.put(vertexLabelSchemaTable, Pair.of(new HashSet<>(), new HashSet<>()));
                for (EdgeLabel edgeLabel : vertexLabel.getInEdgeLabels().values()) {
                    this.schemaTableForeignKeyCache.get(vertexLabelSchemaTable)
                            .getLeft()
                            .add(SchemaTable.of(edgeLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()));
                }
                for (EdgeLabel edgeLabel : vertexLabel.getOutEdgeLabels().values()) {
                    this.schemaTableForeignKeyCache.get(vertexLabelSchemaTable)
                            .getRight()
                            .add(SchemaTable.of(vertexLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()));
                }
            }
            this.edgeForeignKeyCache.putAll(foreignSchema.getAllEdgeForeignKeys());
        }
    }

    public void clearForeignSchemas(Set<String> schemasToClear) {
        Set<String> toRemove = new HashSet<>();
        for (String schemaNameToRemove : schemasToClear) {
            if (this.schemas.containsKey(schemaNameToRemove)) {
                Schema schema = this.schemas.get(schemaNameToRemove);
                if (schema.isForeignSchema()) {
                    toRemove.add(schemaNameToRemove);
                    for (Map.Entry<String, EdgeLabel> edgeLabelEntry : schema.getEdgeLabels().entrySet()) {
                        String key = edgeLabelEntry.getKey();
                        EdgeLabel edgeLabel = edgeLabelEntry.getValue();
                        Preconditions.checkState(edgeLabel.isForeign());
                        Preconditions.checkState(this.allTableCache.remove(key) != null, "Failed to remove '%s' from 'allTableCache'", key);
                        Preconditions.checkState(
                                this.edgeForeignKeyCache.remove(schemaNameToRemove + "." + EDGE_PREFIX + edgeLabel.getLabel()) != null,
                                "Failed to remove '%s' from 'edgeForeignKeyCache'", key);
                    }
                    for (Map.Entry<String, VertexLabel> vertexLabelEntry : schema.getVertexLabels().entrySet()) {
                        String key = vertexLabelEntry.getKey();
                        VertexLabel vertexLabel = vertexLabelEntry.getValue();
                        Preconditions.checkState(vertexLabel.isForeign());
                        Preconditions.checkState(this.allTableCache.remove(key) != null, "Failed to remove '%s' from 'allTableCache'", key);
                        SchemaTable schemaTable = SchemaTable.of(schemaNameToRemove, VERTEX_PREFIX + vertexLabel.getLabel());
                        Preconditions.checkState(this.schemaTableForeignKeyCache.remove(schemaTable) != null, "Failed to remove '%s' from 'schemaTableForeignKeyCache'", key);
                    }
                } else {
                    Pair<Set<Pair<String, String>>, Set<Pair<String, String>>> removed = schema.clearForeignAbstractLabels();
                    for (Pair<String, String> vertex : removed.getLeft()) {
                        Preconditions.checkState(this.allTableCache.remove(vertex.getLeft()) != null, "Failed to remove '%s' from 'allTableCache", vertex.getLeft());
                        SchemaTable schemaTable = SchemaTable.of(schemaNameToRemove, VERTEX_PREFIX + vertex.getRight());
                        Preconditions.checkState(this.schemaTableForeignKeyCache.remove(schemaTable) != null, "Failed to remove '%s' from 'schemaTableForeignKeyCache'", schemaTable.toString());
                    }
                    for (Pair<String, String> edge : removed.getRight()) {
                        Preconditions.checkState(this.allTableCache.remove(edge.getLeft()) != null, "Failed to remove '%s' from 'allTableCache", edge.getLeft());
                        Preconditions.checkState(
                                this.edgeForeignKeyCache.remove(schemaNameToRemove + "." + EDGE_PREFIX + edge.getRight()) != null,
                                "Failed to remove '%s' from 'edgeForeignKeyCache'", edge);
                    }
                }

            }
        }
        for (String remove : toRemove) {
            this.schemas.remove(remove);
        }
    }

    public void clearForeignSchemas() {
        clearForeignSchemas(this.schemas.values().stream().map(Schema::getName).collect(Collectors.toSet()));
    }

    public void importForeignVertexEdgeLabels(Schema importIntoSchema, Set<VertexLabel> vertexLabels, Set<EdgeLabel> edgeLabels) {
        importIntoSchema.importForeignVertexAndEdgeLabels(vertexLabels, edgeLabels);
        for (VertexLabel vertexLabel : vertexLabels) {
            this.allTableCache.put(importIntoSchema.getName() + "." + VERTEX_PREFIX + vertexLabel.getLabel(), vertexLabel.getPropertyDefinitionMap());
        }
        for (EdgeLabel edgeLabel : edgeLabels) {
            this.allTableCache.put(importIntoSchema.getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel.getPropertyDefinitionMap());
            this.edgeForeignKeyCache.put(importIntoSchema.getName() + "." + EDGE_PREFIX + edgeLabel.getLabel(), edgeLabel.getAllEdgeForeignKeys());
        }
        vertexLabels.forEach((v) -> {
            SchemaTable vertexLabelSchemaTable = SchemaTable.of(v.getSchema().getName(), VERTEX_PREFIX + v.getLabel());
            this.schemaTableForeignKeyCache.put(vertexLabelSchemaTable, Pair.of(new HashSet<>(), new HashSet<>()));
            v.getInEdgeLabels().forEach(
                    (edgeLabelName, edgeLabel) -> this.schemaTableForeignKeyCache.get(vertexLabelSchemaTable)
                            .getLeft()
                            .add(SchemaTable.of(edgeLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()))
            );
            v.getOutEdgeLabels().forEach(
                    (edgeLabelName, edgeLabel) -> this.schemaTableForeignKeyCache.get(vertexLabelSchemaTable)
                            .getRight()
                            .add(SchemaTable.of(v.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()))
            );
        });
    }

    /**
     * Ensures that the vertex table exist in the db. The default schema is assumed. @See {@link SqlDialect#getPublicSchema()}
     * If any element does not exist the a lock is first obtained. After the lock is obtained the maps are rechecked to
     * see if the element has not been added in the mean time.
     *
     * @param label The vertex's label. Translates to a table prepended with 'V_'  and the table's name being the label.
     */
    public VertexLabel ensureVertexLabelExist(final String label) {
        return ensureVertexLabelExist(this.sqlgGraph.getSqlDialect().getPublicSchema(), label, Collections.emptyMap());
    }

    /**
     * Ensures that the vertex table and property columns exist in the db. The default schema is assumed. @See {@link SqlDialect#getPublicSchema()}
     * If any element does not exist the a lock is first obtained. After the lock is obtained the maps are rechecked to
     * see if the element has not been added in the mean time.
     *
     * @param label   The vertex's label. Translates to a table prepended with 'V_'  and the table's name being the label.
     * @param columns The properties with their types.
     * @see PropertyType
     */
    public VertexLabel ensureVertexLabelExist(final String label, final Map<String, PropertyDefinition> columns) {
        return ensureVertexLabelExist(this.sqlgGraph.getSqlDialect().getPublicSchema(), label, columns);
    }

    public VertexLabel ensureVertexLabelExist(final String label, final Map<String, PropertyDefinition> columns, ListOrderedSet<String> identifiers) {
        return ensureVertexLabelExist(this.sqlgGraph.getSqlDialect().getPublicSchema(), label, columns, identifiers);
    }

    /**
     * Ensures that the schema, vertex table exist in the db.
     * If any element does not exist the a lock is first obtained. After the lock is obtained the maps are rechecked to
     * see if the element has not been added in the mean time.
     *
     * @param schemaName The schema the vertex is in.
     * @param label      The vertex's label. Translates to a table prepended with 'V_'  and the table's name being the label.
     */
    public VertexLabel ensureVertexLabelExist(final String schemaName, final String label) {
        return ensureVertexLabelExist(schemaName, label, Collections.emptyMap());
    }

    /**
     * Ensures that the schema, vertex table and property columns exist in the db.
     * If any element does not exist the a lock is first obtained. After the lock is obtained the maps are rechecked to
     * see if the element has not been added in the mean time.
     *
     * @param schemaName The schema the vertex is in.
     * @param label      The vertex's label. Translates to a table prepended with 'V_'  and the table's name being the label.
     * @param properties The properties with their types.
     * @see PropertyType
     */
    public VertexLabel ensureVertexLabelExist(final String schemaName, final String label, final Map<String, PropertyDefinition> properties) {
        Objects.requireNonNull(schemaName, "Given tables must not be null");
        Objects.requireNonNull(label, "Given table must not be null");
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not be prefixed with %s", VERTEX_PREFIX);
        Schema schema = this.ensureSchemaExist(schemaName);
        Preconditions.checkState(schema != null, "Schema must be present after calling ensureSchemaExist");
        return schema.ensureVertexLabelExist(label, properties);
    }

    /**
     * Ensures that the schema, vertex table and property columns exist in the db.
     * If any element does not exist the a lock is first obtained. After the lock is obtained the maps are rechecked to
     * see if the element has not been added in the mean time.
     *
     * @param schemaName  The schema the vertex is in.
     * @param label       The vertex's label. Translates to a table prepended with 'V_'  and the table's name being the label.
     * @param properties  The properties with their types.
     * @param identifiers The Vertex's identifiers. i.e. it will be the primary key.
     * @see PropertyType
     */
    public VertexLabel ensureVertexLabelExist(final String schemaName, final String label, final Map<String, PropertyDefinition> properties, ListOrderedSet<String> identifiers) {
        Objects.requireNonNull(schemaName, "Given tables must not be null");
        Objects.requireNonNull(label, "Given table must not be null");
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not be prefixed with %s", VERTEX_PREFIX);

        Schema schema = this.ensureSchemaExist(schemaName);
        Preconditions.checkState(schema != null, "Schema must be present after calling ensureSchemaExist");
        return schema.ensureVertexLabelExist(label, properties, identifiers);
    }

    public void ensureTemporaryVertexTableExist(final String schema, final String label, final Map<String, PropertyDefinition> properties) {
        Objects.requireNonNull(schema, "Given schema may not be null");
        Preconditions.checkState(schema.equals(this.sqlgGraph.getSqlDialect().getPublicSchema()), "Temporary vertices may only be created in the '" + this.sqlgGraph.getSqlDialect().getPublicSchema() + "' schema. Found + " + schema);
        Objects.requireNonNull(label, "Given label may not be null");
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not be prefixed with %s", VERTEX_PREFIX);
        Schema publicSchema = getPublicSchema();
        Preconditions.checkState(publicSchema != null, "Schema must be present after calling ensureSchemaExist");
        publicSchema.ensureTemporaryVertexTableExist(label, properties);
    }

    /**
     * Ensures that the edge table with out and in {@link VertexLabel}s and property columns exists.
     * The edge table will reside in the out vertex's schema.
     * If a table, a foreign key or a column needs to be created a lock is first obtained.
     *
     * @param edgeLabelName  The label of the edge for which a table will be created.
     * @param outVertexLabel The edge's out {@link VertexLabel}
     * @param inVertexLabel  The edge's in {@link VertexLabel}
     * @param properties     The edge's properties with their type.
     * @return The {@link EdgeLabel}
     */
    public EdgeLabel ensureEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> properties) {

        return ensureEdgeLabelExist(edgeLabelName, outVertexLabel, inVertexLabel, properties, new ListOrderedSet<>());
    }

    /**
     * Ensures that the edge table with out and in {@link VertexLabel}s and property columns exists.
     * The edge table will reside in the out vertex's schema.
     * If a table, a foreign key or a column needs to be created a lock is first obtained.
     *
     * @param edgeLabelName  The label of the edge for which a table will be created.
     * @param outVertexLabel The edge's out {@link VertexLabel}
     * @param inVertexLabel  The edge's in {@link VertexLabel}
     * @param properties     The edge's properties with their type.
     * @param identifiers    The edge's user supplied identifiers. They will make up the edge's primary key.
     * @return The {@link EdgeLabel}
     */
    public EdgeLabel ensureEdgeLabelExist(
            final String edgeLabelName,
            final VertexLabel outVertexLabel,
            final VertexLabel inVertexLabel,
            Map<String, PropertyDefinition> properties,
            ListOrderedSet<String> identifiers) {

        Objects.requireNonNull(edgeLabelName, "Given edgeLabelName must not be null");
        Objects.requireNonNull(outVertexLabel, "Given outVertexLabel must not be null");
        Objects.requireNonNull(inVertexLabel, "Given inVertexLabel must not be null");
        Objects.requireNonNull(identifiers, "Given identifiers must not be null");
        Schema outVertexSchema = outVertexLabel.getSchema();
        return outVertexSchema.ensureEdgeLabelExist(edgeLabelName, outVertexLabel, inVertexLabel, properties, identifiers);
    }

    /**
     * Ensures that the edge table with out and in foreign keys and property columns exists.
     * The edge table will reside in the out vertex's schema.
     * If a table, a foreign key or a column needs to be created a lock is first obtained.
     *
     * @param edgeLabelName The label for the edge.
     * @param foreignKeyOut The {@link SchemaTable} that represents the out vertex.
     * @param foreignKeyIn  The {@link SchemaTable} that represents the in vertex.
     * @param properties    The edge's properties with their type.
     */
    public void ensureEdgeLabelExist(
            final String edgeLabelName,
            final SchemaTable foreignKeyOut,
            final SchemaTable foreignKeyIn,
            Map<String, PropertyDefinition> properties) {

        Objects.requireNonNull(edgeLabelName, "Given edgeLabelName must not be null");
        Objects.requireNonNull(foreignKeyOut, "Given outTable must not be null");
        Objects.requireNonNull(foreignKeyIn, "Given inTable must not be null");

        Preconditions.checkState(getVertexLabel(foreignKeyOut.getSchema(), foreignKeyOut.getTable()).isPresent(), "The out vertex must already exist before invoking 'ensureEdgeLabelExist'. \"%s\" does not exist", foreignKeyIn.toString());
        Preconditions.checkState(getVertexLabel(foreignKeyIn.getSchema(), foreignKeyIn.getTable()).isPresent(), "The in vertex must already exist before invoking 'ensureEdgeLabelExist'. \"%s\" does not exist", foreignKeyIn.toString());

        //outVertexSchema will be there as the Precondition checked it.
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Schema outVertexSchema = this.getSchema(foreignKeyOut.getSchema()).get();
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Schema inVertexSchema = this.getSchema(foreignKeyIn.getSchema()).get();
        Optional<VertexLabel> outVertexLabel = outVertexSchema.getVertexLabel(foreignKeyOut.getTable());
        Optional<VertexLabel> inVertexLabel = inVertexSchema.getVertexLabel(foreignKeyIn.getTable());
        Preconditions.checkState(outVertexLabel.isPresent(), "out VertexLabel must be present");
        Preconditions.checkState(inVertexLabel.isPresent(), "in VertexLabel must be present");
        outVertexSchema.ensureEdgeLabelExist(edgeLabelName, outVertexLabel.get(), inVertexLabel.get(), properties);
    }

    /**
     * Ensures that the vertex's table has the required columns.
     * If a columns needs to be created a lock will be obtained.
     * The vertex's schema and table must already exists.
     * The default "public" schema will be used. {@link SqlDialect#getPublicSchema()}
     *
     * @param label      The vertex's label.
     * @param properties The properties to create if they do not exist.
     */
    public void ensureVertexLabelPropertiesExist(String label, Map<String, PropertyDefinition> properties) {
        ensureVertexLabelPropertiesExist(this.sqlgGraph.getSqlDialect().getPublicSchema(), label, properties);
    }

    /**
     * Ensures that the vertex's table has the required columns.
     * If a columns needs to be created a lock will be obtained.
     * The vertex's schema and table must already exists.
     *
     * @param schemaName The schema the vertex resides in.
     * @param label      The vertex's label.
     * @param properties The properties to create if they do not exist.
     */
    public void ensureVertexLabelPropertiesExist(String schemaName, String label, Map<String, PropertyDefinition> properties) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not start with \"%s\"", VERTEX_PREFIX);
        if (!schemaName.equals(SQLG_SCHEMA)) {
            Optional<Schema> schemaOptional = getSchema(schemaName);
            if (schemaOptional.isEmpty()) {
                throw new IllegalStateException(String.format("BUG: schema \"%s\" can not be null", schemaName));
            }
            //createVertexLabel the table
            schemaOptional.get().ensureVertexColumnsExist(label, properties);
        }
    }

    /**
     * Ensures that the edge's table has the required columns.
     * The default schema is assumed. @see {@link SqlDialect#getPublicSchema()}
     * If a columns needs to be created a lock will be obtained.
     * The edge's schema and table must already exists.
     *
     * @param label      The edge's label.
     * @param properties The properties to create if they do not exist.
     */
    public void ensureEdgePropertiesExist(String label, Map<String, PropertyDefinition> properties) {
        ensureEdgePropertiesExist(this.sqlgGraph.getSqlDialect().getPublicSchema(), label, properties);
    }

    /**
     * Ensures that the edge's table has the required columns.
     * If a columns needs to be created a lock will be obtained.
     * The edge's schema and table must already exists.
     *
     * @param schemaName The  schema the edge resides in.
     * @param label      The edge's label.
     * @param properties The properties to create if they do not exist.
     */
    public void ensureEdgePropertiesExist(String schemaName, String label, Map<String, PropertyDefinition> properties) {
        Preconditions.checkArgument(!label.startsWith(EDGE_PREFIX), "label may not start with \"%s\"", EDGE_PREFIX);
        Preconditions.checkState(!schemaName.equals(SQLG_SCHEMA), "Topology.ensureEdgePropertiesExist may not be called for \"%s\"", SQLG_SCHEMA);
        Optional<Schema> schemaOptional = getSchema(schemaName);
        if (schemaOptional.isEmpty()) {
            throw new IllegalStateException(String.format("BUG: schema %s can not be null", schemaName));
        }
        schemaOptional.get().ensureEdgeColumnsExist(label, properties);
    }

    private void beforeCommit() {
        if (this.distributed && isSchemaChanged()) {
            Optional<JsonNode> jsonNodeOptional = this.toNotifyJson();
            if (jsonNodeOptional.isPresent()) {
                SqlSchemaChangeDialect sqlSchemaChangeDialect = (SqlSchemaChangeDialect) this.sqlgGraph.getSqlDialect();
                LocalDateTime timestamp = LocalDateTime.now();
                int pid = sqlSchemaChangeDialect.notifyChange(this.sqlgGraph, timestamp, jsonNodeOptional.get());
                this.ownPids.add(pid);
            }
        }
    }

    private Schema removeSchemaFromCaches(String schema) {
        Schema s = this.schemas.remove(schema);
        this.allTableCache.keySet().removeIf(schemaTable -> schemaTable.startsWith(schema + "."));
        this.edgeForeignKeyCache.keySet().removeIf(schemaTable -> schemaTable.startsWith(schema + "."));
        this.schemaTableForeignKeyCache.keySet().removeIf(schemaTable -> schemaTable.getSchema().equals(schema));
        return s;
    }

    private void afterCommit() {
        try {
            getPublicSchema().removeTemporaryTables();
            if (isSchemaChanged()) {
                for (Iterator<Map.Entry<String, Schema>> it = this.uncommittedSchemas.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry<String, Schema> entry = it.next();
                    this.schemas.put(entry.getKey(), entry.getValue());
                    it.remove();
                }
                for (Iterator<String> it = this.uncommittedRemovedSchemas.iterator(); it.hasNext(); ) {
                    String sch = it.next();
                    removeSchemaFromCaches(sch);
                    it.remove();
                }
                //merge the allTableCache and uncommittedAllTables
                Map<String, AbstractLabel> uncommittedAllTables = getUncommittedAllTables();
                for (Map.Entry<String, AbstractLabel> stringMapEntry : uncommittedAllTables.entrySet()) {
                    String uncommittedSchemaTable = stringMapEntry.getKey();
                    AbstractLabel abstractLabel = stringMapEntry.getValue();
                    // we replace the whole map since getPropertyTypeMap() gives the full map, and we may have removed properties
                    this.allTableCache.put(uncommittedSchemaTable, abstractLabel.getPropertyDefinitionMap());
                }

                Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> uncommittedSchemaTableForeignKeys = getUncommittedSchemaTableForeignKeys();
                for (Map.Entry<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> schemaTablePairEntry : uncommittedSchemaTableForeignKeys.entrySet()) {
                    Pair<Set<SchemaTable>, Set<SchemaTable>> foreignKeys = this.schemaTableForeignKeyCache.get(schemaTablePairEntry.getKey());
                    if (foreignKeys != null) {
                        foreignKeys.getLeft().addAll(schemaTablePairEntry.getValue().getLeft());
                        foreignKeys.getRight().addAll(schemaTablePairEntry.getValue().getRight());
                    } else {
                        this.schemaTableForeignKeyCache.put(schemaTablePairEntry.getKey(), schemaTablePairEntry.getValue());
                    }
                }

                Map<String, Set<ForeignKey>> uncommittedEdgeForeignKeys = getUncommittedEdgeForeignKeys();
                for (Map.Entry<String, Set<ForeignKey>> entry : uncommittedEdgeForeignKeys.entrySet()) {
                    Set<ForeignKey> foreignKeys = this.edgeForeignKeyCache.get(entry.getKey());
                    if (foreignKeys == null) {
                        this.edgeForeignKeyCache.put(entry.getKey(), entry.getValue());
                    } else {
                        foreignKeys.addAll(entry.getValue());
                    }
                }
                Map<String, Set<ForeignKey>> uncommittedRemovedEdgeForeignKeys = getUncommittedRemovedEdgeForeignKeys();
                for (Map.Entry<String, Set<ForeignKey>> entry : uncommittedRemovedEdgeForeignKeys.entrySet()) {
                    Set<ForeignKey> foreignKeys = this.edgeForeignKeyCache.get(entry.getKey());
                    if (foreignKeys != null) {
                        foreignKeys.removeAll(entry.getValue());
                        if (foreignKeys.isEmpty()) {
                            this.edgeForeignKeyCache.remove(entry.getKey());
                        }
                    }
                }
                for (Schema schema : getSchemas()) {
                    for (String uncommittedRemovedEdgeLabel : schema.uncommittedRemovedEdgeLabels) {
                        this.edgeForeignKeyCache.remove(uncommittedRemovedEdgeLabel);
                    }
                }
                for (Schema schema : this.schemas.values()) {
                    schema.afterCommit();
                }
            }
        } finally {
            z_internalSqlWriteUnlock();
            this.schemaChanged.set(false);
        }
    }

    private void afterRollback() {
        getPublicSchema().removeTemporaryTables();
        if (isSchemaChanged()) {
            for (Iterator<Map.Entry<String, Schema>> it = this.uncommittedSchemas.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Schema> entry = it.next();
                entry.getValue().afterRollback();
                it.remove();
            }
            this.uncommittedRemovedSchemas.clear();
            for (Schema schema : this.schemas.values()) {
                schema.afterRollback();
            }
            z_internalSqlWriteUnlock();
            this.schemaChanged.set(false);
        }
    }

    public void cacheTopology() {
        LOGGER.debug("start cacheTopology");
        StopWatch stopWatch = StopWatch.createStarted();
        this.startSchemaChange(
                "Topology cacheTopology"
        );
        GraphTraversalSource traversalSource = this.sqlgGraph.topology();

        StopWatch stopWatch1 = StopWatch.createStarted();
        loadVertexOutEdgesAndProperties(traversalSource);
        stopWatch1.stop();
        LOGGER.debug("cacheTopology.loadVertexOutEdgesAndProperties took: {} {}", sqlgGraph.getJdbcUrl(), stopWatch1);

        stopWatch1.reset();
        stopWatch1.start();
        loadVertexIndices();
        stopWatch1.stop();
        LOGGER.debug("cacheTopology.loadVertexIndices took: {} {}", sqlgGraph.getJdbcUrl(), stopWatch1);

        stopWatch1.reset();
        stopWatch1.start();
        loadEdgeIndices();
        stopWatch1.stop();
        LOGGER.debug("cacheTopology.loadEdgeIndices took: {} {}", sqlgGraph.getJdbcUrl(), stopWatch1);

        //Now load the in edges
        stopWatch1.reset();
        stopWatch1.start();
        loadInEdgeLabels();
        stopWatch1.stop();
        LOGGER.debug("cacheTopology.loadInEdgeLabels took: {} {}", sqlgGraph.getJdbcUrl(), stopWatch1);

        for (String schemaName : this.schemas.keySet()) {
            Optional<Schema> schemaOptional = getSchema(schemaName);
            Preconditions.checkState(schemaOptional.isPresent());
            Schema schema = schemaOptional.get();
            //We can clear all AbstractLabel.identifierMap to save some memory
            for (VertexLabel vertexLabel : schema.getVertexLabels().values()) {
                vertexLabel.clearIdentifiersMap();
            }
            for (EdgeLabel edgeLabel : schema.getEdgeLabels().values()) {
                edgeLabel.clearIdentifiersMap();
            }
        }

        //populate the allTablesCache
        for (Schema schema : this.schemas.values()) {
            if (!schema.isSqlgSchema()) {
                this.allTableCache.putAll(schema.getAllTables());
            }
        }
        //populate the schemaTableForeignKeyCache
        this.schemaTableForeignKeyCache.putAll(loadTableLabels());
        //populate the edgeForeignKey cache
        this.edgeForeignKeyCache.putAll(loadAllEdgeForeignKeys());
        stopWatch.stop();
        LOGGER.debug("end cacheTopology time: {} {}", sqlgGraph.getJdbcUrl(), stopWatch);
    }

    @SuppressWarnings("resource")
    private void loadVertexOutEdgesAndProperties(GraphTraversalSource traversalSource) {
        StopWatch stopWatch = StopWatch.createStarted();
        Connection conn = sqlgGraph.tx().getConnection();
        String propertySql = """
                SELECT
                    "schema"."name" AS "schemaName",
                    "vertex"."name" as "vertexLabelName",
                    "vertex"."partitionType",
                    "vertex"."partitionExpression",
                    "vertex"."shardCount",
                    "property"."name",
                    "property"."type",
                    "property"."defaultLiteral",
                    "property"."checkConstraint",
                    "property"."lowerMultiplicity",
                    "property"."upperMultiplicity"
                FROM
                    "sqlg_schema"."V_schema" as "schema" LEFT JOIN
                	"sqlg_schema"."E_schema_vertex" as "schema_edge" ON "schema"."ID" = "schema_edge"."sqlg_schema.schema__O" LEFT JOIN
                	"sqlg_schema"."V_vertex" as "vertex" ON "schema_edge"."sqlg_schema.vertex__I" = "vertex"."ID" LEFT JOIN
                	"sqlg_schema"."E_vertex_property" as "vertex_property" ON "vertex"."ID" = "vertex_property"."sqlg_schema.vertex__O" LEFT JOIN
                	"sqlg_schema"."V_property" as "property" ON "vertex_property"."sqlg_schema.property__I" = "property"."ID";
                """;
        if (!sqlgGraph.getSqlDialect().getColumnEscapeKey().equals("\"")) {
            propertySql = propertySql.replace("\"", sqlgGraph.getSqlDialect().getColumnEscapeKey());
        }
        try (Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery(propertySql);
            while (rs.next()) {
                String schemaName = rs.getString("schemaName");
                Schema schema;
                if (!this.sqlgGraph.getSqlDialect().getPublicSchema().equals(schemaName)) {
                    if (!this.schemas.containsKey(schemaName)) {
                        schema = Schema.loadUserSchema(this, schemaName);
                        this.schemas.put(schemaName, schema);
                    } else {
                        schema = this.schemas.get(schemaName);
                    }
                } else {
                    schema = sqlgGraph.getTopology().getPublicSchema();
                }

                VertexLabel vertexLabel;
                String vertexLabelName = rs.getString("vertexLabelName");
                if (!rs.wasNull()) {
                    Optional<VertexLabel> vertexLabelOptional = schema.getVertexLabel(vertexLabelName);
                    if (vertexLabelOptional.isEmpty()) {
                        String _partitionType = rs.getString(SQLG_SCHEMA_VERTEX_LABEL_PARTITION_TYPE);
                        Preconditions.checkState(!rs.wasNull(), SQLG_SCHEMA_VERTEX_LABEL_PARTITION_TYPE + " may never be null. BUG!");
                        PartitionType partitionType = PartitionType.valueOf(_partitionType);
                        String partitionExpression = rs.getString(SQLG_SCHEMA_VERTEX_LABEL_PARTITION_EXPRESSION);
                        if (rs.wasNull()) {
                            partitionExpression = null;
                        }
                        Integer shardCount = rs.getInt(SQLG_SCHEMA_VERTEX_LABEL_DISTRIBUTION_SHARD_COUNT);
                        if (rs.wasNull()) {
                            shardCount = null;
                        }
                        vertexLabel = schema.cacheTopologyAddToVertexLabels(vertexLabelName, partitionType, partitionExpression, shardCount);
                    } else {
                        vertexLabel = vertexLabelOptional.get();
                    }

                    String propertyName = rs.getString(SQLG_SCHEMA_PROPERTY_NAME);
                    if (!rs.wasNull()) {
                        String propertyType = rs.getString(SQLG_SCHEMA_PROPERTY_TYPE);
                        String defaultLiteral = rs.getString(SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL);
                        String checkConstraint = rs.getString(SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT);
                        int lower = rs.getInt(SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER);
                        int upper = rs.getInt(SQLG_SCHEMA_PROPERTY_MULTIPLICITY_UPPER);
                        PropertyColumn property = new PropertyColumn(
                                vertexLabel,
                                propertyName,
                                PropertyDefinition.of(
                                        PropertyType.valueOf(propertyType),
                                        Multiplicity.of(
                                                lower,
                                                upper
                                        ),
                                        defaultLiteral,
                                        checkConstraint
                                )
                        );
                        vertexLabel.addToPropertyColumns(property);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        stopWatch.stop();
        LOGGER.debug("load propertyColumns, time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        String identifierSql = """
                SELECT
                	"schema"."name" AS "schemaName",
                	 "vertex"."name" AS "vertexLabelName",
                	 "vertex_identifier"."identifier_index",
                	 "property"."name"
                FROM
                	"sqlg_schema"."V_schema" as "schema" LEFT JOIN
                	"sqlg_schema"."E_schema_vertex" as "schema_edge" ON "schema"."ID" = "schema_edge"."sqlg_schema.schema__O" JOIN
                	"sqlg_schema"."V_vertex" as "vertex" ON "schema_edge"."sqlg_schema.vertex__I" = "vertex"."ID" JOIN
                	"sqlg_schema"."E_vertex_identifier" as "vertex_identifier" ON "vertex"."ID" = "vertex_identifier"."sqlg_schema.vertex__O" JOIN
                	"sqlg_schema"."V_property" as "property" ON "vertex_identifier"."sqlg_schema.property__I" = "property"."ID";
                """;
        if (!sqlgGraph.getSqlDialect().getColumnEscapeKey().equals("\"")) {
            identifierSql = identifierSql.replace("\"", sqlgGraph.getSqlDialect().getColumnEscapeKey());
        }
        conn = sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery(identifierSql);
            while (rs.next()) {
                String schemaName = rs.getString("schemaName");
                Schema schema = this.schemas.get(schemaName);
                Preconditions.checkNotNull(schema, "Schema %s not found!", schemaName);
                String vertexLabelName = rs.getString("vertexLabelName");
                Preconditions.checkNotNull(vertexLabelName, SQLG_SCHEMA_VERTEX_LABEL_NAME + " may never be null. BUG!");
                VertexLabel vertexLabel = schema.getVertexLabel(vertexLabelName).orElseThrow();
                Preconditions.checkNotNull(vertexLabel, "VertexLabel %s not found!", vertexLabelName);
                int identifierIndex = rs.getInt(SQLG_SCHEMA_VERTEX_IDENTIFIER_INDEX_EDGE);
                String propertyName = rs.getString(SQLG_SCHEMA_PROPERTY_NAME);
                vertexLabel.addIdentifier(
                        propertyName,
                        identifierIndex
                );
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        stopWatch.stop();
        LOGGER.debug("load identifiers, time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        List<RecordId> vertexPartitionIds = new ArrayList<>();
        String rootPartitionSql = """
                SELECT
                	"sqlg_schema"."V_partition"."ID" AS "ID",
                	"sqlg_schema"."V_partition"."partitionExpression" AS "partitionExpression",
                	"sqlg_schema"."V_partition"."in" AS "in",
                	"sqlg_schema"."V_partition"."abstractLabelName" AS "abstractLabelName",
                	"sqlg_schema"."V_partition"."name" AS "name",
                	"sqlg_schema"."V_partition"."from" AS "from",
                	"sqlg_schema"."V_partition"."to" AS "to",
                	"sqlg_schema"."V_partition"."schemaName" AS "schemaName",
                	"sqlg_schema"."V_partition"."remainder" AS "remainder",
                	"sqlg_schema"."V_partition"."partitionType" AS "partitionType",
                	"sqlg_schema"."V_partition"."modulus" AS "modulus"
                FROM
                	"sqlg_schema"."V_vertex" INNER JOIN
                	"sqlg_schema"."E_vertex_partition" ON "sqlg_schema"."V_vertex"."ID" = "sqlg_schema"."E_vertex_partition"."sqlg_schema.vertex__O" INNER JOIN
                	"sqlg_schema"."V_partition" ON "sqlg_schema"."E_vertex_partition"."sqlg_schema.partition__I" = "sqlg_schema"."V_partition"."ID"
                """;
        if (!sqlgGraph.getSqlDialect().getColumnEscapeKey().equals("\"")) {
            rootPartitionSql = rootPartitionSql.replace("\"", sqlgGraph.getSqlDialect().getColumnEscapeKey());
        }
        conn = sqlgGraph.tx().getConnection();
        SchemaTable schemaTable = SchemaTable.of("sqlg_schema", "partition");
        try (Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery(rootPartitionSql);
            while (rs.next()) {
                String schemaName = rs.getString("schemaName");
                String abstractLabelName = rs.getString("abstractLabelName");
                Schema schema = this.schemas.get(schemaName);
                Preconditions.checkNotNull(schema, "Schema %s not found!", schemaName);
                Preconditions.checkNotNull(abstractLabelName, SQLG_SCHEMA_PARTITION_ABSTRACT_LABEL_NAME + " may never be null. BUG!");
                VertexLabel vertexLabel = schema.getVertexLabel(abstractLabelName).orElseThrow(() -> new IllegalStateException(String.format("vertexLabel %s not found in schema %s", abstractLabelName, schemaName)));
                String from = rs.getString("from");
                if (rs.wasNull()) {
                    from = null;
                }
                String to = rs.getString("to");
                if (rs.wasNull()) {
                    to = null;
                }
                String in = rs.getString("in");
                if (rs.wasNull()) {
                    in = null;
                }
                Integer modulus = rs.getInt("modulus");
                if (rs.wasNull()) {
                    modulus = null;
                }
                Integer remainder = rs.getInt("remainder");
                if (rs.wasNull()) {
                    remainder = null;
                }
                String partitionType = rs.getString("partitionType");
                if (rs.wasNull()) {
                    partitionType = null;
                }
                String partitionExpression = rs.getString("partitionExpression");
                if (rs.wasNull()) {
                    partitionExpression = null;
                }
                String name = rs.getString("name");
                if (rs.wasNull()) {
                    name = null;
                }
                long id = rs.getLong("ID");
                vertexLabel.addPartition(
                        from, to, in, modulus, remainder, partitionType, partitionExpression, name
                );
                vertexPartitionIds.add(RecordId.from(schemaTable, id));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        stopWatch.stop();
        LOGGER.debug("load vertex root partitions, time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        List<Path> vertexSubPartitions = traversalSource.V().hasId(P.within(vertexPartitionIds))
                .repeat(
                        __.out(SQLG_SCHEMA_PARTITION_PARTITION_EDGE).simplePath()
                )
                .until(
                        __.not(__.out(SQLG_SCHEMA_PARTITION_PARTITION_EDGE).simplePath())
                )
                .path()
                .toList();
        for (Path subPartition : vertexSubPartitions) {
            Partition partition = null;
            //start the index at 1, we are not interested in the 'vertex' vertex here!
            for (int i = 0; i < subPartition.size(); i++) {
                Vertex subPartitionVertex = subPartition.get(i);
                String schemaName = subPartitionVertex.value(Topology.SQLG_SCHEMA_PARTITION_SCHEMA_NAME);
                String abstractLabelName = subPartitionVertex.value(Topology.SQLG_SCHEMA_PARTITION_ABSTRACT_LABEL_NAME);
                Schema schema = this.schemas.get(schemaName);
                Preconditions.checkNotNull(schema, "Schema %s not found!", schemaName);
                Preconditions.checkNotNull(abstractLabelName, SQLG_SCHEMA_PARTITION_ABSTRACT_LABEL_NAME + " may never be null. BUG!");
                VertexLabel vertexLabel = schema.getVertexLabel(abstractLabelName).orElseThrow();
                Preconditions.checkNotNull(vertexLabel, "VertexLabel %s not found!", abstractLabelName);
                String partitionName = subPartitionVertex.value(Topology.SQLG_SCHEMA_PARTITION_NAME);
                if (i == 0) {
                    partition = vertexLabel.getPartition(partitionName).orElseThrow();
                } else {
                    Optional<Partition> partitionOpt = partition.getPartition(partitionName);
                    if (partitionOpt.isPresent()) {
                        partition = partitionOpt.get();
                    } else {
                        partition = partition.addPartition(subPartitionVertex);
                    }
                }
            }
        }
        stopWatch.stop();
        LOGGER.debug("load vertex sub partitions, time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        String edgeSql = """ 
                SELECT
                	"schema"."name" AS "schemaName",
                	"vertex"."name" AS "vertexLabelName",
                	"out_edges"."lowerMultiplicity",
                	"out_edges"."upperMultiplicity",
                	"out_edges"."unique",
                	"out_edges"."ordered",
                	"edge"."name" AS "edgeLabelName",
                	"edge"."partitionType",
                	"edge"."partitionExpression",
                	"edge"."shardCount",
                	"property"."name",
                	"property"."type",
                	"property"."defaultLiteral",
                	"property"."checkConstraint",
                	"property"."lowerMultiplicity" AS "propertyLowerMultiplicity",
                	"property"."upperMultiplicity" AS "propertyUpperMultiplicity"
                FROM
                	"sqlg_schema"."V_schema" as "schema"	JOIN
                	"sqlg_schema"."E_schema_vertex" as "schema_edge" ON "schema"."ID" = "schema_edge"."sqlg_schema.schema__O" JOIN
                	"sqlg_schema"."V_vertex" as "vertex" ON "schema_edge"."sqlg_schema.vertex__I" = "vertex"."ID" JOIN
                	"sqlg_schema"."E_out_edges" as "out_edges" ON "vertex"."ID" = "out_edges"."sqlg_schema.vertex__O" JOIN
                	"sqlg_schema"."V_edge" as "edge" ON "out_edges"."sqlg_schema.edge__I" = "edge"."ID" LEFT JOIN
                	"sqlg_schema"."E_edge_property" as "edge_property" ON "edge"."ID" = "edge_property"."sqlg_schema.edge__O" LEFT JOIN
                	"sqlg_schema"."V_property" as "property" ON "edge_property"."sqlg_schema.property__I" = "property"."ID";
                """;
        if (!sqlgGraph.getSqlDialect().getColumnEscapeKey().equals("\"")) {
            edgeSql = edgeSql.replace("\"", sqlgGraph.getSqlDialect().getColumnEscapeKey());
        }
        conn = sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery(edgeSql);
            while (rs.next()) {
                String schemaName = rs.getString("schemaName");
                Schema schema = this.schemas.get(schemaName);
                Preconditions.checkNotNull(schema, "Schema %s not found!", schemaName);
                String vertexLabelName = rs.getString("vertexLabelName");
                Preconditions.checkNotNull(vertexLabelName, SQLG_SCHEMA_VERTEX_LABEL_NAME + " may never be null. BUG!");
                VertexLabel vertexLabel = schema.getVertexLabel(vertexLabelName).orElseThrow(() -> new IllegalStateException(String.format("Failed to find vertexLabel %s in schema %s", vertexLabelName, schemaName)));
                Preconditions.checkNotNull(vertexLabel, "VertexLabel %s not found!", vertexLabelName);
                Preconditions.checkState(!rs.wasNull(), SQLG_SCHEMA_VERTEX_LABEL_NAME + " may never be null. BUG!");
                String edgeLabelName = rs.getString("edgeLabelName");
                String _partitionType = rs.getString(SQLG_SCHEMA_EDGE_LABEL_PARTITION_TYPE);
                Preconditions.checkState(!rs.wasNull(), SQLG_SCHEMA_EDGE_LABEL_PARTITION_TYPE + " may never be null. BUG!");
                PartitionType partitionType = PartitionType.valueOf(_partitionType);
                String partitionExpression = rs.getString(SQLG_SCHEMA_EDGE_LABEL_PARTITION_EXPRESSION);
                if (rs.wasNull()) {
                    partitionExpression = null;
                }
                Integer shardCount = rs.getInt(SQLG_SCHEMA_EDGE_LABEL_DISTRIBUTION_SHARD_COUNT);
                if (rs.wasNull()) {
                    shardCount = null;
                }
                Optional<EdgeLabel> edgeLabelOptional = schema.getEdgeLabel(edgeLabelName);
                EdgeLabel edgeLabel;
                if (edgeLabelOptional.isEmpty()) {
                    if (partitionType.isNone()) {
                        edgeLabel = EdgeLabel.loadFromDb(vertexLabel.getSchema().getTopology(), edgeLabelName);
                    } else {
                        Preconditions.checkNotNull(partitionExpression, "partitionExpression may not be null for partitionType %s", partitionType);
                        edgeLabel = EdgeLabel.loadFromDb(vertexLabel.getSchema().getTopology(), edgeLabelName, partitionType, partitionExpression);
                    }
                    long lowerMultiplicity = rs.getLong(SQLG_SCHEMA_OUT_EDGES_LOWER_MULTIPLICITY);
                    long upperMultiplicity = rs.getLong(SQLG_SCHEMA_OUT_EDGES_UPPER_MULTIPLICITY);
                    boolean unique = rs.getBoolean(SQLG_SCHEMA_OUT_EDGES_UNIQUE);
                    Multiplicity multiplicity = Multiplicity.of(lowerMultiplicity, upperMultiplicity, unique);
                    vertexLabel.addToOutEdgeRoles(schemaName, new EdgeRole(vertexLabel, edgeLabel, Direction.OUT, true, multiplicity));
                    if (shardCount != null) {
                        edgeLabel.setShardCount(shardCount);
                    }
                    schema.addToOutEdgeLabels(schemaName, edgeLabelName, edgeLabel);
                } else {
                    edgeLabel = edgeLabelOptional.get();
                    if (vertexLabel.getOutEdgeLabel(edgeLabelName).isEmpty()) {
                        long lowerMultiplicity = rs.getLong(SQLG_SCHEMA_OUT_EDGES_LOWER_MULTIPLICITY);
                        long upperMultiplicity = rs.getLong(SQLG_SCHEMA_OUT_EDGES_UPPER_MULTIPLICITY);
                        boolean unique = rs.getBoolean(SQLG_SCHEMA_OUT_EDGES_UNIQUE);
                        Multiplicity multiplicity = Multiplicity.of(lowerMultiplicity, upperMultiplicity, unique);
                        vertexLabel.addToOutEdgeRoles(schemaName, new EdgeRole(vertexLabel, edgeLabel, Direction.OUT, true, multiplicity));
                        if (shardCount != null) {
                            edgeLabel.setShardCount(shardCount);
                        }
                        schema.addToOutEdgeLabels(schemaName, edgeLabelName, edgeLabel);
                    }
                }

                String propertyName = rs.getString(SQLG_SCHEMA_PROPERTY_NAME);
                if (!rs.wasNull()) {
                    String propertyType = rs.getString(SQLG_SCHEMA_PROPERTY_TYPE);
                    String defaultLiteral = rs.getString(SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL);
                    String checkConstraint = rs.getString(SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT);
                    int lower = rs.getInt("propertyLowerMultiplicity");
                    int upper = rs.getInt("propertyUpperMultiplicity");
                    PropertyColumn property = new PropertyColumn(
                            vertexLabel,
                            propertyName,
                            PropertyDefinition.of(
                                    PropertyType.valueOf(propertyType),
                                    Multiplicity.of(
                                            lower,
                                            upper
                                    ),
                                    defaultLiteral,
                                    checkConstraint
                            )
                    );
                    edgeLabel.addToPropertyColumns(property);
                }

            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        stopWatch.stop();
        LOGGER.debug("load edges, time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        String edgeIdentifierSql = """ 
                SELECT
                	"schema"."name" AS "schemaName",
                	"edge"."name" AS "edgeLabelName",
                	"edge_identifier"."identifier_index",
                    "property"."name"
                FROM
                	"sqlg_schema"."V_schema" as "schema"	JOIN
                	"sqlg_schema"."E_schema_vertex" as "schema_edge" ON "schema"."ID" = "schema_edge"."sqlg_schema.schema__O" JOIN
                	"sqlg_schema"."V_vertex" as "vertex" ON "schema_edge"."sqlg_schema.vertex__I" = "vertex"."ID" LEFT JOIN
                	"sqlg_schema"."E_out_edges" as "out_edges" ON "vertex"."ID" = "out_edges"."sqlg_schema.vertex__O" JOIN
                	"sqlg_schema"."V_edge" as "edge" ON "out_edges"."sqlg_schema.edge__I" = "edge"."ID" JOIN
                	"sqlg_schema"."E_edge_identifier" as "edge_identifier" ON "edge"."ID" = "edge_identifier"."sqlg_schema.edge__O" JOIN
                	"sqlg_schema"."V_property" as "property" ON "edge_identifier"."sqlg_schema.property__I" = "property"."ID";
                """;
        if (!sqlgGraph.getSqlDialect().getColumnEscapeKey().equals("\"")) {
            edgeIdentifierSql = edgeIdentifierSql.replace("\"", sqlgGraph.getSqlDialect().getColumnEscapeKey());
        }
        conn = sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery(edgeIdentifierSql);
            while (rs.next()) {
                String schemaName = rs.getString("schemaName");
                Schema schema = this.schemas.get(schemaName);
                Preconditions.checkNotNull(schema, "Schema %s not found!", schemaName);
                String edgeLabelName = rs.getString("edgeLabelName");
                Optional<EdgeLabel> edgeLabelOptional = schema.getEdgeLabel(edgeLabelName);
                Preconditions.checkState(edgeLabelOptional.isPresent(), "Failed to find %s", edgeLabelName);
                EdgeLabel edgeLabel = edgeLabelOptional.get();

                int identifierIndex = rs.getInt(SQLG_SCHEMA_EDGE_IDENTIFIER_INDEX_EDGE);
                String propertyName = rs.getString(SQLG_SCHEMA_PROPERTY_NAME);
                edgeLabel.addIdentifier(
                        propertyName,
                        identifierIndex
                );
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        stopWatch.stop();
        LOGGER.debug("load edges identifiers, time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        List<RecordId> edgePartitionIds = new ArrayList<>();
        String edgeRootPartitionSql = """
                SELECT
                	"sqlg_schema"."V_partition"."ID" AS "id",
                	"sqlg_schema"."V_partition"."partitionExpression" AS "partitionExpression",
                	"sqlg_schema"."V_partition"."in" AS "in",
                	"sqlg_schema"."V_partition"."abstractLabelName" AS "abstractLabelName",
                	"sqlg_schema"."V_partition"."name" AS "name",
                	"sqlg_schema"."V_partition"."from" AS "from",
                	"sqlg_schema"."V_partition"."to" AS "to",
                	"sqlg_schema"."V_partition"."schemaName" AS "schemaName",
                	"sqlg_schema"."V_partition"."remainder" AS "remainder",
                	"sqlg_schema"."V_partition"."partitionType" AS "partitionType",
                	"sqlg_schema"."V_partition"."modulus" AS "modulus"
                FROM
                	"sqlg_schema"."V_edge" INNER JOIN
                	"sqlg_schema"."E_edge_partition" ON "sqlg_schema"."V_edge"."ID" = "sqlg_schema"."E_edge_partition"."sqlg_schema.edge__O" INNER JOIN
                	"sqlg_schema"."V_partition" ON "sqlg_schema"."E_edge_partition"."sqlg_schema.partition__I" = "sqlg_schema"."V_partition"."ID"
                """;

        if (!sqlgGraph.getSqlDialect().getColumnEscapeKey().equals("\"")) {
            edgeRootPartitionSql = edgeRootPartitionSql.replace("\"", sqlgGraph.getSqlDialect().getColumnEscapeKey());
        }
        conn = sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery(edgeRootPartitionSql);
            while (rs.next()) {
                String schemaName = rs.getString("schemaName");
                Schema schema = this.schemas.get(schemaName);
                Preconditions.checkNotNull(schema, "Schema %s not found!", schemaName);
                String abstractLabelName = rs.getString("abstractLabelName");
                long id = rs.getLong("id");
                String name = rs.getString("name");
                if (rs.wasNull()) {
                    name = null;
                }
                String from = rs.getString("from");
                if (rs.wasNull()) {
                    from = null;
                }
                String to = rs.getString("to");
                if (rs.wasNull()) {
                    to = null;
                }
                String in = rs.getString("in");
                if (rs.wasNull()) {
                    in = null;
                }
                Integer modulus = rs.getInt("modulus");
                if (rs.wasNull()) {
                    modulus = null;
                }
                Integer remainder = rs.getInt("remainder");
                if (rs.wasNull()) {
                    remainder = null;
                }
                String partitionType = rs.getString("partitionType");
                if (rs.wasNull()) {
                    partitionType = null;
                }
                String partitionExpression = rs.getString("partitionExpression");
                if (rs.wasNull()) {
                    partitionExpression = null;
                }
                Preconditions.checkNotNull(abstractLabelName, SQLG_SCHEMA_PARTITION_ABSTRACT_LABEL_NAME + " may never be null. BUG!");
                EdgeLabel edgeLabel = schema.getEdgeLabel(abstractLabelName).orElseThrow(() -> new IllegalStateException(String.format("edgeLabel %s not found in schema %s", abstractLabelName, schemaName)));
                edgeLabel.addPartition(from, to, in, modulus, remainder, partitionType, partitionExpression, name);
                edgePartitionIds.add(RecordId.from(schemaTable, id));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        stopWatch.stop();
        LOGGER.debug("load edge root partitions, time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        List<Path> edgeSubPartitions = traversalSource.V().hasId(P.within(edgePartitionIds))
                .repeat(
                        __.out(SQLG_SCHEMA_PARTITION_PARTITION_EDGE).simplePath()
                )
                .until(
                        __.not(__.out(SQLG_SCHEMA_PARTITION_PARTITION_EDGE).simplePath())
                )
                .path()
                .toList();
        for (Path subPartition : edgeSubPartitions) {
            Partition partition = null;
            //start the index at 1, we are not interested in the 'vertex' vertex here!
            for (int i = 0; i < subPartition.size(); i++) {
                Vertex subPartitionVertex = subPartition.get(i);
                String schemaName = subPartitionVertex.value(Topology.SQLG_SCHEMA_PARTITION_SCHEMA_NAME);
                String abstractLabelName = subPartitionVertex.value(Topology.SQLG_SCHEMA_PARTITION_ABSTRACT_LABEL_NAME);
                Schema schema = this.schemas.get(schemaName);
                Preconditions.checkNotNull(schema, "Schema %s not found!", schemaName);
                Preconditions.checkNotNull(abstractLabelName, SQLG_SCHEMA_PARTITION_ABSTRACT_LABEL_NAME + " may never be null. BUG!");
                EdgeLabel edgeLabel = schema.getEdgeLabel(abstractLabelName).orElseThrow();
                Preconditions.checkNotNull(edgeLabel, "EdgeLabel %s not found!", abstractLabelName);
                String partitionName = subPartitionVertex.value(Topology.SQLG_SCHEMA_PARTITION_NAME);
                if (i == 0) {
                    partition = edgeLabel.getPartition(partitionName).orElseThrow();
                } else {
                    Optional<Partition> partitionOpt = partition.getPartition(partitionName);
                    if (partitionOpt.isPresent()) {
                        partition = partitionOpt.get();
                    } else {
                        partition = partition.addPartition(subPartitionVertex);
                    }
                }
            }
        }

        stopWatch.stop();
        LOGGER.debug("load edge sub partitions, time: {}", stopWatch);
    }

    void loadVertexIndices() {
        @SuppressWarnings("SqlRedundantOrderingDirection")
        String indexSql = """
                SELECT
                	"sqlg_schema"."V_schema"."name" AS "schemaName",
                	"sqlg_schema"."V_vertex"."name" AS "vertexLabelName",
                	"sqlg_schema"."V_index"."name" AS "indexName",
                	"sqlg_schema"."V_index"."index_type" AS "indexType",
                    "sqlg_schema"."E_index_property"."sequence" AS "sequence",
                	"sqlg_schema"."V_property"."name" AS "propertyName"
                FROM
                	"sqlg_schema"."V_schema" INNER JOIN
                	"sqlg_schema"."E_schema_vertex" ON "sqlg_schema"."V_schema"."ID" = "sqlg_schema"."E_schema_vertex"."sqlg_schema.schema__O" INNER JOIN
                	"sqlg_schema"."V_vertex" ON "sqlg_schema"."E_schema_vertex"."sqlg_schema.vertex__I" = "sqlg_schema"."V_vertex"."ID" INNER JOIN
                	"sqlg_schema"."E_vertex_index" ON "sqlg_schema"."V_vertex"."ID" = "sqlg_schema"."E_vertex_index"."sqlg_schema.vertex__O" INNER JOIN
                	"sqlg_schema"."V_index" ON "sqlg_schema"."E_vertex_index"."sqlg_schema.index__I" = "sqlg_schema"."V_index"."ID" INNER JOIN
                	"sqlg_schema"."E_index_property" ON "sqlg_schema"."V_index"."ID" = "sqlg_schema"."E_index_property"."sqlg_schema.index__O" INNER JOIN
                	"sqlg_schema"."V_property" ON "sqlg_schema"."E_index_property"."sqlg_schema.property__I" = "sqlg_schema"."V_property"."ID"
                ORDER BY
                	 "sequence" ASC
                """;
        StopWatch stopWatch = StopWatch.createStarted();
        Connection conn = sqlgGraph.tx().getConnection();
        if (!sqlgGraph.getSqlDialect().getColumnEscapeKey().equals("\"")) {
            indexSql = indexSql.replace("\"", sqlgGraph.getSqlDialect().getColumnEscapeKey());
        }
        try (Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery(indexSql);
            while (rs.next()) {
                String schemaName = rs.getString("schemaName");
                Schema schema = this.schemas.get(schemaName);
                Preconditions.checkNotNull(schema, "Schema %s not found!", schemaName);
                String vertexLabelName = rs.getString("vertexLabelName");
                Preconditions.checkNotNull(vertexLabelName, SQLG_SCHEMA_VERTEX_LABEL_NAME + " may never be null. BUG!");
                VertexLabel vertexLabel = schema.getVertexLabel(vertexLabelName).orElseThrow();
                Preconditions.checkNotNull(vertexLabel, "VertexLabel %s not found!", vertexLabelName);

                String indexName = rs.getString("indexName");
                Preconditions.checkNotNull(indexName, "indexName can not be null!");
                Optional<Index> optionalIndex = vertexLabel.getIndex(indexName);
                Index idx;
                if (optionalIndex.isPresent()) {
                    idx = optionalIndex.get();
                } else {
                    String indexType = rs.getString("indexType");
                    Preconditions.checkNotNull(indexType, "indexType can not be null!");
                    idx = new Index(indexName, IndexType.fromString(indexType), vertexLabel);
                    vertexLabel.addIndex(idx);
                }
                String propertyName = rs.getString("propertyName");
                Preconditions.checkNotNull(propertyName, "Index propertyName can not be null!");
                vertexLabel.getProperty(propertyName).ifPresent(idx::addProperty);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        stopWatch.stop();
        LOGGER.debug("load index , time: {}", stopWatch);
    }

    void loadEdgeIndices() {
        @SuppressWarnings("SqlRedundantOrderingDirection")
        String edgeIndexSql = """
                SELECT
                	"sqlg_schema"."V_schema"."name" AS "schemaName",
                	"sqlg_schema"."V_vertex"."name" AS "vertexLabelName",
                	"sqlg_schema"."V_edge"."name" AS "edgeLabelName",
                	"sqlg_schema"."V_index"."name" AS "indexName",
                	"sqlg_schema"."V_index"."index_type" AS "index_type",
                	"sqlg_schema"."E_index_property"."sequence" AS "sequence",
                	"sqlg_schema"."V_property"."name" AS "propertyName"
                FROM
                	"sqlg_schema"."V_schema" INNER JOIN
                	"sqlg_schema"."E_schema_vertex" ON "sqlg_schema"."V_schema"."ID" = "sqlg_schema"."E_schema_vertex"."sqlg_schema.schema__O" INNER JOIN
                	"sqlg_schema"."V_vertex" ON "sqlg_schema"."E_schema_vertex"."sqlg_schema.vertex__I" = "sqlg_schema"."V_vertex"."ID" INNER JOIN
                	"sqlg_schema"."E_out_edges" ON "sqlg_schema"."V_vertex"."ID" = "sqlg_schema"."E_out_edges"."sqlg_schema.vertex__O" INNER JOIN
                	"sqlg_schema"."V_edge" ON "sqlg_schema"."E_out_edges"."sqlg_schema.edge__I" = "sqlg_schema"."V_edge"."ID" INNER JOIN
                	"sqlg_schema"."E_edge_index" ON "sqlg_schema"."V_edge"."ID" = "sqlg_schema"."E_edge_index"."sqlg_schema.edge__O" INNER JOIN
                	"sqlg_schema"."V_index" ON "sqlg_schema"."E_edge_index"."sqlg_schema.index__I" = "sqlg_schema"."V_index"."ID" INNER JOIN
                	"sqlg_schema"."E_index_property" ON "sqlg_schema"."V_index"."ID" = "sqlg_schema"."E_index_property"."sqlg_schema.index__O" INNER JOIN
                	"sqlg_schema"."V_property" ON "sqlg_schema"."E_index_property"."sqlg_schema.property__I" = "sqlg_schema"."V_property"."ID"
                ORDER BY
                	 "sequence" ASC
                """;

        StopWatch stopWatch = StopWatch.createStarted();
        Connection conn = sqlgGraph.tx().getConnection();
        if (!sqlgGraph.getSqlDialect().getColumnEscapeKey().equals("\"")) {
            edgeIndexSql = edgeIndexSql.replace("\"", sqlgGraph.getSqlDialect().getColumnEscapeKey());
        }
        try (Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery(edgeIndexSql);
            while (rs.next()) {
                String schemaName = rs.getString("schemaName");
                Schema schema = this.schemas.get(schemaName);
                Preconditions.checkNotNull(schema, "Schema %s not found!", schemaName);
                String vertexLabelName = rs.getString("vertexLabelName");
                Preconditions.checkNotNull(vertexLabelName, SQLG_SCHEMA_VERTEX_LABEL_NAME + " may never be null. BUG!");
                VertexLabel vertexLabel = schema.getVertexLabel(vertexLabelName).orElseThrow();
                Preconditions.checkNotNull(vertexLabel, "VertexLabel %s not found!", vertexLabelName);

                String edgeLabelName = rs.getString("edgeLabelName");

                String indexName = rs.getString("indexName");
                Preconditions.checkNotNull(indexName, "indexName can not be null!");
                String indexType = rs.getString("index_type");
                String propertyName = rs.getString("propertyName");

                schema.loadEdgeIndices(
                        vertexLabelName,
                        edgeLabelName,
                        indexName,
                        indexType,
                        propertyName
                );
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        stopWatch.stop();
        LOGGER.debug("load edge index , time: {}", stopWatch);

//        List<Path> indices = traversalSource
//                .V().hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA).as("schema")
//                .out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("vertex")
//                .out(SQLG_SCHEMA_OUT_EDGES_EDGE).as("outEdgeVertex")
//                .out(SQLG_SCHEMA_EDGE_INDEX_EDGE).as("index")
//                .outE(SQLG_SCHEMA_INDEX_PROPERTY_EDGE)
//                .order().by(SQLG_SCHEMA_INDEX_PROPERTY_EDGE_SEQUENCE)
//                .inV().as("property")
//                .path()
//                .toList();
//        for (Path vertexIndices : indices) {
//            Vertex schemaVertex = null;
//            Vertex vertexVertex = null;
//            Vertex vertexEdge = null;
//            Vertex vertexIndex = null;
//            Vertex propertyIndex = null;
//            List<Set<String>> labelsList = vertexIndices.labels();
//            for (Set<String> labels : labelsList) {
//                for (String label : labels) {
//                    switch (label) {
//                        case "schema":
//                            schemaVertex = vertexIndices.get("schema");
//                            break;
//                        case "vertex":
//                            vertexVertex = vertexIndices.get("vertex");
//                            break;
//                        case "outEdgeVertex":
//                            vertexEdge = vertexIndices.get("outEdgeVertex");
//                            break;
//                        case "index":
//                            vertexIndex = vertexIndices.get("index");
//                            break;
//                        case "property":
//                            propertyIndex = vertexIndices.get("property");
//                            break;
//                        case BaseStrategy.SQLG_PATH_FAKE_LABEL:
//                        case BaseStrategy.SQLG_PATH_ORDER_RANGE_LABEL:
//                        case Schema.MARKER:
//                            break;
//                        default:
//                            throw new IllegalStateException(String.format("BUG: Only \"vertex\",\"outEdgeVertex\",\"index\" and \"property\" is expected as a label. Found %s", label));
//                    }
//                }
//            }
//            Preconditions.checkState(schemaVertex != null, "BUG: Topology vertex not found.");
//            Preconditions.checkState(vertexVertex != null, "BUG: Topology vertex not found.");
//            String schemaName = schemaVertex.value(SQLG_SCHEMA_SCHEMA_NAME);
//            Optional<Schema> schemaOptional = this.getSchema(schemaName);
//            Preconditions.checkState(schemaOptional.isPresent());
//            Schema schema = schemaOptional.get();
//
//            schema.loadEdgeIndices(vertexVertex, vertexEdge, vertexIndex, propertyIndex);
//        }
    }

    void loadInEdgeLabels() {
        StopWatch stopWatch = StopWatch.createStarted();
        String vertexSql = """
                SELECT
                	a1."alias2" as "schemaName", a1."alias7" as "vertexLabelName", a1."alias13" as "outEdgeLabelName", a1."alias19" as "upperMultiplicity",
                	a1."alias20" as "unique", a1."alias21" as "lowerMultiplicity", a2."alias27" as "inVertexLabelName", a2."alias32" as "inSchemaVertexLabelName"
                FROM (SELECT
                	"sqlg_schema"."E_in_edges"."sqlg_schema.vertex__O" AS "sqlg_schema.E_in_edges.sqlg_schema.vertex__O",
                	"sqlg_schema"."V_schema"."name" AS "alias2",
                	"sqlg_schema"."V_vertex"."name" AS "alias7",
                	"sqlg_schema"."V_edge"."name" AS "alias13",
                	"sqlg_schema"."E_in_edges"."ordered" AS "alias18",
                	"sqlg_schema"."E_in_edges"."upperMultiplicity" AS "alias19",
                	"sqlg_schema"."E_in_edges"."unique" AS "alias20",
                	"sqlg_schema"."E_in_edges"."lowerMultiplicity" AS "alias21"
                FROM
                	"sqlg_schema"."V_schema" INNER JOIN
                	"sqlg_schema"."E_schema_vertex" ON "sqlg_schema"."V_schema"."ID" = "sqlg_schema"."E_schema_vertex"."sqlg_schema.schema__O" INNER JOIN
                	"sqlg_schema"."V_vertex" ON "sqlg_schema"."E_schema_vertex"."sqlg_schema.vertex__I" = "sqlg_schema"."V_vertex"."ID" INNER JOIN
                	"sqlg_schema"."E_out_edges" ON "sqlg_schema"."V_vertex"."ID" = "sqlg_schema"."E_out_edges"."sqlg_schema.vertex__O" INNER JOIN
                	"sqlg_schema"."V_edge" ON "sqlg_schema"."E_out_edges"."sqlg_schema.edge__I" = "sqlg_schema"."V_edge"."ID" INNER JOIN
                	"sqlg_schema"."E_in_edges" ON "sqlg_schema"."V_edge"."ID" = "sqlg_schema"."E_in_edges"."sqlg_schema.edge__I"
                ) a1 INNER JOIN (SELECT
                	"sqlg_schema"."V_vertex"."ID" AS "alias24",
                	"sqlg_schema"."V_vertex"."name" AS "alias27",
                	"sqlg_schema"."V_schema"."name" AS "alias32"
                FROM
                	"sqlg_schema"."V_vertex" INNER JOIN
                	"sqlg_schema"."E_schema_vertex" ON "sqlg_schema"."V_vertex"."ID" = "sqlg_schema"."E_schema_vertex"."sqlg_schema.vertex__I" INNER JOIN
                	"sqlg_schema"."V_schema" ON "sqlg_schema"."E_schema_vertex"."sqlg_schema.schema__O" = "sqlg_schema"."V_schema"."ID"
                ) a2 ON a1."sqlg_schema.E_in_edges.sqlg_schema.vertex__O" = a2."alias24"
                """;
        if (!sqlgGraph.getSqlDialect().getColumnEscapeKey().equals("\"")) {
            vertexSql = vertexSql.replace("\"", sqlgGraph.getSqlDialect().getColumnEscapeKey());
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery(vertexSql);
            while (rs.next()) {
                String schemaName = rs.getString("schemaName");
                Schema schema = this.schemas.get(schemaName);
                Preconditions.checkNotNull(schema, "Schema %s not found!", schemaName);
                String vertexLabelName = rs.getString("vertexLabelName");
                Preconditions.checkState(!rs.wasNull(), "vertexLabelName may never be null. BUG!");

                String outEdgeLabelName = rs.getString("outEdgeLabelName");
//                boolean ordered = rs.getBoolean("ordered");
                int upperMultiplicity = rs.getInt("upperMultiplicity");
                boolean unique = rs.getBoolean("unique");
                int lowerMultiplicity = rs.getInt("lowerMultiplicity");

                String inVertexLabelName = rs.getString("inVertexLabelName");
                String inSchemaVertexLabelName = rs.getString("inSchemaVertexLabelName");

                schema.loadInEdgeLabels(
                        vertexLabelName,
                        outEdgeLabelName,
                        inVertexLabelName,
                        inSchemaVertexLabelName,
                        lowerMultiplicity,
                        upperMultiplicity,
                        unique
                );
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        stopWatch.stop();
        LOGGER.debug("load loadInEdgeLabels, time: {}", stopWatch);
    }

    public void validateTopology() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            List<String> schemaNames = this.sqlgGraph.getSqlDialect().getSchemaNames(metadata);
            for (Schema schema : getSchemas()) {
                if (schemaNames.contains(schema.getName())) {
                    this.validationErrors.addAll(schema.validateTopology(metadata));
                } else {
                    this.validationErrors.add(new TopologyValidationError(schema));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public JsonNode toJson() {
        ObjectNode topologyNode = OBJECT_MAPPER.createObjectNode();
        ArrayNode schemaArrayNode = OBJECT_MAPPER.createArrayNode();
        List<Schema> schemas = new ArrayList<>(this.schemas.values());
        schemas.sort(Comparator.comparing(Schema::getName));
        for (Schema schema : schemas) {
            schemaArrayNode.add(schema.toJson());
        }
        topologyNode.set("schemas", schemaArrayNode);
        return topologyNode;
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    /**
     * Produce the json that goes into the sqlg_schema.V_log table. Other graphs will read it to sync their schema.
     *
     * @return The json.
     */
    private Optional<JsonNode> toNotifyJson() {
        ArrayNode committedSchemaArrayNode = null;
        ObjectNode topologyNode = null;
        for (Schema schema : this.schemas.values()) {
            Optional<JsonNode> jsonNodeOptional = schema.toNotifyJson();
            if (jsonNodeOptional.isPresent() && committedSchemaArrayNode == null) {
                committedSchemaArrayNode = OBJECT_MAPPER.createArrayNode();
            }
            if (jsonNodeOptional.isPresent()) {
                committedSchemaArrayNode.add(jsonNodeOptional.get());
            }
        }
        if (committedSchemaArrayNode != null) {
            topologyNode = OBJECT_MAPPER.createObjectNode();
            topologyNode.set("schemas", committedSchemaArrayNode);
        }
        ArrayNode unCommittedSchemaArrayNode = null;
        if (isSchemaChanged()) {
            for (Schema schema : this.uncommittedSchemas.values()) {
                if (unCommittedSchemaArrayNode == null) {
                    unCommittedSchemaArrayNode = OBJECT_MAPPER.createArrayNode();
                }
                Optional<JsonNode> jsonNodeOptional = schema.toNotifyJson();
                if (jsonNodeOptional.isPresent()) {
                    unCommittedSchemaArrayNode.add(jsonNodeOptional.get());
                } else {
                    ObjectNode schemaNode = OBJECT_MAPPER.createObjectNode();
                    schemaNode.put("name", schema.getName());
                    unCommittedSchemaArrayNode.add(schemaNode);
                }
            }
            ArrayNode removed = OBJECT_MAPPER.createArrayNode();
            for (String schema : this.uncommittedRemovedSchemas) {
                removed.add(schema);
            }
            if (!removed.isEmpty()) {
                if (topologyNode == null) {
                    topologyNode = OBJECT_MAPPER.createObjectNode();
                }
                topologyNode.set("uncommittedRemovedSchemas", removed);
            }
        }
        if (unCommittedSchemaArrayNode != null) {
            if (topologyNode == null) {
                topologyNode = OBJECT_MAPPER.createObjectNode();
            }
            topologyNode.set("uncommittedSchemas", unCommittedSchemaArrayNode);
        }
        if (topologyNode != null) {
            return Optional.of(topologyNode);
        } else {
            return Optional.empty();
        }
    }

    public void fromNotifyJson(int pid, LocalDateTime notifyTimestamp) {
        try {
            if (!this.ownPids.contains(pid)) {
                List<Vertex> logs = this.sqlgGraph.topology().V()
                        .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_LOG)
                        .has(SQLG_SCHEMA_LOG_PID, pid)
                        .has(SQLG_SCHEMA_LOG_TIMESTAMP, notifyTimestamp)
                        .toList();
                Preconditions.checkState(logs.size() == 1, "There must be one and only be one log, found %s", logs.size());
                int backEndPid = logs.get(0).value("pid");
                Preconditions.checkState(backEndPid == pid, "notify pids do not match.");
                //Clear the cache,
                this.sqlgGraph.getSchemaTableTreeCache().clear();
                ObjectNode log = logs.get(0).value("log");
                fromNotifyJson(log);
            }
        } finally {
            this.sqlgGraph.tx().rollback();
        }
    }

    private void fromNotifyJson(ObjectNode log) {
        //First do all the out edges. The in edge logic assumes the out edges are present.
        for (String s : List.of("uncommittedSchemas", "schemas")) {
            ArrayNode schemas = (ArrayNode) log.get(s);
            if (schemas != null) {
                //first load all the schema as they might be required later
                for (JsonNode jsonSchema : schemas) {
                    String schemaName = jsonSchema.get("name").asText();
                    Optional<Schema> schemaOptional = getSchema(schemaName);
                    Schema schema;
                    if (schemaOptional.isEmpty()) {
                        //add to map
                        schema = Schema.instantiateSchema(this, schemaName);
                        this.schemas.put(schemaName, schema);
                        fire(schema, null, TopologyChangeAction.CREATE, false);
                    }
                }
                for (JsonNode jsonSchema : schemas) {
                    String schemaName = jsonSchema.get("name").asText();
                    Optional<Schema> schemaOptional = getSchema(schemaName);
                    Preconditions.checkState(schemaOptional.isPresent(), "Schema must be present here");
                    Schema schema = schemaOptional.get();
                    schema.fromNotifyJsonOutEdges(jsonSchema);
                }
            }
        }
        for (String s : Arrays.asList("uncommittedSchemas", "schemas")) {
            ArrayNode schemas = (ArrayNode) log.get(s);
            if (schemas != null) {
                for (JsonNode jsonSchema : schemas) {
                    String schemaName = jsonSchema.get("name").asText();
                    Optional<Schema> schemaOptional = getSchema(schemaName);
                    Preconditions.checkState(schemaOptional.isPresent(), "Schema must be present here");
                    Schema schema = schemaOptional.get();
                    schema.fromNotifyJsonInEdges(jsonSchema);
                }
            }
        }
        ArrayNode rem = (ArrayNode) log.get("uncommittedRemovedSchemas");
        if (rem != null) {
            for (JsonNode jsonSchema : rem) {
                String name = jsonSchema.asText();
                Schema s = removeSchemaFromCaches(name);
                if (s != null) {
                    fire(s, s, TopologyChangeAction.DELETE, false);
                }
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Topology other)) {
            return false;
        }
        return toJson().equals(other.toJson());
    }

    /////////////////////////////////getters and cache/////////////////////////////
    public Set<Schema> getSchemas() {
        Set<Schema> result = new HashSet<>(this.schemas.values());
        if (isSchemaChanged()) {
            result.addAll(this.uncommittedSchemas.values());
            if (!this.uncommittedRemovedSchemas.isEmpty()) {
                result.removeIf(sch -> this.uncommittedRemovedSchemas.contains(sch.getName()));
            }
        }
        return Collections.unmodifiableSet(result);
    }

    public Schema getPublicSchema() {
        Optional<Schema> schema = getSchema(this.sqlgGraph.getSqlDialect().getPublicSchema());
        Preconditions.checkState(schema.isPresent(), "BUG: The public schema must always be present");
        return schema.get();
    }

    public Optional<Schema> getSchema(String schema) {
        if (schema == null) {
            return Optional.empty();
        }
        if (isSchemaChanged() && this.uncommittedRemovedSchemas.contains(schema)) {
            return Optional.empty();
        }
        Schema result = this.schemas.get(schema);
        if (result == null) {
            if (isSchemaChanged()) {
                result = this.uncommittedSchemas.get(schema);
            }
            if (result == null) {
                result = this.metaSchemas.get(schema);
            }
        }
        return Optional.ofNullable(result);
    }

    public Optional<VertexLabel> getVertexLabel(String schemaName, String label) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "vertex label may not start with %s", VERTEX_PREFIX);
        Optional<Schema> schemaOptional = this.getSchema(schemaName);
        if (schemaOptional.isPresent()) {
            return schemaOptional.get().getVertexLabel(label);
        } else {
            return Optional.empty();
        }
    }

    public Optional<EdgeLabel> getEdgeLabel(String schemaName, String edgeLabelName) {
        Preconditions.checkArgument(!edgeLabelName.startsWith(EDGE_PREFIX), "edge label name may not start with %s", EDGE_PREFIX);
        Optional<Schema> schemaOptional = getSchema(schemaName);
        if (schemaOptional.isPresent()) {
            Schema schema = schemaOptional.get();
            return schema.getEdgeLabel(edgeLabelName);
        } else {
            return Optional.empty();
        }
    }

    private Map<String, AbstractLabel> getUncommittedAllTables() {
        Preconditions.checkState(isSchemaChanged(), "Topology.getUncommittedAllTables must have schemaChanged = true");
        Map<String, AbstractLabel> result = new HashMap<>();
        for (Map.Entry<String, Schema> stringSchemaEntry : this.schemas.entrySet()) {
            Schema schema = stringSchemaEntry.getValue();
            result.putAll(schema.getUncommittedLabels());
        }
        for (Map.Entry<String, Schema> stringSchemaEntry : this.uncommittedSchemas.entrySet()) {
            Schema schema = stringSchemaEntry.getValue();
            result.putAll(schema.getUncommittedLabels());
        }
        return result;
    }

    private Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getUncommittedSchemaTableForeignKeys() {
        Preconditions.checkState(isSchemaChanged(), "Topology.getUncommittedSchemaTableForeignKeys must have schemaChanged = true");
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, Schema> stringSchemaEntry : this.schemas.entrySet()) {
            Schema schema = stringSchemaEntry.getValue();
            result.putAll(schema.getUncommittedSchemaTableForeignKeys());
        }
        for (Map.Entry<String, Schema> stringSchemaEntry : this.uncommittedSchemas.entrySet()) {
            Schema schema = stringSchemaEntry.getValue();
            result.putAll(schema.getUncommittedSchemaTableForeignKeys());
        }
        return result;
    }

    private Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getUncommittedRemovedSchemaTableForeignKeys() {
        Preconditions.checkState(isSchemaChanged(), "Topology.getUncommittedRemovedSchemaTableForeignKeys must have schemaChanged = true");
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = new HashMap<>();
        for (Map.Entry<String, Schema> stringSchemaEntry : this.schemas.entrySet()) {
            Schema schema = stringSchemaEntry.getValue();
            result.putAll(schema.getUncommittedRemovedSchemaTableForeignKeys());
        }
        for (Map.Entry<String, Schema> stringSchemaEntry : this.uncommittedSchemas.entrySet()) {
            Schema schema = stringSchemaEntry.getValue();
            result.putAll(schema.getUncommittedRemovedSchemaTableForeignKeys());
        }
        return result;
    }

    private Map<String, Set<ForeignKey>> getUncommittedEdgeForeignKeys() {
        Preconditions.checkState(isSchemaChanged(), "Topology.getUncommittedEdgeForeignKeys must have schemaChanged = true");
        Map<String, Set<ForeignKey>> result = new HashMap<>();
        for (Map.Entry<String, Schema> stringSchemaEntry : this.schemas.entrySet()) {
            Schema schema = stringSchemaEntry.getValue();
            result.putAll(schema.getUncommittedEdgeForeignKeys());
        }
        for (Map.Entry<String, Schema> stringSchemaEntry : this.uncommittedSchemas.entrySet()) {
            Schema schema = stringSchemaEntry.getValue();
            result.putAll(schema.getUncommittedEdgeForeignKeys());
        }
        //TODO include removedSchemas
        return result;
    }

    private Map<String, Set<ForeignKey>> getUncommittedRemovedEdgeForeignKeys() {
        Preconditions.checkState(isSchemaChanged(), "Topology.getUncommittedRemovedEdgeForeignKeys must have schemaChanged = true");
        Map<String, Set<ForeignKey>> result = new HashMap<>();
        for (Map.Entry<String, Schema> stringSchemaEntry : this.schemas.entrySet()) {
            Schema schema = stringSchemaEntry.getValue();
            result.putAll(schema.getUncommittedRemovedEdgeForeignKeys());
        }
        for (Map.Entry<String, Schema> stringSchemaEntry : this.uncommittedSchemas.entrySet()) {
            Schema schema = stringSchemaEntry.getValue();
            result.putAll(schema.getUncommittedRemovedEdgeForeignKeys());
        }
        return result;
    }

    /**
     * get all tables by schema, with their properties
     * does not return schema tables
     *
     * @return the map of all tables.
     */
    public Map<String, Map<String, PropertyDefinition>> getAllTables() {
        return getAllTables(false);
    }

    /**
     * get all tables by schema, with their properties
     *
     * @param sqlgSchema do we want the sqlg_schema tables?
     * @return a map of all tables and their properties.
     */
    public Map<String, Map<String, PropertyDefinition>> getAllTables(boolean sqlgSchema) {
        if (sqlgSchema) {
            return Collections.unmodifiableMap(this.sqlgSchemaTableCache);
        } else {
            if (isSchemaChanged()) {
                //Need to make a copy so as not to corrupt the allTableCache with uncommitted schema elements
                Map<String, Map<String, PropertyDefinition>> result;
                result = new HashMap<>();
                for (Map.Entry<String, Map<String, PropertyDefinition>> allTableCacheMapEntry : this.allTableCache.entrySet()) {
                    String key = allTableCacheMapEntry.getKey();
                    SchemaTable schemaTable = SchemaTable.from(this.sqlgGraph, key);
                    if (!this.uncommittedRemovedSchemas.contains(schemaTable.getSchema())) {
                        result.put(key, new HashMap<>(allTableCacheMapEntry.getValue()));
                    }
                }
                Map<String, AbstractLabel> uncommittedLabels = this.getUncommittedAllTables();
                for (String table : uncommittedLabels.keySet()) {
                    Map<String, PropertyDefinition> propertyTypeMap = result.get(table);
                    if (propertyTypeMap != null) {
                        propertyTypeMap.putAll(uncommittedLabels.get(table).getPropertyDefinitionMap());
                    } else {
                        result.put(table, uncommittedLabels.get(table).getPropertyDefinitionMap());
                    }
                }
                for (Schema s : this.schemas.values()) {
                    for (String removed : s.uncommittedRemovedVertexLabels) {
                        result.remove(removed);
                    }
                    for (String removed : s.uncommittedRemovedEdgeLabels) {
                        result.remove(removed);
                    }
                }
                return Collections.unmodifiableMap(result);

            } else {
                return Collections.unmodifiableMap(this.allTableCache);
            }
        }
    }

    @SuppressWarnings("unused")
    public Map<String, PropertyColumn> getPropertiesFor(SchemaTable schemaTable) {
        Optional<Schema> schemaOptional = getSchema(schemaTable.getSchema());
        return schemaOptional.map(schema -> Collections.unmodifiableMap(schema.getPropertiesFor(schemaTable))).orElse(Collections.emptyMap());
    }

    public Map<String, PropertyDefinition> getTableFor(SchemaTable schemaTable) {
        Map<String, PropertyDefinition> result = getAllTables(schemaTable.getSchema().equals(Topology.SQLG_SCHEMA))
                .get(schemaTable.toString());
        if (result != null) {
            return Collections.unmodifiableMap(result);
        }
        return Collections.emptyMap();
    }

    public Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getTableLabels() {
        if (isSchemaChanged()) {
            Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> uncommittedSchemaTableForeignKeys = getUncommittedSchemaTableForeignKeys();
            Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> uncommittedRemovedSchemaTableForeignKeys = getUncommittedRemovedSchemaTableForeignKeys();
            for (Map.Entry<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> schemaTablePairEntry : this.schemaTableForeignKeyCache.entrySet()) {
                SchemaTable schemaTable = schemaTablePairEntry.getKey();
                Pair<Set<SchemaTable>, Set<SchemaTable>> foreignKeys = schemaTablePairEntry.getValue();
                Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedForeignKeys = uncommittedSchemaTableForeignKeys.get(schemaTable);
                Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedRemovedForeignKeys = uncommittedRemovedSchemaTableForeignKeys.get(schemaTable);
                if (uncommittedForeignKeys != null) {
                    Set<SchemaTable> leftForeignKeys = new HashSet<>(foreignKeys.getLeft());
                    Set<SchemaTable> rightForeignKeys = new HashSet<>(foreignKeys.getRight());
                    if (uncommittedRemovedForeignKeys != null) {
                        leftForeignKeys.removeAll(uncommittedRemovedForeignKeys.getLeft());
                        rightForeignKeys.removeAll(uncommittedRemovedForeignKeys.getRight());
                    }
                    uncommittedForeignKeys.getLeft().addAll(leftForeignKeys);
                    uncommittedForeignKeys.getRight().addAll(rightForeignKeys);
                } else {
                    Set<SchemaTable> leftForeignKeys = new HashSet<>(foreignKeys.getLeft());
                    Set<SchemaTable> rightForeignKeys = new HashSet<>(foreignKeys.getRight());
                    if (uncommittedRemovedForeignKeys != null) {
                        leftForeignKeys.removeAll(uncommittedRemovedForeignKeys.getLeft());
                        rightForeignKeys.removeAll(uncommittedRemovedForeignKeys.getRight());
                    }
                    uncommittedSchemaTableForeignKeys.put(schemaTable, Pair.of(leftForeignKeys, rightForeignKeys));
                }
            }
            return Collections.unmodifiableMap(uncommittedSchemaTableForeignKeys);
        } else {
            return Collections.unmodifiableMap(this.schemaTableForeignKeyCache);
        }
    }

    private Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> loadTableLabels() {
        Preconditions.checkState(isSchemaChanged());
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> map = new HashMap<>();
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = schemaEntry.getValue().getTableLabels();
            map.putAll(result);
        }
        return map;
    }

    /**
     * Returns all the in and out SchemaTables that schemaTable has edges to.
     *
     * @param schemaTable The schemaTable for whom we want the in and out SchemaTables
     * @return a Pair of in and out SchemaTables.
     */
    public Pair<Set<SchemaTable>, Set<SchemaTable>> getTableLabels(SchemaTable schemaTable) {
        return getTableLabels().get(schemaTable);
    }

    public Map<String, Set<ForeignKey>> getEdgeForeignKeys() {
        if (isSchemaChanged()) {
            Map<String, Set<ForeignKey>> copy = new HashMap<>(this.edgeForeignKeyCache);
            Map<String, Set<ForeignKey>> uncommittedEdgeForeignKeys = getUncommittedEdgeForeignKeys();
            for (Map.Entry<String, Set<ForeignKey>> uncommittedEntry : uncommittedEdgeForeignKeys.entrySet()) {
                Set<ForeignKey> committedForeignKeys = copy.get(uncommittedEntry.getKey());
                if (committedForeignKeys != null) {
                    Set<ForeignKey> originalPlusUncommittedForeignKeys = new HashSet<>(committedForeignKeys);
                    originalPlusUncommittedForeignKeys.addAll(uncommittedEntry.getValue());
                    copy.put(uncommittedEntry.getKey(), originalPlusUncommittedForeignKeys);
                } else {
                    copy.put(uncommittedEntry.getKey(), uncommittedEntry.getValue());
                }
            }
            Map<String, Set<ForeignKey>> uncommittedRemovedEdgeForeignKeys = getUncommittedRemovedEdgeForeignKeys();
            for (Map.Entry<String, Set<ForeignKey>> uncommittedRemovedEntry : uncommittedRemovedEdgeForeignKeys.entrySet()) {
                Set<ForeignKey> removedForeignKeys = uncommittedRemovedEntry.getValue();
                Set<ForeignKey> committedForeignKeys = copy.get(uncommittedRemovedEntry.getKey());
                if (committedForeignKeys != null && !committedForeignKeys.isEmpty()) {
                    committedForeignKeys.removeAll(removedForeignKeys);
                }
            }
            return Collections.unmodifiableMap(copy);
        } else {
            return Collections.unmodifiableMap(this.edgeForeignKeyCache);
        }
    }

    private Map<String, Set<ForeignKey>> loadAllEdgeForeignKeys() {
        Preconditions.checkState(isSchemaChanged());
        Map<String, Set<ForeignKey>> result = new HashMap<>();
        for (Schema schema : this.schemas.values()) {
            result.putAll(schema.getAllEdgeForeignKeys());
        }
        return result;
    }

    void addToEdgeForeignKeyCache(String name, ForeignKey foreignKey) {
        Set<ForeignKey> foreignKeys = this.edgeForeignKeyCache.get(name);
        //noinspection Java8MapApi
        if (foreignKeys == null) {
            foreignKeys = new HashSet<>();
            this.edgeForeignKeyCache.put(name, foreignKeys);
        }
        foreignKeys.add(foreignKey);

    }

    void removeFromEdgeForeignKeyCache(String name, ForeignKey foreignKey) {
        Set<ForeignKey> foreignKeys = this.edgeForeignKeyCache.get(name);
        if (foreignKeys != null) {
            foreignKeys.remove(foreignKey);
            if (foreignKeys.isEmpty()) {
                this.edgeForeignKeyCache.remove(name);
            }
        }
    }

    void addToAllTables(String tableName, Map<String, PropertyDefinition> propertyTypeMap) {
        this.allTableCache.put(tableName, propertyTypeMap);
        SchemaTable schemaTable = SchemaTable.from(this.sqlgGraph, tableName);
        if (schemaTable.getTable().startsWith(VERTEX_PREFIX) && !this.schemaTableForeignKeyCache.containsKey(schemaTable)) {
            //This happens for VertexLabel that have no edges,
            //else the addOutForeignKeysToVertexLabel or addInForeignKeysToVertexLabel would have already added it to the cache.
            this.schemaTableForeignKeyCache.put(schemaTable, Pair.of(new HashSet<>(), new HashSet<>()));
        }
    }

    /**
     * add out foreign key between a vertex label and a edge label
     *
     * @param vertexLabel The VertexLabel to add to the edge
     * @param edgeLabel   The EdgeLabel to add the foreign key to
     */
    void addOutForeignKeysToVertexLabel(VertexLabel vertexLabel, EdgeLabel edgeLabel) {
        SchemaTable schemaTable = SchemaTable.of(vertexLabel.getSchema().getName(), VERTEX_PREFIX + vertexLabel.getLabel());
        Pair<Set<SchemaTable>, Set<SchemaTable>> foreignKeys = this.schemaTableForeignKeyCache.computeIfAbsent(
                schemaTable, k -> Pair.of(new HashSet<>(), new HashSet<>())
        );
        foreignKeys.getRight().add(SchemaTable.of(vertexLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()));
    }

    /**
     * add in foreign key between a vertex label and a edge label
     *
     * @param vertexLabel The VertexLabel to add to the edge
     * @param edgeLabel   The EdgeLabel to add the foreign key to
     */
    void addInForeignKeysToVertexLabel(VertexLabel vertexLabel, EdgeLabel edgeLabel) {
        SchemaTable schemaTable = SchemaTable.of(vertexLabel.getSchema().getName(), VERTEX_PREFIX + vertexLabel.getLabel());
        Pair<Set<SchemaTable>, Set<SchemaTable>> foreignKeys = this.schemaTableForeignKeyCache.computeIfAbsent(
                schemaTable, k -> Pair.of(new HashSet<>(), new HashSet<>())
        );
        foreignKeys.getLeft().add(SchemaTable.of(edgeLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()));
    }

    /**
     * remove out foreign key for a given vertex label and edge label
     *
     * @param vertexLabel the vertexLabel to remove the out edgeLabel from.
     * @param edgeLabel   the out edgeLabel to remove from the vertexLabel.
     */
    void removeOutForeignKeysFromVertexLabel(VertexLabel vertexLabel, EdgeLabel edgeLabel) {
        SchemaTable schemaTable = SchemaTable.of(vertexLabel.getSchema().getName(), VERTEX_PREFIX + vertexLabel.getLabel());
        Pair<Set<SchemaTable>, Set<SchemaTable>> foreignKeys = this.schemaTableForeignKeyCache.get(schemaTable);
        if (foreignKeys != null) {
            foreignKeys.getRight().remove(SchemaTable.of(vertexLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()));
        }
    }

    /**
     * remove in foreign key for a given vertex label and edge label
     *
     * @param vertexLabel the vertexLabel to remove the in edgeLabel from.
     * @param edgeLabel   the edgeLabel to remove from the vertexLabel.
     */
    void removeInForeignKeysFromVertexLabel(VertexLabel vertexLabel, EdgeLabel edgeLabel) {
        SchemaTable schemaTable = SchemaTable.of(vertexLabel.getSchema().getName(), VERTEX_PREFIX + vertexLabel.getLabel());
        Pair<Set<SchemaTable>, Set<SchemaTable>> foreignKeys = this.schemaTableForeignKeyCache.get(schemaTable);
        if (foreignKeys != null && edgeLabel.isValid()) {
            foreignKeys.getLeft().remove(SchemaTable.of(edgeLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()));
        }
    }

    void removeEdgeLabel(String edgeLabelName) {
        this.allTableCache.remove(edgeLabelName);
    }

    /**
     * remove a given vertex label
     *
     * @param vertexLabel the vertexLabel to remove.
     */
    void removeVertexLabel(VertexLabel vertexLabel) {
        SchemaTable schemaTable = SchemaTable.of(vertexLabel.getSchema().getName(), VERTEX_PREFIX + vertexLabel.getLabel());
        this.schemaTableForeignKeyCache.remove(schemaTable);
        this.allTableCache.remove(schemaTable.toString());
        //out
        ForeignKey foreignKey;
        if (vertexLabel.hasIDPrimaryKey()) {
            foreignKey = ForeignKey.of(vertexLabel.getFullName() + OUT_VERTEX_COLUMN_END);
        } else {
            foreignKey = new ForeignKey();
            for (String identifier : vertexLabel.getIdentifiers()) {
                foreignKey.add(vertexLabel.getFullName(), identifier, OUT_VERTEX_COLUMN_END);
            }
        }
        for (EdgeLabel lbl : vertexLabel.getOutEdgeLabels().values()) {
            removeFromEdgeForeignKeyCache(
                    lbl.getSchema().getName() + "." + EDGE_PREFIX + lbl.getLabel(),
                    foreignKey
            );
        }
        //in
        if (vertexLabel.hasIDPrimaryKey()) {
            foreignKey = ForeignKey.of(vertexLabel.getFullName() + IN_VERTEX_COLUMN_END);
        } else {
            foreignKey = new ForeignKey();
            for (String identifier : vertexLabel.getIdentifiers()) {
                foreignKey.add(vertexLabel.getFullName(), identifier, IN_VERTEX_COLUMN_END);
            }
        }
        for (EdgeLabel lbl : vertexLabel.getInEdgeLabels().values()) {
            if (lbl.isValid()) {
                removeFromEdgeForeignKeyCache(
                        lbl.getSchema().getName() + "." + EDGE_PREFIX + lbl.getLabel(),
                        foreignKey
                );
            }
        }
    }

    public void registerListener(TopologyListener topologyListener) {
        this.topologyListeners.add(topologyListener);
    }

    void fire(TopologyInf topologyInf, TopologyInf oldValue, TopologyChangeAction action, boolean beforeCommit) {
        for (TopologyListener topologyListener : this.topologyListeners) {
            topologyListener.change(topologyInf, oldValue, action, beforeCommit);
        }
    }

    /**
     * remove a given schema
     *
     * @param schema       the schema
     * @param preserveData should we preserve the SQL data?
     */
    void removeSchema(Schema schema, boolean preserveData) {
        startSchemaChange(
                String.format("Topology removeSchema with '%s'", schema.getName())
        );
        if (!this.uncommittedRemovedSchemas.contains(schema.getName())) {
            // remove edge roles in other schemas pointing to vertex labels in removed schema
            // TODO undo this in case of rollback?
            for (VertexLabel vlbl : schema.getVertexLabels().values()) {
                for (EdgeRole er : vlbl.getInEdgeRoles().values()) {
                    if (er.getEdgeLabel().getSchema() != schema) {
                        er.removeViaVertexLabelRemove(preserveData);
                    }
                }
                // remove out edge roles in other schemas edges
                for (EdgeRole er : vlbl.getOutEdgeRoles().values()) {
                    if (er.getEdgeLabel().getSchema() == schema) {
                        for (EdgeRole erIn : er.getEdgeLabel().getInEdgeRoles()) {
                            if (erIn.getVertexLabel().getSchema() != schema) {
                                erIn.removeViaVertexLabelRemove(preserveData);
                            }
                        }

                    }
                }
            }

            this.uncommittedRemovedSchemas.add(schema.getName());
            TopologyManager.removeSchema(sqlgGraph, schema.getName());
            if (!preserveData) {
                schema.delete();
            }
            fire(schema, schema, TopologyChangeAction.DELETE, true);
        }
    }

    public static class TopologyValidationError {
        private final TopologyInf error;

        TopologyValidationError(TopologyInf error) {
            this.error = error;
        }

        @Override
        public String toString() {
            return String.format("%s does not exist", error.getName());
        }
    }

    public void markAsDistributed() {
        this.distributed = sqlgGraph.configuration().getBoolean(SqlgGraph.DISTRIBUTED, false);
    }
}
