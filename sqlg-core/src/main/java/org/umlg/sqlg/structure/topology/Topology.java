package org.umlg.sqlg.structure.topology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.sql.dialect.SqlSchemaChangeDialect;
import org.umlg.sqlg.structure.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class Topology {

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

    private SqlgGraph sqlgGraph;
    private boolean distributed;

    //Used to ensure that only one thread can modify the topology.
    private ReentrantLock topologySqlWriteLock;
    //Used to protect the topology maps.
    //The maps are only updated during afterCommit.
    //afterCommit locks access to the map
    //allTableCache, schemaTableForeignKeyCache, edgeForeignKeyCache, metaSchemas and schemas are protected by the topologyMapLock.
    private ReentrantReadWriteLock topologyMapLock;

    private Map<String, Map<String, PropertyType>> allTableCache = new ConcurrentHashMap<>();
    private Map<String, Map<String, PropertyType>> sqlgSchemaTableCache = new HashMap<>();
    //This cache is needed as to much time is taken building it on the fly.
    //The cache is invalidated on every topology change
    private Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> schemaTableForeignKeyCache = new HashMap<>();
    private Map<String, Set<ForeignKey>> edgeForeignKeyCache;
    //Map the topology. This is for regular schemas. i.e. 'public.Person', 'special.Car'
    private Map<String, Schema> schemas = new HashMap<>();
    private Map<String, Schema> globalUniqueIndexSchema = new HashMap<>();

    private Map<String, Schema> uncommittedSchemas = new HashMap<>();
    private Set<String> uncommittedRemovedSchemas = new HashSet<>();
    private Map<String, Schema> metaSchemas = new HashMap<>();
    //A cache of just the sqlg_schema's AbstractLabels
    private Set<TopologyInf> sqlgSchemaAbstractLabels = new HashSet<>();

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final String SQLG_NOTIFICATION_CHANNEL = "SQLG_NOTIFY";

    //ownPids are the pids to ignore as it is what the graph sent a notification for.
    private Set<ImmutablePair<Integer, LocalDateTime>> ownPids = Collections.synchronizedSet(new HashSet<>());

    //every notification will have a unique timestamp.
    //This is so because modification happen one at a time via the lock.
    private SortedSet<LocalDateTime> notificationTimestamps = new TreeSet<>();

    private List<TopologyValidationError> validationErrors = new ArrayList<>();
    private List<TopologyListener> topologyListeners = new ArrayList<>();

    private static final int LOCK_TIMEOUT = 2;

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
    /**
     * The Partition's from spec.
     */
    public static final String SQLG_SCHEMA_PARTITION_FROM = "from";
    /**
     * The Partition's to spec.
     */
    public static final String SQLG_SCHEMA_PARTITION_TO = "to";
    /**
     * The Partition's in spec.
     */
    public static final String SQLG_SCHEMA_PARTITION_IN = "in";
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
    public static final String SQLG_SCHEMA_VERTEX_LABEL_DISTRIBUTION_SHARD_COUNT= "shardCount";


    /**
     * Edge table for the edge's distribution column.
     */
    public static final String SQLG_SCHEMA_EDGE_DISTRIBUTION_COLUMN_EDGE = "edge_distribution";
    /**
     * Edge table for the edge's colocate label. The edge's co-locate will always be to its incoming vertex label.
     */
    public static final String SQLG_SCHEMA_EDGE_DISTRIBUTION_COLOCATE_EDGE = "edge_colocate";
    /**
     * Edge's shard_count property.
     */
    public static final String SQLG_SCHEMA_EDGE_LABEL_DISTRIBUTION_SHARD_COUNT= "shardCount";


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
     * Table storing the graphs unique property constraints.
     */
    public static final String SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX = "globalUniqueIndex";
    /**
     * Edge table for GlobalUniqueIndex to Property
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE = "globalUniqueIndex_property";
    /**
     * GlobalUniqueIndex table's name property
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_NAME = "name";


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

    private static final List<String> SQLG_SCHEMA_SCHEMA_TABLES = Arrays.asList(
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_SCHEMA,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_GRAPH,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_VERTEX_LABEL,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_EDGE_LABEL,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_PARTITION,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_PROPERTY,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_INDEX,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_LOG,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_VERTEX_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_IN_EDGES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_OUT_EDGES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_EDGE_PROPERTIES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_IDENTIFIER_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_EDGE_IDENTIFIER_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_PARTITION_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_EDGE_PARTITION_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_PARTITION_PARTITION_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_DISTRIBUTION_COLUMN_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_DISTRIBUTION_COLOCATE_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_EDGE_DISTRIBUTION_COLUMN_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_EDGE_DISTRIBUTION_COLOCATE_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_INDEX_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_EDGE_INDEX_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_INDEX_PROPERTY_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE
    );

    /**
     * Topology is a singleton created when the {@link SqlgGraph} is opened.
     * As the topology, i.e. sqlg_schema is created upfront the meta topology is pre-loaded.
     *
     * @param sqlgGraph The graph.
     */
    public Topology(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
        this.distributed = sqlgGraph.configuration().getBoolean(SqlgGraph.DISTRIBUTED, false);
        this.topologySqlWriteLock = new ReentrantLock(true);
        this.topologyMapLock = new ReentrantReadWriteLock(true);

        //Pre-create the meta topology.
        Schema sqlgSchema = Schema.instantiateSqlgSchema(this);
        this.metaSchemas.put(SQLG_SCHEMA, sqlgSchema);

        Map<String, PropertyType> columns = new HashMap<>();
        columns.put(SQLG_SCHEMA_GRAPH_VERSION, PropertyType.STRING);
        columns.put(SQLG_SCHEMA_GRAPH_DB_VERSION, PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        columns.put(UPDATED_ON, PropertyType.LOCALDATETIME);
        VertexLabel graphVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_GRAPH, columns);
        this.sqlgSchemaAbstractLabels.add(graphVertexLabel);

        columns = new HashMap<>();
        columns.put(SQLG_SCHEMA_PROPERTY_NAME, PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        VertexLabel schemaVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_SCHEMA, columns);
        this.sqlgSchemaAbstractLabels.add(schemaVertexLabel);

        columns = new HashMap<>();
        columns.put(SQLG_SCHEMA_VERTEX_LABEL_NAME, PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        columns.put(SCHEMA_VERTEX_DISPLAY, PropertyType.STRING);
        columns.put(SQLG_SCHEMA_VERTEX_LABEL_PARTITION_TYPE, PropertyType.STRING);
        columns.put(SQLG_SCHEMA_VERTEX_LABEL_PARTITION_EXPRESSION, PropertyType.STRING);
        columns.put(SQLG_SCHEMA_VERTEX_LABEL_DISTRIBUTION_SHARD_COUNT, PropertyType.INTEGER);
        VertexLabel vertexVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_VERTEX_LABEL, columns);
        this.sqlgSchemaAbstractLabels.add(vertexVertexLabel);

        columns.clear();
        columns.put(SQLG_SCHEMA_PROPERTY_NAME, PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        columns.put(SQLG_SCHEMA_EDGE_LABEL_PARTITION_TYPE, PropertyType.STRING);
        columns.put(SQLG_SCHEMA_EDGE_LABEL_PARTITION_EXPRESSION, PropertyType.STRING);
        columns.put(SQLG_SCHEMA_EDGE_LABEL_DISTRIBUTION_SHARD_COUNT, PropertyType.INTEGER);
        VertexLabel edgeVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_EDGE_LABEL, columns);
        this.sqlgSchemaAbstractLabels.add(edgeVertexLabel);

        VertexLabel partitionVertexLabel;
        columns.clear();
        columns.put(SQLG_SCHEMA_PROPERTY_NAME, PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        columns.put(SQLG_SCHEMA_PARTITION_FROM, PropertyType.STRING);
        columns.put(SQLG_SCHEMA_PARTITION_TO, PropertyType.STRING);
        columns.put(SQLG_SCHEMA_PARTITION_IN, PropertyType.STRING);
        columns.put(SQLG_SCHEMA_PARTITION_PARTITION_TYPE, PropertyType.STRING);
        columns.put(SQLG_SCHEMA_PARTITION_PARTITION_EXPRESSION, PropertyType.STRING);
        partitionVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_PARTITION, columns);
        this.sqlgSchemaAbstractLabels.add(partitionVertexLabel);

        columns.clear();
        columns.put(SQLG_SCHEMA_PROPERTY_NAME, PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        columns.put(SQLG_SCHEMA_PROPERTY_TYPE, PropertyType.STRING);
        VertexLabel propertyVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_PROPERTY, columns);
        this.sqlgSchemaAbstractLabels.add(propertyVertexLabel);

        columns.clear();
        columns.put(SQLG_SCHEMA_INDEX_NAME, PropertyType.STRING);
        columns.put(SQLG_SCHEMA_INDEX_INDEX_TYPE, PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        VertexLabel indexVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_INDEX, columns);
        this.sqlgSchemaAbstractLabels.add(indexVertexLabel);

        columns.clear();
        columns.put(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_NAME, PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        VertexLabel globalUniqueIndexVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX, columns);
        this.sqlgSchemaAbstractLabels.add(globalUniqueIndexVertexLabel);

        columns.clear();
        EdgeLabel schemaToVertexEdgeLabel = schemaVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE, vertexVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(schemaToVertexEdgeLabel);
        EdgeLabel vertexInEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_IN_EDGES_EDGE, edgeVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(vertexInEdgeLabel);
        EdgeLabel vertexOutEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_OUT_EDGES_EDGE, edgeVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(vertexOutEdgeLabel);

        EdgeLabel vertexPartitionEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_PARTITION_EDGE, partitionVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(vertexPartitionEdgeLabel);
        EdgeLabel edgePartitionEdgeLabel = edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_PARTITION_EDGE, partitionVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(edgePartitionEdgeLabel);
        EdgeLabel partitionPartitionEdgeLabel = partitionVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_PARTITION_PARTITION_EDGE, partitionVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(partitionPartitionEdgeLabel);
        EdgeLabel vertexDistributionPropertyColumnEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_DISTRIBUTION_COLUMN_EDGE, propertyVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(vertexDistributionPropertyColumnEdgeLabel);
        EdgeLabel vertexColocatePropertyColumnEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_DISTRIBUTION_COLOCATE_EDGE, vertexVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(vertexColocatePropertyColumnEdgeLabel);

        EdgeLabel edgeDistributionPropertyColumnEdgeLabel = edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_DISTRIBUTION_COLUMN_EDGE, propertyVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(edgeDistributionPropertyColumnEdgeLabel);
        EdgeLabel edgeColocatePropertyColumnEdgeLabel = edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_DISTRIBUTION_COLOCATE_EDGE, vertexVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(edgeColocatePropertyColumnEdgeLabel);

        EdgeLabel vertexPropertyEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE, propertyVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(vertexPropertyEdgeLabel);
        EdgeLabel edgePropertyEdgeLabel = edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, propertyVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(edgePropertyEdgeLabel);

        columns.put(SQLG_SCHEMA_VERTEX_IDENTIFIER_INDEX_EDGE, PropertyType.INTEGER);
        EdgeLabel vertexIdentifierEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_IDENTIFIER_EDGE, propertyVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(vertexIdentifierEdgeLabel);
        columns.clear();

        columns.put(SQLG_SCHEMA_EDGE_IDENTIFIER_INDEX_EDGE, PropertyType.INTEGER);
        EdgeLabel edgeIdentifierEdgeLabel = edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_IDENTIFIER_EDGE, propertyVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(edgeIdentifierEdgeLabel);
        columns.clear();

        EdgeLabel vertexIndexEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_INDEX_EDGE, indexVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(vertexIndexEdgeLabel);
        EdgeLabel edgeIndexEdgeLabel = edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_INDEX_EDGE, indexVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(edgeIndexEdgeLabel);
        columns.put(SQLG_SCHEMA_INDEX_PROPERTY_EDGE_SEQUENCE, PropertyType.INTEGER);
        EdgeLabel indexPropertyEdgeLabel = indexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_INDEX_PROPERTY_EDGE, propertyVertexLabel, columns);
        columns.clear();
        this.sqlgSchemaAbstractLabels.add(indexPropertyEdgeLabel);
        EdgeLabel globalUniqueIndexPropertyEdgeLabel = globalUniqueIndexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE, propertyVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(globalUniqueIndexPropertyEdgeLabel);

        columns.clear();
        columns.put(SQLG_SCHEMA_LOG_TIMESTAMP, PropertyType.LOCALDATETIME);
        columns.put(SQLG_SCHEMA_LOG_LOG, PropertyType.JSON);
        columns.put(SQLG_SCHEMA_LOG_PID, PropertyType.INTEGER);
        VertexLabel logVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_LOG, columns);
        this.sqlgSchemaAbstractLabels.add(logVertexLabel);

        //add the public schema
        this.schemas.put(sqlgGraph.getSqlDialect().getPublicSchema(), Schema.createPublicSchema(sqlgGraph, this, sqlgGraph.getSqlDialect().getPublicSchema()));

        //add the global unique index schema
        this.globalUniqueIndexSchema.put(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA, Schema.createGlobalUniqueIndexSchema(this));

        //populate the schema's allEdgesCache
        sqlgSchema.cacheEdgeLabels();
        //populate the allTablesCache
        sqlgSchema.getVertexLabels().values().forEach((v) -> this.sqlgSchemaTableCache.put(v.getSchema().getName() + "." + VERTEX_PREFIX + v.getLabel(), v.getPropertyTypeMap()));
        sqlgSchema.getEdgeLabels().values().forEach((e) -> this.sqlgSchemaTableCache.put(e.getSchema().getName() + "." + EDGE_PREFIX + e.getLabel(), e.getPropertyTypeMap()));

        sqlgSchema.getVertexLabels().values().forEach((v) -> {
            SchemaTable vertexLabelSchemaTable = SchemaTable.of(v.getSchema().getName(), VERTEX_PREFIX + v.getLabel());
            this.schemaTableForeignKeyCache.put(vertexLabelSchemaTable, Pair.of(new HashSet<>(), new HashSet<>()));
            v.getInEdgeLabels().forEach((edgeLabelName, edgeLabel) -> this.schemaTableForeignKeyCache.get(vertexLabelSchemaTable).getLeft().add(SchemaTable.of(edgeLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel())));
            v.getOutEdgeLabels().forEach((edgeLabelName, edgeLabel) -> this.schemaTableForeignKeyCache.get(vertexLabelSchemaTable).getRight().add(SchemaTable.of(v.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel())));
        });

        this.edgeForeignKeyCache = sqlgSchema.getAllEdgeForeignKeys();

        if (this.distributed) {
            ((SqlSchemaChangeDialect) this.sqlgGraph.getSqlDialect()).registerListener(sqlgGraph);
        }

        this.sqlgGraph.tx().beforeCommit(this::beforeCommit);
        this.sqlgGraph.tx().afterCommit(this::afterCommit);

        this.sqlgGraph.tx().afterRollback(() -> {
            if (this.sqlgGraph.getSqlDialect().supportsTransactionalSchema()) {
                afterRollback();
            } else {
                afterCommit();
            }
        });

        if (this.sqlgGraph.getSqlDialect().isPostgresql()) {
            registerListener((topologyInf, string, topologyChangeAction) -> deallocateAll());
        }

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

    /**
     * Global lock on the topology.
     * For distributed graph (multiple jvm) this happens on the db via a lock sql statement.
     */
    void lock() {
        //only lock if the lock is not already owned by this thread.
        if (!isSqlWriteLockHeldByCurrentThread()) {
            this.sqlgGraph.tx().readWrite();
            z_internalSqlWriteLock();
            if (this.distributed) {
                ((SqlSchemaChangeDialect) this.sqlgGraph.getSqlDialect()).lock(this.sqlgGraph);
                //load the log to see if the schema has not already been created.
                //the last loaded log
                if (!this.notificationTimestamps.isEmpty()) {
                    LocalDateTime timestamp = this.notificationTimestamps.last();
                    List<Vertex> logs = this.sqlgGraph.topology().V()
                            .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_LOG)
                            .has(SQLG_SCHEMA_LOG_TIMESTAMP, P.gt(timestamp))
                            .toList();
                    for (Vertex logVertex : logs) {
                        int pid = logVertex.value("pid");
                        LocalDateTime timestamp2 = logVertex.value("timestamp");
                        if (!ownPids.contains(new ImmutablePair<>(pid, timestamp2))) {
                            ObjectNode log = logVertex.value("log");
                            fromNotifyJson(timestamp, log);
                        }
                    }
                }
            }
        }
    }

    /**
     * Called from {@link Topology#lock()} to attempt to take the lock.
     * This ensures that only one thread at a time may execute schema change sql commands.
     * Sql schema change commands takes table locks on the database and is prone to dead locks.
     * One thread at a time reduces the dead lock risk.
     */
    private void z_internalSqlWriteLock() {
        Preconditions.checkState(!isSqlWriteLockHeldByCurrentThread());
        try {
            if (!this.topologySqlWriteLock.tryLock(LOCK_TIMEOUT, TimeUnit.MINUTES)) {
                throw new RuntimeException("Timeout lapsed to acquire write lock for notification.");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Called from {@link Topology#afterCommit()} and {@link Topology#afterRollback()}
     * Releases the lock.
     */
    private void z_internalSqlWriteUnlock() {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread());
        this.topologySqlWriteLock.unlock();
    }

    private void z_internalTopologyMapReadLock() {
        try {
            this.topologyMapLock.readLock().tryLock(LOCK_TIMEOUT, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void z_internalTopologyMapReadUnLock() {
        this.topologyMapLock.readLock().unlock();
    }

    /**
     * Called from {@link Topology#afterCommit()} and {@link Topology#fromNotifyJson(LocalDateTime, ObjectNode)}
     * These two methods are the only places where the topology maps are updated and therefore write locked.
     */
    private void z_internalTopologyMapWriteLock() {
        try {
            this.topologyMapLock.writeLock().tryLock(LOCK_TIMEOUT, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Called from {@link Topology#afterCommit()} and {@link Topology#fromNotifyJson(LocalDateTime, ObjectNode)}
     * These two methods are the only places where the topology maps are updated and therefore write unlocked.
     */
    private void z_internalInternalTopologyMapWriteUnLock() {
        this.topologyMapLock.writeLock().unlock();
    }

    /**
     * @return true if the current thread owns the sql write lock.
     */
    boolean isSqlWriteLockHeldByCurrentThread() {
        return this.topologySqlWriteLock.isHeldByCurrentThread();
    }

    boolean isTopologyMapWriteLockHeldByCurrentThread() {
        return this.topologyMapLock.writeLock().isHeldByCurrentThread();
    }

    /**
     * Ensures that the schema exists.
     *
     * @param schemaName The schema to create if it does not exist.
     */
    public Schema ensureSchemaExist(final String schemaName) {
        Optional<Schema> schemaOptional = this.getSchema(schemaName);
        Schema schema;
        if (!schemaOptional.isPresent()) {
            this.lock();
            //search again after the lock is obtained.
            schemaOptional = this.getSchema(schemaName);
            if (!schemaOptional.isPresent()) {
                //create the schema and the vertex label.
                schema = Schema.createSchema(this.sqlgGraph, this, schemaName);
                this.uncommittedRemovedSchemas.remove(schemaName);
                this.uncommittedSchemas.put(schemaName, schema);
                fire(schema, "", TopologyChangeAction.CREATE);
                return schema;
            } else {
                return schemaOptional.get();
            }
        } else {
            return schemaOptional.get();
        }
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
    public VertexLabel ensureVertexLabelExist(final String label, final Map<String, PropertyType> columns) {
        return ensureVertexLabelExist(this.sqlgGraph.getSqlDialect().getPublicSchema(), label, columns);
    }

    public VertexLabel ensureVertexLabelExist(final String label, final Map<String, PropertyType> columns, ListOrderedSet<String> identifiers) {
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
    public VertexLabel ensureVertexLabelExist(final String schemaName, final String label, final Map<String, PropertyType> properties) {
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
    public VertexLabel ensureVertexLabelExist(final String schemaName, final String label, final Map<String, PropertyType> properties, ListOrderedSet<String> identifiers) {
        Objects.requireNonNull(schemaName, "Given tables must not be null");
        Objects.requireNonNull(label, "Given table must not be null");
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not be prefixed with %s", VERTEX_PREFIX);

        Schema schema = this.ensureSchemaExist(schemaName);
        Preconditions.checkState(schema != null, "Schema must be present after calling ensureSchemaExist");
        return schema.ensureVertexLabelExist(label, properties, identifiers);
    }

    public void ensureTemporaryVertexTableExist(final String schema, final String label, final Map<String, PropertyType> properties) {
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
    public EdgeLabel ensureEdgeLabelExist(final String edgeLabelName, final VertexLabel outVertexLabel, final VertexLabel inVertexLabel, Map<String, PropertyType> properties) {
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
            Map<String, PropertyType> properties,
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
     * @return The {@link SchemaTable} that represents the edge.
     */
    public SchemaTable ensureEdgeLabelExist(final String edgeLabelName, final SchemaTable foreignKeyOut, final SchemaTable foreignKeyIn, Map<String, PropertyType> properties) {
        Objects.requireNonNull(edgeLabelName, "Given edgeLabelName must not be null");
        Objects.requireNonNull(foreignKeyOut, "Given outTable must not be null");
        Objects.requireNonNull(foreignKeyIn, "Given inTable must not be null");

        Preconditions.checkState(getVertexLabel(foreignKeyOut.getSchema(), foreignKeyOut.getTable()).isPresent(), "The out vertex must already exist before invoking 'ensureEdgeLabelExist'. \"%s\" does not exist", foreignKeyIn.toString());
        Preconditions.checkState(getVertexLabel(foreignKeyIn.getSchema(), foreignKeyIn.getTable()).isPresent(), "The in vertex must already exist before invoking 'ensureEdgeLabelExist'. \"%s\" does not exist", foreignKeyIn.toString());

        //outVertexSchema will be there as the Precondition checked it.
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Schema outVertexSchema = this.getSchema(foreignKeyOut.getSchema()).get();
        Schema inVertexSchema = this.getSchema(foreignKeyIn.getSchema()).get();
        Optional<VertexLabel> outVertexLabel = outVertexSchema.getVertexLabel(foreignKeyOut.getTable());
        Optional<VertexLabel> inVertexLabel = inVertexSchema.getVertexLabel(foreignKeyIn.getTable());
        Preconditions.checkState(outVertexLabel.isPresent(), "out VertexLabel must be present");
        Preconditions.checkState(inVertexLabel.isPresent(), "in VertexLabel must be present");

        @SuppressWarnings("OptionalGetWithoutIsPresent")
        EdgeLabel edgeLabel = outVertexSchema.ensureEdgeLabelExist(edgeLabelName, outVertexLabel.get(), inVertexLabel.get(), properties);
        return SchemaTable.of(foreignKeyOut.getSchema(), edgeLabel.getLabel());
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
    public void ensureVertexLabelPropertiesExist(String label, Map<String, PropertyType> properties) {
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
    public void ensureVertexLabelPropertiesExist(String schemaName, String label, Map<String, PropertyType> properties) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not start with \"%s\"", VERTEX_PREFIX);
        if (!schemaName.equals(SQLG_SCHEMA)) {
            Optional<Schema> schemaOptional = getSchema(schemaName);
            if (!schemaOptional.isPresent()) {
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
    public void ensureEdgePropertiesExist(String label, Map<String, PropertyType> properties) {
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
    public void ensureEdgePropertiesExist(String schemaName, String label, Map<String, PropertyType> properties) {
        Preconditions.checkArgument(!label.startsWith(EDGE_PREFIX), "label may not start with \"%s\"", EDGE_PREFIX);
        Preconditions.checkState(!schemaName.equals(SQLG_SCHEMA), "Topology.ensureEdgePropertiesExist may not be called for \"%s\"", SQLG_SCHEMA);

        if (!schemaName.equals(SQLG_SCHEMA)) {
            Optional<Schema> schemaOptional = getSchema(schemaName);
            if (!schemaOptional.isPresent()) {
                throw new IllegalStateException(String.format("BUG: schema %s can not be null", schemaName));
            }
            schemaOptional.get().ensureEdgeColumnsExist(label, properties);
        }
    }


    public GlobalUniqueIndex ensureGlobalUniqueIndexExist(final Set<PropertyColumn> properties) {
        Objects.requireNonNull(properties, "properties may not be null");
        Schema globalUniqueIndexSchema = getGlobalUniqueIndexSchema();
        return globalUniqueIndexSchema.ensureGlobalUniqueIndexExist(properties);
    }

    private void beforeCommit() {
        if (this.distributed) {
            Optional<JsonNode> jsonNodeOptional = this.toNotifyJson();
            if (jsonNodeOptional.isPresent()) {
                SqlSchemaChangeDialect sqlSchemaChangeDialect = (SqlSchemaChangeDialect) this.sqlgGraph.getSqlDialect();
                LocalDateTime timestamp = LocalDateTime.now();
                int pid = sqlSchemaChangeDialect.notifyChange(sqlgGraph, timestamp, jsonNodeOptional.get());
                this.ownPids.add(new ImmutablePair<>(pid, timestamp));
            }
        }
    }

    private Schema removeSchemaFromCaches(String schema) {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread() || isTopologyMapWriteLockHeldByCurrentThread());
        Schema s = this.schemas.remove(schema);
        this.allTableCache.keySet().removeIf(schemaTable -> schemaTable.startsWith(schema + "."));
        this.edgeForeignKeyCache.keySet().removeIf(schemaTable -> schemaTable.startsWith(schema + "."));
        this.schemaTableForeignKeyCache.keySet().removeIf(schemaTable -> schemaTable.getSchema().equals(schema));
        return s;
    }

    private void afterCommit() {
        if (this.isSqlWriteLockHeldByCurrentThread()) {
            z_internalTopologyMapWriteLock();
            try {
                getPublicSchema().removeTemporaryTables();
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
                    this.allTableCache.put(uncommittedSchemaTable, abstractLabel.getPropertyTypeMap());
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
                for (Schema schema : this.schemas.values()) {
                    schema.afterCommit();
                }
                for (Schema schema : this.globalUniqueIndexSchema.values()) {
                    schema.afterCommit();
                }
            } finally {
                z_internalInternalTopologyMapWriteUnLock();
                z_internalSqlWriteUnlock();
            }
        }
    }

    private void afterRollback() {
        if (this.isSqlWriteLockHeldByCurrentThread()) {
            getPublicSchema().removeTemporaryTables();
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
        }
    }

    /**
     * This is only needed for Postgresql.
     */
    private void deallocateAll() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute("DEALLOCATE ALL");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        //Soft reset the pool will leave active connections running.
        //All connections in the pool will be closed.
        //Running connections will be closed when they reenter the pool.
        this.sqlgGraph.getSqlgDataSource().softResetPool();
    }

    public void cacheTopology() {
        this.lock();
        GraphTraversalSource traversalSource = this.sqlgGraph.topology();
        //load the last log
        //the last timestamp is needed when just after obtaining the lock the log table is queried again to ensure that the last log is indeed
        //loaded as the notification might not have been received yet.
        List<Vertex> logs = traversalSource.V()
                .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_LOG)
                .order().by(SQLG_SCHEMA_LOG_TIMESTAMP, Order.decr)
                .limit(1)
                .toList();
        Preconditions.checkState(logs.size() <= 1, "must load one or zero logs in cacheTopology");

        if (!logs.isEmpty()) {
            Vertex log = logs.get(0);
            LocalDateTime timestamp = log.value("timestamp");
            this.notificationTimestamps.add(timestamp);
        } else {
            this.notificationTimestamps.add(LocalDateTime.now());
        }

        //First load all VertexLabels, their out edges and properties
        List<Vertex> schemaVertices = traversalSource.V().hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA).toList();
        for (Vertex schemaVertex : schemaVertices) {
            String schemaName = schemaVertex.value("name");
            Optional<Schema> schemaOptional = getSchema(schemaName);
            if (schemaName.equals(SQLG_SCHEMA) || schemaName.equals(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA)) {
                Preconditions.checkState(schemaOptional.isPresent(), "\"%s\" schema must always be present.", schemaName);
            }
            Schema schema;
            if (!schemaOptional.isPresent()) {
                schema = Schema.loadUserSchema(this, schemaName);
                if (!schema.getName().equals(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA)) {
                    this.schemas.put(schemaName, schema);
                }
            } else {
                schema = schemaOptional.get();

            }
            schema.loadVertexOutEdgesAndProperties(traversalSource, schemaVertex);
            // load vertex and edge indices
            schema.loadVertexIndices(traversalSource, schemaVertex);
            schema.loadEdgeIndices(traversalSource, schemaVertex);
        }
        //Now load the in edges
        schemaVertices = traversalSource.V().hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA).toList();
        for (Vertex schemaVertex : schemaVertices) {
            String schemaName = schemaVertex.value("name");
            if (!schemaName.equals(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA)) {
                Optional<Schema> schemaOptional = getSchema(schemaName);
                Preconditions.checkState(schemaOptional.isPresent(), "schema \"%s\" must be present when loading in edges.", schemaName);
                @SuppressWarnings("OptionalGetWithoutIsPresent")
                Schema schema = schemaOptional.get();
                schema.loadInEdgeLabels(traversalSource, schemaVertex);
            }
        }

        //Load the globalUniqueIndexes.
        List<Vertex> globalUniqueIndexVertices = traversalSource.V().hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX).toList();
        for (Vertex globalUniqueIndexVertex : globalUniqueIndexVertices) {
            String globalUniqueIndexName = globalUniqueIndexVertex.value("name");
            GlobalUniqueIndex globalUniqueIndex = GlobalUniqueIndex.instantiateGlobalUniqueIndex(this, globalUniqueIndexName);
            getGlobalUniqueIndexSchema().globalUniqueIndexes.put(globalUniqueIndexName, globalUniqueIndex);

            Set<Vertex> globalUniqueIndexProperties = traversalSource.V(globalUniqueIndexVertex).out(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE).toSet();
            Set<PropertyColumn> guiPropertyColumns = new HashSet<>();
            for (Vertex globalUniqueIndexPropertyVertex : globalUniqueIndexProperties) {
                //get the path to the vertex
                List<Map<String, Vertex>> vertexSchema = traversalSource
                        .V(globalUniqueIndexPropertyVertex)
                        .in(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE).as("vertex")
                        .in(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("schema")
                        .<Vertex>select("vertex", "schema")
                        .toList();
                if (!vertexSchema.isEmpty()) {
                    Preconditions.checkState(vertexSchema.size() == 1, "BUG: GlobalUniqueIndex %s property %s has more than one path to the schema.");
                    Vertex schemaVertex = vertexSchema.get(0).get("schema");
                    Vertex vertexVertex = vertexSchema.get(0).get("vertex");
                    Schema guiPropertySchema = getSchema(schemaVertex.<String>property("name").value()).get();
                    VertexLabel guiPropertyVertexLabel = guiPropertySchema.getVertexLabel(vertexVertex.<String>property("name").value()).get();
                    PropertyColumn propertyColumn = guiPropertyVertexLabel.getProperty(globalUniqueIndexPropertyVertex.<String>property("name").value()).get();
                    guiPropertyColumns.add(propertyColumn);
                } else {
                    vertexSchema = traversalSource
                            .V(globalUniqueIndexPropertyVertex)
                            .in(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE).as("edge")
                            .in(SQLG_SCHEMA_OUT_EDGES_EDGE).as("vertex")
                            .in(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).as("schema")
                            .<Vertex>select("edge", "vertex", "schema")
                            .toList();
                    Preconditions.checkState(vertexSchema.size() == 1, "BUG: GlobalUniqueIndex %s property %s has more than one path to the schema.");
                    Vertex schemaVertex = vertexSchema.get(0).get("schema");
                    Vertex vertexVertex = vertexSchema.get(0).get("vertex");
                    Vertex edgeVertex = vertexSchema.get(0).get("edge");
                    Schema guiPropertySchema = getSchema(schemaVertex.<String>property("name").value()).get();
                    VertexLabel guiPropertyVertexLabel = guiPropertySchema.getVertexLabel(vertexVertex.<String>property("name").value()).get();
                    EdgeLabel guiPropertyEdgeLabel = guiPropertyVertexLabel.getOutEdgeLabel(edgeVertex.<String>property("name").value()).get();
                    PropertyColumn propertyColumn = guiPropertyEdgeLabel.getProperty(globalUniqueIndexPropertyVertex.<String>property("name").value()).get();
                    guiPropertyColumns.add(propertyColumn);
                }
            }
            globalUniqueIndex.addGlobalUniqueProperties(guiPropertyColumns);
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
        z_internalTopologyMapReadLock();
        try {
            ObjectNode topologyNode = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
            ArrayNode schemaArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
            for (Schema schema : this.schemas.values()) {
                schemaArrayNode.add(schema.toJson());
            }
            topologyNode.set("schemas", schemaArrayNode);
            return topologyNode;
        } finally {
            z_internalTopologyMapReadUnLock();
        }
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
        z_internalTopologyMapReadLock();
        try {
            ArrayNode committedSchemaArrayNode = null;
            ObjectNode topologyNode = null;
            for (Schema schema : this.schemas.values()) {
                Optional<JsonNode> jsonNodeOptional = schema.toNotifyJson();
                if (jsonNodeOptional.isPresent() && committedSchemaArrayNode == null) {
                    committedSchemaArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
                }
                if (jsonNodeOptional.isPresent()) {
                    //noinspection ConstantConditions
                    committedSchemaArrayNode.add(jsonNodeOptional.get());
                }
            }
            if (committedSchemaArrayNode != null) {
                topologyNode = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
                topologyNode.set("schemas", committedSchemaArrayNode);
            }
            ArrayNode unCommittedSchemaArrayNode = null;
            if (this.isSqlWriteLockHeldByCurrentThread()) {
                for (Schema schema : this.uncommittedSchemas.values()) {
                    if (unCommittedSchemaArrayNode == null) {
                        unCommittedSchemaArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
                    }
                    Optional<JsonNode> jsonNodeOptional = schema.toNotifyJson();
                    if (jsonNodeOptional.isPresent()) {
                        unCommittedSchemaArrayNode.add(jsonNodeOptional.get());
                    } else {
                        ObjectNode schemaNode = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
                        schemaNode.put("name", schema.getName());
                        unCommittedSchemaArrayNode.add(schemaNode);
                    }
                }
                ArrayNode removed = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
                for (String schema : this.uncommittedRemovedSchemas) {
                    removed.add(schema);
                }
                if (removed.size() > 0) {
                    if (topologyNode == null) {
                        topologyNode = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
                    }
                    topologyNode.set("uncommittedRemovedSchemas", removed);
                }
            }
            if (unCommittedSchemaArrayNode != null) {
                if (topologyNode == null) {
                    topologyNode = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
                }
                topologyNode.set("uncommittedSchemas", unCommittedSchemaArrayNode);
            }
            ArrayNode globalUniqueIndexCommittedSchemaArrayNode = null;
            for (Schema schema : this.globalUniqueIndexSchema.values()) {
                Optional<JsonNode> jsonNodeOptional = schema.toNotifyJson();
                if (jsonNodeOptional.isPresent() && globalUniqueIndexCommittedSchemaArrayNode == null) {
                    globalUniqueIndexCommittedSchemaArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
                }
                if (jsonNodeOptional.isPresent()) {
                    //noinspection ConstantConditions
                    globalUniqueIndexCommittedSchemaArrayNode.add(jsonNodeOptional.get());
                }
            }
            if (globalUniqueIndexCommittedSchemaArrayNode != null) {
                if (topologyNode == null) {
                    topologyNode = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
                }
                topologyNode.set("globalUniqueIndexSchema", globalUniqueIndexCommittedSchemaArrayNode);
            }
            if (topologyNode != null) {
                return Optional.of(topologyNode);
            } else {
                return Optional.empty();
            }
        } finally {
            z_internalTopologyMapReadUnLock();
        }
    }

    public void fromNotifyJson(int pid, LocalDateTime notifyTimestamp) {
        try {
            ImmutablePair<Integer, LocalDateTime> p = new ImmutablePair<>(pid, notifyTimestamp);
            if (!this.ownPids.contains(p)) {
                List<Vertex> logs = this.sqlgGraph.topology().V()
                        .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_LOG)
                        .has(SQLG_SCHEMA_LOG_TIMESTAMP, notifyTimestamp)
                        .toList();
                Preconditions.checkState(logs.size() == 1, "There must be one and only be one log, found %d", logs.size());
                LocalDateTime timestamp = logs.get(0).value("timestamp");
                Preconditions.checkState(timestamp.equals(notifyTimestamp), "notify log's timestamp does not match.");
                int backEndPid = logs.get(0).value("pid");
                Preconditions.checkState(backEndPid == pid, "notify pids do not match.");
                ObjectNode log = logs.get(0).value("log");
                fromNotifyJson(timestamp, log);
            } else {
                // why? we get notifications for our own things
                //this.ownPids.remove(p);
            }
        } finally {
            this.sqlgGraph.tx().rollback();
        }
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private void fromNotifyJson(LocalDateTime timestamp, ObjectNode log) {
        z_internalTopologyMapWriteLock();
        try {
            //First do all the out edges. The in edge logic assumes the out edges are present.
            for (String s : Arrays.asList("uncommittedSchemas", "schemas", "globalUniqueIndexSchema")) {
                ArrayNode schemas = (ArrayNode) log.get(s);
                if (schemas != null) {
                    //first load all the schema as they might be required later
                    for (JsonNode jsonSchema : schemas) {
                        String schemaName = jsonSchema.get("name").asText();
                        Optional<Schema> schemaOptional = getSchema(schemaName);
                        Schema schema;
                        if (!schemaOptional.isPresent()) {
                            //add to map
                            schema = Schema.instantiateSchema(this, schemaName);
                            this.schemas.put(schemaName, schema);
                            fire(schema, "", TopologyChangeAction.CREATE);
                        }
                    }
                    for (JsonNode jsonSchema : schemas) {
                        String schemaName = jsonSchema.get("name").asText();
                        Optional<Schema> schemaOptional = getSchema(schemaName);
                        Preconditions.checkState(schemaOptional.isPresent(), "Schema must be present here");
                        @SuppressWarnings("OptionalGetWithoutIsPresent")
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
                        @SuppressWarnings("OptionalGetWithoutIsPresent")
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
                        fire(s, "", TopologyChangeAction.DELETE);
                    }
                }
            }

            this.notificationTimestamps.add(timestamp);
        } finally {
            z_internalInternalTopologyMapWriteUnLock();

        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Topology)) {
            return false;
        }
        z_internalTopologyMapReadLock();
        try {
            Topology other = (Topology) o;
            if (this.schemas.equals(other.schemas)) {
                //check each schema individually as schema equals does not check the VertexLabels
                for (Map.Entry<String, Schema> schemaEntry : schemas.entrySet()) {
                    Schema schema = schemaEntry.getValue();
                    Optional<Schema> otherSchemaOptional = other.getSchema(schemaEntry.getKey());
                    if (otherSchemaOptional.isPresent() && !schema.deepEquals(otherSchemaOptional.get())) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        } finally {
            z_internalTopologyMapReadUnLock();
        }
    }

    /////////////////////////////////getters and cache/////////////////////////////


    public Set<TopologyInf> getSqlgSchemaAbstractLabels() {
        return this.sqlgSchemaAbstractLabels;
    }

    public Set<GlobalUniqueIndex> getGlobalUniqueIndexes() {
        return new HashSet<>(getGlobalUniqueIndexSchema().getGlobalUniqueIndexes().values());
    }

    public Optional<GlobalUniqueIndex> getGlobalUniqueIndexes(String name) {
        return getGlobalUniqueIndexSchema().getGlobalUniqueIndex(name);
    }

    public Set<Schema> getSchemas() {
        z_internalTopologyMapReadLock();
        try {
            Set<Schema> result = new HashSet<>();
            result.addAll(this.schemas.values());
            if (this.isSqlWriteLockHeldByCurrentThread()) {
                result.addAll(this.uncommittedSchemas.values());
                if (uncommittedRemovedSchemas.size() > 0) {
                    for (Iterator<Schema> it = result.iterator(); it.hasNext(); ) {
                        Schema sch = it.next();
                        if (uncommittedRemovedSchemas.contains(sch.getName())) {
                            it.remove();
                        }
                    }
                }
            }
            return Collections.unmodifiableSet(result);
        } finally {
            z_internalTopologyMapReadUnLock();
        }
    }

    public Schema getPublicSchema() {
        Optional<Schema> schema = getSchema(this.sqlgGraph.getSqlDialect().getPublicSchema());
        Preconditions.checkState(schema.isPresent(), "BUG: The public schema must always be present");
        //noinspection OptionalGetWithoutIsPresent
        return schema.get();
    }

    public Schema getGlobalUniqueIndexSchema() {
        return this.globalUniqueIndexSchema.get(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA);
    }

    public Optional<Schema> getSchema(String schema) {
        if (isSqlWriteLockHeldByCurrentThread() && this.uncommittedRemovedSchemas.contains(schema)) {
            return Optional.empty();
        }
        if (schema.equals(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA)) {
            return Optional.of(getGlobalUniqueIndexSchema());
        } else {
            z_internalTopologyMapReadLock();
            try {
                Schema result = this.schemas.get(schema);
                if (result == null) {
                    if (isSqlWriteLockHeldByCurrentThread()) {
                        result = this.uncommittedSchemas.get(schema);
                    }
                    if (result == null) {
                        result = this.metaSchemas.get(schema);
                    }
                }
                return Optional.ofNullable(result);
            } finally {
                z_internalTopologyMapReadUnLock();
            }
        }
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
            Optional<EdgeLabel> edgeLabelOptional = schema.getEdgeLabel(edgeLabelName);
            if (edgeLabelOptional.isPresent()) {
                return edgeLabelOptional;
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    private Map<String, AbstractLabel> getUncommittedAllTables() {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread(), "getUncommittedAllTables must be called with the lock held");
        z_internalTopologyMapReadLock();
        try {
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
        } finally {
            z_internalTopologyMapReadUnLock();
        }
    }

    private Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getUncommittedSchemaTableForeignKeys() {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread(), "getUncommittedSchemaTableForeignKeys must be called with the lock held");
        z_internalTopologyMapReadLock();
        try {
            Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = new HashMap<>();
            for (Map.Entry<String, Schema> stringSchemaEntry : this.schemas.entrySet()) {
                Schema schema = stringSchemaEntry.getValue();
                result.putAll(schema.getUncommittedSchemaTableForeignKeys());
            }
            for (Map.Entry<String, Schema> stringSchemaEntry : this.uncommittedSchemas.entrySet()) {
                Schema schema = stringSchemaEntry.getValue();
                result.putAll(schema.getUncommittedSchemaTableForeignKeys());
            }
            return result;
        } finally {
            z_internalTopologyMapReadUnLock();
        }
    }

    private Map<String, Set<ForeignKey>> getUncommittedEdgeForeignKeys() {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread(), "getUncommittedEdgeForeignKeys must be called with the lock held");
        z_internalTopologyMapReadLock();
        try {
            Map<String, Set<ForeignKey>> result = new HashMap<>();
            for (Map.Entry<String, Schema> stringSchemaEntry : this.schemas.entrySet()) {
                Schema schema = stringSchemaEntry.getValue();
                result.putAll(schema.getUncommittedEdgeForeignKeys());
            }
            for (Map.Entry<String, Schema> stringSchemaEntry : this.uncommittedSchemas.entrySet()) {
                Schema schema = stringSchemaEntry.getValue();
                result.putAll(schema.getUncommittedEdgeForeignKeys());
            }
            return result;
        } finally {
            z_internalTopologyMapReadUnLock();
        }
    }

    /**
     * get all tables by schema, with their properties
     * does not return schema tables
     *
     * @return
     */
    public Map<String, Map<String, PropertyType>> getAllTables() {
        return getAllTables(false);
    }

    public Map<String, Map<String, PropertyType>> getAllTables(boolean sqlgSchema) {
        return getAllTables(sqlgSchema, false);
    }

    /**
     * get all tables by schema, with their properties
     *
     * @param sqlgSchema do we want the sqlg_schema tables?
     * @return a map of all tables and their properties.
     */
    public Map<String, Map<String, PropertyType>> getAllTables(boolean sqlgSchema, boolean guiSchema) {
        Preconditions.checkState(!(sqlgSchema && guiSchema), "Both sqlgSchema and guiSchema can not be true. Only one or none.");
        if (sqlgSchema) {
            return Collections.unmodifiableMap(this.sqlgSchemaTableCache);
        } else if (guiSchema) {
            Map<String, Map<String, PropertyType>> result = new HashMap<>();
            for (GlobalUniqueIndex globalUniqueIndex : this.getGlobalUniqueIndexes()) {
                Map<String, PropertyType> properties = new LinkedHashMap<String, PropertyType>() {{
                    put(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE, globalUniqueIndex.getProperties().stream().findAny().orElseThrow(IllegalStateException::new).getPropertyType());
                    put(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID, PropertyType.STRING);
                    put(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_PROPERTY_NAME, PropertyType.STRING);
                }};
                result.put(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "." + Topology.VERTEX_PREFIX + globalUniqueIndex.getName(), properties);
            }
            return Collections.unmodifiableMap(result);
        } else {
            z_internalTopologyMapReadLock();
            try {
                //Need to make a copy so as not to corrupt the allTableCache with uncommitted schema elements
                Map<String, Map<String, PropertyType>> result;
                if (this.isSqlWriteLockHeldByCurrentThread()) {
                    result = new HashMap<>();
                    for (Map.Entry<String, Map<String, PropertyType>> allTableCacheMapEntry : this.allTableCache.entrySet()) {
                        result.put(allTableCacheMapEntry.getKey(), new HashMap<>(allTableCacheMapEntry.getValue()));
                    }
                    Map<String, AbstractLabel> uncommittedLabels = this.getUncommittedAllTables();
                    for (String table : uncommittedLabels.keySet()) {
                        if (result.containsKey(table)) {
                            result.get(table).putAll(uncommittedLabels.get(table).getPropertyTypeMap());
                        } else {
                            result.put(table, uncommittedLabels.get(table).getPropertyTypeMap());
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
            } finally {
                z_internalTopologyMapReadUnLock();
            }

        }
    }

    public Map<String, PropertyColumn> getPropertiesFor(SchemaTable schemaTable) {
        Optional<Schema> schemaOptional = getSchema(schemaTable.getSchema());
        if (schemaOptional.isPresent()) {
            return Collections.unmodifiableMap(schemaOptional.get().getPropertiesFor(schemaTable));
        }
        return Collections.emptyMap();
    }

    public Map<String, PropertyColumn> getPropertiesWithGlobalUniqueIndexFor(SchemaTable schemaTable) {
        Optional<Schema> schemaOptional = getSchema(schemaTable.getSchema());
        if (schemaOptional.isPresent()) {
            return Collections.unmodifiableMap(schemaOptional.get().getPropertiesWithGlobalUniqueIndexFor(schemaTable));
        }
        return Collections.emptyMap();
    }

    public Map<String, PropertyType> getTableFor(SchemaTable schemaTable) {
        Map<String, PropertyType> result = getAllTables(schemaTable.getSchema().equals(Topology.SQLG_SCHEMA), schemaTable.getSchema().equals(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA)).get(schemaTable.toString());
        if (result != null) {
            return Collections.unmodifiableMap(result);
        }
        return Collections.emptyMap();
    }

    public Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getTableLabels() {
        z_internalTopologyMapReadLock();
        try {
            if (this.isSqlWriteLockHeldByCurrentThread()) {
                Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> uncommittedSchemaTableForeignKeys = getUncommittedSchemaTableForeignKeys();
                for (Map.Entry<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> schemaTablePairEntry : this.schemaTableForeignKeyCache.entrySet()) {
                    Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedForeignKeys = uncommittedSchemaTableForeignKeys.get(schemaTablePairEntry.getKey());
                    if (uncommittedForeignKeys != null) {
                        uncommittedForeignKeys.getLeft().addAll(schemaTablePairEntry.getValue().getLeft());
                        uncommittedForeignKeys.getRight().addAll(schemaTablePairEntry.getValue().getRight());
                    } else {
                        uncommittedSchemaTableForeignKeys.put(schemaTablePairEntry.getKey(), schemaTablePairEntry.getValue());
                    }
                }
                return Collections.unmodifiableMap(uncommittedSchemaTableForeignKeys);
            } else {
                return Collections.unmodifiableMap(this.schemaTableForeignKeyCache);
            }
        } finally {
            z_internalTopologyMapReadUnLock();
        }
    }

    private Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> loadTableLabels() {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread());
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

    //This is for backward compatibility.
    public Map<String, Set<String>> getAllEdgeForeignKeys() {
        Map<String, Set<String>> result = new HashMap<>();
        Map<String, Set<ForeignKey>> allEdgeForiegnKeys = getEdgeForeignKeys();
        for (Map.Entry<String, Set<ForeignKey>> stringSetEntry : allEdgeForiegnKeys.entrySet()) {

            String key = stringSetEntry.getKey();
            Set<ForeignKey> foreignKeys = stringSetEntry.getValue();
            Set<String> foreignKeySet = new HashSet<>();
            result.put(key, foreignKeySet);
            for (ForeignKey foreignKey : foreignKeys) {
                foreignKeySet.add(foreignKey.getCompositeKeys().get(0));
            }
        }
        return result;
    }

    public Map<String, Set<ForeignKey>> getEdgeForeignKeys() {
        z_internalTopologyMapReadLock();
        try {
            if (this.isSqlWriteLockHeldByCurrentThread()) {
                Map<String, Set<ForeignKey>> committed = new HashMap<>(this.edgeForeignKeyCache);
                Map<String, Set<ForeignKey>> uncommittedEdgeForeignKeys = getUncommittedEdgeForeignKeys();
                for (Map.Entry<String, Set<ForeignKey>> uncommittedEntry : uncommittedEdgeForeignKeys.entrySet()) {
                    Set<ForeignKey> committedForeignKeys = committed.get(uncommittedEntry.getKey());
                    if (committedForeignKeys != null) {
                        Set<ForeignKey> originalPlusUncommittedForeignKeys = new HashSet<>(committedForeignKeys);
                        originalPlusUncommittedForeignKeys.addAll(uncommittedEntry.getValue());
                        committed.put(uncommittedEntry.getKey(), originalPlusUncommittedForeignKeys);
                    } else {
                        committed.put(uncommittedEntry.getKey(), uncommittedEntry.getValue());
                    }
                }
                return Collections.unmodifiableMap(committed);
            } else {
                return Collections.unmodifiableMap(this.edgeForeignKeyCache);
            }
        } finally {
            z_internalTopologyMapReadUnLock();
        }
    }

    private Map<String, Set<ForeignKey>> loadAllEdgeForeignKeys() {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread());
        Map<String, Set<ForeignKey>> result = new HashMap<>();
        for (Schema schema : this.schemas.values()) {
            result.putAll(schema.getAllEdgeForeignKeys());
        }
        return result;
    }

    void addToEdgeForeignKeyCache(String name, ForeignKey foreignKey) {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread() || isTopologyMapWriteLockHeldByCurrentThread());
        Set<ForeignKey> foreignKeys = this.edgeForeignKeyCache.get(name);
        //noinspection Java8MapApi
        if (foreignKeys == null) {
            foreignKeys = new HashSet<>();
            this.edgeForeignKeyCache.put(name, foreignKeys);
        }
        foreignKeys.add(foreignKey);

    }

    void removeFromEdgeForeignKeyCache(String name, ForeignKey foreignKey) {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread() || isTopologyMapWriteLockHeldByCurrentThread());
        Set<ForeignKey> foreignKeys = this.edgeForeignKeyCache.get(name);
        if (foreignKeys != null) {
            foreignKeys.remove(foreignKey);
            if (foreignKeys.isEmpty()) {
                this.edgeForeignKeyCache.remove(name);
            }
        }

    }

    void addToAllTables(String tableName, Map<String, PropertyType> propertyTypeMap) {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread() || isTopologyMapWriteLockHeldByCurrentThread());
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
     * @param vertexLabel
     * @param edgeLabel
     */
    void addOutForeignKeysToVertexLabel(VertexLabel vertexLabel, EdgeLabel edgeLabel) {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread() || isTopologyMapWriteLockHeldByCurrentThread());
        SchemaTable schemaTable = SchemaTable.of(vertexLabel.getSchema().getName(), VERTEX_PREFIX + vertexLabel.getLabel());
        Pair<Set<SchemaTable>, Set<SchemaTable>> foreignKeys = this.schemaTableForeignKeyCache.computeIfAbsent(
                schemaTable, k -> Pair.of(new HashSet<>(), new HashSet<>())
        );
        foreignKeys.getRight().add(SchemaTable.of(vertexLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()));
    }

    /**
     * add in foreign key between a vertex label and a edge label
     *
     * @param vertexLabel
     * @param edgeLabel
     */
    void addInForeignKeysToVertexLabel(VertexLabel vertexLabel, EdgeLabel edgeLabel) {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread() || isTopologyMapWriteLockHeldByCurrentThread());
        SchemaTable schemaTable = SchemaTable.of(vertexLabel.getSchema().getName(), VERTEX_PREFIX + vertexLabel.getLabel());
        Pair<Set<SchemaTable>, Set<SchemaTable>> foreignKeys = this.schemaTableForeignKeyCache.computeIfAbsent(
                schemaTable, k -> Pair.of(new HashSet<>(), new HashSet<>())
        );
        foreignKeys.getLeft().add(SchemaTable.of(edgeLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()));
    }

    /**
     * remove out foreign key for a given vertex label and edge label
     *
     * @param vertexLabel
     * @param edgeLabel
     */
    void removeOutForeignKeysFromVertexLabel(VertexLabel vertexLabel, EdgeLabel edgeLabel) {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread() || isTopologyMapWriteLockHeldByCurrentThread());
        SchemaTable schemaTable = SchemaTable.of(vertexLabel.getSchema().getName(), VERTEX_PREFIX + vertexLabel.getLabel());
        Pair<Set<SchemaTable>, Set<SchemaTable>> foreignKeys = this.schemaTableForeignKeyCache.get(schemaTable);
        if (foreignKeys != null) {
            foreignKeys.getRight().remove(SchemaTable.of(vertexLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()));
        }
    }

    /**
     * remove in foreign key for a given vertex label and edge label
     *
     * @param vertexLabel
     * @param edgeLabel
     */
    void removeInForeignKeysFromVertexLabel(VertexLabel vertexLabel, EdgeLabel edgeLabel) {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread() || isTopologyMapWriteLockHeldByCurrentThread());
        SchemaTable schemaTable = SchemaTable.of(vertexLabel.getSchema().getName(), VERTEX_PREFIX + vertexLabel.getLabel());
        Pair<Set<SchemaTable>, Set<SchemaTable>> foreignKeys = this.schemaTableForeignKeyCache.get(schemaTable);
        if (foreignKeys != null && edgeLabel.isValid()) {
            foreignKeys.getLeft().remove(SchemaTable.of(edgeLabel.getSchema().getName(), EDGE_PREFIX + edgeLabel.getLabel()));
        }
    }

    /**
     * remove a given vertex label
     *
     * @param vertexLabel
     */
    void removeVertexLabel(VertexLabel vertexLabel) {
        Preconditions.checkState(isSqlWriteLockHeldByCurrentThread() || isTopologyMapWriteLockHeldByCurrentThread());
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
                foreignKey.add(vertexLabel.getFullName() + "." + identifier + OUT_VERTEX_COLUMN_END);
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
                foreignKey.add(vertexLabel.getFullName() + "." + identifier + IN_VERTEX_COLUMN_END);
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

    void fire(TopologyInf topologyInf, String oldValue, TopologyChangeAction action) {
        for (TopologyListener topologyListener : this.topologyListeners) {
            topologyListener.change(topologyInf, oldValue, action);
        }
    }

    /**
     * remove a given schema
     *
     * @param schema       the schema
     * @param preserveData should we preserve the SQL data?
     */
    void removeSchema(Schema schema, boolean preserveData) {
        lock();
        if (!this.uncommittedRemovedSchemas.contains(schema.getName())) {
            // remove edge roles in other schemas pointing to vertex labels in removed schema
            // TODO undo this in case of rollback?
            for (VertexLabel vlbl : schema.getVertexLabels().values()) {
                for (EdgeRole er : vlbl.getInEdgeRoles().values()) {
                    if (er.getEdgeLabel().getSchema() != schema) {
                        er.remove(preserveData);
                    }
                }
                // remove out edge roles in other schemas edges
                for (EdgeRole er : vlbl.getOutEdgeRoles().values()) {
                    if (er.getEdgeLabel().getSchema() == schema) {
                        for (EdgeRole erIn : er.getEdgeLabel().getInEdgeRoles()) {
                            if (erIn.getVertexLabel().getSchema() != schema) {
                                erIn.remove(preserveData);
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
            fire(schema, "", TopologyChangeAction.DELETE);
        }
    }

    public static class TopologyValidationError {
        private TopologyInf error;

        TopologyValidationError(TopologyInf error) {
            this.error = error;
        }

        @Override
        public String toString() {
            return String.format("%s does not exist", error.getName());
        }
    }
}
