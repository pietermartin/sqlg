package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.sql.dialect.SqlSchemaChangeDialect;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.umlg.sqlg.structure.SchemaManager.EDGE_PREFIX;
import static org.umlg.sqlg.structure.SchemaManager.VERTEX_PREFIX;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class Topology {

    private Logger logger = LoggerFactory.getLogger(Topology.class.getName());
    private SqlgGraph sqlgGraph;
    private boolean distributed;
    private ReentrantReadWriteLock reentrantReadWriteLock;
    private Map<String, Map<String, PropertyType>> allTableCache = new HashMap<>();
    //This cache is needed as to much time is taken building it on the fly.
    //The cache is invalidated on every topology change
    private Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> tableLabelCache;

    //Map the topology. This is for regular schemas. i.e. 'public.Person', 'special.Car'
    //The map needs to be concurrent as elements can be added in one thread and merged via notify from another at the same time.
    private Map<String, Schema> schemas = new HashMap<>();
    private Map<String, Schema> uncommittedSchemas = new HashMap<>();
    private Map<String, Schema> metaSchemas = new HashMap<>();
    //A cache of just the sqlg_schema's AbstractLabels
    private Set<TopologyInf> sqlgSchemaAbstractLabels = new HashSet<>();
    private Set<GlobalUniqueIndex> globalUniqueIndexes = new HashSet<>();
    private Set<GlobalUniqueIndex> uncommittedGlobalUniqueIndexes = new HashSet<>();

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final String SQLG_NOTIFICATION_CHANNEL = "SQLG_NOTIFY";

    //temporary tables
    private Map<String, Map<String, PropertyType>> temporaryTables = new ConcurrentHashMap<>();

    //ownPids are the pids to ignore as it is what the graph sent a notification for.
    private Set<Integer> ownPids = new HashSet<>();

    //every notification will have a unique timestamp.
    //This is so because modification happen one at a time via the lock.
    private SortedSet<LocalDateTime> notificationTimestamps = new TreeSet<>();

    private List<TopologyValidationError> validationErrors = new ArrayList<>();
    private List<TopologyListener> topologyListeners = new ArrayList<>();

    private static final int LOCK_TIMEOUT = 100;



    @SuppressWarnings("WeakerAccess")
    public static final String CREATED_ON = "createdOn";

    @SuppressWarnings("WeakerAccess")
    public static final String SCHEMA_VERTEX_DISPLAY = "schemaVertex";

    /**
     * Rdbms schema that holds sqlg topology.
     */
    public static final String SQLG_SCHEMA = "sqlg_schema";
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
     * Table storing the graphs edge labels.
     */
    public static final String SQLG_SCHEMA_EDGE_LABEL = "edge";
    /**
     * EdgeLabel's name property.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String SQLG_SCHEMA_EDGE_LABEL_NAME = "name";
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

    public static final List<String> SQLG_SCHEMA_SCHEMA_TABLES = Arrays.asList(
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_SCHEMA,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_VERTEX_LABEL,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_EDGE_LABEL,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_PROPERTY,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_INDEX,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_LOG,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_VERTEX_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_IN_EDGES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_OUT_EDGES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_EDGE_PROPERTIES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_INDEX_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_EDGE_INDEX_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_INDEX_PROPERTY_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE
    );

    public static final List<String> SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_SCHEMA = Arrays.asList(
            Schema.GLOBAL_UNIQUE_INDEX_SCHEMA
    );

    /**
     * Topology is a singleton created when the {@link SqlgGraph} is opened.
     * As the topology, i.e. sqlg_schema is created upfront the meta topology is pre-loaded.
     *
     * @param sqlgGraph The graph.
     */
    Topology(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
        this.distributed = sqlgGraph.configuration().getBoolean(SqlgGraph.DISTRIBUTED, false);
        this.reentrantReadWriteLock = new ReentrantReadWriteLock();

        //Pre-create the meta topology.
        Schema sqlgSchema = Schema.instantiateSqlgSchema(this);
        this.metaSchemas.put(SQLG_SCHEMA, sqlgSchema);

        Map<String, PropertyType> columns = new HashedMap<>();
        columns.put(SQLG_SCHEMA_PROPERTY_NAME, PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        VertexLabel schemaVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_SCHEMA, columns);
        this.sqlgSchemaAbstractLabels.add(schemaVertexLabel);
        columns.put(SCHEMA_VERTEX_DISPLAY, PropertyType.STRING);
        VertexLabel vertexVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_VERTEX_LABEL, columns);
        this.sqlgSchemaAbstractLabels.add(vertexVertexLabel);
        columns.remove(SCHEMA_VERTEX_DISPLAY);
        VertexLabel edgeVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_EDGE_LABEL, columns);
        this.sqlgSchemaAbstractLabels.add(edgeVertexLabel);

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
        @SuppressWarnings("unused")
        EdgeLabel schemaToVertexEdgeLabel = schemaVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE, vertexVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(schemaToVertexEdgeLabel);
        @SuppressWarnings("unused")
        EdgeLabel vertexInEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_IN_EDGES_EDGE, edgeVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(vertexInEdgeLabel);
        @SuppressWarnings("unused")
        EdgeLabel vertexOutEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_OUT_EDGES_EDGE, edgeVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(vertexOutEdgeLabel);
        @SuppressWarnings("unused")
        EdgeLabel vertexPropertyEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE, propertyVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(vertexPropertyEdgeLabel);
        @SuppressWarnings("unused")
        EdgeLabel edgePropertyEdgeLabel = edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, propertyVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(edgePropertyEdgeLabel);
        @SuppressWarnings("unused")
        EdgeLabel vertexIndexEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_INDEX_EDGE, indexVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(vertexIndexEdgeLabel);
        @SuppressWarnings("unused")
        EdgeLabel edgeIndexEdgeLabel = edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_INDEX_EDGE, indexVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(edgeIndexEdgeLabel);
        @SuppressWarnings("unused")
        EdgeLabel indexPropertyEdgeLabel = indexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_INDEX_PROPERTY_EDGE, propertyVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(indexPropertyEdgeLabel);
        @SuppressWarnings("unused")
        EdgeLabel globalUniqueIndexPropertyEdgeLabel = globalUniqueIndexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE, propertyVertexLabel, columns);
        this.sqlgSchemaAbstractLabels.add(globalUniqueIndexPropertyEdgeLabel);

        columns.clear();
        columns.put(SQLG_SCHEMA_LOG_TIMESTAMP, PropertyType.LOCALDATETIME);
        columns.put(SQLG_SCHEMA_LOG_LOG, PropertyType.JSON);
        columns.put(SQLG_SCHEMA_LOG_PID, PropertyType.INTEGER);
        @SuppressWarnings("unused")
        VertexLabel logVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_LOG, columns);
        this.sqlgSchemaAbstractLabels.add(logVertexLabel);

        //add the public schema
        this.schemas.put(sqlgGraph.getSqlDialect().getPublicSchema(), Schema.createPublicSchema(this, sqlgGraph.getSqlDialect().getPublicSchema()));

        //add the global unique index schema
        this.schemas.put(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA, Schema.createGlobalUniqueIndexSchema(this));

        //populate the schema's allEdgesCache
        sqlgSchema.cacheEdgeLabels();
        //populate the allTablesCache
        sqlgSchema.getVertexLabels().values().forEach((v) -> this.allTableCache.put(v.getSchema().getName() + "." + VERTEX_PREFIX + v.getLabel(), v.getPropertyTypeMap()));
        sqlgSchema.getEdgeLabels().values().forEach((e) -> this.allTableCache.put(e.getSchema().getName() + "." + EDGE_PREFIX + e.getLabel(), e.getPropertyTypeMap()));

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

        if (this.sqlgGraph.getSqlDialect().requiredPreparedStatementDeallocate()) {
            registerListener((TopologyInf, String, TopologyChangeAction) -> deallocateAll());
        }
    }

    SqlgGraph getSqlgGraph() {
        return this.sqlgGraph;
    }

    void close() {
        if (this.distributed)
            ((SqlSchemaChangeDialect) this.sqlgGraph.getSqlDialect()).unregisterListener();
    }

    public List<TopologyValidationError> getValidationErrors() {
        return validationErrors;
    }

    /**
     * Global lock on the topology.
     * For distributed graph (multiple jvm) this happens on the db via a lock sql statement.
     */
    void lock() {
        //only lock if the lock is not already owned by this thread.
        if (!isWriteLockHeldByCurrentThread()) {
            try {
                this.sqlgGraph.tx().readWrite();
                if (!this.reentrantReadWriteLock.writeLock().tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
                    throw new RuntimeException("timeout lapsed to acquire lock schema creation.");
                }
                if (this.distributed) {
                    ((SqlSchemaChangeDialect) this.sqlgGraph.getSqlDialect()).lock(this.sqlgGraph);
                    //load the log to see if the schema has not already been created.
                    //the last loaded log
                    LocalDateTime timestamp = this.notificationTimestamps.last();
                    List<Vertex> logs = this.sqlgGraph.topology().V()
                            .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_LOG)
                            .has(SQLG_SCHEMA_LOG_TIMESTAMP, P.gt(timestamp))
                            .toList();
                    for (Vertex logVertex : logs) {
                        ObjectNode log = logVertex.value("log");
                        fromNotifyJson(timestamp, log);
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Not for public consumption. Used for the notification.
     */
    private void z_internalWriteLock() {
        //only lock if the lock is not already owned by this thread.
        if (!isWriteLockHeldByCurrentThread()) {
            try {
                this.sqlgGraph.tx().readWrite();
                if (!this.reentrantReadWriteLock.writeLock().tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
                    throw new RuntimeException("Timeout lapsed to acquire write lock for notification.");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Not for public consumption.
     */
    private void z_internalReadLock() {
        this.reentrantReadWriteLock.readLock().lock();
    }

    private void z_internalReadUnLock() {
        this.reentrantReadWriteLock.readLock().unlock();
    }

    /**
     * @return true if the current thread owns the lock.
     */
    boolean isWriteLockHeldByCurrentThread() {
        return this.reentrantReadWriteLock.isWriteLockedByCurrentThread();
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
        Objects.requireNonNull(edgeLabelName, "Given edgeLabelName must not be null");
        Objects.requireNonNull(outVertexLabel, "Given outVertexLabel must not be null");
        Objects.requireNonNull(inVertexLabel, "Given inVertexLabel must not be null");
        Schema outVertexSchema = outVertexLabel.getSchema();
        return outVertexSchema.ensureEdgeLabelExist(edgeLabelName, outVertexLabel, inVertexLabel, properties);
    }

    public void ensureVertexTemporaryTableExist(final String schema, final String table, final Map<String, PropertyType> columns) {
        Objects.requireNonNull(schema, "Given schema may not be null");
        Objects.requireNonNull(table, "Given table may not be null");
        final String prefixedTable = VERTEX_PREFIX + table;
        if (!this.temporaryTables.containsKey(prefixedTable)) {
            lock();
            if (!this.temporaryTables.containsKey(prefixedTable)) {
                this.temporaryTables.put(prefixedTable, columns);
                createTempTable(prefixedTable, columns);
            }
        }
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
        Schema globalUniqueIndexSchema = getSchema(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA).orElseThrow(() -> new IllegalStateException("BUG: Global unique index schema " + Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + " must exist"));
        return globalUniqueIndexSchema.ensureGlobalUniqueIndexExist(properties);
    }

    public void createTempTable(String tableName, Map<String, PropertyType> columns) {
        this.sqlgGraph.getSqlDialect().assertTableName(tableName);
        StringBuilder sql = new StringBuilder(this.sqlgGraph.getSqlDialect().createTemporaryTableStatement());
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(tableName));
        sql.append("(");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlgGraph.getSqlDialect().getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        AbstractLabel.buildColumns(this.sqlgGraph, columns, sql);
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


    private void beforeCommit() {
        Optional<JsonNode> jsonNodeOptional = this.toNotifyJson();
        if (jsonNodeOptional.isPresent() && this.distributed) {
            SqlSchemaChangeDialect sqlSchemaChangeDialect = (SqlSchemaChangeDialect) this.sqlgGraph.getSqlDialect();
            LocalDateTime timestamp = LocalDateTime.now();
            int pid = sqlSchemaChangeDialect.notifyChange(sqlgGraph, timestamp, jsonNodeOptional.get());
            this.ownPids.add(pid);
        }
    }

    private void afterCommit() {
        this.temporaryTables.clear();
        if (this.isWriteLockHeldByCurrentThread()) {
            for (Iterator<Map.Entry<String, Schema>> it = this.uncommittedSchemas.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Schema> entry = it.next();
                this.schemas.put(entry.getKey(), entry.getValue());
                it.remove();
            }

            //merge the allTableCache and uncommittedAllTables
            Map<String, AbstractLabel> uncommittedAllTables = getUncommittedAllTables();
            for (Map.Entry<String, AbstractLabel> stringMapEntry : uncommittedAllTables.entrySet()) {
                String uncommittedSchemaTable = stringMapEntry.getKey();
                AbstractLabel uncommittedProperties = stringMapEntry.getValue();
                Map<String, PropertyType> committedProperties = this.allTableCache.get(uncommittedSchemaTable);
                if (committedProperties != null) {
                    committedProperties.putAll(uncommittedProperties.getPropertyTypeMap());
                } else {
                    this.allTableCache.put(uncommittedSchemaTable, uncommittedProperties.getPropertyTypeMap());
                }
            }
            for (Iterator<GlobalUniqueIndex> it = this.uncommittedGlobalUniqueIndexes.iterator(); it.hasNext(); ) {
                GlobalUniqueIndex globalUniqueIndex = it.next();
                this.globalUniqueIndexes.add(globalUniqueIndex);
                it.remove();
            }
            z_internalReadLock();
            try {
                for (Schema schema : this.schemas.values()) {
                    schema.afterCommit();
                }
            } finally {
                z_internalReadUnLock();
            }
            this.reentrantReadWriteLock.writeLock().unlock();
        }
    }

    private void afterRollback() {
        this.temporaryTables.clear();
        if (this.isWriteLockHeldByCurrentThread()) {
            for (Iterator<Map.Entry<String, Schema>> it = this.uncommittedSchemas.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Schema> entry = it.next();
                entry.getValue().afterRollback();
                it.remove();
            }
            z_internalReadLock();
            try {
                for (Schema schema : this.schemas.values()) {
                    schema.afterRollback();
                }
            } finally {
                z_internalReadUnLock();
            }
            this.reentrantReadWriteLock.writeLock().unlock();
        }
    }

    public void deallocateAll() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute("DEALLOCATE ALL");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void cacheTopology() {
        GraphTraversalSource traversalSource = this.sqlgGraph.topology();
        //load the last log
        //the last timestamp is needed when just after obtaining the lock the log table is queried again to ensure that the last log is indeed
        //loaded as the notification might not have been received yet.
        List<Vertex> logs = traversalSource.V()
                .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_LOG)
                .order().by(SQLG_SCHEMA + "." + SQLG_SCHEMA_LOG_TIMESTAMP, Order.decr)
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
            if (schemaName.equals(SQLG_SCHEMA)) {
                Preconditions.checkState(schemaOptional.isPresent(), "\"public\" schema must always be present.");
            }
            Schema schema;
            if (!schemaOptional.isPresent()) {
                schema = Schema.loadUserSchema(this, schemaName);
                this.schemas.put(schemaName, schema);
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
            Optional<Schema> schemaOptional = getSchema(schemaName);
            Preconditions.checkState(schemaOptional.isPresent(), "schema %s must be present when loading in edges.", schemaName);
            @SuppressWarnings("OptionalGetWithoutIsPresent")
            Schema schema = schemaOptional.get();
            schema.loadInEdgeLabels(traversalSource, schemaVertex);
        }

        //Load the globalUniqueIndexes.
        List<Vertex> globalUniqueIndexVertices = traversalSource.V().hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX).toList();
        for (Vertex globalUniqueIndexVertex : globalUniqueIndexVertices) {
            String globalUniqueIndexName = globalUniqueIndexVertex.value("name");
            GlobalUniqueIndex globalUniqueIndex = GlobalUniqueIndex.instantiateGlobalUniqueIndex(this, globalUniqueIndexName);
            this.globalUniqueIndexes.add(globalUniqueIndex);

            Set<Vertex> globalUniqueIndexProperties = traversalSource.V(globalUniqueIndexVertex).out(SQLG_SCHEMA_GLOBAL_UNIQUE_INDEX_PROPERTY_EDGE).toSet();
            Set<PropertyColumn> guiPropertyColumns = new HashSet<>();
            for (Vertex globalUniqueIndexPropertyVertex : globalUniqueIndexProperties) {
                //get the path to the vertex
                boolean isForVertex = true;
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
    }

    void validateTopology() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            for (Schema schema : getSchemas()) {
                try (ResultSet schemaRs = metadata.getSchemas(null, schema.getName())) {
                    if (!schemaRs.next()) {
                        this.validationErrors.add(new TopologyValidationError(schema));
                    } else {
                        this.validationErrors.addAll(schema.validateTopology(metadata));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public JsonNode toJson() {
        z_internalReadLock();
        try {
            ObjectNode topologyNode = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
            ArrayNode schemaArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
            for (Schema schema : this.schemas.values()) {
                schemaArrayNode.add(schema.toJson());
            }
            topologyNode.set("schemas", schemaArrayNode);
            return topologyNode;
        } finally {
            z_internalReadUnLock();
        }
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    /**
     * Product the json that goes into the sqlg_schema.V_log table. Other graphs will read it to sync their schema.
     *
     * @return The json.
     */
    private Optional<JsonNode> toNotifyJson() {
        z_internalReadLock();
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
            if (this.isWriteLockHeldByCurrentThread()) {
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
            }
            ArrayNode unCommittedGlobalUniqueIndexesArrayNode = null;
            if (this.isWriteLockHeldByCurrentThread()) {
                for (GlobalUniqueIndex globalUniqueIndex : this.uncommittedGlobalUniqueIndexes) {
                    if (unCommittedGlobalUniqueIndexesArrayNode == null) {
                        unCommittedGlobalUniqueIndexesArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
                    }
                    Optional<JsonNode> jsonNodeOptional = globalUniqueIndex.toNotifyJson();
                    if (jsonNodeOptional.isPresent()) {
                        unCommittedGlobalUniqueIndexesArrayNode.add(jsonNodeOptional.get());
                    }
                }
            }

            if (unCommittedSchemaArrayNode != null) {
                if (topologyNode == null) {
                    topologyNode = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
                }
                topologyNode.set("uncommittedSchemas", unCommittedSchemaArrayNode);
            }
            if (unCommittedGlobalUniqueIndexesArrayNode != null) {
                if (topologyNode == null) {
                    topologyNode = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
                }
                topologyNode.set("uncommittedGlobalUniqueIndexes", unCommittedGlobalUniqueIndexesArrayNode);
            }
            if (topologyNode != null) {
                return Optional.of(topologyNode);
            } else {
                return Optional.empty();
            }
        } finally {
            z_internalReadUnLock();
        }
    }

    public void fromNotifyJson(int pid, LocalDateTime notifyTimestamp) {
        z_internalWriteLock();
        try {
            if (!this.ownPids.contains(pid)) {
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
                this.ownPids.remove(pid);
            }
        } finally {
            this.sqlgGraph.tx().rollback();
        }
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private void fromNotifyJson(LocalDateTime timestamp, ObjectNode log) {
        for (String s : Arrays.asList("uncommittedSchemas", "schemas")) {
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
        ArrayNode globalUniqueIndexes = (ArrayNode) log.get("uncommittedGlobalUniqueIndexes");
        if (globalUniqueIndexes != null) {
            for (JsonNode jsonGlobalUniqueIndex : globalUniqueIndexes) {
                String globalUniqueIndexName = jsonGlobalUniqueIndex.get("name").asText();
                Optional<GlobalUniqueIndex> globalUniqueIndexOptional = getGlobalUniqueIndexes(globalUniqueIndexName);
                Preconditions.checkState(!globalUniqueIndexOptional.isPresent(), "GlobalUniqueIndex may not be present in fromNotifyJson");

                GlobalUniqueIndex globalUniqueIndex = GlobalUniqueIndex.instantiateGlobalUniqueIndex(this, globalUniqueIndexName);

                Set<PropertyColumn> properties = new HashSet<>();
                ArrayNode jsonProperties = (ArrayNode) jsonGlobalUniqueIndex.get("uncommittedProperties");
                for (JsonNode jsonProperty : jsonProperties) {
                    ObjectNode propertyObjectNode = (ObjectNode) jsonProperty;
                    String propertyName = propertyObjectNode.get("name").asText();
                    String schemaName = propertyObjectNode.get("schemaName").asText();
                    Optional<Schema> schemaOptional = getSchema(schemaName);
                    Preconditions.checkState(schemaOptional.isPresent(), "Schema must be present for GlobalUniqueIndexes fromNotifyJson");
                    Schema schema = schemaOptional.get();
                    String abstractLabelName = propertyObjectNode.get("abstractLabelLabel").asText();
                    AbstractLabel abstractLabel;
                    Optional<VertexLabel> vertexLabelOptional = schema.getVertexLabel(abstractLabelName);
                    if (!vertexLabelOptional.isPresent()) {
                        Optional<EdgeLabel> edgeLabelOptional = schema.getEdgeLabel(abstractLabelName);
                        Preconditions.checkState(edgeLabelOptional.isPresent(), "VertexLabel or EdgeLabl must be present for GlobalUniqueIndex fromNotifyJson");
                        abstractLabel = edgeLabelOptional.get();
                    } else {
                        abstractLabel = vertexLabelOptional.get();
                    }
                    Optional<PropertyColumn> propertyColumnOptional = abstractLabel.getProperty(propertyName);
                    Preconditions.checkState(propertyColumnOptional.isPresent(), "PropertyColumn must be present for GlobalUniqueIndex fromNotifyJson");
                    properties.add(propertyColumnOptional.get());
                }
                globalUniqueIndex.addGlobalUniqueProperties(properties);
                this.globalUniqueIndexes.add(globalUniqueIndex);
            }

        }
        this.notificationTimestamps.add(timestamp);
    }

    @Override
    public boolean equals(Object o) {
        z_internalReadLock();
        try {
            if (o == null) {
                return false;
            }
            if (!(o instanceof Topology)) {
                return false;
            }
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
            z_internalReadUnLock();
        }
    }

    /////////////////////////////////getters and cache/////////////////////////////


    public Set<TopologyInf> getSqlgSchemaAbstractLabels() {
        return this.sqlgSchemaAbstractLabels;
    }

    public Set<GlobalUniqueIndex> getGlobalUniqueIndexes() {
        Set<GlobalUniqueIndex> result = new HashSet<>(this.globalUniqueIndexes);
        if (this.isWriteLockHeldByCurrentThread()) {
            result.addAll(this.uncommittedGlobalUniqueIndexes);
        }
        return result;
    }

    public Optional<GlobalUniqueIndex> getGlobalUniqueIndexes(String name) {
        for (GlobalUniqueIndex globalUniqueIndex : globalUniqueIndexes) {
            if (globalUniqueIndex.getName().equals(name)) {
                return Optional.of(globalUniqueIndex);
            }
        }
        for (GlobalUniqueIndex globalUniqueIndex : uncommittedGlobalUniqueIndexes) {
            if (globalUniqueIndex.getName().equals(name)) {
                return Optional.of(globalUniqueIndex);
            }
        }
        return Optional.empty();
    }

    public Set<Schema> getSchemas() {
        this.z_internalReadLock();
        try {
            Set<Schema> result = new HashSet<>();
            result.addAll(this.schemas.values());
            if (this.isWriteLockHeldByCurrentThread()) {
                result.addAll(this.uncommittedSchemas.values());
            }
            return Collections.unmodifiableSet(result);
        } finally {
            this.z_internalReadUnLock();
        }
    }

    public Schema getPublicSchema() {
        Optional<Schema> schema = getSchema(this.sqlgGraph.getSqlDialect().getPublicSchema());
        Preconditions.checkState(schema.isPresent(), "BUG: The public schema must always be present");
        //noinspection OptionalGetWithoutIsPresent
        return schema.get();
    }

    public Schema getGlobalUniqueIndexSchema() {
        Optional<Schema> schema = getSchema(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA);
        Preconditions.checkState(schema.isPresent(), "BUG: The global unque index schema %s must always be present", Schema.GLOBAL_UNIQUE_INDEX_SCHEMA);
        //noinspection OptionalGetWithoutIsPresent
        return schema.get();
    }

    public Optional<Schema> getSchema(String schema) {
        this.z_internalReadLock();
        try {
            Schema result = this.schemas.get(schema);
            if (result == null) {
                if (isWriteLockHeldByCurrentThread()) {
                    result = this.uncommittedSchemas.get(schema);
                }
                if (result == null) {
                    result = this.metaSchemas.get(schema);
                }
            }
            return Optional.ofNullable(result);
        } finally {
            this.z_internalReadUnLock();
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
        Preconditions.checkState(isWriteLockHeldByCurrentThread(), "getUncommittedAllTables must be called with the lock held");
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

    /**
     * get all tables by schema, with their properties
     * does not return schema tables
     *
     * @return
     */
    public Map<String, Map<String, PropertyType>> getAllTables() {
        return getAllTables(false);
    }

    /**
     * get all tables by schema, with their properties
     *
     * @param withSqlgSchema do we want the sqlg_schema tables?
     * @return
     */
    public Map<String, Map<String, PropertyType>> getAllTables(boolean withSqlgSchema) {
        this.z_internalReadLock();
        try {
            //Need to make a copy so as not to corrupt the allTableCache with uncommitted schema elements
            Map<String, Map<String, PropertyType>> result;
            if (this.isWriteLockHeldByCurrentThread()) {
                result = new HashMap<>();
                for (Map.Entry<String, Map<String, PropertyType>> allTableCacheMapEntry : this.allTableCache.entrySet()) {
                    result.put(allTableCacheMapEntry.getKey(), new HashMap<>(allTableCacheMapEntry.getValue()));
                }
            } else {
                result = new HashMap<>(this.allTableCache);
            }
            if (this.isWriteLockHeldByCurrentThread()) {
                Map<String, AbstractLabel> uncommittedLabels = this.getUncommittedAllTables();
                for (String table : uncommittedLabels.keySet()) {
                    if (result.containsKey(table)) {
                        result.get(table).putAll(uncommittedLabels.get(table).getPropertyTypeMap());
                    } else {
                        result.put(table, uncommittedLabels.get(table).getPropertyTypeMap());
                    }
                }
            }
            if (!withSqlgSchema) {
                for (String sqlgSchemaSchemaTable : SQLG_SCHEMA_SCHEMA_TABLES) {
                    result.remove(sqlgSchemaSchemaTable);
                }
            }

            return Collections.unmodifiableMap(result);
        } finally {
            z_internalReadUnLock();
        }
    }

    /**
     * Returns the topology schema elements with the filter schema elements removed.
     *
     * @param filter The objects not to include in the result.
     * @return A map without the filter elements present.
     */
    public Map<String, Map<String, PropertyType>> getAllTablesWithout(Set<TopologyInf> filter) {
        this.z_internalReadLock();
        try {
            Map<String, Map<String, PropertyType>> result = new HashMap<>(getAllTables());
            for (TopologyInf f : filter) {
                if (f instanceof AbstractLabel) {
                    AbstractLabel abstractLabel = (AbstractLabel) f;
                    Schema schema = abstractLabel.getSchema();
                    result.remove(schema.getName() + "." + (abstractLabel instanceof VertexLabel ? SchemaManager.VERTEX_PREFIX : SchemaManager.EDGE_PREFIX) + abstractLabel.getLabel());
                }
            }
            return Collections.unmodifiableMap(result);
        } finally {
            z_internalReadUnLock();
        }
    }

    public Map<String, Map<String, PropertyType>> getAllTablesFrom(Set<TopologyInf> selectFrom) {
        z_internalReadLock();
        try {
            Map<String, Map<String, PropertyType>> result = new HashMap<>();
            for (TopologyInf f : selectFrom) {
                if (f instanceof AbstractLabel) {
                    AbstractLabel abstractLabel = (AbstractLabel) f;
                    Schema schema = abstractLabel.getSchema();
                    String key = schema.getName() + "." + (abstractLabel instanceof VertexLabel ? SchemaManager.VERTEX_PREFIX : SchemaManager.EDGE_PREFIX) + abstractLabel.getLabel();
                    Map<String, PropertyType> tmp = this.allTableCache.get(key);
                    result.put(key, tmp);
                } else if (f instanceof GlobalUniqueIndex) {
                    GlobalUniqueIndex globalUniqueIndex = (GlobalUniqueIndex) f;
                    String key = Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "." + SchemaManager.VERTEX_PREFIX + globalUniqueIndex.getName();
                    Map<String, PropertyType> tmp = this.allTableCache.get(key);
                    result.put(key, tmp);
                }
            }
            return Collections.unmodifiableMap(result);
        } finally {
            z_internalReadUnLock();
        }
    }

    public Map<String, PropertyColumn> getPropertiesFor(SchemaTable schemaTable) {
        z_internalReadLock();
        try {
            Optional<Schema> schemaOptional = getSchema(schemaTable.getSchema());
            if (schemaOptional.isPresent()) {
                return Collections.unmodifiableMap(schemaOptional.get().getPropertiesFor(schemaTable));
            }
            return Collections.emptyMap();
        } finally {
            z_internalReadUnLock();
        }
    }

    public Map<String, PropertyColumn> getPropertiesWithGlobalUniqueIndexFor(SchemaTable schemaTable) {
        z_internalReadLock();
        try {
            Optional<Schema> schemaOptional = getSchema(schemaTable.getSchema());
            if (schemaOptional.isPresent()) {
                return Collections.unmodifiableMap(schemaOptional.get().getPropertiesWithGlobalUniqueIndexFor(schemaTable));
            }
            return Collections.emptyMap();
        } finally {
            z_internalReadUnLock();
        }
    }

    public Map<String, PropertyType> getTableFor(SchemaTable schemaTable) {
        z_internalReadLock();
        try {
            Optional<Schema> schemaOptional = getSchema(schemaTable.getSchema());
            if (schemaOptional.isPresent()) {
                return schemaOptional.get().getTableFor(schemaTable);
            }
            if (isWriteLockHeldByCurrentThread()) {
                Map<String, PropertyType> temporaryPropertyMap = this.temporaryTables.get(schemaTable.getTable());
                if (temporaryPropertyMap != null) {
                    return Collections.unmodifiableMap(temporaryPropertyMap);
                }
            }
            return Collections.emptyMap();
        } finally {
            z_internalReadUnLock();
        }
    }

    public Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getTableLabels() {
        z_internalReadLock();
        try {
            Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> map = new HashMap<>();
            for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
                Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = schemaEntry.getValue().getTableLabels();
                map.putAll(result);
            }
            if (this.isWriteLockHeldByCurrentThread()) {
                for (Map.Entry<String, Schema> schemaEntry : this.uncommittedSchemas.entrySet()) {
                    Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = schemaEntry.getValue().getTableLabels();
                    map.putAll(result);
                }
            }
            for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {
                Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = schemaEntry.getValue().getTableLabels();
                map.putAll(result);
            }
            return map;
        } finally {
            z_internalReadUnLock();
        }
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

    public Set<String> getEdgeForeignKeys(String schemaTable) {
        return getAllEdgeForeignKeys().get(schemaTable);
    }

    public Map<String, Set<String>> getAllEdgeForeignKeys() {
        z_internalReadLock();
        try {
            Map<String, Set<String>> result = new HashMap<>();
            for (Schema schema : this.schemas.values()) {
                result.putAll(schema.getAllEdgeForeignKeys());
            }
            for (Schema schema : this.uncommittedSchemas.values()) {
                result.putAll(schema.getAllEdgeForeignKeys());
            }
            for (Schema schema : this.metaSchemas.values()) {
                result.putAll(schema.getAllEdgeForeignKeys());
            }
            return result;
        } finally {
            z_internalReadUnLock();
        }
    }

    void addToAllTables(String tableName, Map<String, PropertyType> propertyTypeMap) {
        this.allTableCache.put(tableName, propertyTypeMap);
    }

    void addToUncommittedGlobalUniqueIndexes(GlobalUniqueIndex globalUniqueIndex) {
        this.uncommittedGlobalUniqueIndexes.add(globalUniqueIndex);
    }

    public void registerListener(TopologyListener topologyListener) {
        this.topologyListeners.add(topologyListener);
    }

    void fire(TopologyInf topologyInf, String oldValue, TopologyChangeAction action) {
        for (TopologyListener topologyListener : this.topologyListeners) {
            topologyListener.change(topologyInf, oldValue, action);
        }
    }

    static class TopologyValidationError {
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
