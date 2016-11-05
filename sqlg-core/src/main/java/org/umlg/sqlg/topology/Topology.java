package org.umlg.sqlg.topology;

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
import org.umlg.sqlg.sql.dialect.SqlSchemaChangeDialect;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaManager;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.umlg.sqlg.structure.SchemaManager.*;

/**
 * Date: 2016/09/04
 * Time: 8:49 AM
 */
public class Topology {

    private Logger logger = LoggerFactory.getLogger(Topology.class.getName());
    private SqlgGraph sqlgGraph;
    private boolean distributed;
    private ReentrantLock schemaLock;

    //Map the topology. This is for regular schemas. i.e. 'public.Person', 'special.Car'
    //The map needs to be concurrent as elements can be added in one thread and merged via notify from another at the same time.
    private Map<String, Schema> schemas = new ConcurrentHashMap<>();
    private Map<String, Schema> uncommittedSchemas = new HashMap<>();

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final String SQLG_NOTIFICATION_CHANNEL = "SQLG_NOTIFY";
    //meta schema
    private Map<String, Schema> metaSchemas = new HashMap<>();

    //temporary tables
    private Map<String, Map<String, PropertyType>> temporaryTables = new ConcurrentHashMap<>();

    //ownPids are the pids to ignore as it is what the graph sent a notification for.
    private Set<Integer> ownPids = new HashSet<>();

    //every notification will have a unique timestamp.
    //This is so because modification happen one at a time via the lock.
    private SortedSet<LocalDateTime> notificationTimestamps = new TreeSet<>();

    private static final int LOCK_TIMEOUT = 100;

    /**
     * Topology is a singleton created when the {@link SqlgGraph} is opened.
     * As the topology, i.e. sqlg_schema is created upfront the meta topology is pre-loaded.
     *
     * @param sqlgGraph The graph.
     */
    public Topology(SqlgGraph sqlgGraph) {
        this.sqlgGraph = sqlgGraph;
        this.distributed = sqlgGraph.configuration().getBoolean(SqlgGraph.DISTRIBUTED, false);
        this.schemaLock = new ReentrantLock();

        //Pre-create the meta topology.
        Schema sqlgSchema = Schema.instantiateSqlgSchema(this);
        this.metaSchemas.put(SQLG_SCHEMA, sqlgSchema);

        Map<String, PropertyType> columns = new HashedMap<>();
        columns.put(SQLG_SCHEMA_PROPERTY_NAME, PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        VertexLabel schemaVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_SCHEMA, columns);
        VertexLabel edgeVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_EDGE_LABEL, columns);

        columns.put(SQLG_SCHEMA_PROPERTY_TYPE, PropertyType.STRING);
        columns.put(SQLG_SCHEMA_PROPERTY_INDEX_TYPE, PropertyType.STRING);
        VertexLabel propertyVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_PROPERTY, columns);
        columns.remove(SQLG_SCHEMA_PROPERTY_TYPE);
        columns.remove(SQLG_SCHEMA_PROPERTY_INDEX_TYPE);

        columns.put(SCHEMA_VERTEX_DISPLAY, PropertyType.STRING);
        VertexLabel vertexVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_VERTEX_LABEL, columns);

        columns.remove(SCHEMA_VERTEX_DISPLAY);

        @SuppressWarnings("unused")
        EdgeLabel schemaVertexEdgeLabel = schemaVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE, vertexVertexLabel, columns);
        @SuppressWarnings("unused")
        EdgeLabel schemaVertexInEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_IN_EDGES_EDGE, edgeVertexLabel, columns);
        @SuppressWarnings("unused")
        EdgeLabel schemaVertexOutEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_OUT_EDGES_EDGE, edgeVertexLabel, columns);
        @SuppressWarnings("unused")
        EdgeLabel schemaVertexPropertyEdgeLabel = vertexVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE, propertyVertexLabel, columns);
        @SuppressWarnings("unused")
        EdgeLabel schemaEdgePropertyEdgeLabel = edgeVertexLabel.loadSqlgSchemaEdgeLabel(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, propertyVertexLabel, columns);

        columns.clear();
        columns.put(SQLG_SCHEMA_LOG_TIMESTAMP, PropertyType.LOCALDATETIME);
        columns.put(SQLG_SCHEMA_LOG_LOG, PropertyType.JSON);
        columns.put(SQLG_SCHEMA_LOG_PID, PropertyType.INTEGER);
        @SuppressWarnings("unused")
        VertexLabel logVertexLabel = sqlgSchema.createSqlgSchemaVertexLabel(SQLG_SCHEMA_LOG, columns);

        //add the public schema
        this.schemas.put(sqlgGraph.getSqlDialect().getPublicSchema(), Schema.createPublicSchema(this, sqlgGraph.getSqlDialect().getPublicSchema()));
    }

    /**
     * Global lock on the topology.
     * For distributed graph (multiple jvm) this happens on the db via a lock sql statement.
     */
    void lock() {
        //only lock if the lock is not already owned by this thread.
        if (!isLockHeldByCurrentThread()) {
            try {
                this.sqlgGraph.tx().readWrite();
                if (!this.schemaLock.tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
                    throw new RuntimeException("timeout lapsed to acquire lock schema creation.");
                }
                if (this.distributed) {
                    ((SqlSchemaChangeDialect) this.sqlgGraph.getSqlDialect()).lock(this.sqlgGraph);
                }
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
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * @return true if the current thread owns the lock.
     */
    boolean isLockHeldByCurrentThread() {
        return this.schemaLock.isHeldByCurrentThread();
    }

    /**
     * Ensures that the schema exists.
     *
     * @param schemaName The schem to create if it does not exist.
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
                return schema;
            } else {
                return schemaOptional.get();
            }
        } else {
            return schemaOptional.get();
        }
    }

    /**
     * Ensures that the vertex table and property columns exist in the db. The default schema is assumed.
     * If any element does not exist the a lock is first obtained. After the lock is obtained the maps are rechecked to
     * see if the element has not been added in the mean time.
     *
     * @param label   The vertex's label. Translates to a table prepended with 'V_'  and the table's name being the label.
     * @param columns The properties with their types.
     * @see {@link PropertyType}
     */
    public VertexLabel ensureVertexTableExist(final String label, final Map<String, PropertyType> columns) {
        return ensureVertexTableExist(this.sqlgGraph.getSqlDialect().getPublicSchema(), label, columns);
    }

    /**
     * Ensures that the schema, vertex table and property columns exist in the db.
     * If any element does not exist the a lock is first obtained. After the lock is obtained the maps are rechecked to
     * see if the element has not been added in the mean time.
     *
     * @param schemaName The schema the vertex is in.
     * @param label      The vertex's label. Translates to a table prepended with 'V_'  and the table's name being the label.
     * @param columns    The properties with their types.
     * @see {@link PropertyType}
     */
    public VertexLabel ensureVertexTableExist(final String schemaName, final String label, final Map<String, PropertyType> columns) {
        Objects.requireNonNull(schemaName, GIVEN_TABLES_MUST_NOT_BE_NULL);
        Objects.requireNonNull(label, GIVEN_TABLE_MUST_NOT_BE_NULL);
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), String.format("label may not be prefixed with %s", VERTEX_PREFIX));

        Schema schema = this.ensureSchemaExist(schemaName);
        Preconditions.checkState(schema != null, "Schema must be present after calling ensureSchemaExist");
        return schema.ensureVertexTableExist(this.sqlgGraph, label, columns);
    }

    /**
     * Ensures that the edge table with out and in foreign keys and property columns exists.
     * The edge table will reside in the out vertex's schema.
     * If a table, a foreign key or a column needs to be created a lock is first obtained.
     *
     * @param edgeLabelName The label for the edge.
     * @param foreignKeyOut The {@link SchemaTable} that represents the out vertex.
     * @param foreignKeyIn  The {@link SchemaTable} that represents the in vertex.
     * @param columns       The edge's properties with their type.
     * @return The {@link SchemaTable} that represents the edge.
     */
    public SchemaTable ensureEdgeTableExist(final String edgeLabelName, final SchemaTable foreignKeyOut, final SchemaTable foreignKeyIn, Map<String, PropertyType> columns) {
        Objects.requireNonNull(edgeLabelName, "Given edgeLabelName must not be null");
        Objects.requireNonNull(foreignKeyOut, "Given outTable must not be null");
        Objects.requireNonNull(foreignKeyIn, "Given inTable must not be null");

        Preconditions.checkState(getVertexLabel(foreignKeyOut.getSchema(), foreignKeyOut.getTable()).isPresent(), "The out vertex must already exist before invoking 'ensureEdgeTableExist'. \"%s\" does not exist", foreignKeyIn.toString());
        Preconditions.checkState(getVertexLabel(foreignKeyIn.getSchema(), foreignKeyIn.getTable()).isPresent(), "The in vertex must already exist before invoking 'ensureEdgeTableExist'. \"%s\" does not exist", foreignKeyIn.toString());

        //outVertexSchema will be there as the Precondition checked it.
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Schema outVertexSchema = this.getSchema(foreignKeyOut.getSchema()).get();
        return outVertexSchema.ensureEdgeTableExist(this.sqlgGraph, edgeLabelName, foreignKeyOut, foreignKeyIn, columns);
    }

    /**
     * Ensures that the vertex's table has the required columns.
     * If a columns needs to be created a lock will be obtained.
     * The vertex's schema and table must already exists.
     *
     * @param schemaName The schema the vertex resides in.
     * @param label      The vertex's label.
     * @param columns    The properties to create if they do not exist.
     */
    public void ensureVertexColumnsExist(String schemaName, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), "label may not start with \"%s\"", VERTEX_PREFIX);
        if (!schemaName.equals(SQLG_SCHEMA)) {
            Schema schema = this.schemas.get(schemaName);
            if (schema == null) {
                schema = this.uncommittedSchemas.get(schemaName);
            }
            if (schema == null) {
                throw new IllegalStateException(String.format("BUG: schema \"%s\" can not be null", schemaName));
            }
            //createVertexLabel the table
            schema.ensureVertexColumnsExist(this.sqlgGraph, label, columns);
        }
    }

    /**
     * Ensures that the edge's table has the required columns.
     * If a columns needs to be created a lock will be obtained.
     * The edge's schema and table must already exists.
     *
     * @param schemaName The  schema the edge resides in.
     * @param label      The edge's label.
     * @param columns    The properties to create if they do not exist.
     */
    public void ensureEdgeColumnsExist(String schemaName, String label, Map<String, PropertyType> columns) {
        Preconditions.checkArgument(!label.startsWith(EDGE_PREFIX), "label may not start with \"%s\"", EDGE_PREFIX);
        Preconditions.checkState(!schemaName.equals(SQLG_SCHEMA), "Topology.ensureEdgeColumnsExist may not be called for \"%s\"", SQLG_SCHEMA);

        if (!schemaName.equals(SQLG_SCHEMA)) {
            Schema schema = this.schemas.get(schemaName);
            if (schema == null) {
                schema = this.uncommittedSchemas.get(schemaName);
            }
            if (schema == null) {
                throw new IllegalStateException(String.format("BUG: schema %s can not be null", schemaName));
            }
            //createVertexLabel the table
            schema.ensureEdgeColumnsExist(this.sqlgGraph, label, columns);
        }
    }

    public void ensureVertexTemporaryTableExist(final String schema, final String table, final Map<String, PropertyType> columns) {
        Objects.requireNonNull(schema, GIVEN_TABLES_MUST_NOT_BE_NULL);
        Objects.requireNonNull(table, GIVEN_TABLE_MUST_NOT_BE_NULL);
        final String prefixedTable = VERTEX_PREFIX + table;
        if (!this.temporaryTables.containsKey(prefixedTable)) {
            this.temporaryTables.put(prefixedTable, columns);
            createTempTable(prefixedTable, columns);
        }
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
        AbstractElement.buildColumns(this.sqlgGraph, columns, sql);
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

    public boolean existSchema(String schema) {
        return getSchema(schema).isPresent();
    }

    public Set<Schema> getSchemas() {
        Set<Schema> result = new HashSet<>();
        result.addAll(this.schemas.values());
        if (this.isLockHeldByCurrentThread()) {
            result.addAll(this.uncommittedSchemas.values());
        }
        return Collections.unmodifiableSet(result);
    }

    public Optional<Schema> getSchema(String schema) {
        Schema result = this.schemas.get(schema);
        if (result == null) {
            result = this.uncommittedSchemas.get(schema);
            if (result == null) {
                result = this.metaSchemas.get(schema);
            }
        }
        return Optional.ofNullable(result);
    }

    public boolean existVertexLabel(String schemaName, String label) {
        return getVertexLabel(schemaName, label).isPresent();
    }

    public Optional<VertexLabel> getVertexLabel(String label) {
        return getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), label);
    }

    public Optional<VertexLabel> getVertexLabel(String schemaName, String label) {
        Preconditions.checkArgument(!label.startsWith(VERTEX_PREFIX), String.format("vertex label may not start with %s", VERTEX_PREFIX));
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

    public void beforeCommit() {
        Optional<JsonNode> jsonNodeOptional = this.toNotifyJson();
        if (jsonNodeOptional.isPresent() && this.distributed) {
            SqlSchemaChangeDialect sqlSchemaChangeDialect = (SqlSchemaChangeDialect) this.sqlgGraph.getSqlDialect();
            LocalDateTime timestamp = LocalDateTime.now();
            int pid = sqlSchemaChangeDialect.notifyChange(sqlgGraph, timestamp, jsonNodeOptional.get());
            this.ownPids.add(pid);
        }
    }

    public void afterCommit() {
        this.temporaryTables.clear();
        if (this.isLockHeldByCurrentThread()) {
            for (Iterator<Map.Entry<String, Schema>> it = this.uncommittedSchemas.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Schema> entry = it.next();
                this.schemas.put(entry.getKey(), entry.getValue());
                it.remove();
            }
        }
        for (Schema schema : this.schemas.values()) {
            schema.afterCommit();
        }
        if (isLockHeldByCurrentThread()) {
//            logger.info(String.format("Unlock %s", this.sqlgGraph.toString()));
            this.schemaLock.unlock();
        }
    }

    public void afterRollback() {
        this.temporaryTables.clear();
        if (this.isLockHeldByCurrentThread()) {
            for (Iterator<Map.Entry<String, Schema>> it = this.uncommittedSchemas.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Schema> entry = it.next();
                entry.getValue().afterRollback();
                it.remove();
            }
        }
        for (Schema schema : this.schemas.values()) {
            schema.afterRollback();
        }
        if (isLockHeldByCurrentThread()) {
            this.schemaLock.unlock();
        }
    }

    public Map<String, Map<String, PropertyType>> getAllTablesWithout(List<String> filter) {
        Map<String, Map<String, PropertyType>> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            result.putAll(schemaEntry.getValue().getAllTablesWithout(filter));
        }

        result.putAll(this.temporaryTables);

        if (!this.uncommittedSchemas.isEmpty() && isLockHeldByCurrentThread()) {
            for (Map.Entry<String, Schema> schemaEntry : this.uncommittedSchemas.entrySet()) {
                result.putAll(schemaEntry.getValue().getAllTablesWithout(filter));
            }
        }
        //And the meta schema tables
        for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {
            result.putAll(schemaEntry.getValue().getAllTablesWithout(filter));
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<String, Map<String, PropertyType>> getAllTables() {
        Map<String, Map<String, PropertyType>> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            result.putAll(schemaEntry.getValue().getAllTables());
        }

        result.putAll(this.temporaryTables);

        if (!this.uncommittedSchemas.isEmpty() && isLockHeldByCurrentThread()) {
            for (Map.Entry<String, Schema> schemaEntry : this.uncommittedSchemas.entrySet()) {
                result.putAll(schemaEntry.getValue().getAllTables());
            }
        }
        for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {
            result.putAll(schemaEntry.getValue().getAllTables());
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<String, Map<String, PropertyType>> getAllTablesFrom(List<String> selectFrom) {
        Map<String, Map<String, PropertyType>> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            result.putAll(schemaEntry.getValue().getAllTablesFrom(selectFrom));
        }

        result.putAll(this.temporaryTables);

        if (!this.uncommittedSchemas.isEmpty() && isLockHeldByCurrentThread()) {
            for (Map.Entry<String, Schema> schemaEntry : this.uncommittedSchemas.entrySet()) {
                result.putAll(schemaEntry.getValue().getAllTablesFrom(selectFrom));
            }
        }
        for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {
            result.putAll(schemaEntry.getValue().getAllTablesFrom(selectFrom));
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<String, PropertyType> getTableFor(SchemaTable schemaTable) {
        Map<String, PropertyType> result = new HashMap<>();
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            if (schemaEntry.getKey().equals(schemaTable.getSchema())) {
                result.putAll(schemaEntry.getValue().getTableFor(schemaTable));
            }
        }

        for (Map<String, PropertyType> stringPropertyTypeMap : this.temporaryTables.values()) {
            result.putAll(stringPropertyTypeMap);
        }

        if (!this.uncommittedSchemas.isEmpty() && isLockHeldByCurrentThread()) {
            for (Map.Entry<String, Schema> schemaEntry : this.uncommittedSchemas.entrySet()) {
                if (schemaEntry.getKey().equals(schemaTable.getSchema())) {
                    result.putAll(schemaEntry.getValue().getTableFor(schemaTable));
                }
            }
        }
        for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {
            if (schemaEntry.getKey().equals(schemaTable.getSchema())) {
                result.putAll(schemaEntry.getValue().getTableFor(schemaTable));
            }
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getTableLabels() {
        Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> map = new HashMap<>();
        for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
            Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> result = schemaEntry.getValue().getTableLabels();
            map.putAll(result);
        }
        for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {
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
        Set<SchemaTable> inSchemaTables = new HashSet<>();
        Set<SchemaTable> outSchemaTables = new HashSet<>();
        if (!schemaTable.getSchema().equals(SQLG_SCHEMA)) {
            for (Map.Entry<String, Schema> schemaEntry : this.schemas.entrySet()) {
                if (schemaEntry.getKey().equals(schemaTable.getSchema())) {
                    Optional<Pair<Set<SchemaTable>, Set<SchemaTable>>> result = schemaEntry.getValue().getTableLabels(schemaTable);
                    if (result.isPresent()) {
                        inSchemaTables.addAll(result.get().getLeft());
                        outSchemaTables.addAll(result.get().getRight());
                        break;
                    }
                }
            }
            for (Map.Entry<String, Schema> schemaEntry : this.uncommittedSchemas.entrySet()) {
                if (schemaEntry.getKey().equals(schemaTable.getSchema())) {
                    Optional<Pair<Set<SchemaTable>, Set<SchemaTable>>> result = schemaEntry.getValue().getTableLabels(schemaTable);
                    if (result.isPresent()) {
                        inSchemaTables.addAll(result.get().getLeft());
                        outSchemaTables.addAll(result.get().getRight());
                        break;
                    }
                }
            }
        } else {
            for (Map.Entry<String, Schema> schemaEntry : this.metaSchemas.entrySet()) {
                if (schemaEntry.getKey().equals(schemaTable.getSchema())) {
                    Optional<Pair<Set<SchemaTable>, Set<SchemaTable>>> result = schemaEntry.getValue().getTableLabels(schemaTable);
                    if (result.isPresent()) {
                        inSchemaTables.addAll(result.get().getLeft());
                        outSchemaTables.addAll(result.get().getRight());
                        break;
                    }
                }
            }
        }
        return Pair.of(
                inSchemaTables,
                outSchemaTables);
    }

    public Map<String, Set<String>> getAllEdgeForeignKeys() {
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
    }

    public void loadUserSchema() {
        GraphTraversalSource traversalSource = this.sqlgGraph.topology();
        //load the last log
        //the last timestamp is needed when just after obtaining the lock the log table is queried again to ensure that the last log is indeed
        //loaded as the notification might not have been received yet.
        List<Vertex> logs = traversalSource.V()
                .hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_LOG)
                .order().by(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_LOG_TIMESTAMP, Order.decr)
                .limit(1)
                .toList();
        Preconditions.checkState(logs.size() <= 1, "must load one or zero logs in loadUserSchema");

        if (!logs.isEmpty()) {
            Vertex log = logs.get(0);
            LocalDateTime timestamp = log.value("timestamp");
            this.notificationTimestamps.add(timestamp);
        } else {
            this.notificationTimestamps.add(LocalDateTime.now());
        }

        //First load all VertexLabels, their out edges and properties
        List<Vertex> schemaVertices = traversalSource.V().hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA).toList();
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
        }
        //Now load the in edges
        schemaVertices = traversalSource.V().hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA).toList();
        for (Vertex schemaVertex : schemaVertices) {
            String schemaName = schemaVertex.value("name");
            Optional<Schema> schemaOptional = getSchema(schemaName);
            Schema schema = schemaOptional.get();
            schema.loadInEdgeLabels(traversalSource, schemaVertex);
        }

    }

    public JsonNode toJson() {
        ObjectNode topologyNode = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
        ArrayNode schemaArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
        for (Schema schema : this.schemas.values()) {
            schemaArrayNode.add(schema.toJson());
        }
        topologyNode.set("schemas", schemaArrayNode);
        return topologyNode;
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    private Optional<JsonNode> toNotifyJson() {
        ArrayNode schemaArrayNode = null;
        for (Schema schema : this.schemas.values()) {
            Optional<JsonNode> jsonNodeOptional = schema.toNotifyJson();
            if (jsonNodeOptional.isPresent() && schemaArrayNode == null) {
                schemaArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
            }
            if (jsonNodeOptional.isPresent()) {
                schemaArrayNode.add(jsonNodeOptional.get());
            }
        }
        if (this.isLockHeldByCurrentThread()) {
            for (Schema schema : this.uncommittedSchemas.values()) {
                schemaArrayNode = new ArrayNode(OBJECT_MAPPER.getNodeFactory());
                Optional<JsonNode> jsonNodeOptional = schema.toNotifyJson();
                if (jsonNodeOptional.isPresent()) {
                    schemaArrayNode.add(jsonNodeOptional.get());
                } else {
                    ObjectNode schemaNode = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
                    schemaNode.put("name", schema.getName());
                    schemaArrayNode.add(schemaNode);
                }
            }
        }
        if (schemaArrayNode != null) {
            ObjectNode topologyNode = new ObjectNode(OBJECT_MAPPER.getNodeFactory());
            topologyNode.set("schemas", schemaArrayNode);
            return Optional.of(topologyNode);
        } else {
            return Optional.empty();
        }
    }

    public void fromNotifyJson(int pid, LocalDateTime notifyTimestamp) {
        if (!this.ownPids.contains(pid)) {
            List<Vertex> logs = this.sqlgGraph.topology().V()
                    .hasLabel(SQLG_SCHEMA + "." + SQLG_SCHEMA_LOG)
                    .has(SQLG_SCHEMA_LOG_TIMESTAMP, notifyTimestamp)
                    .toList();
            Preconditions.checkState(logs.size() == 1, "There must be one and only be one log");
            LocalDateTime timestamp = logs.get(0).value("timestamp");
            Preconditions.checkState(timestamp.equals(notifyTimestamp), "notify log's timestamp does not match.");
            int backEndPid = logs.get(0).value("pid");
            Preconditions.checkState(backEndPid == pid, "notify pids do not match.");
            ObjectNode log = logs.get(0).value("log");
            fromNotifyJson(timestamp, log);
        } else {
            this.ownPids.remove(pid);
        }
    }

    private void fromNotifyJson(LocalDateTime timestamp, ObjectNode log) {
        ArrayNode schemas = (ArrayNode) log.get("schemas");
        for (JsonNode jsonSchema : schemas) {
            String schemaName = jsonSchema.get("name").asText();
            Optional<Schema> schemaOptional = getSchema(schemaName);
            Schema schema;
            if (schemaOptional.isPresent()) {
                schema = schemaOptional.get();
            } else {
                //add to map
                schema = Schema.instantiateSchema(this, schemaName);
                this.schemas.put(schemaName, schema);
            }
            schema.fromNotifyJson(jsonSchema);
        }
        this.notificationTimestamps.add(timestamp);
    }
}
