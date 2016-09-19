package org.umlg.sqlg.structure;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.map.listener.*;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.strategy.TopologyStrategy;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Date: 2014/07/12
 * Time: 11:02 AM
 */
public class SchemaManager {

    public static final String JDBC_URL = "jdbc.url";
    public static final String GIVEN_TABLES_MUST_NOT_BE_NULL = "Given tables must not be null";
    public static final String GIVEN_TABLE_MUST_NOT_BE_NULL = "Given table must not be null";
    public static final String CREATED_ON = "createdOn";
    public static final String SHOULD_NOT_HAPPEN = "Should not happen!";
    private Logger logger = LoggerFactory.getLogger(SchemaManager.class.getName());

    /**
     * Rdbms schema that holds sqlg topology.
     */
    public static final String SQLG_SCHEMA = "sqlg_schema";
    /**
     * Table storing the graph's schemas.
     */
    public static final String SQLG_SCHEMA_SCHEMA = "schema";
    /**
     * Table storing the graphs vertex labels.
     */
    public static final String SQLG_SCHEMA_VERTEX_LABEL = "vertex";
    /**
     * Table storing the graphs edge labels.
     */
    public static final String SQLG_SCHEMA_EDGE_LABEL = "edge";
    /**
     * Table storing the graphs element properties.
     */
    public static final String SQLG_SCHEMA_PROPERTY = "property";
    /**
     * Edge table for the schema to vertex edge.
     */
    public static final String SQLG_SCHEMA_SCHEMA_VERTEX_EDGE = "schema_vertex";
    /**
     * Edge table for the vertices in edges.
     */
    public static final String SQLG_SCHEMA_IN_EDGES_EDGE = "in_edges";
    /**
     * Edge table for the vertices out edges.
     */
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
     * Unique constraint table.
     */
    public static final String SQLG_SCHEMA_UNIQUE_CONSTRAINT = "unique_constraint";

    /**
     * Edge table for unique constraints in a schema.
     */
    public static final String SQLG_SCHEMA_SCHEMA_UNIQUE_CONSTRAINT_EDGE = "schema_unique_constraint";

    /**
     * Edge table for assigning properties to unique constraints.
     */
    public static final String SQLG_SCHEMA_PROPERTY_UNIQUE_CONSTRAINT_EDGE = "property_unique_constraint";

    /**
     * Edge table for assigning vertex properties to unique constraints.
     */
    public static final String SQLG_SCHEMA_VERTEX_UNIQUE_CONSTRAINT_EDGE = "vertex_unique_constraint";

    /**
     * Edge table for assigning edge properties to unique constraints.
     */
    public static final String SQLG_SCHEMA_EDGE_UNIQUE_CONSTRAINT_EDGE = "edge_unique_constraint";

    public static final String VERTEX_PREFIX = "V_";
    public static final String EDGE_PREFIX = "E_";
    public static final String UNIQUE_CONSTRAINT_PREFIX = "UC_";
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

    public static final List<String> SQLG_SCHEMA_SCHEMA_TABLES = Arrays.asList(
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_SCHEMA,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_VERTEX_LABEL,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_EDGE_LABEL,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_PROPERTY,
            SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_UNIQUE_CONSTRAINT,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_VERTEX_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_IN_EDGES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_OUT_EDGES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_EDGE_PROPERTIES_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_UNIQUE_CONSTRAINT_EDGE,
            SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_UNIQUE_CONSTRAINT_EDGE
    );

    private Map<String, String> schemas;
    private Map<String, String> localSchemas = new HashMap<>();
    private Set<String> uncommittedSchemas = new HashSet<>();

    private Map<String, Set<String>> labelSchemas;
    private Map<String, Set<String>> localLabelSchemas = new HashMap<>();
    private Map<String, Set<String>> uncommittedLabelSchemas = new ConcurrentHashMap<>();

    private Map<String, Map<String, PropertyType>> tables;
    private Map<String, Map<String, PropertyType>> localTables = new ConcurrentHashMap<>();
    private Map<String, Map<String, PropertyType>> uncommittedTables = new ConcurrentHashMap<>();

    private Map<String, Set<String>> edgeForeignKeys;
    private Map<String, Set<String>> localEdgeForeignKeys = new ConcurrentHashMap<>();
    private Map<String, Set<String>> uncommittedEdgeForeignKeys = new ConcurrentHashMap<>();

    //tableLabels is a map of every vertex's schemaTable together with its in and out edge schemaTables
    private Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> tableLabels;
    private Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> localTableLabels = new HashMap<>();
    private Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> uncommittedTableLabels = new HashMap<>();

    //keys are property names, values are the names of the tables that are used to check the uniqueness of the property
    //values keyed by the label of the vertices or edges (the of the inner map is VERTEX/EDGE_PREFIX + label)
    //special value - null - for the label means that the property is unique on any label
    private Map<String, Map<String, String>> propertyUniqueConstraints;
    private Map<String, Map<String, String>> localPropertyUniqueConstraints = new HashMap<>();
    private Map<String, Map<String, String>> uncommittedPropertyUniqueConstraints = new HashMap<>();

    //temporary tables
    private Map<String, Map<String, PropertyType>> localTemporaryTables = new ConcurrentHashMap<>();

    private Lock schemaLock;
    private SqlgGraph sqlgGraph;
    private SqlDialect sqlDialect;
    private HazelcastInstance hazelcastInstance;
    private boolean distributed;

    private static final String SCHEMAS_HAZELCAST_MAP = "_schemas";
    private static final String LABEL_SCHEMAS_HAZELCAST_MAP = "_labelSchemas";
    private static final String TABLES_HAZELCAST_MAP = "_tables";
    private static final String EDGE_FOREIGN_KEYS_HAZELCAST_MAP = "_edgeForeignKeys";
    private static final String TABLE_LABELS_HAZELCAST_MAP = "_tableLabels";
    private static final String PROPERTY_UNIQUE_CONSTRAINT_HAZELCAST_MAP = "propertyUniqueConstraints";

    private static final int LOCK_TIMEOUT = 10;

    SchemaManager(SqlgGraph sqlgGraph, SqlDialect sqlDialect, Configuration configuration) {
        this.sqlgGraph = sqlgGraph;
        this.sqlDialect = sqlDialect;
        this.distributed = configuration.getBoolean("distributed", false);

        if (this.distributed) {
            this.hazelcastInstance = Hazelcast.newHazelcastInstance(configHazelcast(configuration));
            this.schemas = this.hazelcastInstance.getMap(this.sqlgGraph.getConfiguration().getString(JDBC_URL) + SCHEMAS_HAZELCAST_MAP);
            this.labelSchemas = this.hazelcastInstance.getMap(this.sqlgGraph.getConfiguration().getString(JDBC_URL) + LABEL_SCHEMAS_HAZELCAST_MAP);
            this.tables = this.hazelcastInstance.getMap(this.sqlgGraph.getConfiguration().getString(JDBC_URL) + TABLES_HAZELCAST_MAP);
            this.edgeForeignKeys = this.hazelcastInstance.getMap(this.sqlgGraph.getConfiguration().getString(JDBC_URL) + EDGE_FOREIGN_KEYS_HAZELCAST_MAP);
            this.tableLabels = this.hazelcastInstance.getMap(this.sqlgGraph.getConfiguration().getString(JDBC_URL) + TABLE_LABELS_HAZELCAST_MAP);
            this.propertyUniqueConstraints = this.hazelcastInstance.getMap(this.sqlgGraph.getConfiguration().getString(JDBC_URL) + PROPERTY_UNIQUE_CONSTRAINT_HAZELCAST_MAP);
            ((IMap) this.schemas).addEntryListener(new SchemasMapEntryListener(), true);
            ((IMap) this.labelSchemas).addEntryListener(new LabelSchemasMapEntryListener(), true);
            ((IMap) this.tables).addEntryListener(new TablesMapEntryListener(), true);
            ((IMap) this.edgeForeignKeys).addEntryListener(new EdgeForeignKeysMapEntryListener(), true);
            ((IMap) this.tableLabels).addEntryListener(new TableLabelMapEntryListener(), true);
            ((IMap) this.propertyUniqueConstraints).addEntryListener(new PropertyUniqueConstraintsMapEntryListener(), true);
            this.schemaLock = this.hazelcastInstance.getLock("schemaLock");
        } else {
            this.schemaLock = new ReentrantLock();
        }

        this.sqlgGraph.tx().afterCommit(() -> {
            this.localTemporaryTables.clear();
            if (this.isLockedByCurrentThread()) {
                for (String schema : this.uncommittedSchemas) {
                    if (distributed) {
                        this.schemas.put(schema, schema);
                    }
                    this.localSchemas.put(schema, schema);
                }
                for (Map.Entry<String, Map<String, PropertyType>> uncommittedTablesEntry : this.uncommittedTables.entrySet()) {
                    if (distributed) {
                        this.tables.put(uncommittedTablesEntry.getKey(), uncommittedTablesEntry.getValue());
                    }
                    this.localTables.put(uncommittedTablesEntry.getKey(), uncommittedTablesEntry.getValue());
                }
                for (Map.Entry<String, Set<String>> uncommittedLabelSchemasEntry : this.uncommittedLabelSchemas.entrySet()) {
                    Set<String> schemas = this.localLabelSchemas.get(uncommittedLabelSchemasEntry.getKey());
                    if (schemas == null) {
                        if (distributed) {
                            this.labelSchemas.put(uncommittedLabelSchemasEntry.getKey(), uncommittedLabelSchemasEntry.getValue());
                        }
                        this.localLabelSchemas.put(uncommittedLabelSchemasEntry.getKey(), uncommittedLabelSchemasEntry.getValue());
                    } else {
                        schemas.addAll(this.uncommittedLabelSchemas.get(uncommittedLabelSchemasEntry.getKey()));
                        if (distributed) {
                            this.labelSchemas.put(uncommittedLabelSchemasEntry.getKey(), schemas);
                        }
                        this.localLabelSchemas.put(uncommittedLabelSchemasEntry.getKey(), schemas);
                    }
                }
                for (Map.Entry<String, Set<String>> uncommittedEdgeForeignKeysEntry : this.uncommittedEdgeForeignKeys.entrySet()) {
                    if (distributed) {
                        this.edgeForeignKeys.put(uncommittedEdgeForeignKeysEntry.getKey(), uncommittedEdgeForeignKeysEntry.getValue());
                    }
                    this.localEdgeForeignKeys.put(uncommittedEdgeForeignKeysEntry.getKey(), uncommittedEdgeForeignKeysEntry.getValue());
                }
                for (Map.Entry<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> schemaTableEntry : this.uncommittedTableLabels.entrySet()) {
                    Pair<Set<SchemaTable>, Set<SchemaTable>> tableLabels = this.localTableLabels.get(schemaTableEntry.getKey());
                    if (tableLabels == null) {
                        tableLabels = Pair.of(new HashSet<>(), new HashSet<>());
                    }
                    tableLabels.getLeft().addAll(schemaTableEntry.getValue().getLeft());
                    tableLabels.getRight().addAll(schemaTableEntry.getValue().getRight());
                    if (distributed) {
                        this.tableLabels.put(schemaTableEntry.getKey(), tableLabels);
                    }
                    this.localTableLabels.put(schemaTableEntry.getKey(), tableLabels);
                }
                for(Map.Entry<String, Map<String, String>> entry : this.uncommittedPropertyUniqueConstraints.entrySet()) {
                    Map<String, String> locals = this.localPropertyUniqueConstraints.get(entry.getKey());
                    if (locals == null) {
                        if (distributed) {
                            this.propertyUniqueConstraints.put(entry.getKey(), entry.getValue());
                        }
                        this.localPropertyUniqueConstraints.put(entry.getKey(), entry.getValue());
                    } else {
                        locals.putAll(entry.getValue());
                        if (distributed) {
                            this.propertyUniqueConstraints.put(entry.getKey(), locals);
                        }
                        this.localPropertyUniqueConstraints.put(entry.getKey(), locals);
                    }
                }
                this.uncommittedSchemas.clear();
                this.uncommittedTables.clear();
                this.uncommittedLabelSchemas.clear();
                this.uncommittedEdgeForeignKeys.clear();
                this.uncommittedTableLabels.clear();
                this.uncommittedPropertyUniqueConstraints.clear();
                this.schemaLock.unlock();
            }
        });
        this.sqlgGraph.tx().afterRollback(() -> {
            this.localTemporaryTables.clear();
            if (this.isLockedByCurrentThread()) {
                if (this.getSqlDialect().supportsTransactionalSchema()) {
                    this.uncommittedSchemas.clear();
                    this.uncommittedTables.clear();
                    this.uncommittedLabelSchemas.clear();
                    this.uncommittedEdgeForeignKeys.clear();
                    this.uncommittedTableLabels.clear();
                    this.uncommittedPropertyUniqueConstraints.clear();
                } else {
                    for (String table : this.uncommittedSchemas) {
                        if (distributed) {
                            this.schemas.put(table, table);
                        }
                        this.localSchemas.put(table, table);
                    }
                    for (Map.Entry<String, Map<String, PropertyType>> tableEntry : this.uncommittedTables.entrySet()) {
                        if (distributed) {
                            this.tables.put(tableEntry.getKey(), tableEntry.getValue());
                        }
                        this.localTables.put(tableEntry.getKey(), tableEntry.getValue());
                    }
                    for (Map.Entry<String, Set<String>> tableEntry : this.uncommittedLabelSchemas.entrySet()) {
                        Set<String> schemas = this.localLabelSchemas.get(tableEntry.getKey());
                        if (schemas == null) {
                            if (distributed) {
                                this.labelSchemas.put(tableEntry.getKey(), tableEntry.getValue());
                            }
                            this.localLabelSchemas.put(tableEntry.getKey(), tableEntry.getValue());
                        } else {
                            schemas.addAll(tableEntry.getValue());
                            if (distributed) {
                                this.labelSchemas.put(tableEntry.getKey(), schemas);
                            }
                            this.localLabelSchemas.put(tableEntry.getKey(), schemas);
                        }
                    }
                    for (Map.Entry<String, Set<String>> tableEntry : this.uncommittedEdgeForeignKeys.entrySet()) {
                        if (distributed) {
                            this.edgeForeignKeys.put(tableEntry.getKey(), tableEntry.getValue());
                        }
                        this.localEdgeForeignKeys.put(tableEntry.getKey(), tableEntry.getValue());
                    }
                    for (Map.Entry<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> schemaTableEntry : this.uncommittedTableLabels.entrySet()) {
                        Pair<Set<SchemaTable>, Set<SchemaTable>> tableLabels = this.localTableLabels.get(schemaTableEntry.getKey());
                        if (tableLabels == null) {
                            tableLabels = Pair.of(new HashSet<>(), new HashSet<>());
                        }
                        tableLabels.getLeft().addAll(schemaTableEntry.getValue().getLeft());
                        tableLabels.getRight().addAll(schemaTableEntry.getValue().getRight());
                        if (distributed) {
                            this.tableLabels.put(schemaTableEntry.getKey(), tableLabels);
                        }
                        this.localTableLabels.put(schemaTableEntry.getKey(), tableLabels);
                    }
                    for(Map.Entry<String, Map<String, String>> entry : this.uncommittedPropertyUniqueConstraints.entrySet()) {
                        Map<String, String> locals = this.localPropertyUniqueConstraints.get(entry.getKey());
                        if (locals == null) {
                            if (distributed) {
                                this.propertyUniqueConstraints.put(entry.getKey(), entry.getValue());
                            }
                            this.localPropertyUniqueConstraints.put(entry.getKey(), entry.getValue());
                        } else {
                            locals.putAll(entry.getValue());
                            if (distributed) {
                                this.propertyUniqueConstraints.put(entry.getKey(), locals);
                            }
                            this.localPropertyUniqueConstraints.put(entry.getKey(), locals);
                        }
                    }
                    this.uncommittedSchemas.clear();
                    this.uncommittedTables.clear();
                    this.uncommittedLabelSchemas.clear();
                    this.uncommittedEdgeForeignKeys.clear();
                    this.uncommittedTableLabels.clear();
                    this.uncommittedPropertyUniqueConstraints.clear();
                }
                this.schemaLock.unlock();
            }
        });
    }

    private Config configHazelcast(Configuration configuration) {
        Config config = new Config();
        config.getNetworkConfig().setPort(5900);
        config.getNetworkConfig().setPortAutoIncrement(true);
        config.setProperty("hazelcast.logging.type", "log4j");
        String[] ips = configuration.getStringArray("hazelcast.members");
        if (ips.length > 0) {
            NetworkConfig network = config.getNetworkConfig();
            JoinConfig join = network.getJoin();
            join.getMulticastConfig().setEnabled(false);
            for (String member : ips) {
                join.getTcpIpConfig().addMember(member);
            }
            join.getTcpIpConfig().setEnabled(true);
        }
        NearCacheConfig nearCacheConfig = new NearCacheConfig();

        MapConfig schemaMapConfig = new MapConfig();
        schemaMapConfig.setName(this.sqlgGraph.getConfiguration().getString(JDBC_URL) + SCHEMAS_HAZELCAST_MAP);
        schemaMapConfig.setNearCacheConfig(nearCacheConfig);
        config.addMapConfig(schemaMapConfig);

        MapConfig labelSchemasMapConfig = new MapConfig();
        labelSchemasMapConfig.setName(this.sqlgGraph.getConfiguration().getString(JDBC_URL) + LABEL_SCHEMAS_HAZELCAST_MAP);
        labelSchemasMapConfig.setNearCacheConfig(nearCacheConfig);
        config.addMapConfig(labelSchemasMapConfig);

        MapConfig tableMapConfig = new MapConfig();
        tableMapConfig.setName(this.sqlgGraph.getConfiguration().getString(JDBC_URL) + TABLES_HAZELCAST_MAP);
        tableMapConfig.setNearCacheConfig(nearCacheConfig);
        config.addMapConfig(tableMapConfig);

        MapConfig edgeForeignKeysMapConfig = new MapConfig();
        edgeForeignKeysMapConfig.setName(this.sqlgGraph.getConfiguration().getString(JDBC_URL) + EDGE_FOREIGN_KEYS_HAZELCAST_MAP);
        edgeForeignKeysMapConfig.setNearCacheConfig(nearCacheConfig);
        config.addMapConfig(edgeForeignKeysMapConfig);

        MapConfig tableLabelMapConfig = new MapConfig();
        tableLabelMapConfig.setName(this.sqlgGraph.getConfiguration().getString(JDBC_URL) + TABLE_LABELS_HAZELCAST_MAP);
        tableLabelMapConfig.setNearCacheConfig(nearCacheConfig);
        config.addMapConfig(tableLabelMapConfig);

        MapConfig propertyUniqueConstraintsConfig = new MapConfig();
        propertyUniqueConstraintsConfig.setName(this.sqlgGraph.getConfiguration().getString(JDBC_URL) + PROPERTY_UNIQUE_CONSTRAINT_HAZELCAST_MAP);
        propertyUniqueConstraintsConfig.setNearCacheConfig(nearCacheConfig);
        config.addMapConfig(propertyUniqueConstraintsConfig);

        return config;

    }

    public SqlDialect getSqlDialect() {
        return sqlDialect;
    }

    void ensureVertexTableExist(final String schema, final String table, final Object... keyValues) {
        Objects.requireNonNull(schema, GIVEN_TABLES_MUST_NOT_BE_NULL);
        Objects.requireNonNull(table, GIVEN_TABLE_MUST_NOT_BE_NULL);
        final String prefixedTable = VERTEX_PREFIX + table;
        final ConcurrentHashMap<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.localTables.containsKey(schema + "." + prefixedTable)) {
            //Make sure the current thread/transaction owns the lock
            lock(schema, table);
            if (!this.localTables.containsKey(schema + "." + prefixedTable) && !this.uncommittedTables.containsKey(schema + "." + prefixedTable)) {
                ensureSchema(schema);
                registerUncommittedTable(schema, prefixedTable, columns);

                if (!SQLG_SCHEMA.equals(schema)) {
                    TopologyManager.addVertexLabel(this.sqlgGraph, schema, prefixedTable, columns);
                }
                createVertexTable(schema, prefixedTable, columns);
            }
        }
        //ensure columns exist
        ensureColumnsExist(schema, prefixedTable, columns);
    }

    private void registerUncommittedTable(String schema, String prefixedTable,
            ConcurrentHashMap<String, PropertyType> columns) {
        Set<String> schemas = this.uncommittedLabelSchemas.get(prefixedTable);
        if (schemas == null) {
            this.uncommittedLabelSchemas.put(prefixedTable, new HashSet<>(Collections.singletonList(schema)));
        } else {
            schemas.add(schema);
            this.uncommittedLabelSchemas.put(prefixedTable, schemas);
        }
        this.uncommittedTables.put(schema + "." + prefixedTable, columns);
    }

    void ensureVertexTemporaryTableExist(final String schema, final String table, final Object... keyValues) {
        Objects.requireNonNull(schema, GIVEN_TABLES_MUST_NOT_BE_NULL);
        Objects.requireNonNull(table, GIVEN_TABLE_MUST_NOT_BE_NULL);
        final String prefixedTable = VERTEX_PREFIX + table;
        final ConcurrentHashMap<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.localTemporaryTables.containsKey(prefixedTable)) {
            this.localTemporaryTables.put(prefixedTable, columns);
            createTempTable(prefixedTable, columns);
        }
    }

    boolean schemaExist(String schema) {
        return this.localSchemas.containsKey(schema);
    }

    public void createSchema(String schema) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE SCHEMA ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
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
     * @param schema        The schema that the table for this edge will reside in.
     * @param table         The table for this edge
     * @param foreignKeyIn  The tables table pair of foreign key to the in vertex
     * @param foreignKeyOut The tables table pair of foreign key to the out vertex
     * @param keyValues
     */
    public void ensureEdgeTableExist(final String schema, final String table, final SchemaTable foreignKeyIn, final SchemaTable foreignKeyOut, Object... keyValues) {
        Objects.requireNonNull(schema, GIVEN_TABLES_MUST_NOT_BE_NULL);
        Objects.requireNonNull(table, GIVEN_TABLE_MUST_NOT_BE_NULL);
        Objects.requireNonNull(foreignKeyIn.getSchema(), "Given inTable must not be null");
        Objects.requireNonNull(foreignKeyOut.getTable(), "Given outTable must not be null");
        final String prefixedTable = EDGE_PREFIX + table;
        final Map<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.localTables.containsKey(schema + "." + prefixedTable)) {
            //Make sure the current thread/transaction owns the lock
            lock(schema, table);
            ensureSchema(schema);
            if (!this.localTables.containsKey(schema + "." + prefixedTable) && !this.uncommittedTables.containsKey(schema + "." + prefixedTable)) {
                Set<String> foreignKeys = new HashSet<>();
                foreignKeys.add(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable() + SchemaManager.IN_VERTEX_COLUMN_END);
                foreignKeys.add(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END);
                this.uncommittedEdgeForeignKeys.put(schema + "." + prefixedTable, foreignKeys);

                Set<String> schemas = this.uncommittedLabelSchemas.get(prefixedTable);
                if (schemas == null) {
                    this.uncommittedLabelSchemas.put(prefixedTable, new HashSet<>(Arrays.asList(schema)));
                } else {
                    schemas.add(schema);
                    this.uncommittedLabelSchemas.put(prefixedTable, schemas);
                }

                this.uncommittedTables.put(schema + "." + prefixedTable, columns);

                if (!SQLG_SCHEMA.equals(schema)) {
                    TopologyManager.addEdgeLabel(this.sqlgGraph, schema, prefixedTable, foreignKeyIn, foreignKeyOut, columns);
                }
                createEdgeTable(schema, prefixedTable, foreignKeyIn, foreignKeyOut, columns);

                //For each table capture its in and out labels
                //This schema information is needed for compiling gremlin
                //first do the in labels
                SchemaTable foreignKeyInWithPrefix = SchemaTable.of(foreignKeyIn.getSchema(), SchemaManager.VERTEX_PREFIX + foreignKeyIn.getTable());
                Pair<Set<SchemaTable>, Set<SchemaTable>> labels = this.uncommittedTableLabels.get(foreignKeyInWithPrefix);
                if (labels == null) {
                    labels = Pair.of(new HashSet<>(), new HashSet<>());
                    this.uncommittedTableLabels.put(foreignKeyInWithPrefix, labels);
                }
                labels.getLeft().add(SchemaTable.of(schema, prefixedTable));
                //now the out labels
                SchemaTable foreignKeyOutWithPrefix = SchemaTable.of(foreignKeyOut.getSchema(), SchemaManager.VERTEX_PREFIX + foreignKeyOut.getTable());
                labels = this.uncommittedTableLabels.get(foreignKeyOutWithPrefix);
                if (labels == null) {
                    labels = Pair.of(new HashSet<>(), new HashSet<>());
                    this.uncommittedTableLabels.put(foreignKeyOutWithPrefix, labels);
                }
                labels.getRight().add(SchemaTable.of(schema, prefixedTable));
            }
        }
        //ensure columns exist
        ensureColumnsExist(schema, prefixedTable, columns);
        ensureEdgeForeignKeysExist(schema, prefixedTable, true, foreignKeyIn);
        ensureEdgeForeignKeysExist(schema, prefixedTable, false, foreignKeyOut);
    }

    void ensureUniqueConstraintTableExists(final String schema, boolean onVertices, final String property, PropertyType propertyType,
            String... labels) {
        String prefixedTable = getUniqueConstraintTableName(onVertices, schema, property, labels);
        if (!this.localTables.containsKey(schema + "." + prefixedTable)) {
            //Make sure the current thread/transaction owns the lock
            lock(schema, prefixedTable);
            ensureSchema(schema);

            if (!this.localTables.containsKey(schema + "." + prefixedTable) && !this.uncommittedTables.containsKey(schema + "." + prefixedTable)) {

                ConcurrentHashMap<String, PropertyType> columns = new ConcurrentHashMap<>();
                columns.put(property, propertyType);

                registerUncommittedTable(schema, prefixedTable, columns);

                if (!SQLG_SCHEMA.equals(schema)) {
                    TopologyManager.addUniqueConstraint(this.sqlgGraph, onVertices, schema, property, labels);
                }
                createUniqueConstraintTable(schema, prefixedTable, property, propertyType);
            }

            String propertyKey = getUniqueConstraintKeyName(schema, property);
            Map<String, String> labelTables = this.uncommittedPropertyUniqueConstraints.get(propertyKey);
            if (labelTables == null) {
                labelTables = new HashMap<>();
                this.uncommittedPropertyUniqueConstraints.put(propertyKey, labelTables);
            }
            if (labels.length == 0) {
                labelTables.put(getUniqueConstraintLabelKey(onVertices, null), prefixedTable);
            } else {
                for (String label : labels) {
                    labelTables.put(getUniqueConstraintLabelKey(onVertices, label), prefixedTable);
                }
            }
        }
    }

    Set<String> getUniqueConstraintTables(boolean onVertices, String schema, String property, String label) {
        List<String> labelSet = new ArrayList<>(2);
        labelSet.add(getUniqueConstraintLabelKey(onVertices, null));
        labelSet.add(getUniqueConstraintLabelKey(onVertices, label));

        Function<Map<String, Map<String, String>>, Stream<String>> extractTables = map ->
                map.getOrDefault(getUniqueConstraintKeyName(schema, property), Collections.emptyMap()).entrySet().stream()
                .filter(e -> labelSet.contains(e.getKey())).map(Map.Entry::getValue);

        return Stream.concat(extractTables.apply(localPropertyUniqueConstraints),
                extractTables.apply(uncommittedPropertyUniqueConstraints))
                .collect(Collectors.toSet());
    }

    private void ensureSchema(String schema) {
        if (!this.sqlDialect.getPublicSchema().equals(schema) && !this.localSchemas.containsKey(schema)) {
            if (!this.uncommittedSchemas.contains(schema)) {
                this.uncommittedSchemas.add(schema);
                createSchema(schema);
                if (!SQLG_SCHEMA.equals(schema)) {
                    TopologyManager.addSchema(this.sqlgGraph, schema);
                }
            }
        }
    }

    /**
     * This is only called from createEdgeIndex
     *
     * @param schema
     * @param table
     * @param keyValues
     */
    public void ensureEdgeTableExist(final String schema, final String table, Object... keyValues) {
        Objects.requireNonNull(schema, GIVEN_TABLES_MUST_NOT_BE_NULL);
        Objects.requireNonNull(table, GIVEN_TABLE_MUST_NOT_BE_NULL);
        final String prefixedTable = EDGE_PREFIX + table;
        final ConcurrentHashMap<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.localTables.containsKey(schema + "." + prefixedTable)) {
            //Make sure the current thread/transaction owns the lock
            lock(schema, table);
            ensureSchema(schema);
            if (!this.localTables.containsKey(schema + "." + prefixedTable) && !this.uncommittedTables.containsKey(schema + "." + prefixedTable)) {
                registerUncommittedTable(schema, prefixedTable, columns);

                //TODO need to create the topology here
                createEdgeTable(schema, prefixedTable, columns);
            }
        }
        //ensure columns exist
        ensureColumnsExist(schema, prefixedTable, columns);
    }

    private void lock(String schema, String table) {
        if (!this.isLockedByCurrentThread()) {
            try {
                if (!this.schemaLock.tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
                    throw new RuntimeException("timeout lapsed for to acquire lock for schema creation for " + schema + "." + table);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Map<String, PropertyType> internalGetColumn(String schema, String table) {
        final Map<String, PropertyType> cachedColumns = this.localTables.get(schema + "." + table);
        Map<String, PropertyType> uncommitedColumns;
        if (cachedColumns == null) {
            uncommitedColumns = this.uncommittedTables.get(schema + "." + table);
        } else {
            uncommitedColumns = this.uncommittedTables.get(schema + "." + table);
            if (uncommitedColumns != null) {
                uncommitedColumns.putAll(cachedColumns);
            } else {
                uncommitedColumns = new HashMap<>(cachedColumns);
            }
        }
        Objects.requireNonNull(uncommitedColumns, "Table must already be present in the cache!");
        return uncommitedColumns;
    }

    private void ensureColumnsExist(String schema, String prefixedTable, Map<String, PropertyType> columns) {
        boolean isVertex = prefixedTable.startsWith(VERTEX_PREFIX);
        boolean isEdge = prefixedTable.startsWith(EDGE_PREFIX);
        if (!isVertex && !isEdge) {
            throw new IllegalStateException("prefixedTable must start with " + VERTEX_PREFIX + " or " + EDGE_PREFIX);
        }
        Map<String, PropertyType> uncommittedColumns = internalGetColumn(schema, prefixedTable);
        Objects.requireNonNull(uncommittedColumns, "Table must already be present in the cache!");
        for (Map.Entry<String, PropertyType> column : columns.entrySet()) {
            String columnName = column.getKey();
            PropertyType columnType = column.getValue();
            if (!uncommittedColumns.containsKey(columnName)) {
                uncommittedColumns = internalGetColumn(schema, prefixedTable);
            }
            if (!uncommittedColumns.containsKey(columnName)) {
                //Make sure the current thread/transaction owns the lock
                lock(schema, prefixedTable);
                if (!uncommittedColumns.containsKey(columnName)) {
                    if (!SQLG_SCHEMA.equals(schema)) {

                        if (isVertex) {
                            TopologyManager.addVertexColumn(this.sqlgGraph, schema, prefixedTable, column);
                        } else {
                            TopologyManager.addEdgeColumn(this.sqlgGraph, schema, prefixedTable, column);
                        }

                    }
                    addColumn(schema, prefixedTable, ImmutablePair.of(columnName, columnType));
                    uncommittedColumns.put(columnName, columnType);
                    this.uncommittedTables.put(schema + "." + prefixedTable, uncommittedColumns);
                }
            }
        }
    }

    public void ensureColumnExist(String schema, String prefixedTable, ImmutablePair<String, PropertyType> keyValue) {
        Map<String, PropertyType> columns = new HashedMap<>();
        columns.put(keyValue.getLeft(), keyValue.getRight());
        ensureColumnsExist(schema, prefixedTable, columns);
    }

    private void ensureEdgeForeignKeysExist(String schema, String prefixedTable, boolean in, SchemaTable vertexSchemaTable) {
        if (!prefixedTable.startsWith(VERTEX_PREFIX) && !prefixedTable.startsWith(EDGE_PREFIX)) {
            throw new IllegalStateException("prefixedTable must start with " + VERTEX_PREFIX + " or " + EDGE_PREFIX);
        }
        Set<String> foreignKeys = this.localEdgeForeignKeys.get(schema + "." + prefixedTable);
        Set<String> uncommittedForeignKeys;
        if (foreignKeys == null) {
            uncommittedForeignKeys = this.uncommittedEdgeForeignKeys.get(schema + "." + prefixedTable);
        } else {
            uncommittedForeignKeys = this.uncommittedEdgeForeignKeys.get(schema + "." + prefixedTable);
            if (uncommittedForeignKeys != null) {
                uncommittedForeignKeys.addAll(foreignKeys);
            } else {
                uncommittedForeignKeys = new HashSet<>(foreignKeys);
            }
        }
        if (uncommittedForeignKeys == null) {
            //This happens on edge indexes which creates the prefixedTable with no foreign keys to vertices.
            uncommittedForeignKeys = new HashSet<>();
        }
        SchemaTable foreignKey = SchemaTable.of(vertexSchemaTable.getSchema(), vertexSchemaTable.getTable() + (in ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END));
        if (!uncommittedForeignKeys.contains(foreignKey.getSchema() + "." + foreignKey.getTable())) {
            //Make sure the current thread/transaction owns the lock
            lock(schema, prefixedTable);
            if (!uncommittedForeignKeys.contains(foreignKey)) {

                if (!SQLG_SCHEMA.equals(schema)) {
                    TopologyManager.addLabelToEdge(this.sqlgGraph, schema, prefixedTable, in, foreignKey);
                }

                addEdgeForeignKey(schema, prefixedTable, foreignKey, vertexSchemaTable);
                uncommittedForeignKeys.add(vertexSchemaTable.getSchema() + "." + foreignKey.getTable());
                this.uncommittedEdgeForeignKeys.put(schema + "." + prefixedTable, uncommittedForeignKeys);

                //For each prefixedTable capture its in and out labels
                //This schema information is needed for compiling gremlin
                //first do the in labels
                SchemaTable foreignKeyInWithPrefix = SchemaTable.of(vertexSchemaTable.getSchema(), SchemaManager.VERTEX_PREFIX + vertexSchemaTable.getTable());
                Pair<Set<SchemaTable>, Set<SchemaTable>> labels = this.uncommittedTableLabels.get(foreignKeyInWithPrefix);
                if (labels == null) {
                    labels = Pair.of(new HashSet<>(), new HashSet<>());
                    this.uncommittedTableLabels.put(foreignKeyInWithPrefix, labels);
                }
                if (in) {
                    labels.getLeft().add(SchemaTable.of(schema, prefixedTable));
                } else {
                    labels.getRight().add(SchemaTable.of(schema, prefixedTable));
                }
            }
        }
    }

    public void close() {
        if (this.distributed) {
            this.hazelcastInstance.shutdown();
        }
    }

    private void createVertexTable(String schema, String tableName, Map<String, PropertyType> columns) {
        StringBuilder sql = new StringBuilder(this.sqlDialect.createTableStatement());
        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
        sql.append(" (");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        buildColumns(columns, sql);
        sql.append(")");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        executeCreateStatement(sql.toString());
    }

    private void buildColumns(Map<String, PropertyType> columns, StringBuilder sql) {
        int i = 1;
        //This is to make the columns sorted
        List<String> keys = new ArrayList<>(columns.keySet());
        Collections.sort(keys);
        for (String column : keys) {
            PropertyType propertyType = columns.get(column);
            int count = 1;
            String[] propertyTypeToSqlDefinition = sqlDialect.propertyTypeToSqlDefinition(propertyType);
            for (String sqlDefinition : propertyTypeToSqlDefinition) {
                if (count > 1) {
                    sql.append(sqlDialect.maybeWrapInQoutes(column + propertyType.getPostFixes()[count - 2])).append(" ").append(sqlDefinition);
                } else {
                    //The first column has no postfix
                    sql.append(sqlDialect.maybeWrapInQoutes(column)).append(" ").append(sqlDefinition);
                }
                if (count++ < propertyTypeToSqlDefinition.length) {
                    sql.append(", ");
                }
            }
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
    }

    private void createEdgeTable(String schema, String tableName, SchemaTable foreignKeyIn, SchemaTable foreignKeyOut, Map<String, PropertyType> columns) {
        this.sqlDialect.assertTableName(tableName);
        StringBuilder sql = new StringBuilder(this.sqlDialect.createTableStatement());
        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
        sql.append("(");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        buildColumns(columns, sql);
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());

        //foreign key definition start
        if (this.sqlgGraph.isImplementForeignKeys()) {
            sql.append(", ");
            sql.append("FOREIGN KEY (");
            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
            sql.append(") REFERENCES ");
            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema()));
            sql.append(".");
            sql.append(this.sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + foreignKeyIn.getTable()));
            sql.append(" (");
            sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
            sql.append("), ");
            sql.append(" FOREIGN KEY (");
            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
            sql.append(") REFERENCES ");
            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema()));
            sql.append(".");
            sql.append(this.sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + foreignKeyOut.getTable()));
            sql.append(" (");
            sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
            sql.append(")");
        }
        //foreign key definition end

        sql.append(")");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }

        if (this.sqlgGraph.getSqlDialect().needForeignKeyIndex()) {
            sql.append("\nCREATE INDEX ON ");
            sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
            sql.append(" (");
            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
            sql.append(");");

            sql.append("\nCREATE INDEX ON ");
            sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
            sql.append(" (");
            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
            sql.append(");");
        }

        executeCreateStatement(sql.toString());
    }

    public void createTempTable(String tableName, Map<String, PropertyType> columns) {
        this.sqlDialect.assertTableName(tableName);
        StringBuilder sql = new StringBuilder(this.sqlDialect.createTemporaryTableStatement());
        sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
        sql.append("(");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        buildColumns(columns, sql);
        sql.append(") ");
        sql.append(this.sqlDialect.afterCreateTemporaryTableStatement());
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }

        executeCreateStatement(sql.toString());
    }

    //This is called from creating edge indexes
    private void createEdgeTable(String schema, String tableName, Map<String, PropertyType> columns) {
        this.sqlDialect.assertTableName(tableName);
        StringBuilder sql = new StringBuilder(this.sqlDialect.createTableStatement());
        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
        sql.append("(");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        buildColumns(columns, sql);
        sql.append(")");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }

        executeCreateStatement(sql.toString());
    }

    private void createUniqueConstraintTable(String schema, String table, String property, PropertyType column) {
        StringBuilder sql = new StringBuilder(this.sqlDialect.createTableStatement());
        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(table));
        sql.append("(");
        buildColumns(Maps.toMap(Collections.singleton("value"), p -> column), sql);
        sql.append(")");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        executeCreateStatement(sql.toString());

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().readWrite();

        sql = new StringBuilder("ALTER TABLE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(table));
        sql.append(" ADD CONSTRAINT ").append(this.sqlDialect.maybeWrapInQoutes("uc_" + table + property));
        sql.append(" UNIQUE (").append(this.sqlDialect.maybeWrapInQoutes("value")).append(")");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        executeCreateStatement(sql.toString());
    }

    private void executeCreateStatement(String sql) {
        if (logger.isDebugEnabled()) {
            logger.debug(sql);
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void addColumn(String schema, String table, ImmutablePair<String, PropertyType> keyValue) {
        int count = 1;
        String[] propertyTypeToSqlDefinition = this.sqlDialect.propertyTypeToSqlDefinition(keyValue.getRight());
        for (String sqlDefinition : propertyTypeToSqlDefinition) {
            StringBuilder sql = new StringBuilder("ALTER TABLE ");
            sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(this.sqlDialect.maybeWrapInQoutes(table));
            sql.append(" ADD ");
            if (count > 1) {
                sql.append(sqlDialect.maybeWrapInQoutes(keyValue.getLeft() + keyValue.getRight().getPostFixes()[count - 2]));
            } else {
                //The first column has no postfix
                sql.append(sqlDialect.maybeWrapInQoutes(keyValue.getLeft()));
            }
            count++;
            sql.append(" ");
            sql.append(sqlDefinition);

            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            Connection conn = this.sqlgGraph.tx().getConnection();
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void addEdgeForeignKey(String schema, String table, SchemaTable foreignKey, SchemaTable otherVertex) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(table));
        sql.append(" ADD COLUMN ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKey.getSchema() + "." + foreignKey.getTable()));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql.setLength(0);
        //foreign key definition start
        if (this.sqlgGraph.isImplementForeignKeys()) {
        	sql.append(" ALTER TABLE ");
            sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(this.sqlDialect.maybeWrapInQoutes(table));
			sql.append(" ADD CONSTRAINT ");
            sql.append(this.sqlDialect.maybeWrapInQoutes(table + "_" + foreignKey.getSchema() + "." + foreignKey.getTable() + "_fkey"));
            sql.append(" FOREIGN KEY (");
			sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKey.getSchema() + "." + foreignKey.getTable()));
			sql.append(") REFERENCES ");
			sql.append(this.sqlDialect.maybeWrapInQoutes(otherVertex.getSchema()));
			sql.append(".");
			sql.append(this.sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + otherVertex.getTable()));
			sql.append(" (");
			sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
			sql.append(")");
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        sql.setLength(0);
        if (this.sqlgGraph.getSqlDialect().needForeignKeyIndex()) {
            sql.append("\nCREATE INDEX ON ");
            sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(this.sqlDialect.maybeWrapInQoutes(table));
            sql.append(" (");
            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKey.getSchema() + "." + foreignKey.getTable()));
            sql.append(")");
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void createVertexIndex(SchemaTable schemaTable, Object... dummykeyValues) {
        this.ensureVertexTableExist(schemaTable.getSchema(), schemaTable.getTable(), dummykeyValues);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().readWrite();
        internalCreateIndex(schemaTable, VERTEX_PREFIX, dummykeyValues);
    }

    public void createEdgeIndex(SchemaTable schemaTable, Object... dummykeyValues) {
        this.ensureEdgeTableExist(schemaTable.getSchema(), schemaTable.getTable(), dummykeyValues);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().readWrite();
        internalCreateIndex(schemaTable, EDGE_PREFIX, dummykeyValues);
    }

    private void internalCreateIndex(SchemaTable schemaTable, String prefix, Object[] dummykeyValues) {
        int i = 1;
        for (Object dummyKeyValue : dummykeyValues) {
            if (i++ % 2 != 0) {
                if (!existIndex(schemaTable, prefix, this.sqlDialect.indexName(schemaTable, prefix, (String) dummyKeyValue))) {
                    StringBuilder sql = new StringBuilder("CREATE INDEX ");
                    sql.append(this.sqlDialect.maybeWrapInQoutes(this.sqlDialect.indexName(schemaTable, prefix, (String) dummyKeyValue)));
                    sql.append(" ON ");
                    sql.append(this.sqlDialect.maybeWrapInQoutes(schemaTable.getSchema()));
                    sql.append(".");
                    sql.append(this.sqlDialect.maybeWrapInQoutes(prefix + schemaTable.getTable()));
                    sql.append(" (");
                    sql.append(this.sqlDialect.maybeWrapInQoutes((String) dummyKeyValue));
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
            }
        }
    }

    private boolean existIndex(SchemaTable schemaTable, String prefix, String indexName) {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            String sql = this.sqlDialect.existIndexQuery(schemaTable, prefix, indexName);
            ResultSet rs = stmt.executeQuery(sql);
            return rs.next();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean tableExist(String schema, String table) {
        this.sqlgGraph.tx().readWrite();
        boolean exists = this.localTables.containsKey(schema + "." + table) || this.uncommittedTables.containsKey(schema + "." + table);
        return exists;
    }

    void loadSchema() {
        if (logger.isDebugEnabled()) {
            logger.debug("SchemaManager.loadSchema()...");
        }
        //check if the topology schema exists, if not create it
        boolean existSqlgSchema = existSqlgSchema();
        if (!existSqlgSchema) {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            createSqlgSchema();
            //committing here will ensure that sqlg creates the tables.
            this.sqlgGraph.tx().commit();
            stopWatch.stop();
            logger.debug("Time to create sqlg topology: " + stopWatch.toString());
        }
        loadTopology();
        if (!existSqlgSchema) {
            addPublicSchema();
            this.sqlgGraph.tx().commit();
        }
        if (!existSqlgSchema) {
            //old versions of sqlg needs the topology populated from the information_schema table.
            logger.debug("Upgrading sqlg from pre sqlg_schema version to sqlg_schema version");
            upgradeSqlgToTopologySchema();
            logger.debug("Done upgrading sqlg from pre sqlg_schema version to sqlg_schema version");
        }
        loadUserSchema();
        this.sqlgGraph.tx().commit();
        if (distributed) {
            this.schemas.putAll(this.localSchemas);
            this.labelSchemas.putAll(this.localLabelSchemas);
            this.tables.putAll(this.localTables);
            this.edgeForeignKeys.putAll(this.localEdgeForeignKeys);
            this.propertyUniqueConstraints.putAll(this.localPropertyUniqueConstraints);
        }

    }

    private void upgradeSqlgToTopologySchema() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String[] types = new String[]{"TABLE"};
            //load the schemas
            ResultSet schemaRs = metadata.getSchemas();
            while (schemaRs.next()) {
                String schema = schemaRs.getString(1);
                if (schema.equals(SQLG_SCHEMA) ||
                        this.sqlDialect.getDefaultSchemas().contains(schema) ||
                        this.sqlDialect.getGisSchemas().contains(schema)) {
                    continue;
                }
                TopologyManager.addSchema(this.sqlgGraph, schema);
            }
            //load the vertices
            ResultSet vertexRs = metadata.getTables(catalog, schemaPattern, "V_%", types);
            while (vertexRs.next()) {
                String schema = vertexRs.getString(2);
                String table = vertexRs.getString(3);

                //check if is internal, if so ignore.
                Set<String> schemasToIgnore = new HashSet<>(this.sqlDialect.getDefaultSchemas());
                schemasToIgnore.remove(this.sqlDialect.getPublicSchema());
                if (schema.equals(SQLG_SCHEMA) ||
                        schemasToIgnore.contains(schema) ||
                        this.sqlDialect.getGisSchemas().contains(schema)) {
                    continue;
                }
                if (this.sqlDialect.getSpacialRefTable().contains(table)) {
                    continue;
                }

                Map<SchemaTable, MutablePair<SchemaTable, SchemaTable>> inOutSchemaTable = new HashMap<>();
                Map<String, PropertyType> columns = new ConcurrentHashMap<>();
                //get the columns
                ResultSet columnsRs = metadata.getColumns(catalog, schema, table, null);
                while (columnsRs.next()) {
                    String column = columnsRs.getString(4);
                    if (!column.equals(SchemaManager.ID)) {
                        int columnType = columnsRs.getInt(5);
                        String typeName = columnsRs.getString("TYPE_NAME");
                        PropertyType propertyType = this.sqlDialect.sqlTypeToPropertyType(this.sqlgGraph, schema, table, column, columnType, typeName);
                        columns.put(column, propertyType);
                    }

                }
                TopologyManager.addVertexLabel(this.sqlgGraph, schema, table, columns);
            }
            //load the edges without their properties
            ResultSet edgeRs = metadata.getTables(catalog, schemaPattern, "E_%", types);
            while (edgeRs.next()) {
                String schema = edgeRs.getString(2);
                String table = edgeRs.getString(3);

                //check if is internal, if so ignore.
                Set<String> schemasToIgnore = new HashSet<>(this.sqlDialect.getDefaultSchemas());
                schemasToIgnore.remove(this.sqlDialect.getPublicSchema());
                if (schema.equals(SQLG_SCHEMA) ||
                        schemasToIgnore.contains(schema) ||
                        this.sqlDialect.getGisSchemas().contains(schema)) {
                    continue;
                }
                if (this.sqlDialect.getSpacialRefTable().contains(table)) {
                    continue;
                }

                Map<SchemaTable, MutablePair<SchemaTable, SchemaTable>> inOutSchemaTableMap = new HashMap<>();
                Map<String, PropertyType> columns = Collections.emptyMap();
                //get the columns
                ResultSet columnsRs = metadata.getColumns(catalog, schema, table, null);
                SchemaTable edgeSchemaTable = SchemaTable.of(schema, table);
                boolean edgeAdded = false;
                while (columnsRs.next()) {
                    String column = columnsRs.getString(4);
                    if (table.startsWith(EDGE_PREFIX) && (column.endsWith(SchemaManager.IN_VERTEX_COLUMN_END) || column.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END))) {
                        String[] split = column.split("\\.");
                        SchemaTable foreignKey = SchemaTable.of(split[0], split[1]);
                        if (column.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                            SchemaTable schemaTable = SchemaTable.of(
                                    split[0],
                                    split[1].substring(0, split[1].length() - SchemaManager.IN_VERTEX_COLUMN_END.length())
                            );
                            if (inOutSchemaTableMap.containsKey(edgeSchemaTable)) {
                                MutablePair<SchemaTable, SchemaTable> inSchemaTable = inOutSchemaTableMap.get(edgeSchemaTable);
                                if (inSchemaTable.getLeft() == null) {
                                    inSchemaTable.setLeft(schemaTable);
                                } else {
                                    TopologyManager.addLabelToEdge(this.sqlgGraph, schema, table, true, foreignKey);
                                }
                            } else {
                                inOutSchemaTableMap.put(edgeSchemaTable, MutablePair.of(schemaTable, null));
                            }
                        } else if (column.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                            SchemaTable schemaTable = SchemaTable.of(
                                    split[0],
                                    split[1].substring(0, split[1].length() - SchemaManager.OUT_VERTEX_COLUMN_END.length())
                            );
                            if (inOutSchemaTableMap.containsKey(edgeSchemaTable)) {
                                MutablePair<SchemaTable, SchemaTable> outSchemaTable = inOutSchemaTableMap.get(edgeSchemaTable);
                                if (outSchemaTable.getRight() == null) {
                                    outSchemaTable.setRight(schemaTable);
                                } else {
                                    TopologyManager.addLabelToEdge(this.sqlgGraph, schema, table, false, foreignKey);
                                }
                            } else {
                                inOutSchemaTableMap.put(edgeSchemaTable, MutablePair.of(null, schemaTable));
                            }
                        }
                        MutablePair<SchemaTable, SchemaTable> inOutLabels = inOutSchemaTableMap.get(edgeSchemaTable);
                        if (!edgeAdded && inOutLabels.getLeft() != null && inOutLabels.getRight() != null) {
                            TopologyManager.addEdgeLabel(this.sqlgGraph, schema, table, inOutLabels.getLeft(), inOutLabels.getRight(), columns);
                            edgeAdded = true;
                        }
                    }
                }
            }
            //load the edges without their in and out vertices
            edgeRs = metadata.getTables(catalog, schemaPattern, "E_%", types);
            while (edgeRs.next()) {
                String schema = edgeRs.getString(2);
                String table = edgeRs.getString(3);

                //check if is internal, if so ignore.
                Set<String> schemasToIgnore = new HashSet<>(this.sqlDialect.getDefaultSchemas());
                schemasToIgnore.remove(this.sqlDialect.getPublicSchema());
                if (schema.equals(SQLG_SCHEMA) ||
                        schemasToIgnore.contains(schema) ||
                        this.sqlDialect.getGisSchemas().contains(schema)) {
                    continue;
                }
                if (this.sqlDialect.getSpacialRefTable().contains(table)) {
                    continue;
                }

                Map<String, PropertyType> columns = new HashMap<>();
                //get the columns
                ResultSet columnsRs = metadata.getColumns(catalog, schema, table, null);
                while (columnsRs.next()) {
                    String column = columnsRs.getString(4);
                    if (!column.equals(SchemaManager.ID) && !column.endsWith(SchemaManager.IN_VERTEX_COLUMN_END) && !column.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                        int columnType = columnsRs.getInt(5);
                        String typeName = columnsRs.getString("TYPE_NAME");
                        PropertyType propertyType = this.sqlDialect.sqlTypeToPropertyType(this.sqlgGraph, schema, table, column, columnType, typeName);
                        columns.put(column, propertyType);
                    }
                }
                TopologyManager.addEdgeColumn(this.sqlgGraph, schema, table, columns);
            }
            if (distributed) {
                this.schemas.putAll(this.localSchemas);
                this.labelSchemas.putAll(this.localLabelSchemas);
                this.tables.putAll(this.localTables);
                this.edgeForeignKeys.putAll(this.localEdgeForeignKeys);
                this.propertyUniqueConstraints.putAll(this.localPropertyUniqueConstraints);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Load Sqlg's actual topology.
     * This is needed because with the {@link TopologyStrategy} it is possible to query the topology itself.
     */
    private void loadTopology() {
        this.localSchemas.put(SQLG_SCHEMA, SQLG_SCHEMA_SCHEMA);

        Set<String> schemas = new HashSet<>();
        schemas.add(SQLG_SCHEMA);
        this.localLabelSchemas.put(VERTEX_PREFIX + SQLG_SCHEMA_SCHEMA, schemas);
        this.localLabelSchemas.put(VERTEX_PREFIX + SQLG_SCHEMA_VERTEX_LABEL, schemas);
        this.localLabelSchemas.put(VERTEX_PREFIX + SQLG_SCHEMA_EDGE_LABEL, schemas);
        this.localLabelSchemas.put(VERTEX_PREFIX + SQLG_SCHEMA_PROPERTY, schemas);
        this.localLabelSchemas.put(VERTEX_PREFIX + SQLG_SCHEMA_UNIQUE_CONSTRAINT, schemas);

        this.localLabelSchemas.put(EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_VERTEX_EDGE, schemas);
        this.localLabelSchemas.put(EDGE_PREFIX + SQLG_SCHEMA_IN_EDGES_EDGE, schemas);
        this.localLabelSchemas.put(EDGE_PREFIX + SQLG_SCHEMA_OUT_EDGES_EDGE, schemas);
        this.localLabelSchemas.put(EDGE_PREFIX + SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE, schemas);
        this.localLabelSchemas.put(EDGE_PREFIX + SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, schemas);

        Map<String, PropertyType> columns = new HashedMap<>();
        columns.put("name", PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        this.localTables.put(SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_SCHEMA, columns);
        columns = new HashedMap<>();
        columns.put("name", PropertyType.STRING);
        columns.put("schemaVertex", PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        this.localTables.put(SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_VERTEX_LABEL, columns);
        columns = new HashedMap<>();
        columns.put("name", PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        this.localTables.put(SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_EDGE_LABEL, columns);
        columns = new HashedMap<>();
        columns.put("name", PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        columns.put("type", PropertyType.STRING);
        this.localTables.put(SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_PROPERTY, columns);
        columns = new HashedMap<>();
        columns.put("name", PropertyType.STRING);
        columns.put(CREATED_ON, PropertyType.LOCALDATETIME);
        columns.put("onVertices", PropertyType.BOOLEAN);
        columns.put("property", PropertyType.STRING);
        this.localTables.put(SQLG_SCHEMA + "." + VERTEX_PREFIX + SQLG_SCHEMA_UNIQUE_CONSTRAINT, columns);

        this.localTables.put(SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_VERTEX_EDGE, Collections.emptyMap());
        this.localTables.put(SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_IN_EDGES_EDGE, Collections.emptyMap());
        this.localTables.put(SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_OUT_EDGES_EDGE, Collections.emptyMap());
        this.localTables.put(SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE, Collections.emptyMap());
        this.localTables.put(SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, Collections.emptyMap());
        this.localTables.put(SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_UNIQUE_CONSTRAINT_EDGE, Collections.emptyMap());
        columns = new HashedMap<>();
        columns.put("property", PropertyType.STRING);
        this.localTables.put(SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_UNIQUE_CONSTRAINT_EDGE, columns);

        Set<String> foreignKeys = new HashSet<>();
        foreignKeys.add(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA + OUT_VERTEX_COLUMN_END);
        foreignKeys.add(SQLG_SCHEMA + "." + SQLG_SCHEMA_VERTEX_LABEL + IN_VERTEX_COLUMN_END);
        this.localEdgeForeignKeys.put(SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_VERTEX_EDGE, foreignKeys);
        foreignKeys = new HashSet<>();
        foreignKeys.add(SQLG_SCHEMA + "." + SQLG_SCHEMA_VERTEX_LABEL + OUT_VERTEX_COLUMN_END);
        foreignKeys.add(SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL + IN_VERTEX_COLUMN_END);
        this.localEdgeForeignKeys.put(SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_IN_EDGES_EDGE, foreignKeys);
        this.localEdgeForeignKeys.put(SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_OUT_EDGES_EDGE, foreignKeys);
        foreignKeys = new HashSet<>();
        foreignKeys.add(SQLG_SCHEMA + "." + SQLG_SCHEMA_VERTEX_LABEL + OUT_VERTEX_COLUMN_END);
        foreignKeys.add(SQLG_SCHEMA + "." + SQLG_SCHEMA_PROPERTY + IN_VERTEX_COLUMN_END);
        this.localEdgeForeignKeys.put(SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE, foreignKeys);
        foreignKeys = new HashSet<>();
        foreignKeys.add(SQLG_SCHEMA + "." + SQLG_SCHEMA_EDGE_LABEL + OUT_VERTEX_COLUMN_END);
        foreignKeys.add(SQLG_SCHEMA + "." + SQLG_SCHEMA_PROPERTY + IN_VERTEX_COLUMN_END);
        this.localEdgeForeignKeys.put(SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_EDGE_PROPERTIES_EDGE, foreignKeys);
        foreignKeys = new HashSet<>();
        foreignKeys.add(SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA + OUT_VERTEX_COLUMN_END);
        foreignKeys.add(SQLG_SCHEMA + "." + SQLG_SCHEMA_UNIQUE_CONSTRAINT + IN_VERTEX_COLUMN_END);
        this.localEdgeForeignKeys.put(SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_UNIQUE_CONSTRAINT_EDGE, foreignKeys);
        foreignKeys = new HashSet<>();
        foreignKeys.add(SQLG_SCHEMA + "." + SQLG_SCHEMA_VERTEX_LABEL + OUT_VERTEX_COLUMN_END);
        foreignKeys.add(SQLG_SCHEMA + "." + SQLG_SCHEMA_UNIQUE_CONSTRAINT + IN_VERTEX_COLUMN_END);
        this.localEdgeForeignKeys.put(SQLG_SCHEMA + "." + EDGE_PREFIX + SQLG_SCHEMA_VERTEX_UNIQUE_CONSTRAINT_EDGE, foreignKeys);

        Pair<Set<SchemaTable>, Set<SchemaTable>> labels = Pair.of(new HashSet<>(), new HashSet<>());
        labels.getRight().add(SchemaTable.of(SQLG_SCHEMA, EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_VERTEX_EDGE));
        labels.getRight().add(SchemaTable.of(SQLG_SCHEMA, EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_UNIQUE_CONSTRAINT_EDGE));
        this.localTableLabels.put(SchemaTable.of(SQLG_SCHEMA, VERTEX_PREFIX + SQLG_SCHEMA_SCHEMA), labels);
        labels = Pair.of(new HashSet<>(), new HashSet<>());
        labels.getLeft().add(SchemaTable.of(SQLG_SCHEMA, EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_VERTEX_EDGE));
        labels.getRight().add(SchemaTable.of(SQLG_SCHEMA, EDGE_PREFIX + SQLG_SCHEMA_IN_EDGES_EDGE));
        labels.getRight().add(SchemaTable.of(SQLG_SCHEMA, EDGE_PREFIX + SQLG_SCHEMA_OUT_EDGES_EDGE));
        labels.getRight().add(SchemaTable.of(SQLG_SCHEMA, EDGE_PREFIX + SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE));
        labels.getRight().add(SchemaTable.of(SQLG_SCHEMA, EDGE_PREFIX + SQLG_SCHEMA_VERTEX_UNIQUE_CONSTRAINT_EDGE));
        this.localTableLabels.put(SchemaTable.of(SQLG_SCHEMA, VERTEX_PREFIX + SQLG_SCHEMA_VERTEX_LABEL), labels);
        labels = Pair.of(new HashSet<>(), new HashSet<>());
        labels.getLeft().add(SchemaTable.of(SQLG_SCHEMA, EDGE_PREFIX + SQLG_SCHEMA_IN_EDGES_EDGE));
        labels.getLeft().add(SchemaTable.of(SQLG_SCHEMA, EDGE_PREFIX + SQLG_SCHEMA_OUT_EDGES_EDGE));
        labels.getRight().add(SchemaTable.of(SQLG_SCHEMA, EDGE_PREFIX + SQLG_SCHEMA_EDGE_PROPERTIES_EDGE));
        this.localTableLabels.put(SchemaTable.of(SQLG_SCHEMA, VERTEX_PREFIX + SQLG_SCHEMA_EDGE_LABEL), labels);
        labels = Pair.of(new HashSet<>(), new HashSet<>());
        labels.getLeft().add(SchemaTable.of(SQLG_SCHEMA, EDGE_PREFIX + SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE));
        labels.getLeft().add(SchemaTable.of(SQLG_SCHEMA, EDGE_PREFIX + SQLG_SCHEMA_EDGE_PROPERTIES_EDGE));
        this.localTableLabels.put(SchemaTable.of(SQLG_SCHEMA, VERTEX_PREFIX + SQLG_SCHEMA_PROPERTY), labels);
        labels = Pair.of(new HashSet<>(), new HashSet<>());
        labels.getLeft().add(SchemaTable.of(SQLG_SCHEMA, EDGE_PREFIX + SQLG_SCHEMA_SCHEMA_UNIQUE_CONSTRAINT_EDGE));
        labels.getLeft().add(SchemaTable.of(SQLG_SCHEMA, EDGE_PREFIX + SQLG_SCHEMA_VERTEX_UNIQUE_CONSTRAINT_EDGE));
        this.localTableLabels.put(SchemaTable.of(SQLG_SCHEMA, VERTEX_PREFIX + SQLG_SCHEMA_UNIQUE_CONSTRAINT), labels);

    }

    /**
     * Load the schema from the topology.
     */
    private void loadUserSchema() {
        GraphTraversalSource traversalSource = this.sqlgGraph.topology();
        List<Vertex> schemas = traversalSource.V().hasLabel(SchemaManager.SQLG_SCHEMA + "." + SchemaManager.SQLG_SCHEMA_SCHEMA).toList();
        Map<String, PropertyType> uniqueConstraintValueType = new HashMap<>();
        for (Vertex schema : schemas) {
            String schemaName = schema.value("name");
            this.localSchemas.put(schemaName, schemaName);

            List<Vertex> vertexLabels = traversalSource.V(schema).out(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).toList();
            for (Vertex vertexLabel : vertexLabels) {
                String tableName = vertexLabel.value("name");

                Set<String> schemaNames = this.localLabelSchemas.get(VERTEX_PREFIX + tableName);
                if (schemaNames == null) {
                    schemaNames = new HashSet<>();
                    this.localLabelSchemas.put(VERTEX_PREFIX + tableName, schemaNames);
                }
                schemaNames.add(schemaName);

                Map<String, PropertyType> uncommittedColumns = new ConcurrentHashMap<>();
                List<Vertex> properties = traversalSource.V(vertexLabel).out(SQLG_SCHEMA_VERTEX_PROPERTIES_EDGE).toList();
                for (Vertex property : properties) {
                    String propertyName = property.value("name");
                    uncommittedColumns.put(propertyName, PropertyType.valueOf(property.value("type")));
                }
                this.localTables.put(schemaName + "." + SchemaManager.VERTEX_PREFIX + tableName, uncommittedColumns);

                List<Map<String, Property<String>>> labelBoundUniqueConstraints = traversalSource.V(vertexLabel)
                        .outE(SQLG_SCHEMA_VERTEX_UNIQUE_CONSTRAINT_EDGE).as("e").properties("property").as("prop").select("e")
                        .inV().properties("name").as("table").<Property<String>>select("prop", "table").toList();

                for (Map<String, Property<String>> uc: labelBoundUniqueConstraints) {
                    String key = getUniqueConstraintKeyName(schemaName, uc.get("prop").value());
                    String table = uc.get("table").value();
                    PropertyType propType = uncommittedColumns.get(uc.get("prop").value());
                    uniqueConstraintValueType.put(table, propType);
                    Map<String, String> existingConstraints = this.localPropertyUniqueConstraints.get(key);
                    if (existingConstraints == null) {
                        existingConstraints = new HashMap<>();
                        this.localPropertyUniqueConstraints.put(key, existingConstraints);
                    }
                    existingConstraints.put(getUniqueConstraintLabelKey(true, tableName), table);
                }

                List<Vertex> outEdges = traversalSource.V(vertexLabel).out(SQLG_SCHEMA_OUT_EDGES_EDGE).toList();
                for (Vertex outEdge : outEdges) {
                    String edgeName = outEdge.value("name");

                    if (!this.localLabelSchemas.containsKey(EDGE_PREFIX + edgeName)) {
                        this.localLabelSchemas.put(EDGE_PREFIX + edgeName, schemaNames);
                    }
                    uncommittedColumns = new ConcurrentHashMap<>();
                    properties = traversalSource.V(outEdge).out(SQLG_SCHEMA_EDGE_PROPERTIES_EDGE).toList();
                    for (Vertex property : properties) {
                        String propertyName = property.value("name");
                        uncommittedColumns.put(propertyName, PropertyType.valueOf(property.value("type")));
                    }
                    this.localTables.put(schemaName + "." + EDGE_PREFIX + edgeName, uncommittedColumns);

                    labelBoundUniqueConstraints = traversalSource.V(outEdge)
                            .outE(SQLG_SCHEMA_EDGE_UNIQUE_CONSTRAINT_EDGE).as("e").properties("property").as("prop").select("e")
                            .inV().properties("name").as("table").<Property<String>>select("prop", "table").toList();

                    for (Map<String, Property<String>> uc: labelBoundUniqueConstraints) {
                        String key = getUniqueConstraintKeyName(schemaName, uc.get("prop").value());
                        String table = uc.get("table").value();
                        PropertyType propType = uncommittedColumns.get(uc.get("prop").value());
                        uniqueConstraintValueType.put(table, propType);
                        Map<String, String> existingConstraints = this.localPropertyUniqueConstraints.get(key);
                        if (existingConstraints == null) {
                            existingConstraints = new HashMap<>();
                            this.localPropertyUniqueConstraints.put(key, existingConstraints);
                        }
                        existingConstraints.put(getUniqueConstraintLabelKey(false, tableName), table);
                    }

                    List<Vertex> inVertices = traversalSource.V(outEdge).in(SQLG_SCHEMA_IN_EDGES_EDGE).toList();
                    for (Vertex inVertex : inVertices) {

                        String inTableName = inVertex.value("name");
                        //Get the inVertex's schema
                        List<Vertex> inVertexSchemas = traversalSource.V(inVertex).in(SQLG_SCHEMA_SCHEMA_VERTEX_EDGE).toList();
                        if (inVertexSchemas.size() != 1) {
                            throw new IllegalStateException("Vertex must be in one and one only schema, found " + inVertexSchemas.size());
                        }
                        Vertex inVertexSchema = inVertexSchemas.get(0);
                        String inVertexSchemaName = inVertexSchema.value("name");

                        Set<String> foreignKeys = this.localEdgeForeignKeys.get(schemaName + "." + EDGE_PREFIX + edgeName);
                        if (foreignKeys == null) {
                            foreignKeys = new HashSet<>();
                            this.localEdgeForeignKeys.put(schemaName + "." + EDGE_PREFIX + edgeName, foreignKeys);
                        }
                        foreignKeys.add(schemaName + "." + tableName + SchemaManager.OUT_VERTEX_COLUMN_END);
                        foreignKeys.add(inVertexSchemaName + "." + inTableName + SchemaManager.IN_VERTEX_COLUMN_END);

                        SchemaTable outSchemaTable = SchemaTable.of(schemaName, VERTEX_PREFIX + tableName);
                        Pair<Set<SchemaTable>, Set<SchemaTable>> labels = this.localTableLabels.get(outSchemaTable);
                        if (labels == null) {
                            labels = Pair.of(new HashSet<>(), new HashSet<>());
                            this.localTableLabels.put(outSchemaTable, labels);
                        }
                        labels.getRight().add(SchemaTable.of(schemaName, EDGE_PREFIX + edgeName));

                        SchemaTable inSchemaTable = SchemaTable.of(inVertexSchemaName, VERTEX_PREFIX + inTableName);
                        labels = this.localTableLabels.get(inSchemaTable);
                        if (labels == null) {
                            labels = Pair.of(new HashSet<>(), new HashSet<>());
                            this.localTableLabels.put(inSchemaTable, labels);
                        }
                        labels.getLeft().add(SchemaTable.of(schemaName, EDGE_PREFIX + edgeName));
                    }

                    //handle globally unique properties
                    //load all the unique constraint tables that handle a property without a label
                    GraphTraversal<?, Vertex> ucs = traversalSource.V(schema)
                            .out(SQLG_SCHEMA_SCHEMA_UNIQUE_CONSTRAINT_EDGE)
                            .has("property");
                    if (ucs.hasNext()) {
                        Vertex uc = ucs.next();

                        String key = getUniqueConstraintKeyName(schemaName, uc.<String>property("property").value());
                        boolean onVertices = uc.<Boolean>property("onVertices").value();
                        String table = uc.<String>property("name").value();

                        Map<String, String> existingConstraints = this.localPropertyUniqueConstraints.get(key);
                        if (existingConstraints == null) {
                            existingConstraints = new HashMap<>();
                            this.localPropertyUniqueConstraints.put(key, existingConstraints);
                        }
                        existingConstraints.put(getUniqueConstraintLabelKey(onVertices, null), table);
                    }

                }
            }

            //register the unique constraint tables with the localTables
            localPropertyUniqueConstraints.values().stream()
                    .flatMap(m -> m.values().stream())
                    .forEach(t -> {
                        Map<String, PropertyType> ucColumnTypes = new HashMap<>(1);
                        ucColumnTypes.put("value", uniqueConstraintValueType.get(t));
                        localTables.put(schemaName + "." + t, ucColumnTypes);
                    });
        }
    }

    private void addPublicSchema() {
        this.sqlgGraph.addVertex(
                T.label, SQLG_SCHEMA + "." + SQLG_SCHEMA_SCHEMA,
                "name", this.sqlDialect.getPublicSchema(),
                CREATED_ON, LocalDateTime.now()
        );
    }

    private void createSqlgSchema() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            Statement statement = conn.createStatement();
            List<String> creationScripts = this.sqlDialect.sqlgTopologyCreationScripts();
            //Hsqldb can not do this in one go
            for (String creationScript : creationScripts) {
                statement.execute(creationScript);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean existSqlgSchema() {
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            if (this.sqlDialect.supportSchemas()) {
                DatabaseMetaData metadata = conn.getMetaData();
                String catalog = null;
                ResultSet schemaRs = metadata.getSchemas(catalog, SQLG_SCHEMA);
                return schemaRs.next();
            } else {
                throw new IllegalStateException("schemas not supported not supported, i.e. probably MariaDB not supported.");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Deletes all tables.
     */
    public void clear() {
        try {
            Connection conn = this.sqlgGraph.getSqlgDataSource().get(this.sqlgGraph.getJdbcUrl()).getConnection();
            DatabaseMetaData metadata;
            metadata = conn.getMetaData();
            if (sqlDialect.supportsCascade()) {
                String catalog = "sqlgraphdb";
                String schemaPattern = null;
                String tableNamePattern = "%";
                String[] types = {"TABLE"};
                ResultSet result = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
                while (result.next()) {
                    StringBuilder sql = new StringBuilder("DROP TABLE ");
                    sql.append(sqlDialect.maybeWrapInQoutes(result.getString(3)));
                    sql.append(" CASCADE");
                    if (sqlDialect.needsSemicolon()) {
                        sql.append(";");
                    }
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                        preparedStatement.executeUpdate();
                    }
                }
            } else {
                throw new RuntimeException("Not yet implemented!");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    Set<String> getEdgeForeignKeys(String schemaTable) {
        Map<String, Set<String>> allForeignKeys = new ConcurrentHashMap<>();
        allForeignKeys.putAll(this.localEdgeForeignKeys);
        if (!this.uncommittedEdgeForeignKeys.isEmpty() && this.isLockedByCurrentThread()) {
            allForeignKeys.putAll(this.uncommittedEdgeForeignKeys);
        }
        return allForeignKeys.get(schemaTable);
    }

    public Map<String, Set<String>> getEdgeForeignKeys() {
        return Collections.unmodifiableMap(this.localEdgeForeignKeys);
    }

    public Map<String, Set<String>> getAllEdgeForeignKeys() {
        Map<String, Set<String>> result = new HashMap<>();
        result.putAll(this.localEdgeForeignKeys);
        if (!this.uncommittedEdgeForeignKeys.isEmpty() && this.isLockedByCurrentThread()) {
            result.putAll(this.uncommittedEdgeForeignKeys);
            for (Map.Entry<String, Set<String>> schemaTableEntry : this.uncommittedEdgeForeignKeys.entrySet()) {
                Set<String> foreignKeys = result.get(schemaTableEntry.getKey());
                if (foreignKeys == null) {
                    foreignKeys = new HashSet<>();
                }
                foreignKeys.addAll(schemaTableEntry.getValue());
                foreignKeys.addAll(schemaTableEntry.getValue());
                result.put(schemaTableEntry.getKey(), foreignKeys);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getLocalTableLabels() {
        return Collections.unmodifiableMap(localTableLabels);
    }

    /**
     * localTableLabels contains committed tables with its labels.
     * uncommittedTableLabels contains uncommitted tables with its labels.
     * However that table in uncommittedTableLabels may be committed with a new uncommitted label.
     *
     * @param schemaTable
     * @return
     */
    public Pair<Set<SchemaTable>, Set<SchemaTable>> getTableLabels(SchemaTable schemaTable) {
        Pair<Set<SchemaTable>, Set<SchemaTable>> result = this.localTableLabels.get(schemaTable);
        if (result == null) {
            if (!this.uncommittedTableLabels.isEmpty() && this.isLockedByCurrentThread()) {
                Pair<Set<SchemaTable>, Set<SchemaTable>> pair = this.uncommittedTableLabels.get(schemaTable);
                if (pair != null) {
                    return Pair.of(Collections.unmodifiableSet(pair.getLeft()), Collections.unmodifiableSet(pair.getRight()));
                }
            }
            return Pair.of(Collections.EMPTY_SET, Collections.EMPTY_SET);
        } else {
            Set<SchemaTable> left = new HashSet<>(result.getLeft());
            Set<SchemaTable> right = new HashSet<>(result.getRight());
            if (!this.uncommittedTableLabels.isEmpty() && this.isLockedByCurrentThread()) {
                Pair<Set<SchemaTable>, Set<SchemaTable>> uncommittedLabels = this.uncommittedTableLabels.get(schemaTable);
                if (uncommittedLabels != null) {
                    left.addAll(uncommittedLabels.getLeft());
                    right.addAll(uncommittedLabels.getRight());
                }
            }
            return Pair.of(
                    Collections.unmodifiableSet(left),
                    Collections.unmodifiableSet(right));
        }
    }

    Map<String, Map<String, PropertyType>> getLocalTables() {
        return Collections.unmodifiableMap(localTables);
    }

    public Map<String, Map<String, PropertyType>> getAllTables() {
        return getAllTablesWithout(SQLG_SCHEMA_SCHEMA_TABLES);
    }

    public Map<String, Map<String, PropertyType>> getAllTablesWithout(List<String> filter) {
        Map<String, Map<String, PropertyType>> result = new ConcurrentHashMap<>();
        result.putAll(this.localTables);
        result.putAll(this.localTemporaryTables);
        if (!this.uncommittedTables.isEmpty() && this.isLockedByCurrentThread()) {
            result.putAll(this.uncommittedTables);
        }
        filter.forEach(s -> result.remove(s));
        return Collections.unmodifiableMap(result);
    }

    public Map<String, Map<String, PropertyType>> getAllTablesFrom(List<String> selectFrom) {
        Map<String, Map<String, PropertyType>> result = new ConcurrentHashMap<>();
        this.localTables.forEach((k, v) -> {
            if (selectFrom.contains(k)) result.put(k, v);
        });
        if (!this.uncommittedTables.isEmpty() && this.isLockedByCurrentThread()) {
            this.uncommittedTables.forEach((k, v) -> {
                if (selectFrom.contains(k)) result.put(k, v);
            });
        }
        return Collections.unmodifiableMap(result);
    }

    public Map<String, PropertyType> getTableFor(SchemaTable schemaTable) {
        Map<String, PropertyType> result = this.localTables.get(schemaTable.toString());
        if (!this.uncommittedTables.isEmpty() && this.isLockedByCurrentThread()) {
            Map<String, PropertyType> tmp = this.uncommittedTables.get(schemaTable.toString());
            if (tmp != null) {
                result = tmp;
            }
        }
        if (result == null) {
            return Collections.emptyMap();
        } else {
            return Collections.unmodifiableMap(result);
        }
    }

    private boolean isLockedByCurrentThread() {
        if (this.distributed) {
            return ((ILock) this.schemaLock).isLockedByCurrentThread();
        } else {
            return ((ReentrantLock) this.schemaLock).isHeldByCurrentThread();
        }
    }

    static private String getUniqueConstraintKeyName(String schema, String property) {
        return schema + "." + property;
    }

    static String getUniqueConstraintTableName(boolean onVertices, String schema, String property, String... labels) {
        String tableName = (onVertices ? VERTEX_PREFIX : EDGE_PREFIX) + property +
                (labels.length == 0 ? "" : Joiner.on("_").join(labels));
        return UNIQUE_CONSTRAINT_PREFIX + tableName;
    }

    static String getUniqueConstraintLabelKey(boolean onVertices, String label) {
        return (onVertices ? VERTEX_PREFIX : EDGE_PREFIX) + (label == null ? "" : label);
    }

    public class SchemasMapEntryListener implements EntryAddedListener<String, String>,
            EntryRemovedListener<String, String>,
            EntryUpdatedListener<String, String>,
            EntryEvictedListener<String, String>,
            MapEvictedListener,
            MapClearedListener {

        @Override
        public void entryAdded(EntryEvent<String, String> event) {
            SchemaManager.this.localSchemas.put(event.getKey(), event.getValue());
        }

        @Override
        public void entryRemoved(EntryEvent<String, String> event) {
            SchemaManager.this.localSchemas.remove(event.getKey());
        }

        @Override
        public void entryUpdated(EntryEvent<String, String> event) {
            SchemaManager.this.localSchemas.put(event.getKey(), event.getValue());
        }

        @Override
        public void entryEvicted(EntryEvent<String, String> event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }

        @Override
        public void mapEvicted(MapEvent event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }

        @Override
        public void mapCleared(MapEvent event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }
    }

    public class LabelSchemasMapEntryListener implements EntryAddedListener<String, Set<String>>,
            EntryRemovedListener<String, Set<String>>,
            EntryUpdatedListener<String, Set<String>>,
            EntryEvictedListener<String, Set<String>>,
            MapEvictedListener,
            MapClearedListener {

        @Override
        public void entryAdded(EntryEvent<String, Set<String>> event) {
            SchemaManager.this.localLabelSchemas.put(event.getKey(), event.getValue());
        }

        @Override
        public void entryRemoved(EntryEvent<String, Set<String>> event) {
            SchemaManager.this.localLabelSchemas.remove(event.getKey());
        }

        @Override
        public void entryUpdated(EntryEvent<String, Set<String>> event) {
            SchemaManager.this.localLabelSchemas.put(event.getKey(), event.getValue());
        }

        @Override
        public void entryEvicted(EntryEvent<String, Set<String>> event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }

        @Override
        public void mapEvicted(MapEvent event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }

        @Override
        public void mapCleared(MapEvent event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }
    }

    public class TablesMapEntryListener implements EntryAddedListener<String, Map<String, PropertyType>>,
            EntryRemovedListener<String, Map<String, PropertyType>>,
            EntryUpdatedListener<String, Map<String, PropertyType>>,
            EntryEvictedListener<String, Map<String, PropertyType>>,
            MapEvictedListener,
            MapClearedListener {

        @Override
        public void entryAdded(EntryEvent<String, Map<String, PropertyType>> event) {
            SchemaManager.this.localTables.put(event.getKey(), event.getValue());
        }

        @Override
        public void entryRemoved(EntryEvent<String, Map<String, PropertyType>> event) {
            SchemaManager.this.localTables.remove(event.getKey());
        }

        @Override
        public void entryUpdated(EntryEvent<String, Map<String, PropertyType>> event) {
            SchemaManager.this.localTables.put(event.getKey(), event.getValue());
        }

        @Override
        public void entryEvicted(EntryEvent<String, Map<String, PropertyType>> event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }

        @Override
        public void mapEvicted(MapEvent event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }

        @Override
        public void mapCleared(MapEvent event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }
    }

    public class EdgeForeignKeysMapEntryListener implements EntryAddedListener<String, Set<String>>,
            EntryRemovedListener<String, Set<String>>,
            EntryUpdatedListener<String, Set<String>>,
            EntryEvictedListener<String, Set<String>>,
            MapEvictedListener,
            MapClearedListener {

        @Override
        public void entryAdded(EntryEvent<String, Set<String>> event) {
            SchemaManager.this.localEdgeForeignKeys.put(event.getKey(), event.getValue());
        }

        @Override
        public void entryRemoved(EntryEvent<String, Set<String>> event) {
            SchemaManager.this.localEdgeForeignKeys.remove(event.getKey());
        }

        @Override
        public void entryUpdated(EntryEvent<String, Set<String>> event) {
            SchemaManager.this.localEdgeForeignKeys.put(event.getKey(), event.getValue());
        }

        @Override
        public void entryEvicted(EntryEvent<String, Set<String>> event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }

        @Override
        public void mapEvicted(MapEvent event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }

        @Override
        public void mapCleared(MapEvent event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }
    }

    public class TableLabelMapEntryListener implements EntryAddedListener<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>>,
            EntryRemovedListener<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>>,
            EntryUpdatedListener<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>>,
            EntryEvictedListener<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>>,
            MapEvictedListener,
            MapClearedListener {

        @Override
        public void entryAdded(EntryEvent<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> event) {
            SchemaManager.this.localTableLabels.put(event.getKey(), event.getValue());
        }

        @Override
        public void entryRemoved(EntryEvent<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> event) {
            SchemaManager.this.localTableLabels.remove(event.getKey());
        }

        @Override
        public void entryUpdated(EntryEvent<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> event) {
            SchemaManager.this.localTableLabels.put(event.getKey(), event.getValue());
        }

        @Override
        public void entryEvicted(EntryEvent<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }

        @Override
        public void mapEvicted(MapEvent event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }

        @Override
        public void mapCleared(MapEvent event) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }
    }

    public class PropertyUniqueConstraintsMapEntryListener implements
            EntryAddedListener<String, Map<String, String>>,
            EntryRemovedListener<String, Map<String, String>>,
            EntryUpdatedListener<String, Map<String, String>>,
            EntryEvictedListener<String, Map<String, String>>,
            MapEvictedListener,
            MapClearedListener {

        @Override
        public void entryAdded(EntryEvent<String, Map<String, String>> entryEvent) {
            SchemaManager.this.localPropertyUniqueConstraints.put(entryEvent.getKey(), entryEvent.getValue());
        }

        @Override
        public void entryEvicted(EntryEvent<String, Map<String, String>> entryEvent) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }

        @Override
        public void entryRemoved(EntryEvent<String, Map<String, String>> entryEvent) {
            SchemaManager.this.localPropertyUniqueConstraints.remove(entryEvent.getKey());
        }

        @Override
        public void entryUpdated(EntryEvent<String, Map<String, String>> entryEvent) {
            SchemaManager.this.localPropertyUniqueConstraints.put(entryEvent.getKey(), entryEvent.getValue());
        }

        @Override
        public void mapCleared(MapEvent mapEvent) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }

        @Override
        public void mapEvicted(MapEvent mapEvent) {
            throw new IllegalStateException(SHOULD_NOT_HAPPEN);
        }
    }
}
