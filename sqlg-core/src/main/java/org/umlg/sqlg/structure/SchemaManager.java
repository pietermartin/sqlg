package org.umlg.sqlg.structure;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.map.listener.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Date: 2014/07/12
 * Time: 11:02 AM
 */
public class SchemaManager {

    private Logger logger = LoggerFactory.getLogger(SchemaManager.class.getName());
    public static final String VERTEX_PREFIX = "V_";
    public static final String EDGE_PREFIX = "E_";
    public static final String VERTICES = "VERTICES";
    public static final String VERTEX_IN_LABELS = "IN_LABELS";
    public static final String VERTEX_OUT_LABELS = "OUT_LABELS";
    public static final String EDGES = "EDGES";
    public static final String ID = "ID";
    public static final String VERTEX_SCHEMA = "VERTEX_SCHEMA";
    public static final String VERTEX_TABLE = "VERTEX_TABLE";
    public static final String EDGE_SCHEMA = "EDGE_SCHEMA";
    public static final String EDGE_TABLE = "EDGE_TABLE";
    public static final String LABEL_SEPARATOR = ":::";
    public static final String IN_VERTEX_COLUMN_END = "__I";
    public static final String OUT_VERTEX_COLUMN_END = "__O";
    public static final String ZONEID = "~~~ZONEID";
    public static final String MONTHS = "~~~MONTHS";
    public static final String DAYS = "~~~DAYS";
    public static final String DURATION_NANOS = "~~~NANOS";
    public static final String BULK_TEMP_EDGE = "BULK_TEMP_EDGE";

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

    private static final int LOCK_TIMEOUT = 10;

    SchemaManager(SqlgGraph sqlgGraph, SqlDialect sqlDialect, Configuration configuration) {
        this.sqlgGraph = sqlgGraph;
        this.sqlDialect = sqlDialect;
        this.distributed = configuration.getBoolean("distributed", false);

        if (this.distributed) {
            this.hazelcastInstance = Hazelcast.newHazelcastInstance(configHazelcast(configuration));
            this.schemas = this.hazelcastInstance.getMap(this.sqlgGraph.getConfiguration().getString("jdbc.url") + SCHEMAS_HAZELCAST_MAP);
            this.labelSchemas = this.hazelcastInstance.getMap(this.sqlgGraph.getConfiguration().getString("jdbc.url") + LABEL_SCHEMAS_HAZELCAST_MAP);
            this.tables = this.hazelcastInstance.getMap(this.sqlgGraph.getConfiguration().getString("jdbc.url") + TABLES_HAZELCAST_MAP);
            this.edgeForeignKeys = this.hazelcastInstance.getMap(this.sqlgGraph.getConfiguration().getString("jdbc.url") + EDGE_FOREIGN_KEYS_HAZELCAST_MAP);
            this.tableLabels = this.hazelcastInstance.getMap(this.sqlgGraph.getConfiguration().getString("jdbc.url") + TABLE_LABELS_HAZELCAST_MAP);
            ((IMap) this.schemas).addEntryListener(new SchemasMapEntryListener(), true);
            ((IMap) this.labelSchemas).addEntryListener(new LabelSchemasMapEntryListener(), true);
            ((IMap) this.tables).addEntryListener(new TablesMapEntryListener(), true);
            ((IMap) this.edgeForeignKeys).addEntryListener(new EdgeForeignKeysMapEntryListener(), true);
            ((IMap) this.tableLabels).addEntryListener(new TableLabelMapEntryListener(), true);
            this.schemaLock = this.hazelcastInstance.getLock("schemaLock");
        } else {
            this.schemaLock = new ReentrantLock();
        }

        this.sqlgGraph.tx().afterCommit(() -> {
            if (this.isLockedByCurrentThread()) {
                for (String schema : this.uncommittedSchemas) {
                    if (distributed) {
                        this.schemas.put(schema, schema);
                    }
                    this.localSchemas.put(schema, schema);
                }
                for (String table : this.uncommittedTables.keySet()) {
                    if (distributed) {
                        this.tables.put(table, this.uncommittedTables.get(table));
                    }
                    this.localTables.put(table, this.uncommittedTables.get(table));
                }
                for (String table : this.uncommittedLabelSchemas.keySet()) {
                    Set<String> schemas = this.localLabelSchemas.get(table);
                    if (schemas == null) {
                        if (distributed) {
                            this.labelSchemas.put(table, this.uncommittedLabelSchemas.get(table));
                        }
                        this.localLabelSchemas.put(table, this.uncommittedLabelSchemas.get(table));
                    } else {
                        schemas.addAll(this.uncommittedLabelSchemas.get(table));
                        if (distributed) {
                            this.labelSchemas.put(table, schemas);
                        }
                        this.localLabelSchemas.put(table, schemas);
                    }
                }
                for (String table : this.uncommittedEdgeForeignKeys.keySet()) {
                    if (distributed) {
                        this.edgeForeignKeys.put(table, this.uncommittedEdgeForeignKeys.get(table));
                    }
                    this.localEdgeForeignKeys.put(table, this.uncommittedEdgeForeignKeys.get(table));
                }
                for (SchemaTable schemaTable : this.uncommittedTableLabels.keySet()) {
                    Pair<Set<SchemaTable>, Set<SchemaTable>> tableLabels = this.localTableLabels.get(schemaTable);
                    if (tableLabels == null) {
                        tableLabels = Pair.of(new HashSet<>(), new HashSet<>());
                    }
                    tableLabels.getLeft().addAll(this.uncommittedTableLabels.get(schemaTable).getLeft());
                    tableLabels.getRight().addAll(this.uncommittedTableLabels.get(schemaTable).getRight());
                    if (distributed) {
                        this.tableLabels.put(schemaTable, tableLabels);
                    }
                    this.localTableLabels.put(schemaTable, tableLabels);
                }
                this.uncommittedSchemas.clear();
                this.uncommittedTables.clear();
                this.uncommittedLabelSchemas.clear();
                this.uncommittedEdgeForeignKeys.clear();
                this.uncommittedTableLabels.clear();
                this.schemaLock.unlock();
            }
        });
        this.sqlgGraph.tx().afterRollback(() -> {
            if (this.isLockedByCurrentThread()) {
                if (this.getSqlDialect().supportsTransactionalSchema()) {
                    this.uncommittedSchemas.clear();
                    this.uncommittedTables.clear();
                    this.uncommittedLabelSchemas.clear();
                    this.uncommittedEdgeForeignKeys.clear();
                    this.uncommittedTableLabels.clear();
                } else {
                    for (String table : this.uncommittedSchemas) {
                        if (distributed) {
                            this.schemas.put(table, table);
                        }
                        this.localSchemas.put(table, table);
                    }
                    for (String table : this.uncommittedTables.keySet()) {
                        if (distributed) {
                            this.tables.put(table, this.uncommittedTables.get(table));
                        }
                        this.localTables.put(table, this.uncommittedTables.get(table));
                    }
                    for (String table : this.uncommittedLabelSchemas.keySet()) {
                        Set<String> schemas = this.localLabelSchemas.get(table);
                        if (schemas == null) {
                            if (distributed) {
                                this.labelSchemas.put(table, this.uncommittedLabelSchemas.get(table));
                            }
                            this.localLabelSchemas.put(table, this.uncommittedLabelSchemas.get(table));
                        } else {
                            schemas.addAll(this.uncommittedLabelSchemas.get(table));
                            if (distributed) {
                                this.labelSchemas.put(table, schemas);
                            }
                            this.localLabelSchemas.put(table, schemas);
                        }
                    }
                    for (String table : this.uncommittedEdgeForeignKeys.keySet()) {
                        if (distributed) {
                            this.edgeForeignKeys.put(table, this.uncommittedEdgeForeignKeys.get(table));
                        }
                        this.localEdgeForeignKeys.put(table, this.uncommittedEdgeForeignKeys.get(table));
                    }
                    for (SchemaTable schemaTable : this.uncommittedTableLabels.keySet()) {
                        Pair<Set<SchemaTable>, Set<SchemaTable>> tableLabels = this.localTableLabels.get(schemaTable);
                        if (tableLabels == null) {
                            tableLabels = Pair.of(new HashSet<>(), new HashSet<>());
                        }
                        tableLabels.getLeft().addAll(this.uncommittedTableLabels.get(schemaTable).getLeft());
                        tableLabels.getRight().addAll(this.uncommittedTableLabels.get(schemaTable).getRight());
                        if (distributed) {
                            this.tableLabels.put(schemaTable, tableLabels);
                        }
                        this.localTableLabels.put(schemaTable, tableLabels);
                    }
                    this.uncommittedSchemas.clear();
                    this.uncommittedTables.clear();
                    this.uncommittedLabelSchemas.clear();
                    this.uncommittedEdgeForeignKeys.clear();
                    this.uncommittedTableLabels.clear();
                }
                this.schemaLock.unlock();
            }
        });
    }

    private Config configHazelcast(Configuration configuration) {
        Config config = new Config();
        config.getNetworkConfig().setPort(5900);
        config.getNetworkConfig().setPortAutoIncrement(true);
        config.setProperty( "hazelcast.logging.type", "log4j" );
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
        schemaMapConfig.setName(this.sqlgGraph.getConfiguration().getString("jdbc.url") + SCHEMAS_HAZELCAST_MAP);
        schemaMapConfig.setNearCacheConfig(nearCacheConfig);
        config.addMapConfig(schemaMapConfig);

        MapConfig labelSchemasMapConfig = new MapConfig();
        labelSchemasMapConfig.setName(this.sqlgGraph.getConfiguration().getString("jdbc.url") + LABEL_SCHEMAS_HAZELCAST_MAP);
        labelSchemasMapConfig.setNearCacheConfig(nearCacheConfig);
        config.addMapConfig(labelSchemasMapConfig);

        MapConfig tableMapConfig = new MapConfig();
        tableMapConfig.setName(this.sqlgGraph.getConfiguration().getString("jdbc.url") + TABLES_HAZELCAST_MAP);
        tableMapConfig.setNearCacheConfig(nearCacheConfig);
        config.addMapConfig(tableMapConfig);

        MapConfig edgeForeignKeysMapConfig = new MapConfig();
        edgeForeignKeysMapConfig.setName(this.sqlgGraph.getConfiguration().getString("jdbc.url") + EDGE_FOREIGN_KEYS_HAZELCAST_MAP);
        edgeForeignKeysMapConfig.setNearCacheConfig(nearCacheConfig);
        config.addMapConfig(edgeForeignKeysMapConfig);

        MapConfig tableLabelMapConfig = new MapConfig();
        tableLabelMapConfig.setName(this.sqlgGraph.getConfiguration().getString("jdbc.url") + TABLE_LABELS_HAZELCAST_MAP);
        tableLabelMapConfig.setNearCacheConfig(nearCacheConfig);
        config.addMapConfig(tableLabelMapConfig);
        return config;

    }

    public SqlDialect getSqlDialect() {
        return sqlDialect;
    }

    void ensureVertexTableExist(final String schema, final String table, final Object... keyValues) {
        Objects.requireNonNull(schema, "Given tables must not be null");
        Objects.requireNonNull(table, "Given table must not be null");
        final String prefixedTable = VERTEX_PREFIX + table;
        final ConcurrentHashMap<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.localTables.containsKey(schema + "." + prefixedTable)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.isLockedByCurrentThread()) {
                try {
                    if (!this.schemaLock.tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
//                        System.out.println("aaaaaaaaaaaaaaaa "  + Thread.currentThread().getName());
                        throw new RuntimeException("timeout lapsed for to acquire lock for schema creation for " + schema + "." + table);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (!this.localTables.containsKey(schema + "." + prefixedTable) && !this.uncommittedTables.containsKey(schema + "." + prefixedTable)) {

                if (!this.sqlDialect.getPublicSchema().equals(schema) && !this.localSchemas.containsKey(schema)) {
                    if (!this.uncommittedSchemas.contains(schema)) {
                        this.uncommittedSchemas.add(schema);
                        createSchema(schema);
                    }
                }
                Set<String> schemas = this.uncommittedLabelSchemas.get(prefixedTable);
                if (schemas == null) {
                    this.uncommittedLabelSchemas.put(prefixedTable, new HashSet<>(Arrays.asList(schema)));
                } else {
                    schemas.add(schema);
                    this.uncommittedLabelSchemas.put(prefixedTable, schemas);
                }
                this.uncommittedTables.put(schema + "." + prefixedTable, columns);
                createVertexTable(schema, prefixedTable, columns);
            }
        }
        //ensure columns exist
        ensureColumnsExist(schema, prefixedTable, columns);
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
     * @param schema        The tables that the table for this edge will reside in.
     * @param table         The table for this edge
     * @param foreignKeyIn  The tables table pair of foreign key to the in vertex
     * @param foreignKeyOut The tables table pair of foreign key to the out vertex
     * @param keyValues
     */
    public void ensureEdgeTableExist(final String schema, final String table, final SchemaTable foreignKeyIn, final SchemaTable foreignKeyOut, Object... keyValues) {
        Objects.requireNonNull(schema, "Given tables must not be null");
        Objects.requireNonNull(table, "Given table must not be null");
        Objects.requireNonNull(foreignKeyIn.getSchema(), "Given inTable must not be null");
        Objects.requireNonNull(foreignKeyOut.getTable(), "Given outTable must not be null");
        final String prefixedTable = EDGE_PREFIX + table;
        final ConcurrentHashMap<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.localTables.containsKey(schema + "." + prefixedTable)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.isLockedByCurrentThread()) {
                try {
                    if (!this.schemaLock.tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
                        throw new RuntimeException("timeout lapsed for to acquire lock for schema creation for " + schema + "." + table);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (!this.sqlDialect.getPublicSchema().equals(schema) && !this.localSchemas.containsKey(schema)) {
                if (!this.uncommittedSchemas.contains(schema)) {
                    this.uncommittedSchemas.add(schema);
                    createSchema(schema);
                }
            }
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

    /**
     * This is only called from createEdgeIndex
     *
     * @param schema
     * @param table
     * @param keyValues
     */
    public void ensureEdgeTableExist(final String schema, final String table, Object... keyValues) {
        Objects.requireNonNull(schema, "Given tables must not be null");
        Objects.requireNonNull(table, "Given table must not be null");
        final String prefixedTable = EDGE_PREFIX + table;
        final ConcurrentHashMap<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.localTables.containsKey(schema + "." + prefixedTable)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.isLockedByCurrentThread()) {
                try {
                    if (!this.schemaLock.tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
                        throw new RuntimeException("timeout lapsed for to acquire lock for schema creation for " + schema + "." + table);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (!this.sqlDialect.getPublicSchema().equals(schema) && !this.localSchemas.containsKey(schema)) {
                if (!this.uncommittedSchemas.contains(schema)) {
                    this.uncommittedSchemas.add(schema);
                    createSchema(schema);
                }
            }
            if (!this.localTables.containsKey(schema + "." + prefixedTable) && !this.uncommittedTables.containsKey(schema + "." + prefixedTable)) {
                Set<String> schemas = this.uncommittedLabelSchemas.get(prefixedTable);
                if (schemas == null) {
                    this.uncommittedLabelSchemas.put(prefixedTable, new HashSet<>(Collections.singletonList(schema)));
                } else {
                    schemas.add(schema);
                    this.uncommittedLabelSchemas.put(prefixedTable, schemas);
                }
                this.uncommittedTables.put(schema + "." + prefixedTable, columns);
                createEdgeTable(schema, prefixedTable, columns);
            }
        }
        //ensure columns exist
        ensureColumnsExist(schema, prefixedTable, columns);
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

    void ensureColumnsExist(String schema, String prefixedTable, ConcurrentHashMap<String, PropertyType> columns) {
        if (!prefixedTable.startsWith(VERTEX_PREFIX) && !prefixedTable.startsWith(EDGE_PREFIX)) {
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
                if (!this.isLockedByCurrentThread()) {
                    try {
                        if (!this.schemaLock.tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
                            throw new RuntimeException("timeout lapsed for to acquire lock for schema creation for " + schema + "." + prefixedTable);
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (!uncommittedColumns.containsKey(columnName)) {
                    addColumn(schema, prefixedTable, ImmutablePair.of(columnName, columnType));
                    uncommittedColumns.put(columnName, columnType);
                    this.uncommittedTables.put(schema + "." + prefixedTable, uncommittedColumns);
                }
            }
        }
    }

    public void ensureColumnExist(String schema, String prefixedTable, ImmutablePair<String, PropertyType> keyValue) {
        if (!prefixedTable.startsWith(VERTEX_PREFIX) && !prefixedTable.startsWith(EDGE_PREFIX)) {
            throw new IllegalStateException("prefixedTable must start with " + VERTEX_PREFIX + " or " + EDGE_PREFIX);
        }
        Map<String, PropertyType> uncommitedColumns = internalGetColumn(schema, prefixedTable);
        Objects.requireNonNull(uncommitedColumns, "Table must already be present in the cache!");
        if (!uncommitedColumns.containsKey(keyValue.left)) {
            uncommitedColumns = internalGetColumn(schema, prefixedTable);
        }
        if (!uncommitedColumns.containsKey(keyValue.left)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.isLockedByCurrentThread()) {
                try {
                    if (!this.schemaLock.tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
                        throw new RuntimeException("timeout lapsed for to acquire lock for schema creation for " + schema + "." + prefixedTable);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (!uncommitedColumns.containsKey(keyValue.left)) {
                addColumn(schema, prefixedTable, keyValue);
                uncommitedColumns.put(keyValue.left, keyValue.right);
                this.uncommittedTables.put(schema + "." + prefixedTable, uncommitedColumns);
            }
        }
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
            if (!this.isLockedByCurrentThread()) {
                try {
                    if (!this.schemaLock.tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
                        throw new RuntimeException("timeout lapsed for to acquire lock for schema creation for " + schema + "." + prefixedTable);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (!uncommittedForeignKeys.contains(foreignKey)) {
                addEdgeForeignKey(schema, prefixedTable, foreignKey);
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
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.sqlgGraph.tx().setSchemaModification(true);
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

        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.sqlgGraph.tx().setSchemaModification(true);
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
            this.sqlgGraph.tx().setSchemaModification(true);
        }
    }

    private void addEdgeForeignKey(String schema, String table, SchemaTable foreignKey) {
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
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
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
        Connection conn = this.sqlgGraph.tx().getConnection();
        try {
            DatabaseMetaData metadata;
            metadata = conn.getMetaData();
            if (this.sqlDialect.supportSchemas()) {
                String catalog = null;
                String schemaPattern = null;
                String tableNamePattern = null;
                String[] types = new String[]{"TABLE"};
                ResultSet tablesRs = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
                while (tablesRs.next()) {
                    String table = tablesRs.getString(3);
                    if (this.sqlDialect.getSpacialRefTable().contains(table)) {
                        continue;
                    }
                    Map<String, PropertyType> uncommittedColumns = new ConcurrentHashMap<>();
                    Set<String> foreignKeys = null;
                    //get the columns
                    String previousSchema = "";
                    ResultSet columnsRs = metadata.getColumns(catalog, schemaPattern, table, null);
                    while (columnsRs.next()) {
                        String schema = columnsRs.getString(2);
                        if (this.sqlDialect.getGisSchemas().contains(schema)) {
                            continue;
                        }
                        this.localSchemas.put(schema, schema);
                        if (!previousSchema.equals(schema)) {
                            foreignKeys = new HashSet<>();
                            uncommittedColumns = new ConcurrentHashMap<>();
                        }
                        previousSchema = schema;
                        String column = columnsRs.getString(4);
                        if (!column.equals(SchemaManager.ID)) {
                            int columnType = columnsRs.getInt(5);
                            String typeName = columnsRs.getString("TYPE_NAME");
                            PropertyType propertyType = this.sqlDialect.sqlTypeToPropertyType(columnType, typeName);
                            uncommittedColumns.put(column, propertyType);
                        }
                        this.localTables.put(schema + "." + table, uncommittedColumns);
                        Set<String> schemas = this.localLabelSchemas.get(table);
                        if (schemas == null) {
                            schemas = new HashSet<>();
                        }
                        schemas.add(schema);
                        this.localLabelSchemas.put(table, schemas);
                        if (table.startsWith(EDGE_PREFIX) && (column.endsWith(SchemaManager.IN_VERTEX_COLUMN_END) || column.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END))) {
                            foreignKeys.add(column);
                            this.localEdgeForeignKeys.put(schema + "." + table, foreignKeys);
                            SchemaTable schemaTable = SchemaTable.of(column.split("\\.")[0], SchemaManager.VERTEX_PREFIX + column.split("\\.")[1].replace(SchemaManager.IN_VERTEX_COLUMN_END, "").replace(SchemaManager.OUT_VERTEX_COLUMN_END, ""));
                            Pair<Set<SchemaTable>, Set<SchemaTable>> labels = this.localTableLabels.get(schemaTable);
                            if (labels == null) {
                                labels = Pair.of(new HashSet<>(), new HashSet<>());
                                this.localTableLabels.put(schemaTable, labels);
                            }
                            if (column.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                                labels.getLeft().add(SchemaTable.of(schema, table));
                            } else if (column.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                                labels.getRight().add(SchemaTable.of(schema, table));
                            }
                        }
                    }
                }
            } else {
                //mariadb
                String catalog = null;
                String schemaPattern = null;
                String tableNamePattern = null;
                String[] types = new String[]{"TABLE"};
                ResultSet tablesRs = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
                while (tablesRs.next()) {
                    String db = tablesRs.getString(1);
                    if (!sqlDialect.getDefaultSchemas().contains(db)) {
                        String table = tablesRs.getString(3);
                        final Map<String, PropertyType> uncomitedColumns = new ConcurrentHashMap<>();
                        final Set<String> foreignKeys = new HashSet<>();
                        //get the columns
                        ResultSet columnsRs = metadata.getColumns(catalog, schemaPattern, table, null);
                        while (columnsRs.next()) {
                            String schema = columnsRs.getString(1);
                            this.localSchemas.put(schema, schema);
                            String column = columnsRs.getString(4);
                            int columnType = columnsRs.getInt(5);
                            String typeName = columnsRs.getString("TYPE_NAME");
                            PropertyType propertyType = this.sqlDialect.sqlTypeToPropertyType(columnType, typeName);
                            uncomitedColumns.put(column, propertyType);
                            this.localTables.put(schema + "." + table, uncomitedColumns);
                            Set<String> schemas = this.localLabelSchemas.get(table);
                            if (schemas == null) {
                                schemas = new HashSet<>();
                                this.localLabelSchemas.put(table, schemas);
                            }
                            schemas.add(schema);
                            if (table.startsWith(EDGE_PREFIX) && (column.endsWith(SchemaManager.IN_VERTEX_COLUMN_END) || column.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END))) {
                                foreignKeys.add(column);
                                this.localEdgeForeignKeys.put(schema + "." + table, foreignKeys);
                            }
                        }
                    }
                }
            }
            if (distributed) {
                this.schemas.putAll(this.localSchemas);
                this.labelSchemas.putAll(this.localLabelSchemas);
                this.tables.putAll(this.localTables);
                this.edgeForeignKeys.putAll(this.localEdgeForeignKeys);
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
            Connection conn = this.sqlgGraph.getSqlgDataSource().get(this.sqlDialect.getJdbcDriver()).getConnection();
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
            for (String schemaTable : this.uncommittedEdgeForeignKeys.keySet()) {
                Set<String> foreignKeys = result.get(schemaTable);
                if (foreignKeys == null) {
                    foreignKeys = new HashSet<>();
                }
                foreignKeys.addAll(this.uncommittedEdgeForeignKeys.get(schemaTable));
                foreignKeys.addAll(this.uncommittedEdgeForeignKeys.get(schemaTable));
                result.put(schemaTable, foreignKeys);
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
        Map<String, Map<String, PropertyType>> result = new ConcurrentHashMap<>();
        result.putAll(this.localTables);
        if (!this.uncommittedTables.isEmpty() && this.isLockedByCurrentThread()) {
            result.putAll(this.uncommittedTables);
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
            throw new IllegalStateException("Should not happen!");
        }

        @Override
        public void mapEvicted(MapEvent event) {
            throw new IllegalStateException("Should not happen!");
        }

        @Override
        public void mapCleared(MapEvent event) {
            throw new IllegalStateException("Should not happen!");
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
            throw new IllegalStateException("Should not happen!");
        }

        @Override
        public void mapEvicted(MapEvent event) {
            throw new IllegalStateException("Should not happen!");
        }

        @Override
        public void mapCleared(MapEvent event) {
            throw new IllegalStateException("Should not happen!");
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
            throw new IllegalStateException("Should not happen!");
        }

        @Override
        public void mapEvicted(MapEvent event) {
            throw new IllegalStateException("Should not happen!");
        }

        @Override
        public void mapCleared(MapEvent event) {
            throw new IllegalStateException("Should not happen!");
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
            throw new IllegalStateException("Should not happen!");
        }

        @Override
        public void mapEvicted(MapEvent event) {
            throw new IllegalStateException("Should not happen!");
        }

        @Override
        public void mapCleared(MapEvent event) {
            throw new IllegalStateException("Should not happen!");
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
            throw new IllegalStateException("Should not happen!");
        }

        @Override
        public void mapEvicted(MapEvent event) {
            throw new IllegalStateException("Should not happen!");
        }

        @Override
        public void mapCleared(MapEvent event) {
            throw new IllegalStateException("Should not happen!");
        }
    }

}
