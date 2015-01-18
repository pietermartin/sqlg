package org.umlg.sqlg.structure;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    public static final String IN_VERTEX_COLUMN_END = "_IN_ID";
    public static final String OUT_VERTEX_COLUMN_END = "_OUT_ID";

    private Map<String, String> schemas;
    private Map<String, String> localSchemas = new HashMap<>();
    private Set<String> uncommittedSchemas = new HashSet<>();

    private Map<String, Set<String>> labelSchemas;
    private Map<String, Set<String>> localLabelSchemas = new HashMap<>();
    private Map<String, Set<String>> uncommittedLabelSchemas = new ConcurrentHashMap<>();

    private Map<String, Map<String, PropertyType>> tables;
    private Map<String, Map<String, PropertyType>> localTables = new HashMap<>();
    private Map<String, Map<String, PropertyType>> uncommittedTables = new ConcurrentHashMap<>();

    private Map<String, Set<String>> edgeForeignKeys;
    private Map<String, Set<String>> localEdgeForeignKeys = new HashMap<>();
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
        return config;

    }

    public SqlDialect getSqlDialect() {
        return sqlDialect;
    }

    /**
     * VERTICES table holds a reference to every vertex.
     * This is to help implement g.v(id) and g.V()
     * This call is not thread safe as it is only called on startup.
     */
    void ensureGlobalVerticesTableExist() {
        try {
            if (!verticesExist()) {
                createVerticesTable();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * EDGES table holds a reference to every edge.
     * This is to help implement g.e(id) and g.E()
     * This call is not thread safe as it is only called on startup.
     */
    void ensureGlobalEdgesTableExist() {
        try {
            if (!edgesExist()) {
                createEdgesTable();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void ensureVertexTableExist(final String schema, final String table, final Object... keyValues) {
        Objects.requireNonNull(schema, "Given tables must not be null");
        Objects.requireNonNull(table, "Given table must not be null");
        final String prefixedTable = VERTEX_PREFIX + table;
        final ConcurrentHashMap<String, PropertyType> columns = SqlgUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.localTables.containsKey(schema + "." + prefixedTable)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.isLockedByCurrentThread()) {
                this.schemaLock.lock();
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
                this.schemaLock.lock();
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
     * This is only called from createVertexIndex
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
                this.schemaLock.lock();
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
                    this.uncommittedLabelSchemas.put(prefixedTable, new HashSet<>(Arrays.asList(schema)));
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

    boolean columnExists(String schema, String table, String column) {
        return internalGetColumn(schema, table).containsKey(column);
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

    void ensureColumnsExist(String schema, String table, ConcurrentHashMap<String, PropertyType> columns) {
        Map<String, PropertyType> uncommitedColumns = internalGetColumn(schema, table);
        Objects.requireNonNull(uncommitedColumns, "Table must already be present in the cache!");

        for (Map.Entry<String, PropertyType> column : columns.entrySet()) {

            String columnName = column.getKey();
            PropertyType columnType = column.getValue();

            if (!uncommitedColumns.containsKey(columnName)) {
                uncommitedColumns = internalGetColumn(schema, table);
            }
            if (!uncommitedColumns.containsKey(columnName)) {
                //Make sure the current thread/transaction owns the lock
                if (!this.isLockedByCurrentThread()) {
                    this.schemaLock.lock();
                }
                if (!uncommitedColumns.containsKey(columnName)) {
                    addColumn(schema, table, ImmutablePair.of(columnName, columnType));
                    uncommitedColumns.put(columnName, columnType);
                    this.uncommittedTables.put(schema + "." + table, uncommitedColumns);
                }
            }

        }

    }

    void ensureColumnExist(String schema, String table, ImmutablePair<String, PropertyType> keyValue) {
        Map<String, PropertyType> uncommitedColumns = internalGetColumn(schema, table);
        Objects.requireNonNull(uncommitedColumns, "Table must already be present in the cache!");
        if (!uncommitedColumns.containsKey(keyValue.left)) {
            uncommitedColumns = internalGetColumn(schema, table);
        }
        if (!uncommitedColumns.containsKey(keyValue.left)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.isLockedByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!uncommitedColumns.containsKey(keyValue.left)) {
                addColumn(schema, table, keyValue);
                uncommitedColumns.put(keyValue.left, keyValue.right);
                this.uncommittedTables.put(schema + "." + table, uncommitedColumns);
            }
        }
    }

    private void ensureEdgeForeignKeysExist(String schema, String table, boolean in, SchemaTable vertexSchemaTable) {
        Set<String> foreignKeys = this.localEdgeForeignKeys.get(schema + "." + table);
        Set<String> uncommittedForeignKeys;
        if (foreignKeys == null) {
            uncommittedForeignKeys = this.uncommittedEdgeForeignKeys.get(schema + "." + table);
        } else {
            uncommittedForeignKeys = this.uncommittedEdgeForeignKeys.get(schema + "." + table);
            if (uncommittedForeignKeys != null) {
                uncommittedForeignKeys.addAll(foreignKeys);
            } else {
                uncommittedForeignKeys = new HashSet<>(foreignKeys);
            }
        }
        if (uncommittedForeignKeys == null) {
            //This happens on edge indexes which creates the table with no foreign keys to vertices.
            uncommittedForeignKeys = new HashSet<>();
        }
        SchemaTable foreignKey = SchemaTable.of(vertexSchemaTable.getSchema(), vertexSchemaTable.getTable() + (in ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END));
        if (!uncommittedForeignKeys.contains(foreignKey.getSchema() + "." + foreignKey.getTable())) {
            //Make sure the current thread/transaction owns the lock
            if (!this.isLockedByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!uncommittedForeignKeys.contains(foreignKey)) {
                addEdgeForeignKey(schema, table, foreignKey);
                uncommittedForeignKeys.add(vertexSchemaTable.getSchema() + "." + foreignKey.getTable());
                this.uncommittedEdgeForeignKeys.put(schema + "." + table, uncommittedForeignKeys);

                //For each table capture its in and out labels
                //This schema information is needed for compiling gremlin
                //first do the in labels
                SchemaTable foreignKeyInWithPrefix = SchemaTable.of(vertexSchemaTable.getSchema(), SchemaManager.VERTEX_PREFIX + vertexSchemaTable.getTable());
                Pair<Set<SchemaTable>, Set<SchemaTable>> labels = this.uncommittedTableLabels.get(foreignKeyInWithPrefix);
                if (labels == null) {
                    labels = Pair.of(new HashSet<>(), new HashSet<>());
                    this.uncommittedTableLabels.put(foreignKeyInWithPrefix, labels);
                }
                if (in) {
                    labels.getLeft().add(SchemaTable.of(schema, table));
                } else {
                    labels.getRight().add(SchemaTable.of(schema, table));
                }
            }
        }
    }

    public void close() {
        if (this.distributed) {
            this.hazelcastInstance.shutdown();
        }
    }

    private boolean verticesExist() throws SQLException {
        return tableExist(this.sqlDialect.getPublicSchema(), SchemaManager.VERTICES);
    }

    private void createVerticesTable() throws SQLException {
        StringBuilder sql = new StringBuilder(this.sqlDialect.createTableStatement());
        sql.append(this.sqlDialect.maybeWrapInQoutes(this.sqlDialect.getPublicSchema()));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" (");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(VERTEX_SCHEMA));
        sql.append(" VARCHAR(255) NOT NULL, ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(VERTEX_TABLE));
        sql.append(" VARCHAR(255) NOT NULL, ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTEX_IN_LABELS));
        sql.append(" ");
        sql.append(this.sqlDialect.propertyTypeToSqlDefinition(PropertyType.STRING));
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTEX_OUT_LABELS));
        sql.append(" ");
        sql.append(this.sqlDialect.propertyTypeToSqlDefinition(PropertyType.STRING));
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
        }
    }

    private boolean edgesExist() throws SQLException {
        return tableExist(this.sqlDialect.getPublicSchema(), SchemaManager.EDGES);
    }

    private void createEdgesTable() throws SQLException {
        StringBuilder sql = new StringBuilder(this.sqlDialect.createTableStatement());
        sql.append(this.sqlDialect.maybeWrapInQoutes(this.sqlDialect.getPublicSchema()));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.EDGES));
        sql.append(" (");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(EDGE_SCHEMA));
        sql.append(" VARCHAR(255), ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(EDGE_TABLE));
        sql.append(" VARCHAR(255))");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }
    }

    private void createVertexTable(String schema, String tableName, Map<String, PropertyType> columns) {
        this.sqlDialect.assertTableName(tableName);
        StringBuilder sql = new StringBuilder(this.sqlDialect.createTableStatement());
        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
        sql.append(" (");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getPrimaryKeyType());
        if (this.sqlgGraph.isImplementForeignKeys()) {
            sql.append(" REFERENCES ");
            sql.append(this.sqlDialect.maybeWrapInQoutes(this.sqlDialect.getPublicSchema()));
            sql.append(".");
            sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTICES));
        }
        if (columns.size() > 0) {
            sql.append(", ");
        }
        int i = 1;
        for (String column : columns.keySet()) {
            PropertyType propertyType = columns.get(column);
            //Columns map 1 to 1 to property keys and are case sensitive
            sql.append(this.sqlDialect.maybeWrapInQoutes(column)).append(" ").append(this.sqlDialect.propertyTypeToSqlDefinition(propertyType));
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
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

    private void createEdgeTable(String schema, String tableName, SchemaTable foreignKeyIn, SchemaTable foreignKeyOut, Map<String, PropertyType> columns) {
        this.sqlDialect.assertTableName(tableName);
        StringBuilder sql = new StringBuilder(this.sqlDialect.createTableStatement());
        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
        sql.append("(");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getPrimaryKeyType());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        int i = 1;
        for (String column : columns.keySet()) {
            PropertyType propertyType = columns.get(column);
            //Columns map 1 to 1 to property keys and are case sensitive
            sql.append(this.sqlDialect.maybeWrapInQoutes(column)).append(" ").append(this.sqlDialect.propertyTypeToSqlDefinition(propertyType));
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());

        //foreign key definition start
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

    private void createEdgeTable(String schema, String tableName, Map<String, PropertyType> columns) {
        this.sqlDialect.assertTableName(tableName);
        StringBuilder sql = new StringBuilder(this.sqlDialect.createTableStatement());
        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
        sql.append("(");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getPrimaryKeyType());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        int i = 1;
        for (String column : columns.keySet()) {
            PropertyType propertyType = columns.get(column);
            //Columns map 1 to 1 to property keys and are case sensitive
            sql.append(this.sqlDialect.maybeWrapInQoutes(column)).append(" ").append(this.sqlDialect.propertyTypeToSqlDefinition(propertyType));
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
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

    public void addEdgeLabelToVerticesTable(SqlgVertex sqlgVertex, String schema, String table, boolean inDirection) {
        Long id = (Long) sqlgVertex.id();
        SchemaTable schemaTable = SchemaTable.of(schema, table);

        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            this.sqlgGraph.tx().getBatchManager().updateVertexCacheWithEdgeLabel(sqlgVertex, schemaTable, inDirection);
        } else {
            Set<SchemaTable> labelSet = getLabelsForVertex(sqlgVertex, inDirection);
            if (!labelSet.contains(schemaTable)) {
                labelSet.add(schemaTable);

                StringBuilder sql = new StringBuilder("UPDATE ");
                sql.append(this.sqlDialect.maybeWrapInQoutes(this.sqlDialect.getPublicSchema()));
                sql.append(".");
                sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTICES));
                sql.append(" SET ");
                sql.append(this.sqlDialect.maybeWrapInQoutes(inDirection ? SchemaManager.VERTEX_IN_LABELS : SchemaManager.VERTEX_OUT_LABELS));
                sql.append(" = ?");
                sql.append(" WHERE ");
                sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
                sql.append(" = ?");
                if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                    sql.append(";");
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(sql.toString());
                }
                Connection conn = this.sqlgGraph.tx().getConnection();
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                    //varchar here must be lowercase
                    int count = 1;
                    StringBuilder sb = new StringBuilder();
                    for (SchemaTable l : labelSet) {
                        sb.append(l.getSchema());
                        sb.append(".");
                        sb.append(l.getTable());
                        if (count++ < labelSet.size()) {
                            sb.append(LABEL_SEPARATOR);
                        }
                    }
                    preparedStatement.setString(1, sb.toString());
                    preparedStatement.setLong(2, id);
                    int numberOfRowsUpdated = preparedStatement.executeUpdate();
                    if (numberOfRowsUpdated != 1) {
                        throw new IllegalStateException(String.format("Only one row should ever be updated! #updated = %d", new Integer[]{numberOfRowsUpdated}));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public Set<SchemaTable> getLabelsForVertex(SqlgVertex sqlgVertex, boolean inDirection) {

        //the inLabelsForVertex and outLabelsForVertex sets are initialized to null to distinguish between having been loaded and having no labels
        if ((inDirection && sqlgVertex.inLabelsForVertex == null) || (!inDirection && sqlgVertex.outLabelsForVertex == null)) {
            Long id = (Long) sqlgVertex.id();
            Set<SchemaTable> labels = new HashSet<>();
            StringBuilder sql = new StringBuilder("SELECT ");
            sql.append(this.sqlDialect.maybeWrapInQoutes(inDirection ? SchemaManager.VERTEX_IN_LABELS : SchemaManager.VERTEX_OUT_LABELS));
            sql.append(" FROM ");
            sql.append(this.sqlDialect.maybeWrapInQoutes(this.sqlDialect.getPublicSchema()));
            sql.append(".");
            sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTICES));
            sql.append(" WHERE ");
            sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
            sql.append(" = ?");
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            Connection conn = this.sqlgGraph.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.setLong(1, id);
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    String commaSeparatedLabels = resultSet.getString(inDirection ? SchemaManager.VERTEX_IN_LABELS : SchemaManager.VERTEX_OUT_LABELS);
                    convertVertexLabelToSet(labels, commaSeparatedLabels);
                    if (resultSet.next()) {
                        throw new IllegalStateException("BUG: There can only be one row per vertex id!");
                    }
                }

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            if (inDirection) {
                sqlgVertex.inLabelsForVertex = new HashSet<>();
                sqlgVertex.inLabelsForVertex.addAll(labels);
            } else {
                sqlgVertex.outLabelsForVertex = new HashSet<>();
                sqlgVertex.outLabelsForVertex.addAll(labels);
            }
        }
        if (inDirection) {
            return sqlgVertex.inLabelsForVertex;
        } else {
            return sqlgVertex.outLabelsForVertex;
        }
    }

    void convertVertexLabelToSet(Set<SchemaTable> labels, String commaSeparatedLabels) throws SQLException {
        if (commaSeparatedLabels != null) {
            String[] schemaLabels = commaSeparatedLabels.split(LABEL_SEPARATOR);
            for (String schemaLabel : schemaLabels) {
                SchemaTable schemaLabelPair = SqlgUtil.parseLabel(schemaLabel, this.sqlDialect.getPublicSchema());
                labels.add(schemaLabelPair);
            }
        }
    }

    private void addColumn(String schema, String table, ImmutablePair<String, PropertyType> keyValue) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(table));
        sql.append(" ADD ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(keyValue.left));
        sql.append(" ");
        sql.append(this.sqlDialect.propertyTypeToSqlDefinition(keyValue.right));
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
                    Map<String, PropertyType> uncommittedColumns = new ConcurrentHashMap<>();
                    Set<String> foreignKeys = null;
                    //get the columns
                    String previousSchema = "";
                    ResultSet columnsRs = metadata.getColumns(catalog, schemaPattern, table, null);
                    while (columnsRs.next()) {
                        String schema = columnsRs.getString(2);
                        this.localSchemas.put(schema, schema);
                        if (!previousSchema.equals(schema)) {
                            foreignKeys = new HashSet<>();
                            uncommittedColumns = new ConcurrentHashMap<>();
                        }
                        previousSchema = schema;
                        String column = columnsRs.getString(4);
                        int columnType = columnsRs.getInt(5);
                        String typeName = columnsRs.getString("TYPE_NAME");
                        PropertyType propertyType = this.sqlDialect.sqlTypeToPropertyType(columnType, typeName);
                        uncommittedColumns.put(column, propertyType);
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
                                labels.getLeft().add(SchemaTable.of(schema, table.replace(SchemaManager.EDGE_PREFIX, "")));
                            } else if (column.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                                labels.getRight().add(SchemaTable.of(schema, table.replace(SchemaManager.EDGE_PREFIX, "")));
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
            Connection conn = SqlgDataSource.INSTANCE.get(this.sqlDialect.getJdbcDriver()).getConnection();
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

    Set<String> getSchemasForTable(String table) {
        Set<String> labels = this.localLabelSchemas.get(table);
        Set<String> uncommittedLabels = this.uncommittedLabelSchemas.get(table);
        Set<String> result = new HashSet<>();
        if (labels != null) {
            result.addAll(labels);
        }
        if (uncommittedLabels != null) {
            result.addAll(uncommittedLabels);
        }
        return result;
    }

    Set<String> getEdgeForeignKeys(String schemaTable) {
        Map<String, Set<String>> allForeignKeys = new HashMap<>();
        allForeignKeys.putAll(this.localEdgeForeignKeys);
        allForeignKeys.putAll(this.uncommittedEdgeForeignKeys);
        return allForeignKeys.get(schemaTable);
    }

    public Map<String, Set<String>> getEdgeForeignKeys() {
        return this.localEdgeForeignKeys;
    }

    public Map<SchemaTable, Pair<Set<SchemaTable>, Set<SchemaTable>>> getLocalTableLabels() {
        return localTableLabels;
    }

    public Map<String, Map<String, PropertyType>> getLocalTables() {
        return localTables;
    }

    class SchemasMapEntryListener implements EntryListener<String, String> {

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

    class LabelSchemasMapEntryListener implements EntryListener<String, Set<String>> {

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

    class TablesMapEntryListener implements EntryListener<String, Map<String, PropertyType>> {

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

    class EdgeForeignKeysMapEntryListener implements EntryListener<String, Set<String>> {

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

    private boolean isLockedByCurrentThread() {
        if (this.distributed) {
            return ((ILock) this.schemaLock).isLockedByCurrentThread();
        } else {
            return ((ReentrantLock) this.schemaLock).isHeldByCurrentThread();
        }
    }
}
