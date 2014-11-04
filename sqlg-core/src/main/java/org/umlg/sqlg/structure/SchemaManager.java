package org.umlg.sqlg.structure;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    public static final String LABEL_SEPERATOR = ":::";
    public static final String IN_VERTEX_COLUMN_END = "_IN_ID";
    public static final String OUT_VERTEX_COLUMN_END = "_OUT_ID";

    private Set<String> schemas = new HashSet<>();
    private Set<String> uncommittedSchemas = new HashSet<>();

    private Map<String, Set<String>> labelSchemas = new ConcurrentHashMap<>();
    private Map<String, Set<String>> uncommittedLabelSchemas = new ConcurrentHashMap<>();
    private Map<String, Map<String, PropertyType>> tables = new ConcurrentHashMap<>();
    private Map<String, Map<String, PropertyType>> uncommittedTables = new ConcurrentHashMap<>();
    private Map<String, Set<String>> edgeForeignKeys = new ConcurrentHashMap<>();
    private Map<String, Set<String>> uncommittedEdgeForeignKeys = new ConcurrentHashMap<>();
    private ReentrantLock schemaLock = new ReentrantLock();
    private SqlgGraph sqlgGraph;
    private SqlDialect sqlDialect;

    SchemaManager(SqlgGraph sqlgGraph, SqlDialect sqlDialect) {
        this.sqlgGraph = sqlgGraph;
        this.sqlDialect = sqlDialect;
        this.sqlgGraph.tx().afterCommit(() -> {
            if (this.schemaLock.isHeldByCurrentThread()) {
                for (String t : this.uncommittedSchemas) {
                    this.schemas.add(t);
                }
                for (String t : this.uncommittedTables.keySet()) {
                    this.tables.put(t, this.uncommittedTables.get(t));
                }
                for (String t : this.uncommittedLabelSchemas.keySet()) {
                    Set<String> schemas = this.labelSchemas.get(t);
                    if (schemas == null) {
                        this.labelSchemas.put(t, this.uncommittedLabelSchemas.get(t));
                    } else {
                        schemas.addAll(this.uncommittedLabelSchemas.get(t));
                        this.labelSchemas.put(t, schemas);
                    }
                }
                for (String t : this.uncommittedEdgeForeignKeys.keySet()) {
                    this.edgeForeignKeys.put(t, this.uncommittedEdgeForeignKeys.get(t));
                }
                this.uncommittedSchemas.clear();
                this.uncommittedTables.clear();
                this.uncommittedLabelSchemas.clear();
                this.uncommittedEdgeForeignKeys.clear();
                this.schemaLock.unlock();
            }
        });
        this.sqlgGraph.tx().afterRollback(() -> {
            if (this.schemaLock.isHeldByCurrentThread()) {
                if (this.getSqlDialect().supportsTransactionalSchema()) {
                    this.uncommittedSchemas.clear();
                    this.uncommittedTables.clear();
                    this.uncommittedLabelSchemas.clear();
                    this.uncommittedEdgeForeignKeys.clear();
                } else {
                    for (String t : this.uncommittedSchemas) {
                        this.schemas.add(t);
                    }
                    for (String t : this.uncommittedTables.keySet()) {
                        this.tables.put(t, this.uncommittedTables.get(t));
                    }
                    for (String t : this.uncommittedLabelSchemas.keySet()) {
                        Set<String> schemas = this.labelSchemas.get(t);
                        if (schemas == null) {
                            this.labelSchemas.put(t, this.uncommittedLabelSchemas.get(t));
                        } else {
                            schemas.addAll(this.uncommittedLabelSchemas.get(t));
                            this.labelSchemas.put(t, schemas);
                        }
                    }
                    for (String t : this.uncommittedEdgeForeignKeys.keySet()) {
                        this.edgeForeignKeys.put(t, this.uncommittedEdgeForeignKeys.get(t));
                    }
                    this.uncommittedSchemas.clear();
                    this.uncommittedTables.clear();
                    this.uncommittedLabelSchemas.clear();
                    this.uncommittedEdgeForeignKeys.clear();
                }
                this.schemaLock.unlock();
            }
        });
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
        if (!this.tables.containsKey(schema + "." + prefixedTable) && !this.uncommittedTables.containsKey(schema + "." + prefixedTable)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.schemaLock.isHeldByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!this.sqlgGraph.tx().isSchemaModification()) {
                loadSchema();
            }
            if (!this.tables.containsKey(schema + "." + prefixedTable)) {

                if (!this.sqlDialect.getPublicSchema().equals(schema) && !this.schemas.contains(schema)) {
                    if (!this.uncommittedSchemas.contains(schema)) {
                        this.uncommittedSchemas.add(schema);
                        createSchema(schema);
                    }
                }
                if (!this.tables.containsKey(schema + "." + prefixedTable)) {
                    if (!this.uncommittedTables.containsKey(schema + "." + prefixedTable)) {
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
            }
        }
        //ensure columns exist
        columns.forEach((k, v) -> ensureColumnExist(schema, prefixedTable, ImmutablePair.of(k, v)));
    }

    boolean schemaExist(String schema) {
        return this.schemas.contains(schema);
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
        if (!this.tables.containsKey(schema + "." + prefixedTable) && !this.uncommittedTables.containsKey(schema + "." + prefixedTable)) {
            if (!this.tables.containsKey(schema + "." + prefixedTable)) {
                //Make sure the current thread/transaction owns the lock
                if (!this.schemaLock.isHeldByCurrentThread()) {
                    this.schemaLock.lock();
                }
                if (!this.sqlgGraph.tx().isSchemaModification()) {
                    loadSchema();
                }
                if (!this.sqlDialect.getPublicSchema().equals(schema) && !this.schemas.contains(schema)) {
                    if (!this.uncommittedSchemas.contains(schema)) {
                        this.uncommittedSchemas.add(schema);
                        createSchema(schema);
                    }
                }
                if (!this.tables.containsKey(schema + "." + prefixedTable)) {
                    if (!this.uncommittedTables.containsKey(schema + "." + prefixedTable)) {
                        this.uncommittedTables.put(schema + "." + prefixedTable, columns);
                        Set<String> foreignKeys = new HashSet<>();
                        foreignKeys.add(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable());
                        foreignKeys.add(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable());
                        this.uncommittedEdgeForeignKeys.put(schema + "." + prefixedTable, foreignKeys);

                        Set<String> schemas = this.uncommittedLabelSchemas.get(prefixedTable);
                        if (schemas == null) {
                            this.uncommittedLabelSchemas.put(prefixedTable, new HashSet<>(Arrays.asList(schema)));
                        } else {
                            schemas.add(schema);
                            this.uncommittedLabelSchemas.put(prefixedTable, schemas);
                        }

                        createEdgeTable(schema, prefixedTable, foreignKeyIn, foreignKeyOut, columns);
                    }
                }
            }
        }
        //ensure columns exist
        columns.forEach((k, v) -> ensureColumnExist(schema, prefixedTable, ImmutablePair.of(k, v)));
        ensureEdgeForeignKeysExist(schema, prefixedTable, foreignKeyIn);
        ensureEdgeForeignKeysExist(schema, prefixedTable, foreignKeyOut);
    }

    /**
     * This is only called from createIndex
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
        if (!this.tables.containsKey(schema + "." + prefixedTable)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.schemaLock.isHeldByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!this.sqlDialect.getPublicSchema().equals(schema) && !this.schemas.contains(schema)) {
                if (!this.uncommittedSchemas.contains(schema)) {
                    this.uncommittedSchemas.add(schema);
                    createSchema(schema);
                }
            }
            if (!this.tables.containsKey(schema + "." + prefixedTable)) {
                if (!this.uncommittedTables.containsKey(schema + "." + prefixedTable)) {
                    this.uncommittedTables.put(schema + "." + prefixedTable, columns);

                    Set<String> schemas = this.uncommittedLabelSchemas.get(prefixedTable);
                    if (schemas == null) {
                        this.uncommittedLabelSchemas.put(prefixedTable, new HashSet<>(Arrays.asList(schema)));
                    } else {
                        schemas.add(schema);
                        this.uncommittedLabelSchemas.put(prefixedTable, schemas);
                    }

                    createEdgeTable(schema, prefixedTable, columns);
                }
            }
        }
        //ensure columns exist
        columns.forEach((k, v) -> ensureColumnExist(schema, prefixedTable, ImmutablePair.of(k, v)));
    }

    boolean columnExists(String schema, String table, String column) {
        return internalGetColumn(schema, table).containsKey(column);
    }

    private Map<String, PropertyType> internalGetColumn(String schema, String table) {
        final Map<String, PropertyType> cachedColumns = this.tables.get(schema + "." + table);
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

    void ensureColumnExist(String schema, String table, ImmutablePair<String, PropertyType> keyValue) {
        Map<String, PropertyType> uncommitedColumns = internalGetColumn(schema, table);
        Objects.requireNonNull(uncommitedColumns, "Table must already be present in the cache!");
        if (!uncommitedColumns.containsKey(keyValue.left)) {
            uncommitedColumns = internalGetColumn(schema, table);
        }
        if (!uncommitedColumns.containsKey(keyValue.left)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.schemaLock.isHeldByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!this.sqlgGraph.tx().isSchemaModification()) {
                loadSchema();
                uncommitedColumns = internalGetColumn(schema, table);
            }
            if (!uncommitedColumns.containsKey(keyValue.left)) {
                addColumn(schema, table, keyValue);
                uncommitedColumns.put(keyValue.left, keyValue.right);
                this.uncommittedTables.put(schema + "." + table, uncommitedColumns);
            }
        }
    }

    private void ensureEdgeForeignKeysExist(String schema, String table, SchemaTable foreignKey) {
        Set<String> foreignKeys = this.edgeForeignKeys.get(schema + "." + table);
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
        if (!uncommittedForeignKeys.contains(foreignKey.getSchema() + "." + foreignKey.getTable())) {
            //Make sure the current thread/transaction owns the lock
            if (!this.schemaLock.isHeldByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!uncommittedForeignKeys.contains(foreignKey)) {
                addEdgeForeignKey(schema, table, foreignKey);
                uncommittedForeignKeys.add(foreignKey.getSchema() + "." + foreignKey.getTable());
                this.uncommittedEdgeForeignKeys.put(schema + "." + table, uncommittedForeignKeys);
            }
        }
    }

    public void close() {
        this.tables.clear();
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
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable()));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable()));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());

        //foreign key definition start
        sql.append(", ");
        sql.append("FOREIGN KEY (");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable()));
        sql.append(") REFERENCES ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema()));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + foreignKeyIn.getTable().replace(SchemaManager.IN_VERTEX_COLUMN_END, "")));
        sql.append(" (");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append("), ");
        sql.append(" FOREIGN KEY (");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable()));
        sql.append(") REFERENCES ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema()));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + foreignKeyOut.getTable().replace(SchemaManager.OUT_VERTEX_COLUMN_END, "")));
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
            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getSchema() + "." + foreignKeyIn.getTable()));
            sql.append(");");

            sql.append("\nCREATE INDEX ON ");
            sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
            sql.append(".");
            sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
            sql.append(" (");
            sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getSchema() + "." + foreignKeyOut.getTable()));
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
    }

    private String foreignKeyNameStrategy(SchemaTable foreignKeyIn, String referenceColumn) {
        return foreignKeyIn.toString() + "_" + referenceColumn;
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
                            sb.append(LABEL_SEPERATOR);
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
                    if (commaSeparatedLabels != null) {
                        String[] schemaLabels = commaSeparatedLabels.split(LABEL_SEPERATOR);
                        for (String schemaLabel : schemaLabels) {
                            SchemaTable schemaLabelPair = SqlgUtil.parseLabel(schemaLabel, this.sqlDialect.getPublicSchema());
                            labels.add(schemaLabelPair);
                        }
                        if (resultSet.next()) {
                            throw new IllegalStateException("BUG: There can only be one row per vertex id!");
                        }
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
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
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

    public void createIndex(SchemaTable schemaTable, Object... dummykeyValues) {

        this.ensureVertexTableExist(schemaTable.getSchema(), schemaTable.getTable(), dummykeyValues);
        this.ensureEdgeTableExist(schemaTable.getSchema(), schemaTable.getTable(), dummykeyValues);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().readWrite();

        int i = 1;
        String[] prefixes = new String[]{VERTEX_PREFIX, EDGE_PREFIX};
        for (String prefix : prefixes) {
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
        boolean exists = this.tables.containsKey(schema + "." + table) || this.uncommittedTables.containsKey(schema + "." + table);
        if (!exists) {
            if (!this.sqlgGraph.tx().isSchemaModification()) {
                loadSchema();
            }
            exists = this.tables.containsKey(schema + "." + table) || this.uncommittedTables.containsKey(schema + "." + table);
        }
        return exists;
    }

    void loadSchema() {
        try {
            Connection conn = SqlgDataSource.INSTANCE.get(this.sqlgGraph.getJdbcUrl()).getConnection();
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
                    final Map<String, PropertyType> uncommittedColumns = new ConcurrentHashMap<>();
                    Set<String> foreignKeys = null;
                    //get the columns
                    String previousSchema = "";
                    ResultSet columnsRs = metadata.getColumns(catalog, schemaPattern, table, null);
                    while (columnsRs.next()) {
                        String schema = columnsRs.getString(2);
                        this.schemas.add(schema);
                        if (!previousSchema.equals(schema)) {
                            foreignKeys = new HashSet<>();
                        }
                        previousSchema = schema;
                        String column = columnsRs.getString(4);
                        int columnType = columnsRs.getInt(5);
                        String typeName = columnsRs.getString("TYPE_NAME");
                        PropertyType propertyType = this.sqlDialect.sqlTypeToPropertyType(columnType, typeName);
                        uncommittedColumns.put(column, propertyType);
                        this.tables.put(schema + "." + table, uncommittedColumns);
                        Set<String> schemas = this.labelSchemas.get(table);
                        if (schemas == null) {
                            schemas = new HashSet<>();
                            this.labelSchemas.put(table, schemas);
                        }
                        schemas.add(schema);
                        if (table.startsWith(EDGE_PREFIX) && (column.endsWith(SchemaManager.IN_VERTEX_COLUMN_END) || column.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END))) {
                            foreignKeys.add(column);
                            this.edgeForeignKeys.put(schema + "." + table, foreignKeys);
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
                            this.schemas.add(schema);
                            String column = columnsRs.getString(4);
                            int columnType = columnsRs.getInt(5);
                            String typeName = columnsRs.getString("TYPE_NAME");
                            PropertyType propertyType = this.sqlDialect.sqlTypeToPropertyType(columnType, typeName);
                            uncomitedColumns.put(column, propertyType);
                            this.tables.put(schema + "." + table, uncomitedColumns);
                            Set<String> schemas = this.labelSchemas.get(table);
                            if (schemas == null) {
                                schemas = new HashSet<>();
                                this.labelSchemas.put(table, schemas);
                            }
                            schemas.add(schema);
                            if (table.startsWith(EDGE_PREFIX) && (column.endsWith(SchemaManager.IN_VERTEX_COLUMN_END) || column.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END))) {
                                foreignKeys.add(column);
                                this.edgeForeignKeys.put(schema + "." + table, foreignKeys);
                            }
                        }
                    }
                }
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
        Set<String> labels = this.labelSchemas.get(table);
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
        allForeignKeys.putAll(this.edgeForeignKeys);
        allForeignKeys.putAll(this.uncommittedEdgeForeignKeys);
        return allForeignKeys.get(schemaTable);
    }

    public Map<String, Set<String>> getEdgeForeignKeys() {
        return this.edgeForeignKeys;
    }

}
