package org.umlg.sqlg.structure;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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

    public static final String VERTEX_PREFIX = "V_";
    public static final String EDGE_PREFIX = "E_";
    public static final String VERTICES = "VERTICES";
    public static final String VERTEX_IN_LABELS = "IN_LABELS";
    public static final String VERTEX_OUT_LABELS = "OUT_LABELS";
    public static final String EDGES = "EDGES";
    private Map<String, Map<String, PropertyType>> schemas = new ConcurrentHashMap<>();
    private Map<String, Map<String, PropertyType>> uncommittedSchemas = new ConcurrentHashMap<>();
    private Map<String, Map<String, PropertyType>> tables = new ConcurrentHashMap<>();
    private Map<String, Map<String, PropertyType>> uncommittedTables = new ConcurrentHashMap<>();
    private Map<String, Set<String>> edgeForeignKeys = new ConcurrentHashMap<>();
    private Map<String, Set<String>> uncommittedEdgeForeignKeys = new ConcurrentHashMap<>();
    private ReentrantLock schemaLock = new ReentrantLock();
    private SqlG sqlG;
    private SqlDialect sqlDialect;

    SchemaManager(SqlG sqlG, SqlDialect sqlDialect) {
        this.sqlG = sqlG;
        this.sqlDialect = sqlDialect;
        this.sqlG.tx().afterCommit(() -> {
            if (this.schemaLock.isHeldByCurrentThread()) {
                for (String t : this.uncommittedSchemas.keySet()) {
                    this.schemas.put(t, this.uncommittedSchemas.get(t));
                }
                for (String t : this.uncommittedTables.keySet()) {
                    this.tables.put(t, this.uncommittedTables.get(t));
                }
                for (String t : this.uncommittedEdgeForeignKeys.keySet()) {
                    this.edgeForeignKeys.put(t, this.uncommittedEdgeForeignKeys.get(t));
                }
                this.uncommittedSchemas.clear();
                this.uncommittedTables.clear();
                this.uncommittedEdgeForeignKeys.clear();
                this.schemaLock.unlock();
            }
        });
        this.sqlG.tx().afterRollback(() -> {
            if (this.schemaLock.isHeldByCurrentThread()) {
                if (this.getSqlDialect().supportsTransactionalSchema()) {
                    this.uncommittedSchemas.clear();
                    this.uncommittedTables.clear();
                    this.uncommittedEdgeForeignKeys.clear();
                } else {
                    for (String t : this.uncommittedSchemas.keySet()) {
                        this.schemas.put(t, this.uncommittedSchemas.get(t));
                    }
                    for (String t : this.uncommittedTables.keySet()) {
                        this.tables.put(t, this.uncommittedTables.get(t));
                    }
                    for (String t : this.uncommittedEdgeForeignKeys.keySet()) {
                        this.edgeForeignKeys.put(t, this.uncommittedEdgeForeignKeys.get(t));
                    }
                    this.uncommittedSchemas.clear();
                    this.uncommittedTables.clear();
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
        final ConcurrentHashMap<String, PropertyType> columns = SqlGUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.tables.containsKey(schema + "." + prefixedTable)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.schemaLock.isHeldByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!this.schemas.containsKey(schema)) {
                 this.schemas.put();
                 createSchema(schema);
            }
            if (!this.tables.containsKey(schema + "." + prefixedTable)) {
                if (!this.uncommittedTables.containsKey(schema + "." + prefixedTable)) {
                    this.uncommittedTables.put(schema + "." + prefixedTable, columns);
                    createVertexTable(schema, prefixedTable, columns);
                }
            }
        }
        //ensure columns exist
        columns.forEach((k, v) -> ensureColumnExist(schema, prefixedTable, ImmutablePair.of(k, v)));
    }

    public void createSchema(String schema) {

    }

    /**
     * @param schema The tables that the table for this edge will reside in.
     * @param table The table for this edge
     * @param foreignKeyIn The tables table pair of foreign key to the in vertex
     * @param foreignKeyOut The tables table pair of foreign key to the out vertex
     * @param keyValues
     */
    public void ensureEdgeTableExist(final String schema, final String table, final Pair<String, String> foreignKeyIn, final Pair<String, String> foreignKeyOut, Object... keyValues) {
        Objects.requireNonNull(schema, "Given tables must not be null");
        Objects.requireNonNull(table, "Given table must not be null");
        Objects.requireNonNull(foreignKeyIn.getRight(), "Given inTable must not be null");
        Objects.requireNonNull(foreignKeyOut.getRight(), "Given outTable must not be null");
        final String prefixedTable = EDGE_PREFIX + table;
        final ConcurrentHashMap<String, PropertyType> columns = SqlGUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.tables.containsKey(schema + "." + prefixedTable)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.schemaLock.isHeldByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!this.tables.containsKey(schema + "." + prefixedTable)) {
                if (!this.uncommittedTables.containsKey(schema + "." + prefixedTable)) {
                    this.uncommittedTables.put(schema + "." + prefixedTable, columns);
                    Set<String> foreignKeys = new HashSet<>();
                    foreignKeys.add(foreignKeyIn.getLeft() + "." + foreignKeyIn.getRight());
                    foreignKeys.add(foreignKeyOut.getLeft() + "." + foreignKeyOut.getRight());
                    this.uncommittedEdgeForeignKeys.put(schema + "." + prefixedTable, foreignKeys);
                    createEdgeTable(schema, prefixedTable, foreignKeyIn, foreignKeyOut, columns);
                }
            }
        }
        //ensure columns exist
        columns.forEach((k, v) -> ensureColumnExist(schema, prefixedTable, ImmutablePair.of(k, v)));
        ensureEdgeForeignKeysExist(schema, prefixedTable, foreignKeyIn);
        ensureEdgeForeignKeysExist(schema, prefixedTable, foreignKeyOut);
    }

    void ensureColumnExist(String schema, String table, ImmutablePair<String, PropertyType> keyValue) {
        final Map<String, PropertyType> cachedColumns = this.tables.get(schema + "." + table);
        final Map<String, PropertyType> uncommitedColumns;
        if (cachedColumns == null) {
            uncommitedColumns = this.uncommittedTables.get(schema + "." + table);
        } else {
            uncommitedColumns = cachedColumns;
        }
        Objects.requireNonNull(uncommitedColumns, "Table must already be present in the cache!");
        if (!uncommitedColumns.containsKey(keyValue.left)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.schemaLock.isHeldByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!uncommitedColumns.containsKey(keyValue.left)) {
                addColumn(schema, table, keyValue);
                uncommitedColumns.put(keyValue.left, keyValue.right);
                this.uncommittedTables.put(schema + "." + table, uncommitedColumns);
            }
        }
    }

    private void ensureEdgeForeignKeysExist(String schema, String table, Pair<String, String> foreignKey) {
        Set<String> foreignKeys = this.edgeForeignKeys.get(schema + "." + table);
        final Set<String> uncommittedForeignKeys;
        if (foreignKeys == null) {
            uncommittedForeignKeys = this.uncommittedEdgeForeignKeys.get(schema + "." + table);
        } else {
            uncommittedForeignKeys = new HashSet<>(foreignKeys);
        }
        Objects.requireNonNull(uncommittedForeignKeys, String.format(
                "Table %s must already be present in the foreign key cache!", new Object[]{table}));
        if (!uncommittedForeignKeys.contains(foreignKey.getLeft() + "." + foreignKey.getRight())) {
            //Make sure the current thread/transaction owns the lock
            if (!this.schemaLock.isHeldByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!uncommittedForeignKeys.contains(foreignKey)) {
                addEdgeForeignKey(schema, table, foreignKey);
                uncommittedForeignKeys.add(foreignKey.getLeft() + "." + foreignKey.getRight());
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
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" (");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes("VERTEX_SCHEMA"));
        sql.append(" VARCHAR(255) NOT NULL, ");
        sql.append(this.sqlDialect.maybeWrapInQoutes("VERTEX_TABLE"));
        sql.append(" VARCHAR(255) NOT NULL, ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTEX_IN_LABELS));
        sql.append(" ");
        sql.append(this.sqlDialect.propertyTypeToSqlDefinition(PropertyType.STRING));
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTEX_OUT_LABELS));
        sql.append(" ");
        sql.append(this.sqlDialect.propertyTypeToSqlDefinition(PropertyType.STRING));
        sql.append(")");
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }
    }

    private boolean edgesExist() throws SQLException {
        return tableExist(this.sqlDialect.getPublicSchema(), SchemaManager.EDGES);
    }

    private void createEdgesTable() throws SQLException {
        StringBuilder sql = new StringBuilder(this.sqlDialect.createTableStatement());
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.EDGES));
        sql.append("(");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes("EDGE_SCHEMA"));
        sql.append(" VARCHAR(255), ");
        sql.append(this.sqlDialect.maybeWrapInQoutes("EDGE_TABLE"));
        sql.append(" VARCHAR(255))");
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }
    }

    public boolean tableExist(String schema, String table) {
        Connection conn = this.sqlG.tx().getConnection();
        DatabaseMetaData metadata;
        try {
            metadata = conn.getMetaData();
            String catalog = null;
            String schemaPattern = schema;
            String tableNamePattern = table;
            String[] types = null;
            ResultSet result = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
            while (result.next()) {
                return true;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
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
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createEdgeTable(String schema, String tableName, Pair<String, String> foreignKeyIn, Pair<String, String> foreignKeyOut, Map<String, PropertyType> columns) {
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
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getLeft() + "." + foreignKeyIn.getRight()));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getLeft() + "." + foreignKeyOut.getRight()));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
        sql.append(", ");
        sql.append("FOREIGN KEY (");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getLeft() + "." + foreignKeyIn.getRight()));
        sql.append(") REFERENCES ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyIn.getLeft()));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + foreignKeyIn.getRight().replace(SqlElement.IN_VERTEX_COLUMN_END, "")));
        sql.append(" (");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append("), ");
        sql.append(" FOREIGN KEY (");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getLeft() + "." + foreignKeyOut.getRight()));
        sql.append(") REFERENCES ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKeyOut.getLeft()));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + foreignKeyOut.getRight().replace(SqlElement.OUT_VERTEX_COLUMN_END, "")));
        sql.append(" (");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append("))");
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void addEdgeLabelToVerticesTable(Long id, String schema, String table, boolean inDirection) {
        Set<Pair<String, String>> labelSet = getLabelsForVertex(id, inDirection);
        labelSet.add(Pair.of(schema, table));
        StringBuilder sql = new StringBuilder("UPDATE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" SET ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(inDirection ? SchemaManager.VERTEX_IN_LABELS : SchemaManager.VERTEX_OUT_LABELS));
        sql.append(" = ?");
        sql.append(" WHERE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" = ?");
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            //varchar here must be lowercase
            int count = 1;
            StringBuilder sb = new StringBuilder();
            for (Pair<String,String> l : labelSet) {
                sb.append(l.getLeft());
                sb.append(".");
                sb.append(l.getRight());
                if (count++ < labelSet.size()) {
                    sb.append(":::");
                }
            }
            preparedStatement.setString(1, sb.toString());
            preparedStatement.setLong(2, id);
            int numberOfRowsUpdated = preparedStatement.executeUpdate();
            if (numberOfRowsUpdated != 1) {
                throw new IllegalStateException("Only one row should ever be updated!");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<Pair<String,String>> getLabelsForVertex(Long id, boolean inDirection) {
        Set<Pair<String, String>> labels = new HashSet<>();
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(inDirection ? SchemaManager.VERTEX_IN_LABELS : SchemaManager.VERTEX_OUT_LABELS));
        sql.append(" FROM ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" WHERE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" = ?");
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String commaSeparatedLabels = resultSet.getString(inDirection ? SchemaManager.VERTEX_IN_LABELS : SchemaManager.VERTEX_OUT_LABELS);
                if (commaSeparatedLabels != null) {
                    String[] schemaLabels = commaSeparatedLabels.split(":::");
                    for (String schemaLabel : schemaLabels) {
                        Pair<String, String> schemaLabelPair = SqlGUtil.parseLabel(schemaLabel, this.sqlDialect.getPublicSchema());
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
        return labels;
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
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void addEdgeForeignKey(String schema, String table, Pair<String, String> foreignKey) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(this.sqlDialect.maybeWrapInQoutes(table));
        sql.append(" ADD COLUMN ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKey.getLeft() + "." + foreignKey.getRight()));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void createIndex(String label, String propertyKey) {

    }

    void loadSchema() {
        try {
            Connection conn = SqlGDataSource.INSTANCE.get(this.sqlG.getJdbcUrl()).getConnection();
            DatabaseMetaData metadata;
            metadata = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = null;
            String[] types = new String[]{"TABLE"};
            ResultSet tablesRs = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
            while (tablesRs.next()) {
                String table = tablesRs.getString(3);
                final Map<String, PropertyType> uncomitedColumns = new ConcurrentHashMap<>();
                final Set<String> foreignKeys = new HashSet<>();
                //get the columns
                ResultSet columnsRs = metadata.getColumns(catalog, schemaPattern, table, null);
                while (columnsRs.next()) {
                    String schema = columnsRs.getString(2);
                    String column = columnsRs.getString(4);
                    int columnType = columnsRs.getInt(5);
                    String typeName = columnsRs.getString("TYPE_NAME");
                    PropertyType propertyType = this.sqlDialect.sqlTypeToPropertyType(columnType, typeName);
                    uncomitedColumns.put(column, propertyType);
                    this.tables.put(schema + "." + table, uncomitedColumns);
                    if (table.startsWith(EDGE_PREFIX) && (column.endsWith(SqlElement.IN_VERTEX_COLUMN_END) || column.endsWith(SqlElement.OUT_VERTEX_COLUMN_END))) {
                        foreignKeys.add(column);
                        this.edgeForeignKeys.put(schema + "." + table, foreignKeys);
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
            Connection conn = SqlGDataSource.INSTANCE.get(this.sqlDialect.getJdbcDriver()).getConnection();
            DatabaseMetaData metadata = null;
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
}
