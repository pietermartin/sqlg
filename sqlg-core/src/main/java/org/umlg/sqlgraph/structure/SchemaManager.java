package org.umlg.sqlgraph.structure;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.umlg.sqlgraph.sql.dialect.SqlDialect;

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
    private Map<String, Map<String, PropertyType>> schema = new ConcurrentHashMap<>();
    private Map<String, Map<String, PropertyType>> uncommittedSchema = new ConcurrentHashMap<>();
    private Map<String, Set<String>> edgeForeignKeys = new ConcurrentHashMap<>();
    private Map<String, Set<String>> uncommittedEdgeForeignKeys = new ConcurrentHashMap<>();
    private ReentrantLock schemaLock = new ReentrantLock();
    private SqlGraph sqlGraph;
    private SqlDialect sqlDialect;

    SchemaManager(SqlGraph sqlGraph, SqlDialect sqlDialect) {
        this.sqlGraph = sqlGraph;
        this.sqlDialect = sqlDialect;
        this.sqlGraph.tx().afterCommit(() -> {
            if (this.schemaLock.isHeldByCurrentThread()) {
                for (String t : this.uncommittedSchema.keySet()) {
                    this.schema.put(t, this.uncommittedSchema.get(t));
                }
                for (String t : this.uncommittedEdgeForeignKeys.keySet()) {
                    this.edgeForeignKeys.put(t, this.uncommittedEdgeForeignKeys.get(t));
                }
                this.uncommittedSchema.clear();
                this.uncommittedEdgeForeignKeys.clear();
                this.schemaLock.unlock();
            }
        });
        this.sqlGraph.tx().afterRollback(() -> {
            if (this.schemaLock.isHeldByCurrentThread()) {
                if (this.getSqlDialect().supportsTransactionalSchema()) {
                    this.uncommittedSchema.clear();
                    this.uncommittedEdgeForeignKeys.clear();
                } else {
                    for (String t : this.uncommittedSchema.keySet()) {
                        this.schema.put(t, this.uncommittedSchema.get(t));
                    }
                    for (String t : this.uncommittedEdgeForeignKeys.keySet()) {
                        this.edgeForeignKeys.put(t, this.uncommittedEdgeForeignKeys.get(t));
                    }
                    this.uncommittedSchema.clear();
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

    public void ensureVertexTableExist(final String table, final Object... keyValues) {
        Objects.requireNonNull(table, "Given table must not be null");
        final String prefixedTable = VERTEX_PREFIX + table;
        final ConcurrentHashMap<String, PropertyType> columns = SqlUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.schema.containsKey(prefixedTable)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.schemaLock.isHeldByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!this.schema.containsKey(prefixedTable)) {
                if (!this.uncommittedSchema.containsKey(prefixedTable)) {
                    this.uncommittedSchema.put(prefixedTable, columns);
                    createVertexTable(prefixedTable, columns);
                }
            }
        }
        //ensure columns exist
        columns.forEach((k, v) -> ensureColumnExist(prefixedTable, ImmutablePair.of(k, v)));
    }

    public void ensureEdgeTableExist(final String table, final Pair<String, String> foreignKey, Object... keyValues) {
        Objects.requireNonNull(table, "Given table must not be null");
        Objects.requireNonNull(table, "Given inTable must not be null");
        Objects.requireNonNull(table, "Given outTable must not be null");
        final String prefixedTable = EDGE_PREFIX + table;
        final ConcurrentHashMap<String, PropertyType> columns = SqlUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.schema.containsKey(prefixedTable)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.schemaLock.isHeldByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!this.schema.containsKey(prefixedTable)) {
                if (!this.uncommittedSchema.containsKey(prefixedTable)) {
                    this.uncommittedSchema.put(prefixedTable, columns);
                    Set<String> foreignKeys = new HashSet<>();
                    foreignKeys.add(foreignKey.getLeft());
                    foreignKeys.add(foreignKey.getRight());
                    this.uncommittedEdgeForeignKeys.put(prefixedTable, foreignKeys);
                    createEdgeTable(prefixedTable, foreignKey, columns);
                }
            }
        }
        //ensure columns exist
        columns.forEach((k, v) -> ensureColumnExist(prefixedTable, ImmutablePair.of(k, v)));
        ensureEdgeForeignKeysExist(prefixedTable, foreignKey.getLeft());
        ensureEdgeForeignKeysExist(prefixedTable, foreignKey.getRight());
    }

    void ensureColumnExist(String table, ImmutablePair<String, PropertyType> keyValue) {
        final Map<String, PropertyType> cachedColumns = this.schema.get(table);
        final Map<String, PropertyType> uncommitedColumns;
        if (cachedColumns == null) {
            uncommitedColumns = this.uncommittedSchema.get(table);
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
                addColumn(table, keyValue);
                uncommitedColumns.put(keyValue.left, keyValue.right);
                this.uncommittedSchema.put(table, uncommitedColumns);
            }
        }
    }

    private void ensureEdgeForeignKeysExist(String table, String foreignKey) {
        Set<String> foreignKeys = this.edgeForeignKeys.get(table);
        final Set<String> uncommittedForeignKeys;
        if (foreignKeys == null) {
            uncommittedForeignKeys = this.uncommittedEdgeForeignKeys.get(table);
        } else {
            uncommittedForeignKeys = new HashSet<>(foreignKeys);
        }
        Objects.requireNonNull(uncommittedForeignKeys, String.format(
                "Table %s must already be present in the foreign key cache!", new String[]{table}));
        if (!uncommittedForeignKeys.contains(foreignKey)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.schemaLock.isHeldByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!uncommittedForeignKeys.contains(foreignKey)) {
                addEdgeForeignKey(table, foreignKey);
                uncommittedForeignKeys.add(foreignKey);
                this.uncommittedEdgeForeignKeys.put(table, uncommittedForeignKeys);
            }
        }
    }

    public void close() {
        this.schema.clear();
    }

    private boolean verticesExist() throws SQLException {
        return tableExist(SchemaManager.VERTICES);
    }

    private void createVerticesTable() throws SQLException {
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" (");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        sql.append(", VERTEX_TABLE VARCHAR(255) NOT NULL, ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTEX_IN_LABELS));
        sql.append(" ");
        sql.append(this.sqlDialect.propertyTypeToSqlDefinition(PropertyType.STRING));
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTEX_OUT_LABELS));
        sql.append(" ");
        sql.append(this.sqlDialect.propertyTypeToSqlDefinition(PropertyType.STRING));
        sql.append(")");
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }
    }

    private boolean edgesExist() throws SQLException {
        return tableExist(SchemaManager.EDGES);
    }

    private void createEdgesTable() throws SQLException {
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.EDGES));
        sql.append("(");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes("EDGE_TABLE"));
        sql.append(" VARCHAR(255))");
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }
    }

    public boolean tableExist(String table) {
        Connection conn = this.sqlGraph.tx().getConnection();
        DatabaseMetaData metadata;
        try {
            metadata = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
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

    private void createVertexTable(String tableName, Map<String, PropertyType> columns) {
        this.sqlDialect.assertTableName(tableName);
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
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
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createEdgeTable(String tableName, Pair<String, String> foreignKey, Map<String, PropertyType> columns) {
        this.sqlDialect.assertTableName(tableName);
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
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
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKey.getLeft()));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKey.getRight()));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
        sql.append(", ");
        sql.append("FOREIGN KEY (");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKey.getLeft()));
        sql.append(") REFERENCES ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + foreignKey.getLeft().replace(SqlElement.IN_VERTEX_COLUMN_END, "")));
        sql.append(" (");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append("), ");
        sql.append(" FOREIGN KEY (");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKey.getRight()));
        sql.append(") REFERENCES ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(VERTEX_PREFIX + foreignKey.getRight().replace(SqlElement.OUT_VERTEX_COLUMN_END, "")));
        sql.append(" (");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append("))");
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void addEdgeLabelToVerticesTable(Long id, String label, boolean inDirection) {
        Set<String> labelSet = getLabelsForVertex(id, inDirection);
        labelSet.add(label);
        StringBuilder sql = new StringBuilder("UPDATE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" SET ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(inDirection ? SchemaManager.VERTEX_IN_LABELS : SchemaManager.VERTEX_OUT_LABELS));
        sql.append(" = ?");
        sql.append(" WHERE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" = ?");
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            //varchar here must be lowercase
            int count = 1;
            StringBuilder sb = new StringBuilder();
            for (String l : labelSet) {
                sb.append(l);
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

    public Set<String> getLabelsForVertex(Long id, boolean inDirection) {
        Set<String> labels = new HashSet<>();
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(inDirection ? SchemaManager.VERTEX_IN_LABELS : SchemaManager.VERTEX_OUT_LABELS));
        sql.append(" FROM ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" WHERE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" = ?");
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String commaSeparatedLabels = resultSet.getString(inDirection ? SchemaManager.VERTEX_IN_LABELS : SchemaManager.VERTEX_OUT_LABELS);
                if (commaSeparatedLabels != null) {
                    labels.addAll(Arrays.asList(commaSeparatedLabels.split(":::")));
                    if (resultSet.next()) {
                        throw new IllegalStateException("!!!!!");
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return labels;
    }

    private void addColumn(String table, ImmutablePair<String, PropertyType> keyValue) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(table));
        sql.append(" ADD ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(keyValue.left));
        sql.append(" ");
        sql.append(this.sqlDialect.propertyTypeToSqlDefinition(keyValue.right));
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void addEdgeForeignKey(String table, String foreignKey) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(table));
        sql.append(" ADD COLUMN ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(foreignKey));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlGraph.tx().getConnection();
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
            Connection conn = SqlGraphDataSource.INSTANCE.get(this.sqlGraph.getJdbcUrl()).getConnection();
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
                    String column = columnsRs.getString(4);
                    int columnType = columnsRs.getInt(5);
                    String typeName = columnsRs.getString("TYPE_NAME");
                    PropertyType propertyType = this.sqlDialect.sqlTypeToPropertyType(columnType, typeName);
                    uncomitedColumns.put(column, propertyType);
                    this.schema.put(table, uncomitedColumns);
                    if (table.startsWith(EDGE_PREFIX) && (column.endsWith(SqlElement.IN_VERTEX_COLUMN_END) || column.endsWith(SqlElement.OUT_VERTEX_COLUMN_END))) {
                        foreignKeys.add(column);
                        this.edgeForeignKeys.put(table, foreignKeys);
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
            Connection conn = SqlGraphDataSource.INSTANCE.get(this.sqlDialect.getJdbcDriver()).getConnection();
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
