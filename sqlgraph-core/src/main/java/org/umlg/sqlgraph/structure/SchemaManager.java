package org.umlg.sqlgraph.structure;

import org.apache.commons.lang3.tuple.ImmutablePair;
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

    public static final String VERTICES = "VERTICES";
    public static final String VERTEX_IN_LABELS = "IN_LABELS";
    public static final String VERTEX_OUT_LABELS = "OUT_LABELS";
    public static final String EDGES = "EDGES";
    private Map<String, Map<String, PropertyType>> schema = new ConcurrentHashMap<>();
    private Map<String, ReentrantLock> schemaLocks = new ConcurrentHashMap<>();
    private SqlGraph sqlGraph;
    private SqlDialect sqlDialect;

    SchemaManager(SqlGraph sqlGraph, SqlDialect sqlDialect) {
        this.sqlGraph = sqlGraph;
        this.sqlDialect = sqlDialect;
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

    public void ensureVertexTableExist(String table, Object... keyValues) {
        Objects.requireNonNull(table, "Given table must not be null");
        ConcurrentHashMap<String, PropertyType> columns = SqlUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.schema.containsKey(table)) {
            ReentrantLock reentrantLock = this.schemaLocks.computeIfAbsent(table,
                    (key) -> new ReentrantLock()
            );
            reentrantLock.lock();
            if (!this.schema.containsKey(table)) {
                createVertexTable(table, columns);
                this.sqlGraph.tx().afterCommit(() -> {
                    if (reentrantLock.isHeldByCurrentThread()) {
                        this.schema.put(table, columns);
                        reentrantLock.unlock();
                    }
                });
                this.sqlGraph.tx().afterRollback(() -> {
                    if (reentrantLock.isHeldByCurrentThread()) {
                        reentrantLock.unlock();
                    }
                });
            } else {
                reentrantLock.unlock();
            }
        }
        //ensure columns exist
        columns.forEach((k, v) -> ensureColumnExist(table, ImmutablePair.of(k, v)));
    }

    public void ensureEdgeTableExist(String table, String inTable, String outTable, Object... keyValues) {
        Objects.requireNonNull(table, "Given table must not be null");
        Objects.requireNonNull(table, "Given inTable must not be null");
        Objects.requireNonNull(table, "Given outTable must not be null");
        Map<String, PropertyType> columns = SqlUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.schema.containsKey(table)) {
            ReentrantLock reentrantLock = this.schemaLocks.computeIfAbsent(table,
                    (key) -> new ReentrantLock()
            );
            reentrantLock.lock();
            if (!this.schema.containsKey(table)) {
                createEdgeTable(table, inTable, outTable, columns);
                this.sqlGraph.tx().afterCommit(() -> {
                    if (reentrantLock.isHeldByCurrentThread()) {
                        this.schema.put(table, columns);
                        reentrantLock.unlock();
                    }
                });
                this.sqlGraph.tx().afterRollback(() -> {
                    if (reentrantLock.isHeldByCurrentThread()) {
                        reentrantLock.unlock();
                    }
                });
            } else {
                reentrantLock.unlock();
            }
        }
        //ensure columns exist
        columns.forEach((k, v) -> ensureColumnExist(table, ImmutablePair.of(k, v)));
    }

    public void ensureColumnExist(String table, ImmutablePair<String, PropertyType> keyValue) {
        Map<String, PropertyType> cachedColumns = this.schema.get(table);
        Objects.requireNonNull(cachedColumns, "Table must already be present in the cache!");
        if (!cachedColumns.containsKey(keyValue.left)) {
            ReentrantLock reentrantLock = this.schemaLocks.computeIfAbsent(table,
                    (key) -> new ReentrantLock()
            );
            reentrantLock.lock();
            if (!cachedColumns.containsKey(keyValue.left)) {
                addColumn(table, keyValue);
                this.sqlGraph.tx().afterCommit(() -> {
                    if (reentrantLock.isHeldByCurrentThread()) {
                        reentrantLock.unlock();
                        cachedColumns.put(keyValue.left, keyValue.right);
                    }
                });
            } else {
                reentrantLock.unlock();
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
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS \"");
        sql.append(SchemaManager.VERTICES);
        sql.append("\"(\"ID\" ");
        sql.append(this.sqlDialect.getPrimaryKeyType());
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        sql.append(", VERTEX_TABLE VARCHAR(255) NOT NULL, \"");
        sql.append(SchemaManager.VERTEX_IN_LABELS);
        sql.append("\" ");
        sql.append(this.sqlDialect.getVarcharArray());
        sql.append(", \"");
        sql.append(SchemaManager.VERTEX_OUT_LABELS);
        sql.append("\" ");
        sql.append(this.sqlDialect.getVarcharArray());
        sql.append(");");

        Connection conn = this.sqlGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }
    }

    private boolean edgesExist() throws SQLException {
        return tableExist(SchemaManager.EDGES);
    }

    private void createEdgesTable() throws SQLException {
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS \"");
        sql.append(SchemaManager.EDGES);
        sql.append("\"(\"ID\" ");
        sql.append(this.sqlDialect.getPrimaryKeyType());
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        sql.append(", \"EDGE_TABLE\" VARCHAR(255));");
        Connection conn = this.sqlGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }
    }

    public boolean tableExist(String table) {
        try {
            Connection conn = this.sqlGraph.tx().getConnection();
            DatabaseMetaData metadata = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = table;
            String[] types = null;
            ResultSet result = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
            while (result.next()) {
                return true;
            }
            return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createVertexTable(String tableName, Map<String, PropertyType> columns) {
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS \"");
        sql.append(tableName);
        sql.append("\"(\"ID\" ");
        sql.append(this.sqlDialect.getPrimaryKeyType());
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        int i = 1;
        for (String column : columns.keySet()) {
            PropertyType propertyType = columns.get(column);
            //Columns map 1 to 1 to property keys and are case sensitive
            sql.append("\"").append(column).append("\" ").append(this.sqlDialect.propertyTypeToSqlDefinition(propertyType));
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
        sql.append(");");
        Connection conn = this.sqlGraph.tx().getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createEdgeTable(String tableName, String inTable, String outTable, Map<String, PropertyType> columns) {
        StringBuilder sql = new StringBuilder("CREATE TABLE \"");
        sql.append(tableName);
        sql.append("\"(\"ID\" ");
        sql.append(this.sqlDialect.getPrimaryKeyType());
        sql.append(" ");
        sql.append(this.sqlDialect.getAutoIncrementPrimaryKeyConstruct());
        if (columns.size() > 0) {
            sql.append(", ");
        }
        int i = 1;
        for (String column : columns.keySet()) {
            PropertyType propertyType = columns.get(column);
            //Columns map 1 to 1 to property keys and are case sensitive
            sql.append("\"").append(column).append("\" ").append(this.sqlDialect.propertyTypeToSqlDefinition(propertyType));
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
        sql.append(", \"");
        sql.append(inTable);
        sql.append(SqlElement.IN_VERTEX_COLUMN_END);
        sql.append("\" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
        sql.append(", \"");
        sql.append(outTable);
        sql.append(SqlElement.OUT_VERTEX_COLUMN_END);
        sql.append("\" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
        sql.append(", ");
        sql.append("FOREIGN KEY (\"");
        sql.append(inTable);
        sql.append(SqlElement.IN_VERTEX_COLUMN_END);
        sql.append("\") REFERENCES \"");
        sql.append(inTable);
        sql.append("\" (\"ID\"), ");
        sql.append(" FOREIGN KEY (\"");
        sql.append(outTable);
        sql.append(SqlElement.OUT_VERTEX_COLUMN_END);
        sql.append("\") REFERENCES \"");
        sql.append(outTable);
        sql.append("\" (\"ID\"));");

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
        StringBuilder sql = new StringBuilder("UPDATE \"");
        sql.append(SchemaManager.VERTICES);
        sql.append("\" SET ");
        sql.append("\"");
        sql.append(inDirection ? SchemaManager.VERTEX_IN_LABELS : SchemaManager.VERTEX_OUT_LABELS);
        sql.append("\" = ?");
        sql.append(" WHERE \"ID\" = ?;");
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            //varchar here must be lowercase
            Array labelArray = conn.createArrayOf("varchar", labelSet.toArray());
            preparedStatement.setArray(1, labelArray);
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
        StringBuilder sql = new StringBuilder("SELECT \"");
        sql.append(inDirection ? SchemaManager.VERTEX_IN_LABELS : SchemaManager.VERTEX_OUT_LABELS);
        sql.append("\" FROM \"");
        sql.append(SchemaManager.VERTICES);
        sql.append("\" WHERE \"ID\" = ?");
        sql.append(";");
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Array array = resultSet.getArray(inDirection ? SchemaManager.VERTEX_IN_LABELS : SchemaManager.VERTEX_OUT_LABELS);
                if (array != null) {
                    Object[] labelsAsObject = (Object[]) array.getArray();
                    for (Object o : labelsAsObject) {
                        labels.add((String) o);
                    }
                }
                if (resultSet.next()) {
                    throw new IllegalStateException("!!!!!");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return labels;
    }

    private void addColumn(String table, ImmutablePair<String, PropertyType> keyValue) {
        StringBuilder sql = new StringBuilder("ALTER TABLE \"");
        sql.append(table);
        sql.append("\" ADD ");
        sql.append("\"");
        sql.append(keyValue.left);
        sql.append("\" ");
        sql.append(this.sqlDialect.propertyTypeToSqlDefinition(keyValue.right));
        sql.append(";");
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


}
