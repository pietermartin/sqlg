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
    private Map<String, Map<String, PropertyType>> uncommittedSchema = new ConcurrentHashMap<>();
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
                this.uncommittedSchema.clear();
                this.schemaLock.unlock();
            }
        });
        this.sqlGraph.tx().afterRollback(() -> {
            if (this.schemaLock.isHeldByCurrentThread()) {
                if (this.getSqlDialect().supportsTransactionalSchema()) {
                    this.uncommittedSchema.clear();
                } else {
                    for (String t : this.uncommittedSchema.keySet()) {
                        this.schema.put(t, this.uncommittedSchema.get(t));
                    }
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

    public void ensureVertexTableExist(String table, Object... keyValues) {
        Objects.requireNonNull(table, "Given table must not be null");
        final ConcurrentHashMap<String, PropertyType> columns = SqlUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.schema.containsKey(table)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.schemaLock.isHeldByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!this.schema.containsKey(table)) {
                if (!this.uncommittedSchema.containsKey(table)) {
                    this.uncommittedSchema.put(table, columns);
                    createVertexTable(table, columns);
                }
            }
        }
        //ensure columns exist
        columns.forEach((k, v) -> ensureColumnExist(table, ImmutablePair.of(k, v)));
    }

    public void ensureEdgeTableExist(String table, String inTable, String outTable, Object... keyValues) {
        Objects.requireNonNull(table, "Given table must not be null");
        Objects.requireNonNull(table, "Given inTable must not be null");
        Objects.requireNonNull(table, "Given outTable must not be null");
        final ConcurrentHashMap<String, PropertyType> columns = SqlUtil.transformToColumnDefinitionMap(keyValues);
        if (!this.schema.containsKey(table)) {
            //Make sure the current thread/transaction owns the lock
            if (!this.schemaLock.isHeldByCurrentThread()) {
                this.schemaLock.lock();
            }
            if (!this.schema.containsKey(table)) {
                if (!this.uncommittedSchema.containsKey(table)) {
                    this.uncommittedSchema.put(table, columns);
                    createEdgeTable(table, inTable, outTable, columns);
                }
            }
        }
        //ensure columns exist
        columns.forEach((k, v) -> ensureColumnExist(table, ImmutablePair.of(k, v)));
    }

    public void ensureColumnExist(String table, ImmutablePair<String, PropertyType> keyValue) {
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

    public void close() {
        this.schema.clear();
    }

    private boolean verticesExist() throws SQLException {
        return tableExist(SchemaManager.VERTICES);
    }

    private void createVerticesTable() throws SQLException {
//        IF NOT EXISTS
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" (");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getPrimaryKeyType());
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
        //IF NOT EXISTS
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(SchemaManager.EDGES));
        sql.append("(");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
        sql.append(this.sqlDialect.getPrimaryKeyType());
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

    public boolean tableExist(String table)  {
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
//        IF NOT EXISTS
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
        sql.append(" (");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
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

    private void createEdgeTable(String tableName, String inTable, String outTable, Map<String, PropertyType> columns) {
//        IF NOT EXISTS
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(tableName));
        sql.append("(");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append(" ");
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
            sql.append(this.sqlDialect.maybeWrapInQoutes(column)).append(" ").append(this.sqlDialect.propertyTypeToSqlDefinition(propertyType));
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(inTable + SqlElement.IN_VERTEX_COLUMN_END));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
        sql.append(", ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(outTable + SqlElement.OUT_VERTEX_COLUMN_END));
        sql.append(" ");
        sql.append(this.sqlDialect.getForeignKeyTypeDefinition());
        sql.append(", ");
        sql.append("FOREIGN KEY (");
        sql.append(this.sqlDialect.maybeWrapInQoutes(inTable + SqlElement.IN_VERTEX_COLUMN_END));
        sql.append(") REFERENCES ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(inTable));
        sql.append(" (");
        sql.append(this.sqlDialect.maybeWrapInQoutes("ID"));
        sql.append("), ");
        sql.append(" FOREIGN KEY (");
        sql.append(this.sqlDialect.maybeWrapInQoutes(outTable + SqlElement.OUT_VERTEX_COLUMN_END));
        sql.append(") REFERENCES ");
        sql.append(this.sqlDialect.maybeWrapInQoutes(outTable));
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


}
