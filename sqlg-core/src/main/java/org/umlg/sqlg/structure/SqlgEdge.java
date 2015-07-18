package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.util.*;

/**
 * Date: 2014/07/12
 * Time: 5:41 AM
 */
public class SqlgEdge extends SqlgElement implements Edge {

    private Logger logger = LoggerFactory.getLogger(SqlgEdge.class.getName());
    private SqlgVertex inVertex;
    private SqlgVertex outVertex;

    /**
     * This is called when creating a new edge. from inVertex.addEdge(label, outVertex)
     *
     * @param sqlgGraph
     * @param schema
     * @param table
     * @param inVertex
     * @param outVertex
     * @param keyValues
     */
    public SqlgEdge(SqlgGraph sqlgGraph, boolean complete, String schema, String table, SqlgVertex inVertex, SqlgVertex outVertex, Object... keyValues) {
        super(sqlgGraph, schema, table);
        this.inVertex = inVertex;
        this.outVertex = outVertex;
        try {
            insertEdge(complete, keyValues);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This is used when loading a vertex incident edges.
     * @param sqlgGraph
     * @param id
     * @param schema
     * @param table
     * @param inVertex
     * @param outVertex
     * @param keyValues
     */
    public SqlgEdge(SqlgGraph sqlgGraph, Long id, String schema, String table, SqlgVertex inVertex, SqlgVertex outVertex, Object... keyValues) {
        super(sqlgGraph, id, schema, table);
        this.inVertex = inVertex;
        this.outVertex = outVertex;
    }

    /**
     * This is for loading edges directly.
     * @param sqlgGraph
     * @param id
     * @param schema
     * @param table
     */
    public SqlgEdge(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        super(sqlgGraph, id, schema, table);
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Edge.class, this.id());
        this.sqlgGraph.tx().readWrite();
        return super.property(key, value);
    }

    private Iterator<Vertex> internalGetVertices(Direction direction) {
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
            vertices.add(getOutVertex());
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
            vertices.add(getInVertex());
        return vertices.iterator();
    }

    @Override
    public void remove() {
        this.sqlgGraph.tx().readWrite();
        if (this.removed)
            throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id());

        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            this.sqlgGraph.tx().getBatchManager().removeEdge(this.schema, this.table, this);
        } else {
            super.remove();
        }

    }

    public SqlgVertex getInVertex() {
        if (this.inVertex == null) {
            load();
        }
        return inVertex;
    }

    public SqlgVertex getOutVertex() {
        if (this.outVertex == null) {
            load();
        }
        return outVertex;
    }

    @Override
    public String toString() {
        if (this.inVertex == null) {
            load();
        }
        return StringFactory.edgeString(this);
    }

    protected void insertEdge(boolean complete, Object... keyValues) throws SQLException {
        Map<String, Object> keyValueMap = SqlgUtil.transformToInsertValues(keyValues);
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            internalBatchAddEdge(complete, keyValueMap);
        } else {
            internalAddEdge(keyValueMap);
        }
        //Cache the properties
        this.properties.putAll(keyValueMap);
    }

    private void internalAddEdge(Map<String, Object> keyValueMap) throws SQLException {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema));
        sql.append(".");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + this.table));
        sql.append(" (");
        int i = 1;
        for (String column : keyValueMap.keySet()) {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
            if (i++ < keyValueMap.size()) {
                sql.append(", ");
            }
        }
        if (keyValueMap.size() > 0) {
            sql.append(", ");
        }
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.inVertex.schema + "." + this.inVertex.table + SchemaManager.IN_VERTEX_COLUMN_END));
        sql.append(", ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.outVertex.schema + "." + this.outVertex.table + SchemaManager.OUT_VERTEX_COLUMN_END));
        sql.append(") VALUES (");
        i = 1;
        for (String column : keyValueMap.keySet()) {
            sql.append("?");
            if (i++ < keyValueMap.size()) {
                sql.append(", ");
            }
        }
        if (keyValueMap.size() > 0) {
            sql.append(", ");
        }
        sql.append("?, ?");
        sql.append(")");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        i = 1;
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {
            i = setKeyValuesAsParameter(this.sqlgGraph, i, conn, preparedStatement, keyValueMap);
            preparedStatement.setLong(i++, this.inVertex.recordId.getId());
            preparedStatement.setLong(i++, this.outVertex.recordId.getId());
            preparedStatement.executeUpdate();
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
                this.recordId = RecordId.from(SchemaTable.of(this.schema, this.table), generatedKeys.getLong(1));
            } else {
                throw new RuntimeException("Could not retrieve the id after an insert into " + SchemaManager.VERTICES);
            }
        }

    }

    private void internalBatchAddEdge(boolean complete, Map<String, Object> keyValueMap) {
        this.sqlgGraph.tx().getBatchManager().addEdge(complete, this, this.outVertex, this.inVertex, keyValueMap);
    }

    //TODO this needs optimizing, an edge created in the transaction need not go to the db to load itself again
    @Override
    protected void load() {
        //recordId can be null when in batchMode
        if (recordId != null && this.properties.isEmpty()) {
            StringBuilder sql = new StringBuilder("SELECT * FROM ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + this.table));
            sql.append(" WHERE ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
            sql.append(" = ?");
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            Connection conn = this.sqlgGraph.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.setLong(1, this.recordId.getId());
                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    loadResultSet(resultSet);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    void loadResultSet(ResultSet resultSet) throws SQLException {
        SchemaTable inVertexColumnName = null;
        SchemaTable outVertexColumnName = null;
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnName(i);
            Object o = resultSet.getObject(columnName);
            if (!columnName.equals("ID") &&
                    !Objects.isNull(o) &&
                    !columnName.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END) &&
                    !columnName.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {

                loadProperty(resultSetMetaData, i, columnName, o);

            }
            if (!Objects.isNull(o)) {
                if (columnName.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                    inVertexColumnName = SchemaTable.from(this.sqlgGraph, columnName, this.sqlgGraph.getSqlDialect().getPublicSchema());
                } else if (columnName.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                    outVertexColumnName = SchemaTable.from(this.sqlgGraph, columnName, this.sqlgGraph.getSqlDialect().getPublicSchema());
                }
            }
        }
        if (inVertexColumnName == null || outVertexColumnName == null) {
            throw new IllegalStateException("in or out vertex id not set!!!!");
        }
        Long inId = resultSet.getLong(inVertexColumnName.getSchema() + "." + inVertexColumnName.getTable());
        Long outId = resultSet.getLong(outVertexColumnName.getSchema() + "." + outVertexColumnName.getTable());

        this.inVertex = SqlgVertex.of(this.sqlgGraph, inId, inVertexColumnName.getSchema(), SqlgUtil.removeTrailingInId(inVertexColumnName.getTable()));
        this.outVertex = SqlgVertex.of(this.sqlgGraph, outId, outVertexColumnName.getSchema(), SqlgUtil.removeTrailingOutId(outVertexColumnName.getTable()));
    }


    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return (Iterator) super.properties(propertyKeys);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        SqlgEdge.this.sqlgGraph.tx().readWrite();
        return internalGetVertices(direction);
    }
}
