package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Date: 2014/07/12
 * Time: 5:41 AM
 */
public class SqlEdge extends SqlElement implements Edge {

    private SqlVertex inVertex;
    private SqlVertex outVertex;

    /**
     * This is called when creating a new edge. from vin.addEdge(label, vout)
     *
     * @param sqlGraph
     * @param label
     * @param inVertex
     * @param outVertex
     * @param keyValues
     */
    public SqlEdge(SqlGraph sqlGraph, String label, SqlVertex inVertex, SqlVertex outVertex, Object... keyValues) {
        super(sqlGraph, label, keyValues);
        this.inVertex = inVertex;
        this.outVertex = outVertex;
        try {
            insertEdge(keyValues);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public SqlEdge(SqlGraph sqlGraph, Long id, String label, SqlVertex inVertex, SqlVertex outVertex, Object... keyValues) {
        super(sqlGraph, id, label);
        this.inVertex = inVertex;
        this.outVertex = outVertex;
    }

    public SqlEdge(SqlGraph sqlGraph, Long id, String label) {
        super(sqlGraph, id, label);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
            vertices.add(getOutVertex());
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
            vertices.add(getInVertex());
        return vertices.iterator();
    }

    @Override
    public void remove() {
        this.sqlGraph.tx().readWrite();
        StringBuilder sql = new StringBuilder("DELETE FROM \"");
        sql.append(SchemaManager.EDGES);
        sql.append("\" WHERE \"ID\" = ?;");
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(1, (Long) this.id());
            int numberOfRowsUpdated = preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        super.remove();
    }

    public SqlVertex getInVertex() {
        load();
        return inVertex;
    }

    public SqlVertex getOutVertex() {
        return outVertex;
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    protected void insertEdge(Object... keyValues) throws SQLException {

        long edgeId = insertGlobalEdge();

        StringBuilder sql = new StringBuilder("INSERT INTO \"");
        sql.append(this.label);
        sql.append("\" (\"ID\", ");
        int i = 1;
        List<String> columns = SqlUtil.transformToInsertColumns(keyValues);
        for (String column : columns) {
            sql.append("\"").append(column).append("\"");
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
        if (columns.size() > 0) {
            sql.append(", ");
        }
        sql.append("\"");
        sql.append(this.inVertex.label());
        sql.append(SqlElement.IN_VERTEX_COLUMN_END);
        sql.append("\", \"");
        sql.append(this.outVertex.label());
        sql.append(SqlElement.OUT_VERTEX_COLUMN_END);
        sql.append("\") VALUES (?, ");
        i = 1;
        List<String> values = SqlUtil.transformToInsertValues(keyValues);
        for (String value : values) {
            sql.append("?");
            if (i++ < values.size()) {
                sql.append(", ");
            }
        }
        if (values.size() > 0) {
            sql.append(", ");
        }
        sql.append("?, ?");
        sql.append(");");
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            i = 1;
            preparedStatement.setLong(i++, edgeId);
            for (ImmutablePair<PropertyType, Object> pair : SqlUtil.transformToTypeAndValue(keyValues)) {
                switch (pair.left) {
                    case BOOLEAN:
                        preparedStatement.setBoolean(i++, (Boolean) pair.right);
                        break;
                    case BYTE:
                        preparedStatement.setByte(i++, (Byte) pair.right);
                        break;
                    case SHORT:
                        preparedStatement.setShort(i++, (Short) pair.right);
                        break;
                    case INTEGER:
                        preparedStatement.setInt(i++, (Integer) pair.right);
                        break;
                    case LONG:
                        preparedStatement.setLong(i++, (Long) pair.right);
                        break;
                    case FLOAT:
                        preparedStatement.setFloat(i++, (Float) pair.right);
                        break;
                    case DOUBLE:
                        preparedStatement.setDouble(i++, (Double) pair.right);
                        break;
                    case STRING:
                        preparedStatement.setString(i++, (String) pair.right);
                        break;
                    default:
                        throw new IllegalStateException("Unhandled type " + pair.left.name());
                }
            }
            preparedStatement.setLong(i++, this.inVertex.primaryKey);
            preparedStatement.setLong(i++, this.outVertex.primaryKey);
            preparedStatement.executeUpdate();
            this.primaryKey = edgeId;
        }
    }

    private long insertGlobalEdge() throws SQLException {
        long vertexId;
        StringBuilder sql = new StringBuilder("INSERT INTO \"");
        sql.append(SchemaManager.EDGES);
        sql.append("\" (\"EDGE_TABLE\") VALUES (?);");
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setString(1, this.label);
            preparedStatement.executeUpdate();
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
                vertexId = generatedKeys.getLong(1);
            } else {
                throw new RuntimeException("Could not retrieve the id after an insert into " + SchemaManager.EDGES);
            }
        }
        return vertexId;
    }

    @Override
    protected Object[] load() {
        List<Object> keyValues = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT * FROM \"");
        sql.append(this.label);
        sql.append("\" WHERE \"ID\" = ?;");
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(1, this.primaryKey);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String inVertexColumnName = null;
                String outVertexColumnName = null;
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    String columnName = resultSetMetaData.getColumnName(i);
                    if (!columnName.equals("ID") && !columnName.endsWith(SqlElement.OUT_VERTEX_COLUMN_END) && !columnName.endsWith(SqlElement.IN_VERTEX_COLUMN_END)) {
                        keyValues.add(columnName);
                        keyValues.add(resultSet.getObject(columnName));
                    }
                    if (columnName.endsWith(SqlElement.IN_VERTEX_COLUMN_END)) {
                        inVertexColumnName = columnName;
                    } else if (columnName.endsWith(SqlElement.OUT_VERTEX_COLUMN_END)) {
                        outVertexColumnName = columnName;
                    }
                }
                if (inVertexColumnName == null || outVertexColumnName == null) {
                    throw new IllegalStateException("in or out vertex id not set!!!!");
                }
                Long inId = resultSet.getLong(inVertexColumnName);
                Long outId = resultSet.getLong(outVertexColumnName);

                this.inVertex = new SqlVertex(this.sqlGraph, inId, inVertexColumnName.replace(SqlElement.IN_VERTEX_COLUMN_END, ""));
                this.outVertex = new SqlVertex(this.sqlGraph, outId, outVertexColumnName.replace(SqlElement.OUT_VERTEX_COLUMN_END, ""));
                break;
            }
            return keyValues.toArray();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
