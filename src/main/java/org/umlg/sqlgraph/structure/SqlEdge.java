package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.umlg.sqlgraph.process.step.map.SqlEdgeVertexStep;
import org.umlg.sqlgraph.sql.impl.SchemaManager;
import org.umlg.sqlgraph.sql.impl.SqlUtil;

import java.sql.*;
import java.util.ArrayList;
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
        insertEdge();
    }

    public SqlEdge(SqlGraph sqlGraph, Long id, String label, SqlVertex inVertex, SqlVertex outVertex, Object... keyValues) {
        super(sqlGraph, id, label);
        this.inVertex = inVertex;
        this.outVertex = outVertex;
        this.keyValues = keyValues;
    }

    public SqlEdge(SqlGraph sqlGraph, Long id, String label) {
        super(sqlGraph, id, label);
    }

    @Override
    public GraphTraversal<Edge, Vertex> toV(final Direction direction) {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new SqlEdgeVertexStep(traversal, this.sqlGraph, direction));
        return traversal;
    }

    @Override
    public void remove() {
        this.sqlGraph.tx().readWrite();
        StringBuilder sql = new StringBuilder("DELETE FROM \"");
        sql.append(SchemaManager.EDGES);
        sql.append("\" WHERE ID = ?;");
        Connection conn;
        PreparedStatement preparedStatement = null;
        try {
            conn = this.sqlGraph.tx().getConnection();
            preparedStatement = conn.prepareStatement(sql.toString());
            preparedStatement.setLong(1, (Long) this.id());
            int numberOfRowsUpdated = preparedStatement.executeUpdate();
            if (numberOfRowsUpdated != 1) {
                throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Edge.class, this.id());
            }
            preparedStatement.close();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (preparedStatement != null)
                    preparedStatement.close();
            } catch (SQLException se2) {
            }
        }
        super.remove();
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    protected void insertEdge() {

        long edgeId = insertGlobalEdge();

        StringBuilder sql = new StringBuilder("INSERT INTO \"");
        sql.append(this.label);
        sql.append("\" (ID, ");
        int i = 1;
        List<String> columns = SqlUtil.transformToInsertColumns(this.keyValues);
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
        List<String> values = SqlUtil.transformToInsertValues(this.keyValues);
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
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = this.sqlGraph.tx().getConnection();
            preparedStatement = conn.prepareStatement(sql.toString());
            i = 1;
            preparedStatement.setLong(i++, edgeId);
            for (ImmutablePair<SchemaManager.PropertyType, Object> pair : SqlUtil.transformToTypeAndValue(this.keyValues)) {
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
            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (preparedStatement != null)
                    preparedStatement.close();
            } catch (SQLException se2) {
            }
        }
    }

    private long insertGlobalEdge() {
        long vertexId;
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(SchemaManager.EDGES);
        sql.append("(EDGE_TABLE) VALUES (?);");
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = this.sqlGraph.tx().getConnection();
            preparedStatement = conn.prepareStatement(sql.toString());
            preparedStatement.setString(1, this.label);
            preparedStatement.executeUpdate();
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
                vertexId = generatedKeys.getLong(1);
            } else {
                throw new RuntimeException("Could not retrieve the id after an insert into " + SchemaManager.EDGES);
            }
            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (preparedStatement != null)
                    preparedStatement.close();
            } catch (SQLException se2) {
            }
        }
        return vertexId;
    }

    public SqlVertex getInVertex() {
        return inVertex;
    }

    public SqlVertex getOutVertex() {
        return outVertex;
    }

    @Override
    protected void load() {
        StringBuilder sql = new StringBuilder("SELECT * FROM \"");
        sql.append(this.label);
        sql.append("\" WHERE ID = ?;");
        Connection conn;
        PreparedStatement preparedStatement = null;
        try {
            conn = this.sqlGraph.tx().getConnection();
            preparedStatement = conn.prepareStatement(sql.toString());
            preparedStatement.setLong(1, this.primaryKey);
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<Object> keyValues = new ArrayList<>();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    String columnName = resultSetMetaData.getColumnName(i);
                    if (!columnName.equals("ID") && !columnName.endsWith(SqlElement.OUT_VERTEX_COLUMN_END) && !columnName.endsWith(SqlElement.IN_VERTEX_COLUMN_END)) {
                        keyValues.add(columnName);
                        keyValues.add(resultSet.getObject(columnName));
                    }
                }
                this.keyValues = keyValues.toArray();
                break;
            }
            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (preparedStatement != null)
                    preparedStatement.close();
            } catch (SQLException se2) {
            }
        }
    }
}
