package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.umlg.sqlgraph.process.step.map.SqlEdgeVertexStep;
import org.umlg.sqlgraph.sql.impl.SchemaManager;
import org.umlg.sqlgraph.sql.impl.SqlUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Date: 2014/07/12
 * Time: 5:41 AM
 */
public class SqlEdge extends SqlElement implements Edge {

    private SqlVertex inVertex;
    private SqlVertex outVertex;
    private Long inVertexId;
    private Long outVertexId;

    /**
     * This is called when creating a new edge. from vin.addEdge(label, vout)
     * @param sqlGraph
     * @param label
     * @param inVertex
     * @param outVertex
     * @param keyValues
     */
    public SqlEdge(SqlGraph sqlGraph, String label,SqlVertex inVertex, SqlVertex outVertex, Object... keyValues) {
        super(sqlGraph, label, keyValues);
        this.inVertex = inVertex;
        this.outVertex = outVertex;
        insertEdge();
    }

    public SqlEdge(Long id, SqlGraph sqlGraph) {
        super(id, sqlGraph);
    }

    @Override
    public void remove() {
    }

    @Override
    public GraphTraversal<Edge, Vertex> toV(final Direction direction) {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new SqlEdgeVertexStep(traversal, this.sqlGraph, direction));
        return traversal;
    }


    public String toString() {
        return StringFactory.edgeString(this);
    }

    public Long getInVertexId() {
        return inVertexId;
    }

    public Long getOutVertexId() {
        return outVertexId;
    }

    protected void insertEdge() {

        long edgeId = insertGlobalEdge();

        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(this.label);
        sql.append(" (ID, ");
        int i = 1;
        List<String> columns = SqlUtil.transformToInsertColumns(this.keyValues);
        for (String column : columns) {
            sql.append(column);
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
        if (columns.size()>0) {
            sql.append(", ");
        }
        sql.append("VERTEX_INID, VERTEX_OUTID");
        sql.append(") VALUES (?, ");
        i = 1;
        List<String> values = SqlUtil.transformToInsertValues(this.keyValues);
        for (String value : values) {
            sql.append("?");
            if (i++ < values.size()) {
                sql.append(", ");
            }
        }
        sql.append("'");
        sql.append(this.inVertex.primaryKey);
        sql.append("'");
        sql.append(", '");
        sql.append(this.outVertex.primaryKey);
        sql.append("'");
        sql.append(");");
        Connection conn;
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
        Connection conn;
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

}
