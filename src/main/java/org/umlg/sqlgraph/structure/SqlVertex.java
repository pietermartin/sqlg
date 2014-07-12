package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.umlg.sqlgraph.process.step.map.SqlVertexStep;
import org.umlg.sqlgraph.sql.impl.SchemaManager;
import org.umlg.sqlgraph.sql.impl.SqlUtil;

import java.sql.*;
import java.util.List;

/**
 * Date: 2014/07/12
 * Time: 5:42 AM
 */
public class SqlVertex extends SqlElement implements Vertex {

    public SqlVertex(SqlGraph sqlGraph, String label, Object... keyValues) {
        super(sqlGraph, label, keyValues);
        insertVertex();
    }

    public SqlVertex(Long id, SqlGraph sqlGraph) {
        super(id, sqlGraph);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (label == null)
            throw Edge.Exceptions.edgeLabelCanNotBeNull();
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        this.sqlGraph.tx().readWrite();
        SchemaManager.INSTANCE.ensureEdgeTableExist(label, inVertex.label(), this.label, keyValues);
        final SqlEdge edge = new SqlEdge(this.sqlGraph, label, (SqlVertex) inVertex, this, keyValues);
        return edge;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        this.sqlGraph.tx().readWrite();
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<>(traversal, this));
        traversal.addStep(new SqlVertexStep<>(this.sqlGraph, traversal, Vertex.class, direction, branchFactor, labels));
        return traversal;
    }

    @Override
    public GraphTraversal<Vertex, Edge> toE(final Direction direction, final int branchFactor, final String... labels) {
        this.sqlGraph.tx().readWrite();
        final GraphTraversal<Vertex, Edge> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<>(traversal, this));
        traversal.addStep(new SqlVertexStep(this.sqlGraph, traversal, Edge.class, direction, branchFactor, labels));
        return traversal;
    }

    protected void insertVertex() {

        long vertexId = insertGlobalVertex();

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
        sql.append(") VALUES (?, ");
        i = 1;
        List<String> values = SqlUtil.transformToInsertValues(this.keyValues);
        for (String value : values) {
            sql.append("?");
            if (i++ < values.size()) {
                sql.append(", ");
            }
        }
        sql.append(");");
        Connection conn;
        PreparedStatement preparedStatement = null;
        try {
            conn = this.sqlGraph.tx().getConnection();
            preparedStatement = conn.prepareStatement(sql.toString());
            i = 1;
            preparedStatement.setLong(i++, vertexId);
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
            this.primaryKey = vertexId;
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

    private long insertGlobalVertex() {
        long vertexId;
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(SchemaManager.VERTICES);
        sql.append("(VERTEX_TABLE) VALUES (?);");
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
                throw new RuntimeException("Could not retrieve the id after an insert into " + SchemaManager.VERTICES);
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
