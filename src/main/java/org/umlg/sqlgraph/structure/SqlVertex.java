package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.umlg.sqlgraph.process.step.map.SqlVertexStep;
import org.umlg.sqlgraph.sql.impl.SqlUtil;

import java.sql.*;
import java.util.*;

/**
 * Date: 2014/07/12
 * Time: 5:42 AM
 */
public class SqlVertex extends SqlElement implements Vertex {

    public SqlVertex(SqlGraph sqlGraph, String label, Object... keyValues) {
        super(sqlGraph, label, keyValues);
        insertVertex();
    }

    public SqlVertex(SqlGraph sqlGraph, Long id, String label) {
        super(sqlGraph, id, label);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (label == null)
            throw Edge.Exceptions.edgeLabelCanNotBeNull();
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Edge.Exceptions.userSuppliedIdsNotSupported();

        int i = 0;
        String key = "";
        Object value;
        for (Object keyValue : keyValues) {
            if (i++ % 2 == 0) {
                key = (String) keyValue;
            } else {
                value = keyValue;
                ElementHelper.validateProperty(key, value);
            }
        }
        this.sqlGraph.tx().readWrite();
        this.sqlGraph.getSchemaManager().ensureEdgeTableExist(label, inVertex.label(), this.label, keyValues);
        this.sqlGraph.getSchemaManager().addEdgeLabelToVerticesTable((Long) this.id(), label, false);
        this.sqlGraph.getSchemaManager().addEdgeLabelToVerticesTable((Long) inVertex.id(), label, true);
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

    @Override
    public void remove() {
        this.sqlGraph.tx().readWrite();
        //Remove all edges
        Iterator<SqlEdge> edges = this.getEdges(Direction.BOTH);
        while (edges.hasNext()) {
            edges.next().remove();
        }

        StringBuilder sql = new StringBuilder("DELETE FROM \"");
        sql.append(SchemaManager.VERTICES);
        sql.append("\" WHERE ID = ?;");
        Connection conn;
        PreparedStatement preparedStatement = null;
        try {
            conn = this.sqlGraph.tx().getConnection();
            preparedStatement = conn.prepareStatement(sql.toString());
            preparedStatement.setLong(1, (Long) this.id());
            int numberOfRowsUpdated = preparedStatement.executeUpdate();
            if (numberOfRowsUpdated != 1) {
                throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Vertex.class, this.id());
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


    protected void insertVertex() {

        long vertexId = insertGlobalVertex();

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
            for (ImmutablePair<PropertyType, Object> pair : SqlUtil.transformToTypeAndValue(this.keyValues)) {
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

    ///TODO make this lazy
    public Iterator<SqlEdge> getEdges(Direction direction, String... labels) {
        List<Direction> directions = new ArrayList<>(2);
        List<SqlEdge> edges = new ArrayList<>();
        Set<String> inVertexLabels = new HashSet<>();
        Set<String> outVertexLabels = new HashSet<>();
        if (direction == Direction.IN) {
            inVertexLabels.addAll(this.sqlGraph.getSchemaManager().getLabelsForVertex((Long) this.id(), true));
            if (labels.length > 0)
                inVertexLabels.retainAll(Arrays.asList(labels));
            directions.add(direction);
        } else if (direction == Direction.OUT) {
            outVertexLabels.addAll(this.sqlGraph.getSchemaManager().getLabelsForVertex((Long) this.id(), false));
            if (labels.length > 0)
                outVertexLabels.retainAll(Arrays.asList(labels));
            directions.add(direction);
        } else {
            inVertexLabels.addAll(this.sqlGraph.getSchemaManager().getLabelsForVertex((Long) this.id(), true));
            outVertexLabels.addAll(this.sqlGraph.getSchemaManager().getLabelsForVertex((Long) this.id(), false));
            if (labels.length > 0) {
                inVertexLabels.retainAll(Arrays.asList(labels));
                outVertexLabels.retainAll(Arrays.asList(labels));
            }
            directions.add(Direction.IN);
            directions.add(Direction.OUT);
        }
        for (Direction d : directions) {
            for (String label : (d == Direction.IN ? inVertexLabels : outVertexLabels)) {
                if (this.sqlGraph.getSchemaManager().tableExist(label)) {
                    StringBuilder sql = new StringBuilder("SELECT * FROM \"");
                    sql.append(label);
                    sql.append("\" WHERE ");
                    switch (d) {
                        case IN:
                            sql.append(" \"");
                            sql.append(this.label);
                            sql.append(SqlElement.IN_VERTEX_COLUMN_END);
                            sql.append("\" = ?");
                            break;
                        case OUT:
                            sql.append(" \"");
                            sql.append(this.label);
                            sql.append(SqlElement.OUT_VERTEX_COLUMN_END);
                            sql.append("\" = ?");
                            break;
                        case BOTH:
                            sql.append(" \"");
                            sql.append(this.label);
                            sql.append(SqlElement.OUT_VERTEX_COLUMN_END);
                            sql.append("\" = ? OR \"");
                            sql.append(this.label);
                            sql.append(SqlElement.IN_VERTEX_COLUMN_END);
                            sql.append("\" = ?");
                            break;
                    }
                    sql.append(";");
                    Connection conn;
                    PreparedStatement preparedStatement = null;
                    try {
                        conn = this.sqlGraph.tx().getConnection();
                        preparedStatement = conn.prepareStatement(sql.toString());

                        switch (d) {
                            case IN:
                                preparedStatement.setLong(1, this.primaryKey);
                                break;
                            case OUT:
                                preparedStatement.setLong(1, this.primaryKey);
                                break;
                            case BOTH:
                                preparedStatement.setLong(1, this.primaryKey);
                                preparedStatement.setLong(2, this.primaryKey);
                                break;
                        }

                        ResultSet resultSet = preparedStatement.executeQuery();
                        while (resultSet.next()) {
                            String inVertexColumnName = null;
                            String outVertexColumnName = null;
                            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                                String columnName = resultSetMetaData.getColumnName(i);
                                if (columnName.endsWith(SqlElement.IN_VERTEX_COLUMN_END)) {
                                    inVertexColumnName = columnName;
                                } else if (columnName.endsWith(SqlElement.OUT_VERTEX_COLUMN_END)) {
                                    outVertexColumnName = columnName;
                                }
                            }
                            if (inVertexColumnName == null || outVertexColumnName == null) {
                                throw new IllegalStateException("in or out vertex id not set!!!!");
                            }


                            Long edgeId = resultSet.getLong("ID");
                            Long inId = resultSet.getLong(inVertexColumnName);
                            Long outId = resultSet.getLong(outVertexColumnName);

                            List<Object> keyValues = new ArrayList<>();
                            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                                String columnName = resultSetMetaData.getColumnName(i);
                                if (!((columnName.equals("ID") || columnName.equals(inVertexColumnName) || columnName.equals(outVertexColumnName)))) {
                                    keyValues.add(columnName);
                                    keyValues.add(resultSet.getObject(columnName));
                                }
                            }
                            SqlEdge sqlEdge = null;
                            switch (direction) {
                                case IN:
                                    sqlEdge = new SqlEdge(this.sqlGraph, edgeId, label, this, new SqlVertex(this.sqlGraph, outId, outVertexColumnName.replace(SqlElement.OUT_VERTEX_COLUMN_END, "")), keyValues.toArray());
                                    break;
                                case OUT:
                                    sqlEdge = new SqlEdge(this.sqlGraph, edgeId, label, new SqlVertex(this.sqlGraph, inId, inVertexColumnName.replace(SqlElement.IN_VERTEX_COLUMN_END, "")), this, keyValues.toArray());
                                    break;
                                case BOTH:
                                    if (inId == this.primaryKey && outId != this.primaryKey) {
                                        sqlEdge = new SqlEdge(this.sqlGraph, edgeId, label, this, new SqlVertex(this.sqlGraph, outId, outVertexColumnName.replace(SqlElement.OUT_VERTEX_COLUMN_END, "")), keyValues.toArray());
                                    } else if (inId != this.primaryKey && outId == this.primaryKey) {
                                        sqlEdge = new SqlEdge(this.sqlGraph, edgeId, label, new SqlVertex(this.sqlGraph, inId, inVertexColumnName.replace(SqlElement.IN_VERTEX_COLUMN_END, "")), this, keyValues.toArray());
                                    } else if (inId == this.primaryKey && outId == this.primaryKey) {
                                        sqlEdge = new SqlEdge(this.sqlGraph, edgeId, label, this, this, keyValues.toArray());
                                    } else {
                                        throw new IllegalStateException("inId or outId must match!");
                                    }
                                    break;
                            }
                            edges.add(sqlEdge);
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

        }
        return edges.iterator();

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
            while (resultSet.next()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<Object> keyValues = new ArrayList<>();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    String columnName = resultSetMetaData.getColumnName(i);
                    Object o = resultSet.getObject(columnName);
                    if (!columnName.equals("ID") && !Objects.isNull(o)) {
                        keyValues.add(columnName);
                        keyValues.add(o);
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
