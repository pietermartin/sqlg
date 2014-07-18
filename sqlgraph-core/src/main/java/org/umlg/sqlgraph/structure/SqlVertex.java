package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.sql.*;
import java.util.*;

/**
 * Date: 2014/07/12
 * Time: 5:42 AM
 */
public class SqlVertex extends SqlElement implements Vertex {

    public SqlVertex(SqlGraph sqlGraph, String label, Object... keyValues) {
        super(sqlGraph, label, keyValues);
        insertVertex(keyValues);
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
    public Iterator<Edge> edges(Direction direction, int branchFactor, String... labels) {
        this.sqlGraph.tx().readWrite();
        return (Iterator) StreamFactory.stream(getEdges(direction, labels)).limit(branchFactor).iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, int branchFactor, String... labels) {
        Iterator<SqlEdge> itty = getEdges(direction, labels);
        Iterator<Vertex> vertexIterator = new Iterator<Vertex>() {
            public SqlVertex next() {
                SqlEdge sqlEdge = itty.next();
                SqlVertex inVertex = sqlEdge.getInVertex();
                SqlVertex outVertex = sqlEdge.getInVertex();
                if (direction == Direction.IN) {
                    return outVertex;
                } else {
                    return inVertex;
                }
            }

            public boolean hasNext() {
                return itty.hasNext();
            }

            public void remove() {
                itty.remove();
            }
        };
        return (Iterator) StreamFactory.stream(vertexIterator).limit(branchFactor).iterator();
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


    protected void insertVertex(Object... keyValues) {

        long vertexId = insertGlobalVertex();

        StringBuilder sql = new StringBuilder("INSERT INTO \"");
        sql.append(this.label);
        sql.append("\" (\"ID\"");
        int i = 1;
        List<String> columns = SqlUtil.transformToInsertColumns(keyValues);
        if (columns.size() > 0) {
            sql.append(", ");
        } else {
            sql.append(" ");
        }
        for (String column : columns) {
            sql.append("\"").append(column).append("\"");
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
        sql.append(") VALUES (?");
        if (columns.size() > 0) {
            sql.append(", ");
        } else {
            sql.append(" ");
        }
        i = 1;
        List<String> values = SqlUtil.transformToInsertValues(keyValues);
        for (String value : values) {
            sql.append("?");
            if (i++ < values.size()) {
                sql.append(", ");
            }
        }
        sql.append(");");
        i = 1;
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(i++, vertexId);
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
            preparedStatement.executeUpdate();
            this.primaryKey = vertexId;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long insertGlobalVertex() {
        long vertexId;
        StringBuilder sql = new StringBuilder("INSERT INTO \"");
        sql.append(SchemaManager.VERTICES);
        sql.append("\"(VERTEX_TABLE) VALUES (?);");
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setString(1, this.label);
            preparedStatement.executeUpdate();
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
                vertexId = generatedKeys.getLong(1);
            } else {
                throw new RuntimeException("Could not retrieve the id after an insert into " + SchemaManager.VERTICES);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return vertexId;
    }

    ///TODO make this lazy
    public Iterator<SqlEdge> getEdges(Direction direction, String... labels) {
        List<Direction> directions = new ArrayList<>(2);
        Set<SqlEdge> edges = new HashSet<>();
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
                    Connection conn = this.sqlGraph.tx().getConnection();
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
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

                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return edges.iterator();

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
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    String columnName = resultSetMetaData.getColumnName(i);
                    Object o = resultSet.getObject(columnName);
                    if (!columnName.equals("ID") && !Objects.isNull(o)) {
                        keyValues.add(columnName);
                        keyValues.add(o);
                    }
                }
                break;
            }
            return keyValues.toArray();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

}
