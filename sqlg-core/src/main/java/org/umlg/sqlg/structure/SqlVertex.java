package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.*;
import java.util.*;

/**
 * Date: 2014/07/12
 * Time: 5:42 AM
 */
public class SqlVertex extends SqlElement implements Vertex {

    public SqlVertex(SqlG sqlG, String label, Object... keyValues) {
        super(sqlG, label, keyValues);
        insertVertex(keyValues);
    }

    public SqlVertex(SqlG sqlG, Long id, String label) {
        super(sqlG, id, label);
    }

    public Edge addEdgeWithMap(String label, Vertex inVertex, Map<String, Object> keyValues) {
        Object[] parameters = SqlUtil.mapTokeyValues(keyValues);
        return addEdge(label, inVertex, parameters);
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
                this.sqlG.getSqlDialect().validateProperty(key, value);
            }
        }
        Pair<String, String> schemaTablePair = SqlUtil.parseLabel(label);
        this.sqlG.tx().readWrite();
        this.sqlG.getSchemaManager().ensureEdgeTableExist(
                schemaTablePair.getLeft(), schemaTablePair.getRight(),
                ImmutablePair.of(
                        inVertex.label() + SqlElement.IN_VERTEX_COLUMN_END,
                        this.label + SqlElement.OUT_VERTEX_COLUMN_END
                ),
                keyValues);
        this.sqlG.getSchemaManager().addEdgeLabelToVerticesTable((Long) this.id(), label, false);
        this.sqlG.getSchemaManager().addEdgeLabelToVerticesTable((Long) inVertex.id(), label, true);
        final SqlEdge edge = new SqlEdge(this.sqlG, label, (SqlVertex) inVertex, this, keyValues);
        return edge;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, int branchFactor, String... labels) {
        this.sqlG.tx().readWrite();
        return (Iterator) StreamFactory.stream(getEdges(direction, labels)).limit(branchFactor).iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, int branchFactor, String... labels) {
        this.sqlG.tx().readWrite();
        Iterator<SqlEdge> itty = getEdges(direction, labels);
        List<Vertex> vertices = new ArrayList();
        while (itty.hasNext()) {
            SqlEdge sqlEdge = itty.next();
            SqlVertex inVertex = sqlEdge.getInVertex();
            SqlVertex outVertex = sqlEdge.getOutVertex();
            if (inVertex.id().equals(SqlVertex.this.id())) {
                vertices.add(outVertex);
            } else {
                vertices.add(inVertex);
            }
        }
        return (Iterator) StreamFactory.stream(vertices.iterator()).limit(branchFactor).iterator();

//        Iterator<Vertex> vertexIterator = new Iterator<Vertex>() {
//            public SqlVertex next() {
//                SqlEdge sqlEdge = itty.next();
//                SqlVertex inVertex = sqlEdge.getInVertex();
//                SqlVertex outVertex = sqlEdge.getOutVertex();
//                if (inVertex.id().equals(SqlVertex.this.id())) {
//                     return outVertex;
//                } else {
//                    return inVertex;
//                }
//            }
//
//            public boolean hasNext() {
//                return itty.hasNext();
//            }
//
//            public void remove() {
//                itty.remove();
//            }
//        };
//        return (Iterator) StreamFactory.stream(vertexIterator).limit(branchFactor).iterator();
    }

    @Override
    public void remove() {
        this.sqlG.tx().readWrite();
        //Remove all edges
        Iterator<SqlEdge> edges = this.getEdges(Direction.BOTH);
        while (edges.hasNext()) {
            edges.next().remove();
        }
        StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" WHERE ");
        sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" = ?");
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(1, (Long) this.id());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        super.remove();
    }


    protected void insertVertex(Object... keyValues) {

        long vertexId = insertGlobalVertex();

        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + this.label));
        sql.append(" (");
        sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        int i = 1;
        List<String> columns = SqlUtil.transformToInsertColumns(keyValues);
        if (columns.size() > 0) {
            sql.append(", ");
        } else {
            sql.append(" ");
        }
        for (String column : columns) {
            sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(column));
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
        sql.append(")");
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
                sql.append(";");
        }
        i = 1;
        Connection conn = this.sqlG.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(i++, vertexId);
            setKeyValuesAsParameter(i, conn, preparedStatement, keyValues);
            preparedStatement.executeUpdate();
            this.primaryKey = vertexId;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long insertGlobalVertex() {
        long vertexId;
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" (");
        sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes("VERTEX_TABLE"));
        sql.append(") VALUES (?, ?)");
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
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
            inVertexLabels.addAll(this.sqlG.getSchemaManager().getLabelsForVertex((Long) this.id(), true));
            if (labels.length > 0)
                inVertexLabels.retainAll(Arrays.asList(labels));
            directions.add(direction);
        } else if (direction == Direction.OUT) {
            outVertexLabels.addAll(this.sqlG.getSchemaManager().getLabelsForVertex((Long) this.id(), false));
            if (labels.length > 0)
                outVertexLabels.retainAll(Arrays.asList(labels));
            directions.add(direction);
        } else {
            inVertexLabels.addAll(this.sqlG.getSchemaManager().getLabelsForVertex((Long) this.id(), true));
            outVertexLabels.addAll(this.sqlG.getSchemaManager().getLabelsForVertex((Long) this.id(), false));
            if (labels.length > 0) {
                inVertexLabels.retainAll(Arrays.asList(labels));
                outVertexLabels.retainAll(Arrays.asList(labels));
            }
            directions.add(Direction.IN);
            directions.add(Direction.OUT);
        }
        for (Direction d : directions) {
            for (String label : (d == Direction.IN ? inVertexLabels : outVertexLabels)) {
                if (this.sqlG.getSchemaManager().tableExist(SchemaManager.EDGE_PREFIX + label)) {
                    StringBuilder sql = new StringBuilder("SELECT * FROM ");
                    sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + label));
                    sql.append(" WHERE ");
                    switch (d) {
                        case IN:
                            sql.append(" ");
                            sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.label + SqlElement.IN_VERTEX_COLUMN_END));
                            sql.append(" = ?");
                            break;
                        case OUT:
                            sql.append(" ");
                            sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.label + SqlElement.OUT_VERTEX_COLUMN_END));
                            sql.append(" = ?");
                            break;
                        case BOTH:
                            throw new IllegalStateException("BUG: Direction.BOTH should never fire here!");
                    }
                    if (this.sqlG.getSqlDialect().needsSemicolon()) {
                        sql.append(";");
                    }
                    Connection conn = this.sqlG.tx().getConnection();
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                        switch (d) {
                            case IN:
                                preparedStatement.setLong(1, this.primaryKey);
                                break;
                            case OUT:
                                preparedStatement.setLong(1, this.primaryKey);
                                break;
                            case BOTH:
                                throw new IllegalStateException("BUG: Direction.BOTH should never fire here!");
                        }

                        ResultSet resultSet = preparedStatement.executeQuery();
                        while (resultSet.next()) {
                            Set<String> inVertexColumnNames = new HashSet<>();
                            Set<String> outVertexColumnNames = new HashSet<>();
                            String inVertexColumnName = "";
                            String outVertexColumnName = "";
                            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                                String columnName = resultSetMetaData.getColumnName(i);
                                if (columnName.endsWith(SqlElement.IN_VERTEX_COLUMN_END)) {
                                    inVertexColumnNames.add(columnName);
                                } else if (columnName.endsWith(SqlElement.OUT_VERTEX_COLUMN_END)) {
                                    outVertexColumnNames.add(columnName);
                                }
                            }
                            if (inVertexColumnNames.isEmpty() || outVertexColumnNames.isEmpty()) {
                                throw new IllegalStateException("in or out vertex id not set!!!!");
                            }

                            Long edgeId = resultSet.getLong("ID");
                            Long inId = null;
                            Long outId = null;

                            //Only one in out pair should ever be set per row
                            for (String inColumnName : inVertexColumnNames) {
                                if (inId != null) {
                                    Long tempInId = resultSet.getLong(inColumnName);
                                    if (!resultSet.wasNull()) {
                                        throw new IllegalStateException("Multiple in columns are set in vertex row!");
                                    }
                                } else {
                                    Long tempInId = resultSet.getLong(inColumnName);
                                    if (!resultSet.wasNull()) {
                                        inId = tempInId;
                                        inVertexColumnName = inColumnName;
                                    }
                                }
                            }
                            for (String outColumnName : outVertexColumnNames) {
                                if (outId != null) {
                                    Long tempOutId = resultSet.getLong(outColumnName);
                                    if (!resultSet.wasNull()) {
                                        throw new IllegalStateException("Multiple out columns are set in vertex row!");
                                    }
                                } else {
                                    Long tempOutId = resultSet.getLong(outColumnName);
                                    if (!resultSet.wasNull()) {
                                        outId = tempOutId;
                                        outVertexColumnName = outColumnName;
                                    }
                                }
                            }
                            if (inVertexColumnName.isEmpty() || outVertexColumnName.isEmpty()) {
                                throw new IllegalStateException("inVertexColumnName or outVertexColumnName is empty!");
                            }

                            List<Object> keyValues = new ArrayList<>();
                            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                                String columnName = resultSetMetaData.getColumnName(i);
                                if (!((columnName.equals("ID") || columnName.equals(inVertexColumnNames) || columnName.equals(outVertexColumnNames)))) {
                                    keyValues.add(columnName);
                                    keyValues.add(resultSet.getObject(columnName));
                                }
                            }
                            SqlEdge sqlEdge = null;
                            switch (d) {
                                case IN:
                                    sqlEdge = new SqlEdge(this.sqlG, edgeId, label, this, new SqlVertex(this.sqlG, outId, outVertexColumnName.replace(SqlElement.OUT_VERTEX_COLUMN_END, "")), keyValues.toArray());
                                    break;
                                case OUT:
                                    sqlEdge = new SqlEdge(this.sqlG, edgeId, label, new SqlVertex(this.sqlG, inId, inVertexColumnName.replace(SqlElement.IN_VERTEX_COLUMN_END, "")), this, keyValues.toArray());
                                    break;
                                case BOTH:
                                    throw new IllegalStateException("This should not be possible!");
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
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + this.label));
        sql.append(" WHERE ");
        sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" = ?");
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(1, this.primaryKey);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    String columnName = resultSetMetaData.getColumnName(i);
                    Object o = resultSet.getObject(columnName);
                    if (!columnName.equals("ID") && !Objects.isNull(o)) {
                        keyValues.add(columnName);
                        int type = resultSetMetaData.getColumnType(i);
                        switch (type) {
                            case Types.SMALLINT:
                                keyValues.add(((Integer)o).shortValue());
                                break;
                            case Types.TINYINT:
                                keyValues.add(((Integer)o).byteValue());
                                break;
                            case Types.REAL:
                                keyValues.add(((Number)o).floatValue());
                                break;
                            case Types.DOUBLE:
                                keyValues.add(((Number)o).doubleValue());
                                break;
                            case Types.ARRAY:
                                Array array = (Array) o;
                                int baseType = array.getBaseType();
                                Object[] objectArray = (Object[]) array.getArray();
                                keyValues.add(convertObjectArrayToPrimitiveArray(objectArray, baseType));
                                break;
                            default:
                                keyValues.add(o);
                        }
                    }
                }
            } else {
                throw new IllegalStateException(String.format("Vertex with label %s and id %d does exist.", new Object[]{this.label, this.primaryKey}));
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
