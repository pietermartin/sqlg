package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

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
        StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGES));
        sql.append(" WHERE ");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" = ?");
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(1, (Long) this.id());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        super.remove();
    }

    public SqlVertex getInVertex() {
        if (this.inVertex == null) {
            load();
        }
        return inVertex;
    }

    public SqlVertex getOutVertex() {
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

    protected void insertEdge(Object... keyValues) throws SQLException {

        long edgeId = insertGlobalEdge();

        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.label));
        sql.append(" (");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(", ");
        int i = 1;
        List<String> columns = SqlUtil.transformToInsertColumns(keyValues);
        for (String column : columns) {
            sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(column));
            if (i++ < columns.size()) {
                sql.append(", ");
            }
        }
        if (columns.size() > 0) {
            sql.append(", ");
        }
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.inVertex.label() + SqlElement.IN_VERTEX_COLUMN_END));
        sql.append(", ");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.outVertex.label() + SqlElement.OUT_VERTEX_COLUMN_END));
        sql.append(") VALUES (?, ");
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
        sql.append(")");
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        i = 1;
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(i++, edgeId);
            i = setKeyValuesAsParameter(i, conn, preparedStatement, keyValues);
            preparedStatement.setLong(i++, this.inVertex.primaryKey);
            preparedStatement.setLong(i++, this.outVertex.primaryKey);
            preparedStatement.executeUpdate();
            this.primaryKey = edgeId;
        }
    }

    private long insertGlobalEdge() throws SQLException {
        long vertexId;
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGES));
        sql.append(" (");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("EDGE_TABLE"));
        sql.append(") VALUES (?)");
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
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
        StringBuilder sql = new StringBuilder("SELECT * FROM ");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.label));
        sql.append(" WHERE ");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" = ?");
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
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
                    Object o = resultSet.getObject(columnName);
                    if (!columnName.equals("ID") &&
                            !Objects.isNull(o) &&
                            !columnName.endsWith(SqlElement.OUT_VERTEX_COLUMN_END) &&
                            !columnName.endsWith(SqlElement.IN_VERTEX_COLUMN_END)) {

                        keyValues.add(columnName);

                        int type = resultSetMetaData.getColumnType(i);
                        switch (type) {
                            case Types.SMALLINT:
                                keyValues.add(((Integer)o).shortValue());
                                break;
                            case Types.TINYINT:
                                keyValues.add(((Integer)o).byteValue());
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
