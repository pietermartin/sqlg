package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.process.graph.util.SqlgHasStepStrategy;
import org.umlg.sqlg.strategy.SqlGGraphStepStrategy;

import java.sql.*;
import java.util.*;

/**
 * Date: 2014/07/12
 * Time: 5:42 AM
 */
public class SqlgVertex extends SqlgElement implements Vertex {

    private Logger logger = LoggerFactory.getLogger(SqlgVertex.class.getName());

    public SqlgVertex(SqlG sqlG, String schema, String table, Object... keyValues) {
        super(sqlG, schema, table);
        insertVertex(keyValues);
    }

    public SqlgVertex(SqlG sqlG, Long id, String label) {
        super(sqlG, id, label);
    }

    public SqlgVertex(SqlG sqlG, Long id, String schema, String table) {
        super(sqlG, id, schema, table);
    }

    public Edge addEdgeWithMap(String label, Vertex inVertex, Map<String, Object> keyValues) {
        Object[] parameters = SqlgUtil.mapTokeyValues(keyValues);
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
        SchemaTable schemaTablePair = SchemaTable.of(this.schema, label);
        this.sqlG.tx().readWrite();
        this.sqlG.getSchemaManager().ensureEdgeTableExist(
                schemaTablePair.getSchema(),
                schemaTablePair.getTable(),
                SchemaTable.of(
                        ((SqlgVertex) inVertex).schema,
                        ((SqlgVertex) inVertex).table + SqlgElement.IN_VERTEX_COLUMN_END
                ),
                SchemaTable.of(
                        this.schema,
                        this.table + SqlgElement.OUT_VERTEX_COLUMN_END
                ),
                keyValues);
        //TODO sort out schema and label
        this.sqlG.getSchemaManager().addEdgeLabelToVerticesTable((Long) this.id(), this.schema, label, false);
        this.sqlG.getSchemaManager().addEdgeLabelToVerticesTable((Long) inVertex.id(), this.schema, label, true);
        final SqlgEdge edge = new SqlgEdge(this.sqlG, schemaTablePair.getSchema(), schemaTablePair.getTable(), (SqlgVertex) inVertex, this, keyValues);
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
        Iterator<SqlgEdge> itty = getEdges(direction, labels);
        List<Vertex> vertices = new ArrayList();
        while (itty.hasNext()) {
            SqlgEdge sqlGEdge = itty.next();
            SqlgVertex inVertex = sqlGEdge.getInVertex();
            SqlgVertex outVertex = sqlGEdge.getOutVertex();
            if (inVertex.id().equals(SqlgVertex.this.id())) {
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
        Iterator<SqlgEdge> edges = this.getEdges(Direction.BOTH);
        while (edges.hasNext()) {
            edges.next().remove();
        }
        StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes(this.sqlG.getSqlDialect().getPublicSchema()));
        sql.append(".");
        sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" WHERE ");
        sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" = ?");
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
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


    private void insertVertex(Object... keyValues) {

        long vertexId = insertGlobalVertex();

        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.schema));
        sql.append(".");
        sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + this.table));
        sql.append(" (");
        sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        int i = 1;
        Map<String, Object> keyValueMap = SqlgUtil.transformToInsertValues(keyValues);
        if (keyValueMap.size() > 0) {
            sql.append(", ");
        } else {
            sql.append(" ");
        }
        for (String column : keyValueMap.keySet()) {
            sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(column));
            if (i++ < keyValueMap.size()) {
                sql.append(", ");
            }
        }
        sql.append(") VALUES (?");
        if (keyValueMap.size() > 0) {
            sql.append(", ");
        } else {
            sql.append(" ");
        }
        i = 1;
        for (String column : keyValueMap.keySet()) {
            sql.append("?");
            if (i++ < keyValueMap.size()) {
                sql.append(", ");
            }
        }
        sql.append(")");
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        i = 1;
        Connection conn = this.sqlG.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(i++, vertexId);
            setKeyValuesAsParameter(this.sqlG, i, conn, preparedStatement, keyValueMap);
            preparedStatement.executeUpdate();
            this.primaryKey = vertexId;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        //Cache the properties
        this.properties.putAll(keyValueMap);
    }

    private long insertGlobalVertex() {
        long vertexId;
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes(this.sqlG.getSqlDialect().getPublicSchema()));
        sql.append(".");
        sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" (");
        sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes("VERTEX_SCHEMA"));
        sql.append(", ");
        sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes("VERTEX_TABLE"));
        sql.append(") VALUES (?, ?)");
        if (this.sqlG.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlG.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setString(1, this.schema);
            preparedStatement.setString(2, this.table);
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

    private void retainLabels(Set<SchemaTable> vertexLabels, String... labels) {
        Set<SchemaTable> toRemove = new HashSet<>();
        for (SchemaTable schemaTable : vertexLabels) {
            boolean retain = false;
            for (String label : labels) {
                if (schemaTable.getTable().equals(label)) {
                    retain = true;
                    break;
                }
            }
            if (!retain) {
                toRemove.add(schemaTable);
            }
        }
        vertexLabels.removeAll(toRemove);
    }

    ///TODO make this lazy
    public Iterator<SqlgEdge> getEdges(Direction direction, String... labels) {
        List<Direction> directions = new ArrayList<>(2);
        Set<SqlgEdge> edges = new HashSet<>();
        Set<SchemaTable> inVertexLabels = new HashSet<>();
        Set<SchemaTable> outVertexLabels = new HashSet<>();
        if (direction == Direction.IN) {
            inVertexLabels.addAll(this.sqlG.getSchemaManager().getLabelsForVertex((Long) this.id(), true));
            if (labels.length > 0) {
                retainLabels(inVertexLabels, labels);
            }
            directions.add(direction);
        } else if (direction == Direction.OUT) {
            outVertexLabels.addAll(this.sqlG.getSchemaManager().getLabelsForVertex((Long) this.id(), false));
            if (labels.length > 0) {
                retainLabels(outVertexLabels, labels);
            }
            directions.add(direction);
        } else {
            inVertexLabels.addAll(this.sqlG.getSchemaManager().getLabelsForVertex((Long) this.id(), true));
            outVertexLabels.addAll(this.sqlG.getSchemaManager().getLabelsForVertex((Long) this.id(), false));
            if (labels.length > 0) {
                retainLabels(inVertexLabels, labels);
                retainLabels(outVertexLabels, labels);
            }
            directions.add(Direction.IN);
            directions.add(Direction.OUT);
        }
        for (Direction d : directions) {
            for (SchemaTable schemaTable : (d == Direction.IN ? inVertexLabels : outVertexLabels)) {
                if (this.sqlG.getSchemaManager().tableExist(schemaTable.getSchema(), SchemaManager.EDGE_PREFIX + schemaTable.getTable())) {
                    StringBuilder sql = new StringBuilder("SELECT * FROM ");
                    switch (d) {
                        case IN:
                            sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                            sql.append(".");
                            sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + schemaTable.getTable()));
                            sql.append(" WHERE ");
                            sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes(this.schema + "." + this.table + SqlgElement.IN_VERTEX_COLUMN_END));
                            sql.append(" = ?");
                            break;
                        case OUT:
                            sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                            sql.append(".");
                            sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + schemaTable.getTable()));
                            sql.append(" WHERE ");
                            sql.append(this.sqlG.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.schema + "." + this.table + SqlgElement.OUT_VERTEX_COLUMN_END));
                            sql.append(" = ?");
                            break;
                        case BOTH:
                            throw new IllegalStateException("BUG: Direction.BOTH should never fire here!");
                    }
                    if (this.sqlG.getSqlDialect().needsSemicolon()) {
                        sql.append(";");
                    }
                    Connection conn = this.sqlG.tx().getConnection();
                    if(logger.isDebugEnabled()) {
                        logger.debug(sql.toString());
                    }
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
                                if (columnName.endsWith(SqlgElement.IN_VERTEX_COLUMN_END)) {
                                    inVertexColumnNames.add(columnName);
                                } else if (columnName.endsWith(SqlgElement.OUT_VERTEX_COLUMN_END)) {
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
                            SchemaTable inSchemaTable = SqlgUtil.parseLabel(inVertexColumnName, this.sqlG.getSqlDialect().getPublicSchema());
                            SchemaTable outSchemaTable = SqlgUtil.parseLabel(outVertexColumnName, this.sqlG.getSqlDialect().getPublicSchema());

                            List<Object> keyValues = new ArrayList<>();
                            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                                String columnName = resultSetMetaData.getColumnName(i);
                                if (!((columnName.equals("ID") || columnName.equals(inVertexColumnNames) || columnName.equals(outVertexColumnNames)))) {
                                    keyValues.add(columnName);
                                    keyValues.add(resultSet.getObject(columnName));
                                }
                            }
                            SqlgEdge sqlGEdge = null;
                            switch (d) {
                                case IN:
                                    sqlGEdge = new SqlgEdge(
                                            this.sqlG,
                                            edgeId,
                                            schemaTable.getSchema(),
                                            schemaTable.getTable(),
                                            this,
                                            new SqlgVertex(
                                                    this.sqlG,
                                                    outId,
                                                    outSchemaTable.getSchema(),
                                                    outSchemaTable.getTable().replace(SqlgElement.OUT_VERTEX_COLUMN_END, "")
                                            ),
                                            keyValues.toArray());
                                    break;
                                case OUT:
                                    sqlGEdge = new SqlgEdge(
                                            this.sqlG,
                                            edgeId,
                                            schemaTable.getSchema(),
                                            schemaTable.getTable(),
                                            new SqlgVertex(
                                                    this.sqlG,
                                                    inId,
                                                    inSchemaTable.getSchema(),
                                                    inSchemaTable.getTable().replace(SqlgElement.IN_VERTEX_COLUMN_END, "")
                                            ),
                                            this,
                                            keyValues.toArray());
                                    break;
                                case BOTH:
                                    throw new IllegalStateException("This should not be possible!");
                            }
                            edges.add(sqlGEdge);
                        }

                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        //Illustrating tp3 bug
//        List<SqlgEdge> result = new ArrayList<>(edges);
//        Collections.reverse(result);
        return edges.iterator();

    }

    @Override
    protected void load() {
        if (this.properties.isEmpty()) {
            StringBuilder sql = new StringBuilder("SELECT * FROM ");
            sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes(this.schema));
            sql.append(".");
            sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + this.table));
            sql.append(" WHERE ");
            sql.append(this.sqlG.getSqlDialect().maybeWrapInQoutes("ID"));
            sql.append(" = ?");
            if (this.sqlG.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            Connection conn = this.sqlG.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.setLong(1, this.primaryKey);
                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    loadResultSet(resultSet);
                } else {
                    throw new IllegalStateException(String.format("Vertex with label %s and id %d does exist.", new Object[]{this.schema + "." + this.table, this.primaryKey}));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    void loadResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnName(i);
            Object o = resultSet.getObject(columnName);
            if (!columnName.equals("ID") && !Objects.isNull(o)) {
                int type = resultSetMetaData.getColumnType(i);
                switch (type) {
                    case Types.SMALLINT:
                        this.properties.put(columnName, ((Integer) o).shortValue());
                        break;
                    case Types.TINYINT:
                        this.properties.put(columnName, ((Integer) o).byteValue());
                        break;
                    case Types.REAL:
                        this.properties.put(columnName, ((Number) o).floatValue());
                        break;
                    case Types.DOUBLE:
                        this.properties.put(columnName, ((Number) o).doubleValue());
                        break;
                    case Types.ARRAY:
                        Array array = (Array) o;
                        int baseType = array.getBaseType();
                        Object[] objectArray = (Object[]) array.getArray();
                        this.properties.put(columnName, convertObjectArrayToPrimitiveArray(objectArray, baseType));
                        break;
                    default:
                        this.properties.put(columnName, o);
                }
            }
        }
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

//    @Override
//    public GraphTraversal<Vertex, Vertex> start() {
//        final Neo4jTraversal<Vertex, Vertex> traversal = new DefaultNeo4jTraversal<>(this.graph);
//        return (GraphTraversal)traversal.addStep(new StartStep<>(traversal, this));
//    }

    @Override
    public GraphTraversal<Vertex, Vertex> start() {
        final GraphTraversal<Vertex, Vertex> traversal = GraphTraversal.of();
        traversal.strategies().register(SqlGGraphStepStrategy.instance());
        traversal.strategies().register(SqlgHasStepStrategy.instance());
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }
}
