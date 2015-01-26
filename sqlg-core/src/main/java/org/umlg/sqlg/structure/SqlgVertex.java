package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Date: 2014/07/12
 * Time: 5:42 AM
 */
public class SqlgVertex extends SqlgElement implements Vertex, Vertex.Iterators {

    private Logger logger = LoggerFactory.getLogger(SqlgVertex.class.getName());
    Set<SchemaTable> inLabelsForVertex = null;
    Set<SchemaTable> outLabelsForVertex = null;

    /**
     * Called from SqlG.addVertex
     *
     * @param sqlgGraph
     * @param schema
     * @param table
     * @param keyValues
     */
    public SqlgVertex(SqlgGraph sqlgGraph, String schema, String table, Object... keyValues) {
        super(sqlgGraph, schema, table);
        insertVertex(keyValues);
        if (!sqlgGraph.tx().isInBatchMode()) {
            sqlgGraph.tx().add(this);
        }
    }

    public SqlgVertex(SqlgGraph sqlgGraph, Long id, String label) {
        super(sqlgGraph, id, label);
    }

    public static SqlgVertex of(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        if (!sqlgGraph.tx().isInBatchMode()) {
            return sqlgGraph.tx().putVertexIfAbsent(sqlgGraph, id, schema, table);
        } else {
            return new SqlgVertex(sqlgGraph, id, schema, table);
        }
    }

    /**
     * This is the primary constructor to create a vertex that already exist
     *
     * @param sqlgGraph
     * @param id
     * @param schema
     * @param table
     */
    SqlgVertex(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        super(sqlgGraph, id, schema, table);
    }

    private SqlgVertex(SqlgGraph sqlgGraph, Long id, String schema, String table, Object... keyValues) {
        super(sqlgGraph, id, schema, table);
        Map<String, Object> keyValueMap = SqlgUtil.transformToInsertValues(keyValues);
        this.properties.putAll(keyValueMap);
    }

    public Edge addEdgeWithMap(String label, Vertex inVertex, Map<String, Object> keyValues) {
        Object[] parameters = SqlgUtil.mapToStringKeyValues(keyValues);
        return addEdge(label, inVertex, parameters);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id());
        ElementHelper.validateLabel(label);
        if (label.contains("."))
            throw new IllegalStateException(String.format("Edge label may not contain a '.' , the edge will be stored in the schema of the owning vertex. label = %s", new Object[]{label}));
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
                this.sqlgGraph.getSqlDialect().validateProperty(key, value);
            }
        }
        SchemaTable schemaTablePair = SchemaTable.of(this.schema, label);
        this.sqlgGraph.tx().readWrite();
        this.sqlgGraph.getSchemaManager().ensureEdgeTableExist(
                schemaTablePair.getSchema(),
                schemaTablePair.getTable(),
                SchemaTable.of(
                        ((SqlgVertex) inVertex).schema,
                        ((SqlgVertex) inVertex).table
                ),
                SchemaTable.of(
                        this.schema,
                        this.table
                ),
                keyValues);
        this.sqlgGraph.getSchemaManager().addEdgeLabelToVerticesTable(this, this.schema, label, false);
        this.sqlgGraph.getSchemaManager().addEdgeLabelToVerticesTable((SqlgVertex) inVertex, this.schema, label, true);
        final SqlgEdge edge = new SqlgEdge(this.sqlgGraph, schemaTablePair.getSchema(), schemaTablePair.getTable(), (SqlgVertex) inVertex, this, keyValues);
        return edge;
    }

    @Override
    protected <V> Map<String, VertexProperty<V>> internalGetAllProperties(final String... propertyKeys) {
        this.sqlgGraph.tx().readWrite();
        Map<String, ? extends Property<V>> metaPropertiesMap = super.internalGetAllProperties(propertyKeys);
        return (Map<String, VertexProperty<V>>) metaPropertiesMap;
    }

    @Override
    protected <V> Map<String, VertexProperty<V>> internalGetProperties(final String... propertyKeys) {
        this.sqlgGraph.tx().readWrite();
        Map<String, ? extends Property<V>> metaPropertiesMap = super.internalGetProperties(propertyKeys);
        return (Map<String, VertexProperty<V>>) metaPropertiesMap;
    }

    @Override
    protected <V> Map<String, VertexProperty<V>> internalGetHiddens(final String... propertyKeys) {
        this.sqlgGraph.tx().readWrite();
        Map<String, ? extends Property<V>> metaPropertiesMap = super.internalGetHiddens(propertyKeys);
        return (Map<String, VertexProperty<V>>) metaPropertiesMap;
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        this.sqlgGraph.tx().readWrite();
        if (this.removed) {
            throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id());
        } else {
            if (!sqlgGraph.tx().isInBatchMode()) {
                SqlgVertex sqlgVertex = this.sqlgGraph.tx().putVertexIfAbsent(this);
                if (sqlgVertex != this) {
                    //sync the properties
                    this.properties = sqlgVertex.properties;
                }
            }
            return (VertexProperty) super.property(key);
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id());
        ElementHelper.validateProperty(key, value);
        this.sqlgGraph.tx().readWrite();
        return (VertexProperty) super.property(key, value);
    }

    @Override
    protected <V> SqlgProperty<V> instantiateProperty(String key, V value) {
        return new SqlgVertexProperty<>(this.sqlgGraph, this, key, value);
    }

    @Override
    protected Property emptyProperty() {
        return VertexProperty.empty();
    }

    private Iterator<Edge> internalGetEdges(Direction direction, String... labels) {
        this.sqlgGraph.tx().readWrite();
        return (Iterator) StreamFactory.stream(_internalGetEdges(direction, labels)).iterator();
    }

    public Iterator<Vertex> vertices(List<HasContainer> hasContainers, Direction direction, String... labels) {
        this.sqlgGraph.tx().readWrite();
        Iterator<Vertex> itty = internalGetVertices(hasContainers, direction, labels);
        return itty;
    }

    public Iterator<Element> elements(List<Pair<VertexStep, List<HasContainer>>> replacedSteps) {
        this.sqlgGraph.tx().readWrite();
        Iterator<Element> itty = internalGetElements(replacedSteps);
        return itty;
    }


    @Override
    public void remove() {
        this.sqlgGraph.tx().readWrite();

        if (this.removed)
            throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id());

        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            this.sqlgGraph.tx().getBatchManager().removeVertex(this.schema, this.table, this);
        } else {
            //Remove all edges
            Iterator<SqlgEdge> edges = this._internalGetEdges(Direction.BOTH);
            while (edges.hasNext()) {
                edges.next().remove();
            }
            super.remove();
            StringBuilder sql = new StringBuilder("DELETE FROM ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
            sql.append(" WHERE ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
            sql.append(" = ?");
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            Connection conn = this.sqlgGraph.tx().getConnection();
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.setLong(1, (Long) this.id());
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

    }


    private void insertVertex(Object... keyValues) {
        Map<String, Object> keyValueMap = SqlgUtil.transformToInsertValues(keyValues);
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            internalBatchAddVertex(keyValueMap);
        } else {
            internalAddVertex(keyValueMap);
        }
        //Cache the properties
        this.properties.putAll(keyValueMap);
        //its a new vertex so we know it has no labels
        this.inLabelsForVertex = new HashSet<>();
        this.outLabelsForVertex = new HashSet<>();
    }

    private void internalBatchAddVertex(Map<String, Object> keyValueMap) {
        this.sqlgGraph.tx().getBatchManager().addVertex(this, keyValueMap);
    }

    private void internalAddVertex(Map<String, Object> keyValueMap) {
        long vertexId = insertGlobalVertex();
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.schema));
        sql.append(".");
        sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + this.table));
        sql.append(" (");
        sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        int i = 1;
        if (keyValueMap.size() > 0) {
            sql.append(", ");
        } else {
            sql.append(" ");
        }
        for (String column : keyValueMap.keySet()) {
            sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(column));
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
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        i = 1;
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(i++, vertexId);
            setKeyValuesAsParameter(this.sqlgGraph, i, conn, preparedStatement, keyValueMap);
            preparedStatement.executeUpdate();
            this.primaryKey = vertexId;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long insertGlobalVertex() {
        long vertexId;
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()));
        sql.append(".");
        sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
        sql.append(" (");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("VERTEX_SCHEMA"));
        sql.append(", ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("VERTEX_TABLE"));
        sql.append(") VALUES (?, ?)");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
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

    //TODO make this lazy
    private Iterator<Vertex> internalGetVertices(List<HasContainer> hasContainers, Direction direction, String... labels) {

        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode() && this.sqlgGraph.tx().getBatchManager().vertexIsCached(this)) {
            List<Vertex> vertices = this.sqlgGraph.tx().getBatchManager().getVertices(this, direction, labels);
            return vertices.stream().filter(v -> HasContainer.testAll(v, hasContainers)).iterator();
        } else {

            List<Vertex> vertices = new ArrayList<>();
            List<HasContainer> labelHasContainers = filterHasContainerOnKey(hasContainers, T.label.getAccessor());
            Set<String> hasContainerLabels = extractLabelsFromHasContainer(labelHasContainers);

            List<Direction> directions = new ArrayList<>(2);
            Set<SchemaTable> inVertexLabels = new HashSet<>();
            Set<SchemaTable> outVertexLabels = new HashSet<>();
            if (direction == Direction.IN) {
                inVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getLabelsForVertex(this, true));
                if (labels.length > 0) {
                    retainLabels(inVertexLabels, labels);
                }
                directions.add(direction);
            } else if (direction == Direction.OUT) {
                outVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getLabelsForVertex(this, false));
                if (labels.length > 0) {
                    retainLabels(outVertexLabels, labels);
                }
                directions.add(direction);
            } else {
                inVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getLabelsForVertex(this, true));
                outVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getLabelsForVertex(this, false));
                if (labels.length > 0) {
                    retainLabels(inVertexLabels, labels);
                    retainLabels(outVertexLabels, labels);
                }
                directions.add(Direction.IN);
                directions.add(Direction.OUT);
            }
            for (Direction d : directions) {
                for (SchemaTable schemaTable : (d == Direction.IN ? inVertexLabels : outVertexLabels)) {
                    if (this.sqlgGraph.getSchemaManager().tableExist(schemaTable.getSchema(), SchemaManager.EDGE_PREFIX + schemaTable.getTable())) {

                        Set<String> edgeForeignKeys = this.sqlgGraph.getSchemaManager().getEdgeForeignKeys(schemaTable.getSchema() + "." + SchemaManager.EDGE_PREFIX + schemaTable.getTable());
                        Set<SchemaTable> tables;
                        switch (d) {
                            case IN:
                                tables = transformToOutSchemaTables(edgeForeignKeys, hasContainerLabels);
                                break;
                            case OUT:
                                tables = transformToInSchemaTables(edgeForeignKeys, hasContainerLabels);
                                break;
                            default:
                                throw new IllegalStateException("BUG: Direction.BOTH should never fire here!");
                        }

                        for (SchemaTable joinSchemaTable : tables) {

                            StringBuilder sql = new StringBuilder("SELECT b.*, c.");
                            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_IN_LABELS));
                            sql.append(", c.");
                            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_OUT_LABELS));
                            sql.append(", a.");
                            switch (d) {
                                case IN:
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(joinSchemaTable.getSchema() + "." + joinSchemaTable.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
                                    sql.append(" FROM ");

                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                                    sql.append(".");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + schemaTable.getTable()));

                                    //Need to join here on all of the out columns.
                                    sql.append(" a JOIN ");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(joinSchemaTable.getSchema()));
                                    sql.append(".");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + joinSchemaTable.getTable()));
                                    sql.append(" b ON a.");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(joinSchemaTable.getSchema() + "." + joinSchemaTable.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
                                    sql.append(" = b.");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                                    sql.append(" JOIN ");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()));
                                    sql.append(".");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
                                    sql.append(" c ON b.");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                                    sql.append(" = c.");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                                    sql.append(" WHERE ");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema + "." + this.table + SchemaManager.IN_VERTEX_COLUMN_END));
                                    sql.append(" = ? ");

                                    for (HasContainer hasContainer : hasContainers) {
                                        sql.append(" AND b.");
                                        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().hasContainerKeyToColumn(hasContainer.key)));
                                        if (!hasContainer.predicate.equals(Compare.eq)) {
                                            throw new IllegalStateException("Only equals is supported at the moment");
                                        }
                                        sql.append(" = ?");
                                    }

                                    break;
                                case OUT:
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(joinSchemaTable.getSchema() + "." + joinSchemaTable.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
                                    sql.append(" FROM ");

                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                                    sql.append(".");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + schemaTable.getTable()));

                                    //Need to join here on all of the in columns.
                                    sql.append(" a JOIN ");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(joinSchemaTable.getSchema()));
                                    sql.append(".");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + joinSchemaTable.getTable()));
                                    sql.append(" b ON a.");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(joinSchemaTable.getSchema() + "." + joinSchemaTable.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
                                    sql.append(" = b.");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                                    sql.append(" JOIN ");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()));
                                    sql.append(".");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
                                    sql.append(" c ON b.");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                                    sql.append(" = c.");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                                    sql.append(" WHERE ");
                                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema + "." + this.table + SchemaManager.OUT_VERTEX_COLUMN_END));
                                    sql.append(" = ? ");

                                    for (HasContainer hasContainer : hasContainers) {
                                        sql.append(" AND b.");
                                        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().hasContainerKeyToColumn(hasContainer.key)));
                                        if (!hasContainer.predicate.equals(Compare.eq)) {
                                            throw new IllegalStateException("Only equals is supported at the moment");
                                        }
                                        sql.append(" = ?");
                                    }

                                    break;
                                default:
                                    throw new IllegalStateException("BUG: Direction.BOTH should never fire here!");
                            }
                            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                                sql.append(";");
                            }
                            Connection conn = this.sqlgGraph.tx().getConnection();
                            if (logger.isDebugEnabled()) {
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
                                int countHasContainers = 2;
                                for (HasContainer hasContainer : hasContainers) {
                                    if (!hasContainer.predicate.equals(Compare.eq)) {
                                        throw new IllegalStateException("Only equals is supported at present.");
                                    }
                                    Map<String, Object> keyValues = new HashMap<>();
                                    keyValues.put(hasContainer.key, hasContainer.value);
                                    SqlgElement.setKeyValuesAsParameter(this.sqlgGraph, countHasContainers++, conn, preparedStatement, keyValues);
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
                                        if (columnName.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                                            inVertexColumnNames.add(columnName);
                                        } else if (columnName.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                                            outVertexColumnNames.add(columnName);
                                        }
                                    }
                                    if (inVertexColumnNames.isEmpty() && outVertexColumnNames.isEmpty()) {
                                        throw new IllegalStateException("BUG: in or out vertex id not set!!!!");
                                    }

                                    Long inId = null;
                                    Long outId = null;

                                    //Only one in out pair should ever be set per row
                                    for (String inColumnName : inVertexColumnNames) {
                                        if (inId != null) {
                                            resultSet.getLong(inColumnName);
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
                                            resultSet.getLong(outColumnName);
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
                                    if (inVertexColumnName.isEmpty() && outVertexColumnName.isEmpty()) {
                                        throw new IllegalStateException("inVertexColumnName or outVertexColumnName is empty!");
                                    }

                                    List<Object> keyValues = new ArrayList<>();
                                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                                        String columnName = resultSetMetaData.getColumnName(i);
                                        if (!(columnName.equals("ID") ||
                                                columnName.equals(SchemaManager.VERTEX_IN_LABELS) || columnName.equals(SchemaManager.VERTEX_OUT_LABELS) ||
                                                inVertexColumnNames.contains(columnName) || outVertexColumnNames.contains(columnName))) {
                                            keyValues.add(columnName);
                                            keyValues.add(resultSet.getObject(columnName));
                                        }

                                    }
                                    SqlgVertex sqlGVertex;
                                    switch (d) {
                                        case IN:
                                            sqlGVertex = SqlgVertex.of(this.sqlgGraph, outId, joinSchemaTable.getSchema(), joinSchemaTable.getTable());
                                            Map<String, Object> keyValueMap = SqlgUtil.transformToInsertValues(keyValues.toArray());
                                            sqlGVertex.properties.clear();
                                            sqlGVertex.properties.putAll(keyValueMap);
                                            this.sqlgGraph.loadVertexAndLabels(resultSet, sqlGVertex);
                                            vertices.add(sqlGVertex);
                                            break;
                                        case OUT:
                                            sqlGVertex = SqlgVertex.of(this.sqlgGraph, inId, joinSchemaTable.getSchema(), joinSchemaTable.getTable());
                                            keyValueMap = SqlgUtil.transformToInsertValues(keyValues.toArray());
                                            sqlGVertex.properties.clear();
                                            sqlGVertex.properties.putAll(keyValueMap);
                                            this.sqlgGraph.loadVertexAndLabels(resultSet, sqlGVertex);
                                            vertices.add(sqlGVertex);
                                            break;
                                        case BOTH:
                                            throw new IllegalStateException("This should not be possible!");
                                    }
                                }

                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }

                        }

                    }
                }
            }
            return vertices.iterator();
        }
    }

    /**
     * Generate a query for the replaced steps.
     * Each replaced step translates to a join statement and a section of the where clause.
     *
     * @param replacedSteps
     * @return The results of the query
     */
    private Iterator<Element> internalGetElements(List<Pair<VertexStep, List<HasContainer>>> replacedSteps) {
        List<Element> vertices = new ArrayList<>();

        SchemaTable schemaTable = SchemaTable.of(this.schema, SchemaManager.VERTEX_PREFIX + this.table);
        SchemaTableTree schemaTableTree = this.sqlgGraph.getGremlinParser().parse(schemaTable, replacedSteps);
        List<Pair<SchemaTable, String>> sqlStatements = schemaTableTree.constructSql();
        for (Pair<SchemaTable, String> sqlPair : sqlStatements) {
            Connection conn = this.sqlgGraph.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sqlPair.getRight());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sqlPair.getRight())) {
                preparedStatement.setLong(1, this.primaryKey);
                schemaTableTree.setParametersOnStatement(preparedStatement);
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    this.sqlgGraph.getGremlinParser().loadElements(resultSet, sqlPair.getLeft(), vertices);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return vertices.iterator();
    }

    private Set<String> extractLabelsFromHasContainer(List<HasContainer> labelHasContainers) {
        Set<String> result = new HashSet<>();
        for (HasContainer hasContainer : labelHasContainers) {
            result.add((String) hasContainer.value);
        }
        return result;
    }

    /**
     * filters the hasContainer on its key.
     *
     * @param hasContainers all HasContainers matching the key will be removed from this list
     * @param key
     * @return the HasContainers matching the key.
     */
    private List<HasContainer> filterHasContainerOnKey(List<HasContainer> hasContainers, String key) {
        List<HasContainer> toRemove = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            if (hasContainer.key.equals(key)) {
                toRemove.add(hasContainer);
            }
        }
        hasContainers.removeAll(toRemove);
        return toRemove;
    }

    private Set<SchemaTable> transformToInSchemaTables(Set<String> edgeForeignKeys, Set<String> labels) {
        Set<SchemaTable> result = new HashSet<>();
        for (String edgeForeignKey : edgeForeignKeys) {
            String[] schemaTableArray = edgeForeignKey.split("\\.");
            String schema = schemaTableArray[0];
            String table = schemaTableArray[1];
            if (table.endsWith("_IN_ID")) {
                table = table.substring(0, table.length() - 6);
                if (labels.isEmpty() || labels.contains(table)) {
                    result.add(SchemaTable.of(schema, table));
                }
            }
        }
        return result;
    }

    //TODO make this lazy
    private Iterator<SqlgEdge> _internalGetEdges(Direction direction, String... labels) {

        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode() && this.sqlgGraph.tx().getBatchManager().vertexIsCached(this)) {
            List<SqlgEdge> edges = this.sqlgGraph.tx().getBatchManager().getEdges(this, direction, labels);
            return edges.iterator();
        } else {
            List<Direction> directions = new ArrayList<>(2);
            List<SqlgEdge> edges = new ArrayList<>();
            Set<SchemaTable> inVertexLabels = new HashSet<>();
            Set<SchemaTable> outVertexLabels = new HashSet<>();
            if (direction == Direction.IN) {
                inVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getLabelsForVertex(this, true));
                if (labels.length > 0) {
                    retainLabels(inVertexLabels, labels);
                }
                directions.add(direction);
            } else if (direction == Direction.OUT) {
                outVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getLabelsForVertex(this, false));
                if (labels.length > 0) {
                    retainLabels(outVertexLabels, labels);
                }
                directions.add(direction);
            } else {
                inVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getLabelsForVertex(this, true));
                outVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getLabelsForVertex(this, false));
                if (labels.length > 0) {
                    retainLabels(inVertexLabels, labels);
                    retainLabels(outVertexLabels, labels);
                }
                directions.add(Direction.IN);
                directions.add(Direction.OUT);
            }
            for (Direction d : directions) {
                for (SchemaTable schemaTable : (d == Direction.IN ? inVertexLabels : outVertexLabels)) {
                    if (this.sqlgGraph.getSchemaManager().tableExist(schemaTable.getSchema(), SchemaManager.EDGE_PREFIX + schemaTable.getTable())) {
                        StringBuilder sql = new StringBuilder("SELECT * FROM ");
                        switch (d) {
                            case IN:
                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                                sql.append(".");
                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + schemaTable.getTable()));
                                sql.append(" WHERE ");
                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema + "." + this.table + SchemaManager.IN_VERTEX_COLUMN_END));
                                sql.append(" = ?");
                                break;
                            case OUT:
                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                                sql.append(".");
                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + schemaTable.getTable()));
                                sql.append(" WHERE ");
                                sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.schema + "." + this.table + SchemaManager.OUT_VERTEX_COLUMN_END));
                                sql.append(" = ?");
                                break;
                            case BOTH:
                                throw new IllegalStateException("BUG: Direction.BOTH should never fire here!");
                        }
                        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                            sql.append(";");
                        }
                        Connection conn = this.sqlgGraph.tx().getConnection();
                        if (logger.isDebugEnabled()) {
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
                                    if (columnName.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                                        inVertexColumnNames.add(columnName);
                                    } else if (columnName.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                                        outVertexColumnNames.add(columnName);
                                    }
                                }
                                if (inVertexColumnNames.isEmpty() || outVertexColumnNames.isEmpty()) {
                                    throw new IllegalStateException("BUG: in or out vertex id not set!!!!");
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
                                SchemaTable inSchemaTable = SqlgUtil.parseLabel(inVertexColumnName, this.sqlgGraph.getSqlDialect().getPublicSchema());
                                SchemaTable outSchemaTable = SqlgUtil.parseLabel(outVertexColumnName, this.sqlgGraph.getSqlDialect().getPublicSchema());

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
                                                this.sqlgGraph,
                                                edgeId,
                                                schemaTable.getSchema(),
                                                schemaTable.getTable(),
                                                this,
                                                SqlgVertex.of(this.sqlgGraph, outId, outSchemaTable.getSchema(), outSchemaTable.getTable().replace(SchemaManager.OUT_VERTEX_COLUMN_END, "")),
                                                keyValues.toArray());
                                        break;
                                    case OUT:
                                        sqlGEdge = new SqlgEdge(
                                                this.sqlgGraph,
                                                edgeId,
                                                schemaTable.getSchema(),
                                                schemaTable.getTable(),
                                                SqlgVertex.of(this.sqlgGraph, inId, inSchemaTable.getSchema(), inSchemaTable.getTable().replace(SchemaManager.IN_VERTEX_COLUMN_END, "")),
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
            return edges.iterator();
        }
    }

    @Override
    protected void load() {

        //if in batch mode only load vertexes that are not new.
        //new vertexes have no id, impossible to load, but then all its properties are already cached.
        if ((!this.sqlgGraph.tx().isInBatchMode() && this.properties.isEmpty()) ||
                (this.properties.isEmpty() && this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode() &&
                        !this.sqlgGraph.tx().getBatchManager().vertexIsCached(this))) {

            StringBuilder sql = new StringBuilder("SELECT * FROM ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + this.table));
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
            if (!columnName.equals("ID")
                    && !columnName.equals(SchemaManager.VERTEX_IN_LABELS)
                    && !columnName.equals(SchemaManager.VERTEX_OUT_LABELS)
                    && !columnName.equals(SchemaManager.VERTEX_SCHEMA)
                    && !columnName.equals(SchemaManager.VERTEX_TABLE)
                    && !Objects.isNull(o)) {
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

    @Override
    public Vertex.Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction, final String... labels) {
        SqlgVertex.this.sqlgGraph.tx().readWrite();
        return SqlgVertex.this.vertices(Collections.emptyList(), direction, labels);
    }

    @Override
    public Iterator<Edge> edgeIterator(final Direction direction, final String... labels) {
        SqlgVertex.this.sqlgGraph.tx().readWrite();
        return SqlgVertex.this.internalGetEdges(direction, labels);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(final String... propertyKeys) {
        SqlgVertex.this.sqlgGraph.tx().readWrite();
        return SqlgVertex.this.<V>internalGetAllProperties(propertyKeys).values().iterator();
    }

    public void reset() {
        if (this.inLabelsForVertex != null) {
            this.inLabelsForVertex.clear();
        }
        if (this.outLabelsForVertex != null) {
            this.outLabelsForVertex.clear();
        }
        this.inLabelsForVertex = null;
        this.outLabelsForVertex = null;
    }

    private Set<SchemaTable> transformToOutSchemaTables(Set<String> edgeForeignKeys, Set<String> labels) {
        Set<SchemaTable> result = new HashSet<>();
        for (String edgeForeignKey : edgeForeignKeys) {
            String[] schemaTableArray = edgeForeignKey.split("\\.");
            String schema = schemaTableArray[0];
            String table = schemaTableArray[1];

            if (table.endsWith("_OUT_ID")) {
                table = table.substring(0, table.length() - 7);
                if (labels.isEmpty() || labels.contains(table)) {
                    result.add(SchemaTable.of(schema, table));
                }
            }

        }
        return result;
    }

}
