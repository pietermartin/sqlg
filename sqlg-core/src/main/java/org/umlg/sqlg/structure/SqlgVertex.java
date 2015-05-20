package org.umlg.sqlg.structure;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * Date: 2014/07/12
 * Time: 5:42 AM
 */
public class SqlgVertex extends SqlgElement implements Vertex {

    private Logger logger = LoggerFactory.getLogger(SqlgVertex.class.getName());

    /**
     * Called from SqlG.addVertex
     *
     * @param sqlgGraph
     * @param schema
     * @param table
     * @param keyValues
     */
    public SqlgVertex(SqlgGraph sqlgGraph, boolean complete, String schema, String table, Object... keyValues) {
        super(sqlgGraph, schema, table);
        insertVertex(complete, keyValues);
        if (!sqlgGraph.tx().isInBatchMode()) {
            sqlgGraph.tx().add(this);
        }
    }

    public static SqlgVertex of(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        if (!sqlgGraph.tx().isInBatchMode()) {
            return sqlgGraph.tx().putVertexIfAbsent(sqlgGraph, RecordId.from(SchemaTable.of(schema, table), id));
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

    public Edge addEdgeWithMap(String label, Vertex inVertex, Map<String, Object> keyValues) {
        Object[] parameters = SqlgUtil.mapToStringKeyValues(keyValues);
        return addEdge(label, inVertex, parameters);
    }

    public Edge addCompleteEdge(String label, Vertex inVertex, Object... keyValues) {
        return addEdgeInternal(true, label, inVertex, keyValues);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        return addEdgeInternal(false, label, inVertex, keyValues);
    }

    private Edge addEdgeInternal(boolean complete, String label, Vertex inVertex, Object... keyValues) {
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
        final SqlgEdge edge = new SqlgEdge(this.sqlgGraph, complete, schemaTablePair.getSchema(), schemaTablePair.getTable(), (SqlgVertex) inVertex, this, keyValues);
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
    public <V> VertexProperty<V> property(String key, V value, Object... keyValues) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        if (keyValues.length > 0)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();

        return property(key, value);
    }


    @Override
    protected <V> SqlgProperty<V> instantiateProperty(String key, V value) {
        return new SqlgVertexProperty<>(this.sqlgGraph, this, key, value);
    }

    @Override
    protected Property emptyProperty() {
        return VertexProperty.empty();
    }

    private Iterator<Edge> internalEdges(Direction direction, String... labels) {
        this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode() && this.sqlgGraph.tx().getBatchManager().vertexIsCached(this)) {
            List<Edge> edges = this.sqlgGraph.tx().getBatchManager().getEdges(this, direction, labels);
            return edges.iterator();
        } else {
            List<Edge> edges = new ArrayList<>();
            List<Direction> directions = new ArrayList<>(2);
            Set<SchemaTable> inVertexLabels = new HashSet<>();
            Set<SchemaTable> outVertexLabels = new HashSet<>();
            if (direction == Direction.IN) {
                inVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getTableLabels(this.getSchemaTablePrefixed()).getLeft());
                if (labels.length > 0) {
                    retainLabels(inVertexLabels, labels);
                }
                directions.add(direction);
            } else if (direction == Direction.OUT) {
                outVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getTableLabels(this.getSchemaTablePrefixed()).getRight());
                if (labels.length > 0) {
                    retainLabels(outVertexLabels, labels);
                }
                directions.add(direction);
            } else {
                inVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getTableLabels(this.getSchemaTablePrefixed()).getLeft());
                outVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getTableLabels(this.getSchemaTablePrefixed()).getRight());
                if (labels.length > 0) {
                    retainLabels(inVertexLabels, labels);
                    retainLabels(outVertexLabels, labels);
                }
                directions.add(Direction.IN);
                directions.add(Direction.OUT);
            }
            for (Direction d : directions) {
                for (SchemaTable schemaTable : (d == Direction.IN ? inVertexLabels : outVertexLabels)) {
                    StringBuilder sql = new StringBuilder("SELECT * FROM ");
                    switch (d) {
                        case IN:
                            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                            sql.append(".");
                            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getTable()));
                            sql.append(" WHERE ");
                            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema + "." + this.table + SchemaManager.IN_VERTEX_COLUMN_END));
                            sql.append(" = ?");
                            break;
                        case OUT:
                            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                            sql.append(".");
                            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getTable()));
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
                                preparedStatement.setLong(1, this.recordId.getId());
                                break;
                            case OUT:
                                preparedStatement.setLong(1, this.recordId.getId());
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
                            SchemaTable inSchemaTable = SchemaTable.from(this.sqlgGraph, inVertexColumnName, this.sqlgGraph.getSqlDialect().getPublicSchema());
                            SchemaTable outSchemaTable = SchemaTable.from(this.sqlgGraph, outVertexColumnName, this.sqlgGraph.getSqlDialect().getPublicSchema());

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
                                            schemaTable.getTable().substring(SchemaManager.EDGE_PREFIX.length()),
                                            this,
                                            SqlgVertex.of(this.sqlgGraph, outId, outSchemaTable.getSchema(), SqlgUtil.removeTrailingOutId(outSchemaTable.getTable())),
                                            keyValues.toArray());
                                    break;
                                case OUT:
                                    sqlGEdge = new SqlgEdge(
                                            this.sqlgGraph,
                                            edgeId,
                                            schemaTable.getSchema(),
                                            schemaTable.getTable().substring(SchemaManager.EDGE_PREFIX.length()),
                                            SqlgVertex.of(this.sqlgGraph, inId, inSchemaTable.getSchema(), SqlgUtil.removeTrailingInId(inSchemaTable.getTable())),
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
            return edges.iterator();
        }
    }

    public Iterator<Vertex> vertices(List<HasContainer> hasContainers, Direction direction, String... labels) {
        this.sqlgGraph.tx().readWrite();
        return internalGetVertices(hasContainers, direction, labels);
    }

    /**
     * Called from SqlgVertexStepCompiler which compiled VertexStep and HasSteps.
     * This is only called when not in BatchMode
     *
     * @param replacedSteps The original VertexStep and HasSteps that were replaced.
     * @return The result of the query.
     */
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
            //Remove all internalEdges
            Iterator<Edge> edges = this.internalEdges(Direction.BOTH);
            while (edges.hasNext()) {
                edges.next().remove();
            }
            super.remove();
        }

    }

    private void insertVertex(boolean complete, Object... keyValues) {
        Map<String, Object> keyValueMap = SqlgUtil.transformToInsertValues(keyValues);
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            internalBatchAddVertex(complete, keyValueMap);
        } else {
            internalAddVertex(keyValueMap);
        }
        //Cache the properties
        this.properties.putAll(keyValueMap);
    }

    private void internalBatchAddVertex(boolean complete, Map<String, Object> keyValueMap) {
        this.sqlgGraph.tx().getBatchManager().addVertex(complete, this, keyValueMap);
    }

    private void internalAddVertex(Map<String, Object> keyValueMap) {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.schema));
        sql.append(".");
        sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + this.table));
        int i = 1;
        if (!keyValueMap.isEmpty()) {
            sql.append(" ( ");
            for (String column : keyValueMap.keySet()) {
                sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(column));
                if (i++ < keyValueMap.size()) {
                    sql.append(", ");
                }
            }
            sql.append(") VALUES ( ");
            i = 1;
            for (String column : keyValueMap.keySet()) {
                sql.append("?");
                if (i++ < keyValueMap.size()) {
                    sql.append(", ");
                }
            }
            sql.append(")");
        } else {
            sql.append(" DEFAULT VALUES");
        }
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        i = 1;
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {
            setKeyValuesAsParameter(this.sqlgGraph, i, conn, preparedStatement, keyValueMap);
            preparedStatement.executeUpdate();
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
                this.recordId = RecordId.from(SchemaTable.of(this.schema, this.table), generatedKeys.getLong(1));
            } else {
                throw new RuntimeException("Could not retrieve the id after an insert into " + SchemaManager.VERTICES);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void retainLabels(Set<SchemaTable> vertexLabels, String... labels) {
        Set<SchemaTable> toRemove = new HashSet<>();
        for (SchemaTable schemaTable : vertexLabels) {
            boolean retain = false;
            for (String label : labels) {
                if (label.startsWith(SchemaManager.EDGE_PREFIX)) {
                    throw new IllegalStateException("labels may not start with " + SchemaManager.EDGE_PREFIX);
                }
                if (schemaTable.getTable().equals(SchemaManager.EDGE_PREFIX + label)) {
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
                inVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getTableLabels(this.getSchemaTablePrefixed()).getLeft());
                if (labels.length > 0) {
                    retainLabels(inVertexLabels, labels);
                }
                directions.add(direction);
            } else if (direction == Direction.OUT) {
                outVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getTableLabels(this.getSchemaTablePrefixed()).getRight());
                if (labels.length > 0) {
                    retainLabels(outVertexLabels, labels);
                }
                directions.add(direction);
            } else {
                inVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getTableLabels(this.getSchemaTablePrefixed()).getLeft());
                outVertexLabels.addAll(this.sqlgGraph.getSchemaManager().getTableLabels(this.getSchemaTablePrefixed()).getRight());
                if (labels.length > 0) {
                    retainLabels(inVertexLabels, labels);
                    retainLabels(outVertexLabels, labels);
                }
                directions.add(Direction.IN);
                directions.add(Direction.OUT);
            }
            for (Direction d : directions) {
                for (SchemaTable schemaTable : (d == Direction.IN ? inVertexLabels : outVertexLabels)) {
                    Set<String> edgeForeignKeys = this.sqlgGraph.getSchemaManager().getEdgeForeignKeys(schemaTable.getSchema() + "." + schemaTable.getTable());
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

                        StringBuilder sql = new StringBuilder("SELECT b.*");
                        sql.append(", a.");
                        switch (d) {
                            case IN:
                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(joinSchemaTable.getSchema() + "." + joinSchemaTable.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
                                sql.append(" FROM ");

                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                                sql.append(".");
                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getTable()));

                                //Need to join here on all of the out columns.
                                sql.append(" a JOIN ");
                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(joinSchemaTable.getSchema()));
                                sql.append(".");
                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + joinSchemaTable.getTable()));
                                sql.append(" b ON a.");
                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(joinSchemaTable.getSchema() + "." + joinSchemaTable.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
                                sql.append(" = b.");
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
                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getTable()));

                                //Need to join here on all of the in columns.
                                sql.append(" a JOIN ");
                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(joinSchemaTable.getSchema()));
                                sql.append(".");
                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + joinSchemaTable.getTable()));
                                sql.append(" b ON a.");
                                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(joinSchemaTable.getSchema() + "." + joinSchemaTable.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
                                sql.append(" = b.");
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
                                    preparedStatement.setLong(1, this.recordId.getId());
                                    break;
                                case OUT:
                                    preparedStatement.setLong(1, this.recordId.getId());
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
                                        vertices.add(sqlGVertex);
                                        break;
                                    case OUT:
                                        sqlGVertex = SqlgVertex.of(this.sqlgGraph, inId, joinSchemaTable.getSchema(), joinSchemaTable.getTable());
                                        keyValueMap = SqlgUtil.transformToInsertValues(keyValues.toArray());
                                        sqlGVertex.properties.clear();
                                        sqlGVertex.properties.putAll(keyValueMap);
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
        List<Element> elements = new ArrayList<>();
        SchemaTable schemaTable = SchemaTable.of(this.schema, SchemaManager.VERTEX_PREFIX + this.table);
        SchemaTableTree schemaTableTree = this.sqlgGraph.getGremlinParser().parse(schemaTable, replacedSteps);
        List<Triple<LinkedList<SchemaTableTree>, SchemaTable, String>> sqlStatements = schemaTableTree.constructSql();
        for (Triple<LinkedList<SchemaTableTree>, SchemaTable, String> sqlTriple : sqlStatements) {
            Connection conn = this.sqlgGraph.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sqlTriple.getRight());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sqlTriple.getRight())) {
                preparedStatement.setLong(1, this.recordId.getId());
                setParametersOnStatement(sqlTriple.getLeft(), conn, preparedStatement);
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    this.sqlgGraph.getGremlinParser().loadElements(resultSet, sqlTriple.getMiddle(), elements);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return elements.iterator();
    }

    private void setParametersOnStatement(LinkedList<SchemaTableTree> schemaTableTreeStack, Connection conn, PreparedStatement preparedStatement) throws SQLException {
        //start the index at 2 as sql starts at 1 and the first is the id that is already set.
        int parameterIndex = 2;
        Multimap<String, Object> keyValueMap = LinkedListMultimap.create();
        for (SchemaTableTree schemaTableTree : schemaTableTreeStack) {
            for (HasContainer hasContainer : schemaTableTree.getHasContainers()) {
                keyValueMap.put(hasContainer.key, hasContainer.value);
            }
        }
        SqlgElement.setKeyValuesAsParameter(this.sqlgGraph, parameterIndex, conn, preparedStatement, keyValueMap);
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

    private Set<SchemaTable> transformToOutSchemaTables(Set<String> edgeForeignKeys, Set<String> labels) {
        Set<SchemaTable> result = new HashSet<>();
        for (String edgeForeignKey : edgeForeignKeys) {
            String[] schemaTableArray = edgeForeignKey.split("\\.");
            String schema = schemaTableArray[0];
            String table = schemaTableArray[1];

            if (table.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
                table = table.substring(0, table.length() - SchemaManager.OUT_VERTEX_COLUMN_END.length());
                if (labels.isEmpty() || labels.contains(table)) {
                    result.add(SchemaTable.of(schema, table));
                }
            }

        }
        return result;
    }

    private Set<SchemaTable> transformToInSchemaTables(Set<String> edgeForeignKeys, Set<String> labels) {
        Set<SchemaTable> result = new HashSet<>();
        for (String edgeForeignKey : edgeForeignKeys) {
            String[] schemaTableArray = edgeForeignKey.split("\\.");
            String schema = schemaTableArray[0];
            String table = schemaTableArray[1];
            if (table.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
                table = table.substring(0, table.length() - SchemaManager.IN_VERTEX_COLUMN_END.length());
                if (labels.isEmpty() || labels.contains(table)) {
                    result.add(SchemaTable.of(schema, table));
                }
            }
        }
        return result;
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
                preparedStatement.setLong(1, this.recordId.getId());
                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    loadResultSet(resultSet);
                } else {
                    throw new IllegalStateException(String.format("Vertex with label %s and id %d does exist.", new Object[]{this.schema + "." + this.table, this.recordId.getId()}));
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

                loadProperty(resultSetMetaData, i, columnName, o);
            }
        }
    }


    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        SqlgVertex.this.sqlgGraph.tx().readWrite();
        return SqlgVertex.this.internalEdges(direction, edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        SqlgVertex.this.sqlgGraph.tx().readWrite();
        return SqlgVertex.this.vertices(Collections.emptyList(), direction, edgeLabels);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        SqlgVertex.this.sqlgGraph.tx().readWrite();
        return SqlgVertex.this.<V>internalGetAllProperties(propertyKeys).values().iterator();
    }


    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
