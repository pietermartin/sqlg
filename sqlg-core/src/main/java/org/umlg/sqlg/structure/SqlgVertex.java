package org.umlg.sqlg.structure;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.util.*;

import static org.umlg.sqlg.structure.SchemaManager.VERTEX_TABLE;

/**
 * Date: 2014/07/12
 * Time: 5:42 AM
 */
public class SqlgVertex extends SqlgElement implements Vertex {

    public static final String WHERE = " WHERE ";
    private Logger logger = LoggerFactory.getLogger(SqlgVertex.class.getName());

    /**
     * Called from SqlG.addVertex
     *
     * @param sqlgGraph
     * @param schema
     * @param table
     * @param keyValueMapPair
     */
    public SqlgVertex(SqlgGraph sqlgGraph, boolean complete, String schema, String table, Pair<Map<String, Object>, Map<String, Object>> keyValueMapPair) {
        super(sqlgGraph, schema, table);
        insertVertex(complete, keyValueMapPair);
        if (!sqlgGraph.tx().isInBatchMode()) {
            sqlgGraph.tx().add(this);
        }
    }

    /**
     * Only called for streaming temporary vertices. {@link SqlgGraph#internalStreamTemporaryVertex(Object...)} (Object...)}
     */
    SqlgVertex(SqlgGraph sqlgGraph, String table, Map<String, Object> keyValueMap) {
        super(sqlgGraph, "", table);
        this.sqlgGraph.tx().getBatchManager().addTemporaryVertex(this, keyValueMap);
    }

    public static SqlgVertex of(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        if (!sqlgGraph.tx().isInBatchMode()) {
            return sqlgGraph.tx().putVertexIfAbsent(sqlgGraph, schema, table, id);
        } else {
            return new SqlgVertex(sqlgGraph, id, schema, table);
        }
    }

    /**
     * This is the primary constructor to createVertexLabel a vertex that already exist
     *
     * @param sqlgGraph
     * @param id
     * @param schema
     * @param table
     */
    SqlgVertex(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        super(sqlgGraph, id, schema, table);
    }

    @Override
    public String label() {
        if (this.schema != null && this.schema.length() > 0 && !schema.equals(sqlgGraph.getSqlDialect().getPublicSchema())) {
            return this.schema + "." + this.table;
        }
        return super.label();
    }


    public Edge addEdgeWithMap(String label, Vertex inVertex, Map<String, Object> keyValues) {
        Object[] parameters = SqlgUtil.mapToStringKeyValues(keyValues);
        return addEdge(label, inVertex, parameters);
    }

    public void streamEdge(String label, Vertex inVertex) {
        this.streamEdge(label, inVertex, new LinkedHashMap<>());
    }

    public void streamEdge(String label, Vertex inVertex, LinkedHashMap<String, Object> keyValues) {
        if (!sqlgGraph.tx().isInStreamingBatchMode()) {
            throw SqlgExceptions.invalidMode("Transaction must be in " + BatchManager.BatchModeType.STREAMING + " mode for streamEdge");
        }
        if (this.sqlgGraph.tx().isOpen() && this.sqlgGraph.tx().getBatchManager().getStreamingBatchModeVertexSchemaTable() != null) {
            throw new IllegalStateException("Streaming vertex for label " + this.sqlgGraph.tx().getBatchManager().getStreamingBatchModeVertexSchemaTable().getTable() + " is in progress. Commit the transaction or call SqlgGraph.flush()");
        }
        SchemaTable streamingBatchModeEdgeLabel = this.sqlgGraph.tx().getBatchManager().getStreamingBatchModeEdgeSchemaTable();
        if (streamingBatchModeEdgeLabel != null && !streamingBatchModeEdgeLabel.getTable().substring(SchemaManager.EDGE_PREFIX.length()).equals(label)) {
            throw new IllegalStateException("Streaming batch mode must occur for one label at a time. Expected \"" + streamingBatchModeEdgeLabel + "\" found \"" + label + "\". First commit the transaction or call SqlgGraph.flush() before streaming a different label");
        }
        Map<Object, Object> tmp = new LinkedHashMap<>(keyValues);
        Object[] keyValues1 = SqlgUtil.mapTokeyValues(tmp);
        addEdgeInternal(true, label, inVertex, keyValues1);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        this.sqlgGraph.tx().readWrite();
        boolean streaming = this.sqlgGraph.tx().isInStreamingBatchMode() || this.sqlgGraph.tx().isInStreamingWithLockBatchMode();
        if (streaming) {
            SchemaTable streamingBatchModeEdgeLabel = this.sqlgGraph.tx().getBatchManager().getStreamingBatchModeEdgeSchemaTable();
            if (streamingBatchModeEdgeLabel != null && !streamingBatchModeEdgeLabel.getTable().substring(SchemaManager.EDGE_PREFIX.length()).equals(label)) {
                throw new IllegalStateException("Streaming batch mode must occur for one label at a time. Expected \"" + streamingBatchModeEdgeLabel + "\" found \"" + label + "\". First commit the transaction or call SqlgGraph.flush() before streaming a different label");
            }
        }
        return addEdgeInternal(streaming, label, inVertex, keyValues);
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

        List<String> previousBatchModeKeys;
        if (complete) {
            previousBatchModeKeys = this.sqlgGraph.tx().getBatchManager().getStreamingBatchModeEdgeKeys();
        } else {
            previousBatchModeKeys = Collections.emptyList();
        }
        Triple<Map<String, PropertyType>, Map<String, Object>, Map<String, Object>> keyValueMapTriple = SqlgUtil.validateVertexKeysValues(this.sqlgGraph.getSqlDialect(), keyValues, previousBatchModeKeys);
        if (!complete && keyValueMapTriple.getRight().size() != keyValueMapTriple.getMiddle().size()) {
            throw Property.Exceptions.propertyValueCanNotBeNull();
        }
        final Pair<Map<String, Object>, Map<String, Object>> keyValueMapPair = Pair.of(keyValueMapTriple.getMiddle(), keyValueMapTriple.getRight());
        final Map<String, PropertyType> columns = keyValueMapTriple.getLeft();
        Optional<VertexLabel> outVertexLabelOptional = this.sqlgGraph.getTopology().getVertexLabel(this.schema, this.table);
        Optional<VertexLabel> inVertexLabelOptional = this.sqlgGraph.getTopology().getVertexLabel(((SqlgVertex) inVertex).schema, ((SqlgVertex) inVertex).table);
        Preconditions.checkState(outVertexLabelOptional.isPresent(), "Out VertexLabel must be present. Not found for %s", this.schema + "." + this.table);
        Preconditions.checkState(inVertexLabelOptional.isPresent(), "In VertexLabel must be present. Not found for %s", ((SqlgVertex) inVertex).schema + "." + ((SqlgVertex) inVertex).table);
        //noinspection OptionalGetWithoutIsPresent
        this.sqlgGraph.getTopology().ensureEdgeLabelExist(label, outVertexLabelOptional.get(), inVertexLabelOptional.get(), columns);
        return new SqlgEdge(this.sqlgGraph, complete, this.schema, label, (SqlgVertex) inVertex, this, keyValueMapPair);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <V> Map<String, VertexProperty<V>> internalGetProperties(final String... propertyKeys) {
        Map<String, ? extends Property<V>> propertiesMap = super.internalGetProperties(propertyKeys);
        return (Map<String, VertexProperty<V>>) propertiesMap;
    }

    @SuppressWarnings("unchecked")
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
            return (VertexProperty<V>) super.property(key);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id());
        ElementHelper.validateProperty(key, value);
        this.sqlgGraph.tx().readWrite();
        return (VertexProperty<V>) super.property(key, value);
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
            this.sqlgGraph.tx().flush();
        }
        // need topology when we're a topology vertex
        GraphTraversalSource gts = Topology.SQLG_SCHEMA.equals(schema) ?
                this.sqlgGraph.topology()
                : this.sqlgGraph.traversal();
        switch (direction) {
            case OUT:
                return gts.V(this).outE(labels);
            case IN:
                return gts.V(this).inE(labels);
            case BOTH:
                return gts.V(this).bothE(labels);
        }
        return Collections.emptyIterator();
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
            Pair<Set<SchemaTable>, Set<SchemaTable>> foreignKeys = this.sqlgGraph.getTopology().getTableLabels(this.getSchemaTablePrefixed());
            //in edges
            for (SchemaTable schemaTable : foreignKeys.getLeft()) {
                deleteEdgesWithInKey(schemaTable, this.id());
            }
            //out edges
            for (SchemaTable schemaTable : foreignKeys.getRight()) {
                deleteEdgesWithOutKey(schemaTable, this.id());
            }
            super.remove();
        }
    }

    private void deleteEdgesWithOutKey(SchemaTable edgeSchemaTable, Object id) {
        deleteEdges(Direction.OUT, edgeSchemaTable);
    }

    private void deleteEdgesWithInKey(SchemaTable edgeSchemaTable, Object id) {
        deleteEdges(Direction.IN, edgeSchemaTable);
    }

    private void deleteEdges(Direction direction, SchemaTable edgeSchemaTable) {
        StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(edgeSchemaTable.getSchema()));
        sql.append(".");
        sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(edgeSchemaTable.getTable()));
        sql.append(WHERE);
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema + "." + this.table + (direction == Direction.OUT ? SchemaManager.OUT_VERTEX_COLUMN_END : SchemaManager.IN_VERTEX_COLUMN_END)));
        sql.append(" = ?");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(1, ((RecordId) this.id()).getId());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insertVertex(boolean complete, Pair<Map<String, Object>, Map<String, Object>> keyValueMapPair) {
        Map<String, Object> keyAllValueMap = keyValueMapPair.getLeft();
        Map<String, Object> keyNotNullValueMap = keyValueMapPair.getRight();
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            internalBatchAddVertex(complete, keyAllValueMap);
        } else {
            internalAddVertex(keyNotNullValueMap);
        }
        //Cache the properties
        this.properties.putAll(keyNotNullValueMap);
    }

    private void internalBatchAddVertex(boolean complete, Map<String, Object> keyValueMap) {
        this.sqlgGraph.tx().getBatchManager().addVertex(complete, this, keyValueMap);
    }

    private void internalAddVertex(Map<String, Object> keyValueMap) {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.schema));
        sql.append(".");
        sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + this.table));

        Map<String, Pair<PropertyColumn, Object>> propertyColumnValueMap = new HashMap<>();
        Map<String, PropertyColumn> propertyColumns = this.sqlgGraph.getTopology()
                .getSchema(this.schema).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", this.schema)))
                .getVertexLabel(this.table).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel %s not found", this.table)))
                .getProperties();
        if (!keyValueMap.isEmpty()) {
            //sync up the keyValueMap with its PropertyColumn
            for (Map.Entry<String, Object> keyValueEntry : keyValueMap.entrySet()) {
                PropertyColumn propertyColumn = propertyColumns.get(keyValueEntry.getKey());
                Pair<PropertyColumn, Object> propertyColumnObjectPair = Pair.of(propertyColumn, keyValueEntry.getValue());
                propertyColumnValueMap.put(keyValueEntry.getKey(), propertyColumnObjectPair);
            }
            sql.append(" ( ");
            writeColumnNames(propertyColumnValueMap, sql);
            sql.append(") VALUES ( ");
            writeColumnParameters(propertyColumnValueMap, sql);
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
        int i = 1;
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {
            SqlgUtil.setKeyValuesAsParameterUsingPropertyColumn(this.sqlgGraph, i, preparedStatement, propertyColumnValueMap);
            preparedStatement.executeUpdate();
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
                this.recordId = RecordId.from(SchemaTable.of(this.schema, this.table), generatedKeys.getLong(1));
            } else {
                throw new RuntimeException(String.format("Could not retrieve the id after an insert into %s", SchemaManager.VERTICES));
            }
            insertGlobalUniqueIndex(keyValueMap, propertyColumns);
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
            if (hasContainer.getKey().equals(key)) {
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
        //if in batch mode, only load vertexes that are not new.
        //new vertexes have no id, impossible to load, but then all its properties are already cached.
        if ((this.properties.isEmpty() && !this.sqlgGraph.tx().isInBatchMode()) ||
                (this.properties.isEmpty() && this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode() &&
                        !this.sqlgGraph.tx().getBatchManager().vertexIsCached(this))) {

            if (this.sqlgGraph.tx().isOpen() && this.sqlgGraph.tx().getBatchManager().isStreaming()) {
                throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
            }

            //Generate the columns to prevent 'ERROR: cached plan must not change result type" error'
            //This happens when the schema changes after the statement is prepared.
            @SuppressWarnings("OptionalGetWithoutIsPresent")
            VertexLabel vertexLabel = this.sqlgGraph.getTopology().getSchema(this.schema).get().getVertexLabel(this.table).get();
            StringBuilder sql = new StringBuilder("SELECT\n\t");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
            for (PropertyColumn propertyColumn : vertexLabel.properties.values()) {
                sql.append(", ");
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(propertyColumn.getName()));
                // additional columns for time zone, etc.
                String[] ps = propertyColumn.getPropertyType().getPostFixes();
                if (ps != null) {
                    for (String p : propertyColumn.getPropertyType().getPostFixes()) {
                        sql.append(", ");
                        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(propertyColumn.getName() + p));
                    }
                }
            }
            sql.append("\nFROM\n\t");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + this.table));
            sql.append("\nWHERE\n\t");
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
                    throw new IllegalStateException(String.format("Vertex with label %s and id %d does not exist.", new Object[]{this.schema + "." + this.table, this.recordId.getId()}));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //TODO optimize the if statement here to be outside the main ResultSet loop
    @Override
    public void loadResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnLabel(i);
            if (!columnName.equals("ID")
                    && !columnName.equals(SchemaManager.VERTEX_SCHEMA)
                    && !columnName.equals(VERTEX_TABLE)
                    && !this.sqlgGraph.getSqlDialect().columnsToIgnore().contains(columnName)) {
                loadProperty(resultSet, columnName, i);
            }
        }
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        SqlgVertex.this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.tx().getBatchManager().isStreaming()) {
            throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
        }
        return internalEdges(direction, edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        SqlgVertex.this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.tx().getBatchManager().isStreaming()) {
            throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
        }
        // need topology when we're a topology vertex
        GraphTraversalSource gts = Topology.SQLG_SCHEMA.equals(schema) ?
                this.sqlgGraph.topology()
                : this.sqlgGraph.traversal();
        //for some very bezaar reason not adding toList().iterator() return one extra element.
        switch (direction) {
            case OUT:
                return gts.V(this).out(edgeLabels).toList().iterator();
            case IN:
                return gts.V(this).in(edgeLabels).toList().iterator();
            case BOTH:
                return gts.V(this).both(edgeLabels).toList().iterator();
        }
        return Collections.emptyIterator();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
//        SqlgVertex.this.sqlgGraph.tx().readWrite();
        return SqlgVertex.this.<V>internalGetProperties(propertyKeys).values().iterator();
    }

    public SchemaTable getSchemaTablePrefixed() {
        return SchemaTable.of(this.getSchema(), SchemaManager.VERTEX_PREFIX + this.getTable());
    }

    SchemaTable getSchemaTable() {
        return SchemaTable.of(this.getSchema(), this.getTable());
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
