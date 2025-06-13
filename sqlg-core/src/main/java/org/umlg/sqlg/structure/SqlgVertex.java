package org.umlg.sqlg.structure;

import org.umlg.sqlg.util.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.util.*;

import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * Date: 2014/07/12
 * Time: 5:42 AM
 */
public class SqlgVertex extends SqlgElement implements Vertex {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlgVertex.class);

    /**
     * @param sqlgGraph   The graph.
     * @param schema      The database schema.
     * @param table       The table name.
     * @param keyValueMap The properties.
     */
    public SqlgVertex(
            SqlgGraph sqlgGraph,
            String schema,
            String table,
            Map<String, Object> keyValueMap) {

        super(sqlgGraph, schema, table);
        insertTemporaryVertex(keyValueMap);
    }

    /**
     * @param sqlgGraph   The graph
     * @param vertexLabel The vertex's VertexLabel
     * @param schema      The vertex's schema
     * @param table       The vertex's table
     * @param keyValueMap The vertex's properties
     */
    public SqlgVertex(
            SqlgGraph sqlgGraph,
            VertexLabel vertexLabel,
            String schema,
            String table,
            Map<String, Object> keyValueMap) {

        super(sqlgGraph, schema, table);
        insertVertex(vertexLabel, keyValueMap);
    }

    /**
     * Used for streamingVertex
     *
     * @param sqlgGraph   The graph.
     * @param schemaTable The SchemaTable being streamed into.
     */
    public SqlgVertex(
            SqlgGraph sqlgGraph,
            VertexLabel vertexLabel,
            SchemaTable schemaTable,
            Map<String, Object> keyValueMap) {

        super(sqlgGraph, schemaTable.getSchema(), schemaTable.getTable());
        Preconditions.checkState(
                this.sqlgGraph.getSqlDialect().supportsBatchMode() &&
                        (this.sqlgGraph.tx().isInStreamingBatchMode() || this.sqlgGraph.tx().isInStreamingWithLockBatchMode())
        );
        internalStreamBatchAddVertex(vertexLabel, schemaTable, keyValueMap);
    }

    SqlgVertex(SqlgGraph sqlgGraph, String table, Map<String, Object> keyValueMap) {
        super(sqlgGraph, "", table);
        Preconditions.checkState(this.sqlgGraph.getSqlDialect().supportsBatchMode());
        this.sqlgGraph.tx().getBatchManager().addTemporaryVertex(this, keyValueMap);
    }

    public static SqlgVertex of(SqlgGraph sqlgGraph, Long id, String schema, String table, int columnCount) {
        return new SqlgVertex(sqlgGraph, id, schema, table, columnCount);
    }

    public static SqlgVertex of(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        return new SqlgVertex(sqlgGraph, id, schema, table);
    }

    public static SqlgVertex of(SqlgGraph sqlgGraph, List<Comparable> identifiers, String schema, String table, int columnCount) {
        return new SqlgVertex(sqlgGraph, identifiers, schema, table, columnCount);
    }

    public static SqlgVertex of(SqlgGraph sqlgGraph, List<Comparable> identifiers, String schema, String table) {
        return new SqlgVertex(sqlgGraph, identifiers, schema, table);
    }

    /**
     * This is the primary constructor to createVertexLabel a vertex that already exist
     *
     * @param sqlgGraph The graph.
     * @param id        The vertex's id.
     * @param schema    The schema the vertex is in.
     * @param table     The vertex's table/label.
     */
    SqlgVertex(SqlgGraph sqlgGraph, Long id, String schema, String table, int columnCount) {
        super(sqlgGraph, id, schema, table, columnCount);
    }

    /**
     * This is the primary constructor to createVertexLabel a vertex that already exist
     *
     * @param sqlgGraph The graph.
     * @param id        The vertex's id.
     * @param schema    The schema the vertex is in.
     * @param table     The vertex's table/label.
     */
    SqlgVertex(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        super(sqlgGraph, id, schema, table);
    }

    SqlgVertex(SqlgGraph sqlgGraph, List<Comparable> identifiers, String schema, String table) {
        super(sqlgGraph, identifiers, schema, table);
    }

    SqlgVertex(SqlgGraph sqlgGraph, List<Comparable> identifiers, String schema, String table, int columnCount) {
        super(sqlgGraph, identifiers, schema, table, columnCount);
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
        if (streamingBatchModeEdgeLabel != null && !streamingBatchModeEdgeLabel.getTable().substring(EDGE_PREFIX.length()).equals(label)) {
            throw new IllegalStateException("Streaming batch mode must occur for one label at a time. Expected \"" + streamingBatchModeEdgeLabel + "\" found \"" + label + "\". First commit the transaction or call SqlgGraph.flush() before streaming a different label");
        }
        Map<Object, Object> tmp = new LinkedHashMap<>(keyValues);
        Object[] keyValues1 = SqlgUtil.mapTokeyValues(tmp);
        addEdgeInternal(true, label, inVertex, keyValues1);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        this.sqlgGraph.tx().readWrite();
        boolean streaming = this.sqlgGraph.getSqlDialect().supportsBatchMode() && (this.sqlgGraph.tx().isInStreamingBatchMode() || this.sqlgGraph.tx().isInStreamingWithLockBatchMode());
        if (streaming) {
            SchemaTable streamingBatchModeEdgeLabel = this.sqlgGraph.tx().getBatchManager().getStreamingBatchModeEdgeSchemaTable();
            if (streamingBatchModeEdgeLabel != null && !streamingBatchModeEdgeLabel.getTable().substring(EDGE_PREFIX.length()).equals(label)) {
                throw new IllegalStateException("Streaming batch mode must occur for one label at a time. Expected \"" + streamingBatchModeEdgeLabel + "\" found \"" + label + "\". First commit the transaction or call SqlgGraph.flush() before streaming a different label");
            }
        }
        return addEdgeInternal(streaming, label, inVertex, keyValues);
    }

    private Edge addEdgeInternal(boolean complete, String label, Vertex inVertex, Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) {
            throw new IllegalStateException(String.format("Vertex with id %s was removed.", id().toString()));
        }

        ElementHelper.validateLabel(label);

        Preconditions.checkArgument(!label.contains("."), String.format("Edge label may not contain a '.' , the edge will be stored in the schema of the owning vertex. label = %s", label));

        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Edge.Exceptions.userSuppliedIdsNotSupported();

        List<String> previousBatchModeKeys;
        if (complete) {
            previousBatchModeKeys = this.sqlgGraph.tx().getBatchManager().getStreamingBatchModeEdgeKeys();
        } else {
            previousBatchModeKeys = Collections.emptyList();
        }
        Pair<Map<String, PropertyDefinition>, Map<String, Object>> keyValueMapPair = SqlgUtil.validateVertexKeysValues(this.sqlgGraph.getSqlDialect(), keyValues, previousBatchModeKeys);
        if (!complete && keyValueMapPair.getRight().size() != keyValueMapPair.getRight().size()) {
            throw Property.Exceptions.propertyKeyCanNotBeNull();
        }
        final Map<String, PropertyDefinition> columns = keyValueMapPair.getLeft();
        Optional<VertexLabel> outVertexLabelOptional = this.sqlgGraph.getTopology().getVertexLabel(this.schema, this.table);
        Optional<VertexLabel> inVertexLabelOptional = this.sqlgGraph.getTopology().getVertexLabel(((SqlgVertex) inVertex).schema, ((SqlgVertex) inVertex).table);
        Preconditions.checkState(outVertexLabelOptional.isPresent(), "Out VertexLabel must be present. Not found for %s", this.schema + "." + this.table);
        Preconditions.checkState(inVertexLabelOptional.isPresent(), "In VertexLabel must be present. Not found for %s", ((SqlgVertex) inVertex).schema + "." + ((SqlgVertex) inVertex).table);

        this.sqlgGraph.getTopology().threadWriteLock();

        EdgeLabel edgeLabel = this.sqlgGraph.getTopology().ensureEdgeLabelExist(label, outVertexLabelOptional.get(), inVertexLabelOptional.get(), columns);
        if (!edgeLabel.hasIDPrimaryKey()) {
            Preconditions.checkArgument(columns.keySet().containsAll(edgeLabel.getIdentifiers()), "identifiers must be present %s", edgeLabel.getIdentifiers());
        } else if (edgeLabel.isForeign()) {
            throw SqlgExceptions.invalidMode("Foreign EdgeLabel must have user defined identifiers to support addition.");
        }
        return new SqlgEdge(
                this.sqlgGraph,
                complete,
                this.schema,
                label,
                this,
                (SqlgVertex) inVertex,
                keyValueMapPair.getRight());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <V> Map<String, VertexProperty<V>> internalGetProperties(final String... propertyKeys) {
        Map<String, ? extends Property<V>> propertiesMap = super.internalGetProperties(propertyKeys);
        return (Map<String, VertexProperty<V>>) propertiesMap;
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        this.sqlgGraph.tx().readWrite();
        if (this.removed) {
            throw new IllegalStateException(String.format("Vertex with id %s was removed.", id().toString()));
        } else {
            return (VertexProperty<V>) super.property(key);
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        if (this.removed) {
            throw new IllegalStateException(String.format("Vertex with id %s was removed.", id().toString()));
        }
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
    protected Property<?> emptyProperty() {
        return VertexProperty.empty();
    }

    private Iterator<Edge> internalEdges(Direction direction, String... labels) {
        this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode() && this.sqlgGraph.tx().getBatchManager().vertexIsCached(this)) {
            this.sqlgGraph.tx().flush();
        }
        // need topology when we're a topology vertex
        GraphTraversalSource gts = Topology.SQLG_SCHEMA.equals(schema) ?
                this.sqlgGraph.topology()
                : this.sqlgGraph.traversal();
        return switch (direction) {
            case OUT -> gts.V(this).outE(labels);
            case IN -> gts.V(this).inE(labels);
            case BOTH -> gts.V(this).bothE(labels);
        };
    }

    @Override
    public void remove() {
        this.sqlgGraph.tx().readWrite();
        this.sqlgGraph.getTopology().threadWriteLock();

        if (this.removed)
            throw new IllegalStateException(String.format("Vertex with id %s was removed.", id().toString()));

        if (this.sqlgGraph.getSqlDialect().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            this.sqlgGraph.tx().getBatchManager().removeVertex(this.schema, this.table, this);
        } else {
            //Remove all internalEdges
            Pair<Set<SchemaTable>, Set<SchemaTable>> foreignKeys = this.sqlgGraph.getTopology().getTableLabels(this.getSchemaTablePrefixed());
            //in edges
            for (SchemaTable schemaTable : foreignKeys.getLeft()) {
                deleteEdgesWithInKey(schemaTable);
            }
            //out edges
            for (SchemaTable schemaTable : foreignKeys.getRight()) {
                deleteEdgesWithOutKey(schemaTable);
            }
            super.remove();
        }
    }

    private void deleteEdgesWithOutKey(SchemaTable edgeSchemaTable) {
        deleteEdges(Direction.OUT, edgeSchemaTable);
    }

    private void deleteEdgesWithInKey(SchemaTable edgeSchemaTable) {
        deleteEdges(Direction.IN, edgeSchemaTable);
    }

    private void deleteEdges(Direction direction, SchemaTable edgeSchemaTable) {
        this.sqlgGraph.getTopology().threadWriteLock();
        StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTable.getSchema()));
        sql.append(".");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(edgeSchemaTable.getTable()));
        sql.append(" WHERE ");
        AbstractLabel abstractLabel;
        if (this.recordId.hasSequenceId()) {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema + "." + this.table + (direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END)));
            sql.append(" = ?");
        } else {
            int count = 1;
            Schema schema = this.sqlgGraph.getTopology().getSchema(this.schema).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found.", this.schema)));
            abstractLabel = getAbstractLabel(schema);
            for (String identifier : abstractLabel.getIdentifiers()) {
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema + "." + this.table + "." + identifier + (direction == Direction.OUT ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END)));
                sql.append(" = ?");
                if (count++ < this.recordId.getIdentifiers().size()) {
                    sql.append(" AND ");
                }
            }
        }
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            if (this.recordId.hasSequenceId()) {
                preparedStatement.setLong(1, this.recordId.sequenceId());
            } else {
                int count = 1;
                for (Comparable identifierValue : this.recordId.getIdentifiers()) {
                    preparedStatement.setObject(count++, identifierValue);
                }
            }
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insertTemporaryVertex(Map<String, Object> keyValueMap) {
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            SchemaTable schemaTable = SchemaTable.of(getSchema(), getTable(), true);
            internalBatchAddVertex(schemaTable, keyValueMap);
        } else {
            internalAddVertex(true, null, keyValueMap);
        }
        //Cache the properties
        this.properties.putAll(keyValueMap);
    }

    private void insertVertex(VertexLabel vertexLabel, Map<String, Object> keyValueMap) {
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode() && !this.sqlgGraph.tx().isInStreamingBatchMode() && !this.sqlgGraph.tx().isInStreamingWithLockBatchMode()) {
            SchemaTable schemaTable = SchemaTable.of(getSchema(), getTable(), false);
            internalBatchAddVertex(schemaTable, keyValueMap);
        } else {
            internalAddVertex(false, vertexLabel, keyValueMap);
        }
        //Cache the properties
        this.properties.putAll(keyValueMap);
    }

    private void internalStreamBatchAddVertex(VertexLabel vertexLabel, SchemaTable schemaTable, Map<String, Object> keyValueMap) {
        Preconditions.checkState(this.sqlgGraph.getSqlDialect().supportsBatchMode());
        this.sqlgGraph.tx().getBatchManager().streamVertex(this, vertexLabel, schemaTable, keyValueMap);
    }

    private void internalBatchAddVertex(SchemaTable schemaTable, Map<String, Object> keyValueMap) {
        Preconditions.checkState(this.sqlgGraph.getSqlDialect().supportsBatchMode());
        this.sqlgGraph.tx().getBatchManager().addVertex(this, schemaTable, keyValueMap);
    }

    private void internalAddVertex(boolean temporary, VertexLabel vertexLabel, Map<String, Object> keyValueMap) {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        //temporary tables have no schema
        if (!temporary || this.sqlgGraph.getSqlDialect().needsTemporaryTableSchema()) {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema));
            sql.append(".");
        }
        if (!temporary) {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + this.table));
        } else {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                    this.sqlgGraph.getSqlDialect().temporaryTablePrefix() +
                            VERTEX_PREFIX + this.table));
        }

        Map<String, Pair<PropertyDefinition, Object>> propertyDefinitionValueMap = new HashMap<>();
        Map<String, PropertyColumn> propertyColumns = null;
        if (!temporary) {
            Preconditions.checkNotNull(vertexLabel, "VertexLabel can not be null!");
            propertyColumns = vertexLabel.getProperties();
        }
        if (!keyValueMap.isEmpty()) {
            if (!temporary) {
                //sync up the keyValueMap with its PropertyColumn
                for (Map.Entry<String, Object> keyValueEntry : keyValueMap.entrySet()) {
                    PropertyDefinition propertyDefinition = propertyColumns.get(keyValueEntry.getKey()).getPropertyDefinition();
                    Pair<PropertyDefinition, Object> propertyTypeObjectPair = Pair.of(propertyDefinition, keyValueEntry.getValue());
                    propertyDefinitionValueMap.put(keyValueEntry.getKey(), propertyTypeObjectPair);
                }
            } else {
                Map<String, PropertyDefinition> properties = this.sqlgGraph.getTopology().getPublicSchema().getTemporaryTable(VERTEX_PREFIX + this.table);
                //sync up the keyValueMap with its PropertyColumn
                for (Map.Entry<String, Object> keyValueEntry : keyValueMap.entrySet()) {
                    PropertyDefinition propertyDefinition = properties.get(keyValueEntry.getKey());
                    Pair<PropertyDefinition, Object> propertyTypeObjectPair = Pair.of(propertyDefinition, keyValueEntry.getValue());
                    propertyDefinitionValueMap.put(keyValueEntry.getKey(), propertyTypeObjectPair);
                }
            }
            sql.append(" (");
            writeColumnNames(propertyDefinitionValueMap, sql);
            sql.append(") VALUES (");
            writeColumnParameters(propertyDefinitionValueMap, sql);
            sql.append(")");
        } else {
            sql.append(this.sqlgGraph.getSqlDialect().sqlInsertEmptyValues());
        }
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(sql.toString());
        }
        int i = 1;
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {
            SqlgUtil.setKeyValuesAsParameterUsingPropertyColumn(this.sqlgGraph, i, preparedStatement, propertyDefinitionValueMap);
            preparedStatement.executeUpdate();
            if (!temporary && !vertexLabel.getIdentifiers().isEmpty()) {
                List<Comparable> identifiers = new ArrayList<>();
                for (String identifier : vertexLabel.getIdentifiers()) {
                    identifiers.add((Comparable) propertyDefinitionValueMap.get(identifier).getRight());
                }
                this.recordId = RecordId.from(SchemaTable.of(this.schema, this.table), identifiers);
            } else {
                ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
                if (generatedKeys.next()) {
                    this.recordId = RecordId.from(SchemaTable.of(this.schema, this.table), generatedKeys.getLong(1));
                } else {
                    throw new RuntimeException(String.format("Could not retrieve the id after an insert into %s", Topology.VERTICES));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void load() {
        //if in batch mode, only load vertexes that are not new.
        //new vertexes have no id, impossible to load, but then all its properties are already cached.

        //sequenceId == -1 for aggregate functions
        if (this.recordId != null && this.recordId.hasSequenceId() && this.recordId.sequenceId() != -1 &&
                (this.properties.isEmpty() && !this.sqlgGraph.tx().isInBatchMode()) ||
                (this.properties.isEmpty() && this.sqlgGraph.getSqlDialect().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode() &&
                        !this.sqlgGraph.tx().getBatchManager().vertexIsCached(this))) {

            if (this.sqlgGraph.getSqlDialect().supportsBatchMode() && this.sqlgGraph.tx().isOpen() && this.sqlgGraph.tx().getBatchManager().isStreaming()) {
                throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
            }

            //Generate the columns to prevent 'ERROR: cached plan must not change result type" error'
            //This happens when the schema changes after the statement is prepared.
            VertexLabel vertexLabel = this.sqlgGraph.getTopology().getSchema(this.schema).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", this.schema))).getVertexLabel(this.table).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel %s not found", this.table)));
            StringBuilder sql = new StringBuilder("SELECT\n\t");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
            appendProperties(vertexLabel, sql);
            sql.append("\nFROM\n\t");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + this.table));
            sql.append("\nWHERE\n\t");
            //noinspection Duplicates
            if (vertexLabel.hasIDPrimaryKey()) {
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                sql.append(" = ?");
            } else {
                int count = 1;
                for (String identifier : vertexLabel.getIdentifiers()) {
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                    sql.append(" = ?");
                    if (count++ < vertexLabel.getIdentifiers().size()) {
                        sql.append(" AND ");
                    }
                }
            }
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            Connection conn = this.sqlgGraph.tx().getConnection();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                if (vertexLabel.hasIDPrimaryKey()) {
                    preparedStatement.setLong(1, this.recordId.sequenceId());
                } else {
                    int count = 1;
                    for (Comparable identifierValue : this.recordId.getIdentifiers()) {
                        preparedStatement.setObject(count++, identifierValue);
                    }
                }
                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    loadResultSet(resultSet);
                } else {
                    throw new IllegalStateException(String.format("Vertex with label %s and id %s does not exist.", this.schema + "." + this.table, this.recordId.getID().toString()));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //TODO optimize the if statement here to be outside the main ResultSet loop
//    @Override
    public void loadResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnLabel(i);
            if (!columnName.equals("ID")
                    && !columnName.equals(Topology.VERTEX_SCHEMA)
                    && !columnName.equals(VERTEX_TABLE)
                    && !this.sqlgGraph.getSqlDialect().columnsToIgnore().contains(columnName)) {
                loadProperty(resultSet, columnName, i);
            }
        }
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        SqlgVertex.this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode() && this.sqlgGraph.tx().getBatchManager().isStreaming()) {
            throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
        }
        return internalEdges(direction, edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        SqlgVertex.this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode() && this.sqlgGraph.tx().getBatchManager().isStreaming()) {
            throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
        }
        // need topology when we're a topology vertex
        GraphTraversalSource gts = Topology.SQLG_SCHEMA.equals(schema) ?
                this.sqlgGraph.topology()
                : this.sqlgGraph.traversal();
        //for some very bezaar reason not adding toList().iterator() return one extra element.
        return switch (direction) {
            case OUT -> gts.V(this).out(edgeLabels).toList().iterator();
            case IN -> gts.V(this).in(edgeLabels).toList().iterator();
            case BOTH -> gts.V(this).both(edgeLabels).toList().iterator();
        };
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return SqlgVertex.this.<V>internalGetProperties(propertyKeys).values().iterator();
    }

    @Override
    public SchemaTable getSchemaTablePrefixed() {
        return SchemaTable.of(this.getSchema(), VERTEX_PREFIX + this.getTable());
    }

    @Override
    public SchemaTable getSchemaTable() {
        return SchemaTable.of(this.getSchema(), this.getTable());
    }

    @Override
    AbstractLabel getAbstractLabel(Schema schema) {
        return schema.getVertexLabel(this.table).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel %s not found.", this.table)));
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
