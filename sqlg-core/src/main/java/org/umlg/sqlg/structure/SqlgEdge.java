package org.umlg.sqlg.structure;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.ColumnList;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.util.*;

import static org.umlg.sqlg.structure.topology.Topology.EDGE_PREFIX;

/**
 * Date: 2014/07/12
 * Time: 5:41 AM
 */
public class SqlgEdge extends SqlgElement implements Edge {

    private static final Logger logger = LoggerFactory.getLogger(SqlgEdge.class);
    private SqlgVertex inVertex;
    private SqlgVertex outVertex;

    /**
     * Called from @link {@link SqlgVertex} to create a brand new edge.
     *
     * @param sqlgGraph       The graph.
     * @param streaming       If in batch mode this indicates if its streaming or not.
     * @param schema          The schema the edge is in.
     * @param table           The edge's label which translates to a table name.
     * @param inVertex        The edge's in vertex.
     * @param outVertex       The edge's out vertex.
     * @param keyValueMapPair A pair of properties of the edge. Left contains all the properties and right the null valued properties.
     */
    public SqlgEdge(
            SqlgGraph sqlgGraph,
            boolean streaming,
            String schema,
            String table,
            SqlgVertex inVertex,
            SqlgVertex outVertex,
            Pair<Map<String, Object>, Map<String, Object>> keyValueMapPair) {

        super(sqlgGraph, schema, table);
        this.inVertex = inVertex;
        this.outVertex = outVertex;
        try {
            insertEdge(streaming, keyValueMapPair);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static SqlgEdge of(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        return new SqlgEdge(sqlgGraph, id, schema, table);
    }

    /**
     * This is the primary constructor for loading edges from the db via gremlin.
     *
     * @param sqlgGraph The graph.
     * @param id        The edge's id. This the edge's table's id. Not a {@link RecordId}.
     * @param schema    The schema the edge is in.
     * @param table     The table the edge is in. This translates to its label.
     */
    public SqlgEdge(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        super(sqlgGraph, id, schema, table);
    }

    public SqlgEdge(SqlgGraph sqlgGraph, List<Comparable> identifiers, String schema, String table) {
        super(sqlgGraph, identifiers, schema, table);
    }

    private Iterator<Vertex> internalGetVertices(Direction direction) {
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
            vertices.add(getOutVertex());
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
            vertices.add(getInVertex());
        return vertices.iterator();
    }

    @Override
    public void remove() {
        this.sqlgGraph.tx().readWrite();
        this.sqlgGraph.getTopology().threadWriteLock();
        if (this.removed)
            throw new IllegalStateException(String.format("Edge with id %s was removed.", id().toString()));

        if (this.sqlgGraph.getSqlDialect().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            this.sqlgGraph.tx().getBatchManager().removeEdge(this.schema, this.table, this);
        } else {
            super.remove();
        }
    }

    public SqlgVertex getInVertex() {
        if (this.inVertex == null) {
            load();
        }
        return this.inVertex;
    }

    public SqlgVertex getOutVertex() {
        if (this.outVertex == null) {
            load();
        }
        return this.outVertex;
    }

    @Override
    public String toString() {
        if (this.inVertex == null) {
            load();
        }
        return StringFactory.edgeString(this);
    }

    private void insertEdge(boolean complete, Pair<Map<String, Object>, Map<String, Object>> keyValueMapPair) throws SQLException {
        Map<String, Object> allKeyValueMap = keyValueMapPair.getLeft();
        Map<String, Object> notNullKeyValueMap = keyValueMapPair.getRight();
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            internalBatchAddEdge(complete, allKeyValueMap);
        } else {
            internalAddEdge(notNullKeyValueMap);
        }
        //Cache the properties
        this.properties.putAll(notNullKeyValueMap);
    }

    private void internalBatchAddEdge(boolean streaming, Map<String, Object> keyValueMap) {
        Preconditions.checkState(this.sqlgGraph.getSqlDialect().supportsBatchMode());
        this.sqlgGraph.tx().getBatchManager().addEdge(streaming, this, this.outVertex, this.inVertex, keyValueMap);
    }

    private void internalAddEdge(Map<String, Object> keyValueMap) throws SQLException {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema));
        sql.append(".");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(EDGE_PREFIX + this.table));
        sql.append(" (");

        Map<String, Pair<PropertyType, Object>> propertyTypeValueMap = new HashMap<>();
        Map<String, PropertyColumn> propertyColumns = null;
        EdgeLabel edgeLabel = this.sqlgGraph.getTopology()
                .getSchema(this.schema).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", this.schema)))
                .getEdgeLabel(this.table).orElseThrow(() -> new IllegalStateException(String.format("EdgeLabel %s not found in schema %s", this.table, this.schema)));
        VertexLabel inVertexLabel = this.sqlgGraph.getTopology()
                .getSchema(inVertex.getSchema()).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", inVertex.getSchema())))
                .getVertexLabel(inVertex.getTable()).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel %s not found in schema %s", inVertex.getTable(), inVertex.getSchema())));
        VertexLabel outVertexLabel = this.sqlgGraph.getTopology()
                .getSchema(outVertex.getSchema()).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", outVertex.getSchema())))
                .getVertexLabel(outVertex.getTable()).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel %s not found in schema %s", outVertex.getTable(), outVertex.getSchema())));

        if (!keyValueMap.isEmpty()) {
            propertyColumns = edgeLabel.getProperties();
            //sync up the keyValueMap with its PropertyColumn
            for (Map.Entry<String, Object> keyValueEntry : keyValueMap.entrySet()) {
                PropertyColumn propertyColumn = propertyColumns.get(keyValueEntry.getKey());
                Pair<PropertyType, Object> propertyColumnObjectPair = Pair.of(propertyColumn.getPropertyType(), keyValueEntry.getValue());
                propertyTypeValueMap.put(keyValueEntry.getKey(), propertyColumnObjectPair);
            }
        }
        writeColumnNames(propertyTypeValueMap, sql);
        if (keyValueMap.size() > 0) {
            sql.append(", ");
        }
        if (inVertexLabel.getIdentifiers().isEmpty()) {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.inVertex.schema + "." + this.inVertex.table + Topology.IN_VERTEX_COLUMN_END));
        } else {
            int i = 1;
            for (String identifier : inVertexLabel.getIdentifiers()) {
                if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                    i++;
                } else {
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.inVertex.schema + "." + this.inVertex.table + "." + identifier + Topology.IN_VERTEX_COLUMN_END));
                    if (outVertexLabel.isDistributed()) {
                        if (i++ < inVertexLabel.getIdentifiers().size() - 1) {
                            sql.append(", ");
                        }
                    } else {
                        if (i++ < inVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    }
                }
            }
        }
        sql.append(", ");
        if (outVertexLabel.getIdentifiers().isEmpty()) {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.outVertex.schema + "." + this.outVertex.table + Topology.OUT_VERTEX_COLUMN_END));
        } else {
            int i = 1;
            for (String identifier : outVertexLabel.getIdentifiers()) {
                if (outVertexLabel.isDistributed() && outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                    i++;
                } else {
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.outVertex.schema + "." + this.outVertex.table + "." + identifier + Topology.OUT_VERTEX_COLUMN_END));
                    if (outVertexLabel.isDistributed()) {
                        if (i++ < outVertexLabel.getIdentifiers().size() - 1) {
                            sql.append(", ");
                        }
                    } else {
                        if (i++ < outVertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    }
                }
            }
        }
        sql.append(") VALUES (");
        writeColumnParameters(propertyTypeValueMap, sql);
        if (keyValueMap.size() > 0) {
            sql.append(", ");
        }
        buildQuestionMark(sql, inVertexLabel);
        sql.append(", ");
        buildQuestionMark(sql, outVertexLabel);
        sql.append(")");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        int i = 1;
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {
            i = SqlgUtil.setKeyValuesAsParameterUsingPropertyColumn(this.sqlgGraph, i, preparedStatement, propertyTypeValueMap);

            if (inVertexLabel.getIdentifiers().isEmpty()) {
                preparedStatement.setLong(i++, this.inVertex.recordId.sequenceId());
            } else {
                for (String identifier : inVertexLabel.getIdentifiers()) {
                    if (!inVertexLabel.isDistributed() || !inVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        i = SqlgUtil.setKeyValueAsParameter(
                                this.sqlgGraph,
                                false,
                                i,
                                preparedStatement,
                                ImmutablePair.of(inVertexLabel.getProperty(identifier).orElseThrow(() -> new IllegalStateException(String.format("identifier %s not a property.", identifier))).getPropertyType(), inVertex.value(identifier)));
                    }
                }
            }
            if (outVertexLabel.hasIDPrimaryKey()) {
                preparedStatement.setLong(i, this.outVertex.recordId.sequenceId());
            } else {
                for (String identifier : outVertexLabel.getIdentifiers()) {
                    if (!outVertexLabel.isDistributed() || !outVertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                        i = SqlgUtil.setKeyValueAsParameter(
                                this.sqlgGraph,
                                false,
                                i,
                                preparedStatement,
                                ImmutablePair.of(outVertexLabel.getProperty(identifier).orElseThrow(() -> new IllegalStateException(String.format("identifier %s not a property.", identifier))).getPropertyType(), outVertex.value(identifier)));
                    }
                }
            }
            preparedStatement.executeUpdate();
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            if (edgeLabel.hasIDPrimaryKey()) {
                if (generatedKeys.next()) {
                    this.recordId = RecordId.from(SchemaTable.of(this.schema, this.table), generatedKeys.getLong(1));
                } else {
                    throw new RuntimeException("Could not retrieve the id after an insert into " + Topology.VERTICES);
                }
            } else {
                List<Comparable> identifiers = new ArrayList<>();
                for (String identifier : edgeLabel.getIdentifiers()) {
                    //noinspection unchecked
                    identifiers.add((Comparable<Object>) propertyTypeValueMap.get(identifier).getRight());
                }
                this.recordId = RecordId.from(SchemaTable.of(this.schema, this.table), identifiers);
            }
        }
    }

    private void buildQuestionMark(StringBuilder sql, VertexLabel vertexLabel) {
        if (vertexLabel.getIdentifiers().isEmpty()) {
            sql.append("?");
        } else {
            for (String identifier : vertexLabel.getIdentifiers()) {
                if (!vertexLabel.isDistributed() || !vertexLabel.getDistributionPropertyColumn().getName().equals(identifier)) {
                    sql.append("?, ");
                }
            }
            //remove the extra comma
            sql.delete(sql.length() - 2, sql.length());
        }
    }

    //TODO this needs optimizing, an edge created in the transaction need not go to the db to load itself again
    @Override
    protected void load() {
        //recordId can be null when in batchMode
        if (this.recordId != null && this.properties.isEmpty()) {
            this.sqlgGraph.tx().readWrite();
            if (this.sqlgGraph.getSqlDialect().supportsBatchMode() && this.sqlgGraph.tx().getBatchManager().isStreaming()) {
                throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
            }

            //Generate the columns to prevent 'ERROR: cached plan must not change result type" error'
            //This happens when the schema changes after the statement is prepared.
            @SuppressWarnings("OptionalGetWithoutIsPresent")
            EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getSchema(this.schema).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", this.schema))).getEdgeLabel(this.table).orElseThrow(() -> new IllegalStateException(String.format("EdgeLabel %s not found", this.table)));
            StringBuilder sql = new StringBuilder("SELECT\n\t");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
            appendProperties(edgeLabel, sql);
            List<VertexLabel> outForeignKeys = new ArrayList<>();
            for (VertexLabel vertexLabel : edgeLabel.getOutVertexLabels()) {
                outForeignKeys.add(vertexLabel);
                sql.append(", ");
                if (vertexLabel.hasIDPrimaryKey()) {
                    String foreignKey = vertexLabel.getSchema().getName() + "." + vertexLabel.getName() + Topology.OUT_VERTEX_COLUMN_END;
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKey));
                } else {
                    int countIdentifier = 1;
                    for (String identifier : vertexLabel.getIdentifiers()) {
                        PropertyColumn propertyColumn = vertexLabel.getProperty(identifier).orElseThrow(
                                () -> new IllegalStateException(String.format("identifier %s column must be a property", identifier))
                        );
                        PropertyType propertyType = propertyColumn.getPropertyType();
                        String[] propertyTypeToSqlDefinition = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                        int count = 1;
                        for (String ignored : propertyTypeToSqlDefinition) {
                            if (count > 1) {
                                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(vertexLabel.getFullName() + "." + identifier + propertyType.getPostFixes()[count - 2] + Topology.OUT_VERTEX_COLUMN_END));
                            } else {
                                //The first column existVertexLabel no postfix
                                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(vertexLabel.getFullName() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END));
                            }
                            if (count++ < propertyTypeToSqlDefinition.length) {
                                sql.append(", ");
                            }
                        }
                        if (countIdentifier++ < vertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    }
                }
            }
            List<VertexLabel> inForeignKeys = new ArrayList<>();
            for (VertexLabel vertexLabel : edgeLabel.getInVertexLabels()) {
                sql.append(", ");
                inForeignKeys.add(vertexLabel);
                if (vertexLabel.hasIDPrimaryKey()) {
                    String foreignKey = vertexLabel.getSchema().getName() + "." + vertexLabel.getName() + Topology.IN_VERTEX_COLUMN_END;
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(foreignKey));
                } else {
                    int countIdentifier = 1;
                    for (String identifier : vertexLabel.getIdentifiers()) {
                        PropertyColumn propertyColumn = vertexLabel.getProperty(identifier).orElseThrow(
                                () -> new IllegalStateException(String.format("identifier %s column must be a property", identifier))
                        );
                        PropertyType propertyType = propertyColumn.getPropertyType();
                        String[] propertyTypeToSqlDefinition = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                        int count = 1;
                        for (String ignored : propertyTypeToSqlDefinition) {
                            if (count > 1) {
                                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(vertexLabel.getFullName() + "." + identifier + propertyType.getPostFixes()[count - 2] + Topology.IN_VERTEX_COLUMN_END));
                            } else {
                                //The first column existVertexLabel no postfix
                                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(vertexLabel.getFullName() + "." + identifier + Topology.IN_VERTEX_COLUMN_END));
                            }
                            if (count++ < propertyTypeToSqlDefinition.length) {
                                sql.append(", ");
                            }
                        }
                        if (countIdentifier++ < vertexLabel.getIdentifiers().size()) {
                            sql.append(", ");
                        }
                    }
                }
            }
            sql.append("\nFROM\n\t");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(EDGE_PREFIX + this.table));
            sql.append(" WHERE ");
            //noinspection Duplicates
            if (edgeLabel.hasIDPrimaryKey()) {
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                sql.append(" = ?");
            } else {
                int count = 1;
                for (String identifier : edgeLabel.getIdentifiers()) {
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                    sql.append(" = ?");
                    if (count++ < edgeLabel.getIdentifiers().size()) {
                        sql.append(" AND ");
                    }

                }
            }
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            Connection conn = this.sqlgGraph.tx().getConnection();
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                if (edgeLabel.hasIDPrimaryKey()) {
                    preparedStatement.setLong(1, this.recordId.sequenceId());
                } else {
                    int count = 1;
                    for (Comparable identifierValue : this.recordId.getIdentifiers()) {
                        preparedStatement.setObject(count++, identifierValue);
                    }
                }
                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    loadResultSet(resultSet, inForeignKeys, outForeignKeys);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }



    public void loadInVertex(ResultSet resultSet, SchemaTable inVertexSchemaTable, int columnIdx) throws SQLException {
        Long inId = resultSet.getLong(columnIdx);
        if (!resultSet.wasNull()) {
            this.inVertex = SqlgVertex.of(this.sqlgGraph, inId, inVertexSchemaTable.getSchema(), inVertexSchemaTable.getTable());
        }
    }

    public void loadInVertex(ResultSet resultSet, List<ColumnList.Column> inForeignKeyColumns) {
        List<Comparable> identifiers = SqlgUtil.getValue(resultSet, inForeignKeyColumns);
        if (!identifiers.isEmpty()) {
            ColumnList.Column column = inForeignKeyColumns.get(0);
            this.inVertex = SqlgVertex.of(
                    this.sqlgGraph,
                    identifiers,
                    column.getForeignSchemaTable().getSchema(),
                    column.getForeignSchemaTable().getTable()
            );
        }
    }

    public void loadOutVertex(ResultSet resultSet, SchemaTable outVertexSchemaTable, int columnIdx) throws SQLException {
        Long outId = resultSet.getLong(columnIdx);
        if (!resultSet.wasNull()) {
            this.outVertex = SqlgVertex.of(this.sqlgGraph, outId, outVertexSchemaTable.getSchema(), outVertexSchemaTable.getTable());
        }
    }

    public void loadOutVertex(ResultSet resultSet, List<ColumnList.Column> outForeignKeyColumns) {
        List<Comparable> identifiers = SqlgUtil.getValue(resultSet, outForeignKeyColumns);
        if (!identifiers.isEmpty()) {
            ColumnList.Column column = outForeignKeyColumns.get(0);
            this.outVertex = SqlgVertex.of(
                    this.sqlgGraph,
                    identifiers,
                    column.getForeignSchemaTable().getSchema(),
                    column.getForeignSchemaTable().getTable()
            );
        }
    }

    private void loadResultSet(ResultSet resultSet, List<VertexLabel> inForeignKeys, List<VertexLabel> outForeignKeys) throws SQLException {
        SchemaTable inVertexColumnName = null;
        SchemaTable outVertexColumnName = null;
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnLabel(i);
            if (!columnName.equals("ID") &&
                    !columnName.endsWith(Topology.OUT_VERTEX_COLUMN_END) &&
                    !columnName.endsWith(Topology.IN_VERTEX_COLUMN_END)) {

                loadProperty(resultSet, columnName, i);
            }
        }
        long inId = -1;
        List<Comparable> inComparables = new ArrayList<>();
        for (VertexLabel inVertexLabel: inForeignKeys) {
            inVertexColumnName = SchemaTable.of(inVertexLabel.getSchema().getName(), inVertexLabel.getLabel());
            if (inVertexLabel.hasIDPrimaryKey()) {
                String foreignKey = inVertexLabel.getSchema().getName() + "." + inVertexLabel.getName() + Topology.IN_VERTEX_COLUMN_END;
                inId = resultSet.getLong(foreignKey);
                if (!resultSet.wasNull()) {
                    break;
                }
            } else {
                for (String identifier : inVertexLabel.getIdentifiers()) {
                    PropertyColumn propertyColumn = inVertexLabel.getProperty(identifier).orElseThrow(
                            () -> new IllegalStateException(String.format("identifier %s column must be a property", identifier))
                    );
                    PropertyType propertyType = propertyColumn.getPropertyType();
                    String[] propertyTypeToSqlDefinition = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                    int count = 1;
                    for (String ignored : propertyTypeToSqlDefinition) {
                        if (count > 1) {
                            inComparables.add((Comparable)resultSet.getObject(inVertexLabel.getFullName() + "." + identifier + propertyType.getPostFixes()[count - 2] + Topology.IN_VERTEX_COLUMN_END));
                        } else {
                            //The first column existVertexLabel no postfix
                            inComparables.add((Comparable)resultSet.getObject(inVertexLabel.getFullName() + "." + identifier + Topology.IN_VERTEX_COLUMN_END));
                        }
                        count++;
                    }
                }

            }

        }
        long outId = -1;
        List<Comparable> outComparables = new ArrayList<>();
        for (VertexLabel outVertexLabel: outForeignKeys) {
            outVertexColumnName = SchemaTable.of(outVertexLabel.getSchema().getName(), outVertexLabel.getLabel());
            if (outVertexLabel.hasIDPrimaryKey()) {
                String foreignKey = outVertexLabel.getSchema().getName() + "." + outVertexLabel.getName() + Topology.OUT_VERTEX_COLUMN_END;
                outId = resultSet.getLong(foreignKey);
                if (!resultSet.wasNull()) {
                    break;
                }
            } else {
                for (String identifier : outVertexLabel.getIdentifiers()) {
                    PropertyColumn propertyColumn = outVertexLabel.getProperty(identifier).orElseThrow(
                            () -> new IllegalStateException(String.format("identifier %s column must be a property", identifier))
                    );
                    PropertyType propertyType = propertyColumn.getPropertyType();
                    String[] propertyTypeToSqlDefinition = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                    int count = 1;
                    for (String ignored : propertyTypeToSqlDefinition) {
                        if (count > 1) {
                            outComparables.add((Comparable)resultSet.getObject(outVertexLabel.getFullName() + "." + identifier + propertyType.getPostFixes()[count - 2] + Topology.OUT_VERTEX_COLUMN_END));
                        } else {
                            //The first column existVertexLabel no postfix
                            outComparables.add((Comparable)resultSet.getObject(outVertexLabel.getFullName() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END));
                        }
                        count++;
                    }
                }
            }
        }
        if (inId != -1) {
            this.inVertex = SqlgVertex.of(this.sqlgGraph, inId, inVertexColumnName.getSchema(), SqlgUtil.removeTrailingInId(inVertexColumnName.getTable()));
        } else {
            Preconditions.checkState(!inComparables.isEmpty(), "The in ids are not found for the edge!");
            this.inVertex = SqlgVertex.of(this.sqlgGraph, inComparables, inVertexColumnName.getSchema(), SqlgUtil.removeTrailingInId(inVertexColumnName.getTable()));
        }
        if (outId != -1) {
            this.outVertex = SqlgVertex.of(this.sqlgGraph, outId, outVertexColumnName.getSchema(), SqlgUtil.removeTrailingOutId(outVertexColumnName.getTable()));
        } else {
            Preconditions.checkState(!outComparables.isEmpty(), "The out ids are not found for the edge!");
            this.outVertex = SqlgVertex.of(this.sqlgGraph, outComparables, outVertexColumnName.getSchema(), SqlgUtil.removeTrailingOutId(outVertexColumnName.getTable()));
        }
    }


    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        //noinspection unchecked
        return (Iterator) super.properties(propertyKeys);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        SqlgEdge.this.sqlgGraph.tx().readWrite();
        return internalGetVertices(direction);
    }

    @Override
    public SchemaTable getSchemaTablePrefixed() {
        return SchemaTable.of(this.getSchema(), EDGE_PREFIX + this.getTable());
    }

    @Override
    AbstractLabel getAbstractLabel(Schema schema) {
        return schema.getEdgeLabel(this.table).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel %s not found.", this.table)));
    }
}
