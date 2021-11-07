package org.umlg.sqlg.sql.dialect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.util.SqlgUtil;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import static org.umlg.sqlg.structure.PropertyType.JSON_ORDINAL;
import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * Date: 2014/08/21
 * Time: 6:52 PM
 */
public abstract class BaseSqlDialect implements SqlDialect, SqlBulkDialect, SqlSchemaChangeDialect {

    final Logger logger = LoggerFactory.getLogger(getClass().getName());

    protected BaseSqlDialect() {
    }

    public void validateColumnName(String column) {
        if (column.endsWith(IN_VERTEX_COLUMN_END) || column.endsWith(OUT_VERTEX_COLUMN_END)) {
            throw SqlgExceptions.invalidColumnName("Column names may not end with " + IN_VERTEX_COLUMN_END + " or " + OUT_VERTEX_COLUMN_END + ". column = " + column);
        }
    }

    @Override
    public List<String> getSchemaNames(DatabaseMetaData metaData) {
        List<String> schemaNames = new ArrayList<>();
        try {
            try (ResultSet schemaRs = metaData.getSchemas()) {
                while (schemaRs.next()) {
                    String schema = schemaRs.getString(1);
                    if (!this.getInternalSchemas().contains(schema)) {
                        schemaNames.add(schema);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return schemaNames;
    }


    @Override
    public List<Triple<String, String, String>> getVertexTables(DatabaseMetaData metaData) {
        List<Triple<String, String, String>> vertexTables = new ArrayList<>();
        String[] types = new String[]{"TABLE"};
        try {
            //load the vertices
            try (ResultSet vertexRs = metaData.getTables(null, null, Topology.VERTEX_PREFIX + "%", types)) {
                while (vertexRs.next()) {
                    String tblCat = vertexRs.getString(1);
                    String schema = vertexRs.getString(2);
                    String table = vertexRs.getString(3);

                    //verify the table name matches our pattern
                    if (!table.startsWith(Topology.VERTEX_PREFIX)) {
                        continue;
                    }
                    vertexTables.add(Triple.of(tblCat, schema, table));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return vertexTables;
    }

    @Override
    public List<Triple<String, String, String>> getEdgeTables(DatabaseMetaData metaData) {
        List<Triple<String, String, String>> edgeTables = new ArrayList<>();
        String[] types = new String[]{"TABLE"};
        try {
            //load the edges without their properties
            try (ResultSet edgeRs = metaData.getTables(null, null, Topology.EDGE_PREFIX + "%", types)) {
                while (edgeRs.next()) {
                    String edgCat = edgeRs.getString(1);
                    String schema = edgeRs.getString(2);
                    String table = edgeRs.getString(3);
                    //verify the table name matches our pattern
                    if (!table.startsWith(Topology.EDGE_PREFIX)) {
                        continue;
                    }
                    edgeTables.add(Triple.of(edgCat, schema, table));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return edgeTables;
    }

    @Override
    public void flushVertexCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexCache) {
        for (Map.Entry<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> entry : vertexCache.entrySet()) {
            SchemaTable schemaTable = entry.getKey();
            Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> vertices = entry.getValue();
            SortedSet<String> columns = vertices.getLeft();
            Map<SqlgVertex, Map<String, Object>> rows = vertices.getRight();

            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO ");
            if (!schemaTable.isTemporary() || sqlgGraph.getSqlDialect().needsTemporaryTableSchema()) {
                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                sql.append(".");
            }
            if (!schemaTable.isTemporary() || !sqlgGraph.getSqlDialect().needsTemporaryTablePrefix()) {
                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + schemaTable.getTable()));
            } else {
                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(
                        sqlgGraph.getSqlDialect().temporaryTablePrefix() +
                                VERTEX_PREFIX + schemaTable.getTable()));
            }

            VertexLabel vertexLabel = null;
            Map<String, PropertyColumn> propertyColumns = null;
            Map<String, PropertyType> properties = null;
            if (!schemaTable.isTemporary()) {
                vertexLabel = sqlgGraph.getTopology()
                        .getSchema(schemaTable.getSchema()).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", schemaTable.getSchema())))
                        .getVertexLabel(schemaTable.getTable()).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel %s not found", schemaTable.getTable())));
                propertyColumns = vertexLabel.getProperties();
            } else {
                properties = sqlgGraph.getTopology().getPublicSchema().getTemporaryTable(VERTEX_PREFIX + schemaTable.getTable());
            }
            if (!columns.isEmpty()) {
                Map<String, PropertyType> propertyTypeMap = new HashMap<>();
                for (String column : columns) {
                    if (!schemaTable.isTemporary()) {
                        PropertyColumn propertyColumn = propertyColumns.get(column);
                        propertyTypeMap.put(column, propertyColumn.getPropertyType());
                    } else {
                        propertyTypeMap.put(column, properties.get(column));
                    }
                }
                sql.append(" (");
                int i = 1;
                //noinspection Duplicates
                for (String column : columns) {
                    PropertyType propertyType = propertyTypeMap.get(column);
                    String[] sqlDefinitions = sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                    int count = 1;
                    for (@SuppressWarnings("unused") String sqlDefinition : sqlDefinitions) {
                        if (count > 1) {
                            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column + propertyType.getPostFixes()[count - 2]));
                        } else {
                            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
                        }
                        if (count++ < sqlDefinitions.length) {
                            sql.append(",");
                        }
                    }
                    if (i++ < columns.size()) {
                        sql.append(", ");
                    }
                }
                sql.append(") VALUES ( ");

                i = 1;
                //noinspection Duplicates
                for (String column : columns) {
                    PropertyType propertyType = propertyTypeMap.get(column);
                    String[] sqlDefinitions = sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                    int count = 1;
                    //noinspection Duplicates
                    for (@SuppressWarnings("unused") String sqlDefinition : sqlDefinitions) {
                        if (count > 1) {
                            sql.append("?");
                        } else {
                            sql.append("?");
                        }
                        if (count++ < sqlDefinitions.length) {
                            sql.append(",");
                        }
                    }
                    if (i++ < columns.size()) {
                        sql.append(", ");
                    }
                }
                sql.append(")");
            } else {
                sql.append(sqlgGraph.getSqlDialect().sqlInsertEmptyValues());
            }
            if (sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            Connection conn = sqlgGraph.tx().getConnection();
            try (PreparedStatement preparedStatement = (vertexLabel != null && !vertexLabel.hasIDPrimaryKey() ? conn.prepareStatement(sql.toString()) : conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS))) {
                List<SqlgVertex> sqlgVertices = new ArrayList<>();
                for (Map.Entry<SqlgVertex, Map<String, Object>> rowEntry : rows.entrySet()) {
                    int i = 1;
                    SqlgVertex sqlgVertex = rowEntry.getKey();
                    sqlgVertices.add(sqlgVertex);
                    if (!columns.isEmpty()) {
                        Map<String, Object> parameterValueMap = rowEntry.getValue();
                        List<Pair<PropertyType, Object>> typeAndValues = new ArrayList<>();
                        for (String column : columns) {
                            if (!schemaTable.isTemporary()) {
                                PropertyColumn propertyColumn = propertyColumns.get(column);
                                typeAndValues.add(Pair.of(propertyColumn.getPropertyType(), parameterValueMap.get(column)));
                            } else {
                                typeAndValues.add(Pair.of(properties.get(column), parameterValueMap.get(column)));
                            }
                        }
                        if (vertexLabel != null && !vertexLabel.hasIDPrimaryKey()) {
                            List<Comparable> identifiers = new ArrayList<>();
                            for (String identifier : vertexLabel.getIdentifiers()) {
                                identifiers.add((Comparable) parameterValueMap.get(identifier));
                            }
                            sqlgVertex.setInternalPrimaryKey(RecordId.from(SchemaTable.of(schemaTable.getSchema(), schemaTable.getTable()), identifiers));

                        }
                        SqlgUtil.setKeyValuesAsParameterUsingPropertyColumn(sqlgGraph, true, i, preparedStatement, typeAndValues);
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                if (vertexLabel == null || vertexLabel.hasIDPrimaryKey()) {
                    ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
                    int i = 0;
                    while (generatedKeys.next()) {
                        sqlgVertices.get(i++).setInternalPrimaryKey(RecordId.from(schemaTable, generatedKeys.getLong(1)));
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void flushEdgeCache(SqlgGraph sqlgGraph, Map<MetaEdge, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>>> edgeCache) {
        for (MetaEdge metaEdge : edgeCache.keySet()) {

            SchemaTable outSchemaTable = SchemaTable.from(sqlgGraph, metaEdge.getOutLabel());
            SchemaTable inSchemaTable = SchemaTable.from(sqlgGraph, metaEdge.getInLabel());
            EdgeLabel edgeLabel = sqlgGraph.getTopology().getEdgeLabel(metaEdge.getSchemaTable().getSchema(), metaEdge.getSchemaTable().getTable()).orElseThrow(() -> new IllegalStateException(String.format("EdgeLabel not found for %s.%s", metaEdge.getSchemaTable().getSchema(), metaEdge.getSchemaTable().getTable())));
            VertexLabel outVertexLabel = sqlgGraph.getTopology().getVertexLabel(outSchemaTable.getSchema(), outSchemaTable.getTable()).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel not found for %s.%s", outSchemaTable.getSchema(), outSchemaTable.getTable())));
            VertexLabel inVertexLabel = sqlgGraph.getTopology().getVertexLabel(inSchemaTable.getSchema(), inSchemaTable.getTable()).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel not found for %s.%s", inSchemaTable.getSchema(), inSchemaTable.getTable())));

            Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> triples = edgeCache.get(metaEdge);
            Map<String, PropertyType> propertyTypeMap = sqlgGraph.getTopology().getTableFor(metaEdge.getSchemaTable().withPrefix(EDGE_PREFIX));
            SortedSet<String> columns = triples.getLeft();
            Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> rows = triples.getRight();

            StringBuilder sql = new StringBuilder("INSERT INTO ");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(metaEdge.getSchemaTable().getSchema()));
            sql.append(".");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(EDGE_PREFIX + metaEdge.getSchemaTable().getTable()));
            sql.append(" (");

            Map<String, PropertyColumn> propertyColumns = sqlgGraph.getTopology()
                    .getSchema(metaEdge.getSchemaTable().getSchema()).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", metaEdge.getSchemaTable().getSchema())))
                    .getEdgeLabel(metaEdge.getSchemaTable().getTable()).orElseThrow(() -> new IllegalStateException(String.format("EdgeLabel %s not found", metaEdge.getSchemaTable().getTable())))
                    .getProperties();

            int i = 1;
            for (String column : columns) {
                PropertyType propertyType = propertyTypeMap.get(column);
                String[] sqlDefinitions = sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                int count = 1;
                for (@SuppressWarnings("unused") String sqlDefinition : sqlDefinitions) {
                    if (count > 1) {
                        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column + propertyType.getPostFixes()[count - 2]));
                    } else {
                        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
                    }
                    if (count++ < sqlDefinitions.length) {
                        sql.append(",");
                    }
                }
                if (i++ < columns.size()) {
                    sql.append(", ");
                }
            }
            if (!columns.isEmpty()) {
                sql.append(", ");
            }
            if (outVertexLabel.hasIDPrimaryKey()) {
                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(metaEdge.getOutLabel() + OUT_VERTEX_COLUMN_END));
            } else {
                int j = 1;
                for (String identifier : outVertexLabel.getIdentifiers()) {
                    sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(metaEdge.getOutLabel() + "." + identifier + OUT_VERTEX_COLUMN_END));
                    if (j++ < outVertexLabel.getIdentifiers().size()) {
                        sql.append(", ");
                    }
                }
            }
            sql.append(", ");
            if (inVertexLabel.hasIDPrimaryKey()) {
                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(metaEdge.getInLabel() + IN_VERTEX_COLUMN_END));
            } else {
                int j = 1;
                for (String identifier : inVertexLabel.getIdentifiers()) {
                    sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(metaEdge.getInLabel() + "." + identifier + IN_VERTEX_COLUMN_END));
                    if (j++ < inVertexLabel.getIdentifiers().size()) {
                        sql.append(", ");
                    }
                }
            }
            sql.append(") VALUES (");

            i = 1;
            for (String column : columns) {
                PropertyType propertyType = propertyTypeMap.get(column);
                String[] sqlDefinitions = sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                int count = 1;
                //noinspection Duplicates
                for (@SuppressWarnings("unused") String sqlDefinition : sqlDefinitions) {
                    if (count > 1) {
                        sql.append("?");
                    } else {
                        sql.append("?");
                    }
                    if (count++ < sqlDefinitions.length) {
                        sql.append(",");
                    }
                }
                if (i++ < columns.size()) {
                    sql.append(", ");
                }
            }
            if (!columns.isEmpty()) {
                sql.append(", ");
            }
            if (outVertexLabel.hasIDPrimaryKey()) {
                sql.append("?");
            } else {
                int j = 1;
                for (String identifier : outVertexLabel.getIdentifiers()) {
                    sql.append("?");
                    if (j++ < outVertexLabel.getIdentifiers().size()) {
                        sql.append(", ");
                    }
                }
            }
            sql.append(", ");
            if (inVertexLabel.hasIDPrimaryKey()) {
                sql.append("?");
            } else {
                int j = 1;
                for (String identifier : inVertexLabel.getIdentifiers()) {
                    sql.append("?");
                    if (j++ < inVertexLabel.getIdentifiers().size()) {
                        sql.append(", ");
                    }
                }
            }
            sql.append(")");
            if (sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            Connection conn = sqlgGraph.tx().getConnection();
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS)) {
                List<SqlgEdge> sqlgEdges = new ArrayList<>();
                for (Map.Entry<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> rowEntry : rows.entrySet()) {
                    i = 1;
                    SqlgEdge sqlgEdge = rowEntry.getKey();
                    sqlgEdges.add(sqlgEdge);
                    Triple<SqlgVertex, SqlgVertex, Map<String, Object>> parameterValueMap = rowEntry.getValue();
                    List<Pair<PropertyType, Object>> typeAndValues = new ArrayList<>();
                    for (String column : columns) {
                        PropertyColumn propertyColumn = propertyColumns.get(column);
                        typeAndValues.add(Pair.of(propertyColumn.getPropertyType(), parameterValueMap.getRight().get(column)));
                    }
                    i = SqlgUtil.setKeyValuesAsParameterUsingPropertyColumn(sqlgGraph, true, i, preparedStatement, typeAndValues);

                    if (outVertexLabel.hasIDPrimaryKey()) {
                        preparedStatement.setLong(i++, ((RecordId) parameterValueMap.getLeft().id()).sequenceId());
                    } else {
                        for (String identifier : outVertexLabel.getIdentifiers()) {
                            i = SqlgUtil.setKeyValueAsParameter(
                                    sqlgGraph,
                                    false,
                                    i,
                                    preparedStatement,
                                    ImmutablePair.of(outVertexLabel.getProperty(identifier).orElseThrow(
                                            () -> new IllegalStateException(String.format("Property for identifier %s not found", identifier))
                                    ).getPropertyType(), parameterValueMap.getLeft().value(identifier)));
                        }
                    }
                    if (inVertexLabel.hasIDPrimaryKey()) {
                        preparedStatement.setLong(i, ((RecordId) parameterValueMap.getMiddle().id()).sequenceId());
                    } else {
                        for (String identifier : inVertexLabel.getIdentifiers()) {
                            i = SqlgUtil.setKeyValueAsParameter(
                                    sqlgGraph,
                                    false,
                                    i,
                                    preparedStatement,
                                    ImmutablePair.of(inVertexLabel.getProperty(identifier).orElseThrow(
                                            () -> new IllegalStateException(String.format("Property for identifier %s not found", identifier))
                                    ).getPropertyType(), parameterValueMap.getMiddle().value(identifier)));
                        }
                    }
                    if (!edgeLabel.hasIDPrimaryKey()) {
                        List<Comparable> identifiers = new ArrayList<>();
                        for (String identifier : edgeLabel.getIdentifiers()) {
                            identifiers.add((Comparable) parameterValueMap.getRight().get(identifier));
                        }
                        sqlgEdge.setInternalPrimaryKey(RecordId.from(SchemaTable.of(metaEdge.getSchemaTable().getSchema(), metaEdge.getSchemaTable().getTable()), identifiers));
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                if (edgeLabel.hasIDPrimaryKey()) {
                    ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
                    i = 0;
                    while (generatedKeys.next()) {
                        sqlgEdges.get(i++).setInternalPrimaryKey(RecordId.from(metaEdge.getSchemaTable(), generatedKeys.getLong(1)));
                    }
                }
//                insertGlobalUniqueIndex(keyValueMap, propertyColumns);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void flushVertexPropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexPropertyCache) {
        for (Map.Entry<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> entry : vertexPropertyCache.entrySet()) {
            SchemaTable schemaTable = entry.getKey();
            Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> vertices = entry.getValue();
            SortedSet<String> columns = vertices.getLeft();
            Map<SqlgVertex, Map<String, Object>> rows = vertices.getRight();

            StringBuilder sql = new StringBuilder();
            sql.append("UPDATE ");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
            sql.append(".");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + schemaTable.getTable()));
            sql.append(" SET ");

            VertexLabel vertexLabel = sqlgGraph.getTopology()
                    .getSchema(schemaTable.getSchema()).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", schemaTable.getSchema())))
                    .getVertexLabel(schemaTable.getTable()).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel %s not found", schemaTable.getTable())));
            Map<String, PropertyColumn> propertyColumns = vertexLabel.getProperties();
            if (!columns.isEmpty()) {
                Map<String, PropertyType> propertyTypeMap = new HashMap<>();
                for (String column : columns) {
                    PropertyColumn propertyColumn = propertyColumns.get(column);
                    propertyTypeMap.put(column, propertyColumn.getPropertyType());
                }
                sql.append(" ");
                int i = 1;
                //noinspection Duplicates
                for (String column : columns) {
                    PropertyType propertyType = propertyTypeMap.get(column);
                    String[] sqlDefinitions = sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                    int count = 1;
                    for (@SuppressWarnings("unused") String sqlDefinition : sqlDefinitions) {
                        if (count > 1) {
                            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column + propertyType.getPostFixes()[count - 2]));
                            sql.append(" = ?");
                        } else {
                            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
                            sql.append(" = ?");
                        }
                        if (count++ < sqlDefinitions.length) {
                            sql.append(",");
                        }
                    }
                    if (i++ < columns.size()) {
                        sql.append(", ");
                    }
                }
            }
            sql.append(" WHERE ");
            if (vertexLabel.hasIDPrimaryKey()) {
                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
                sql.append(" = ?");
            } else {
                for (String identifier : vertexLabel.getIdentifiers()) {
                    sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                    sql.append(" = ?");
                }
            }
            if (sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            Connection conn = sqlgGraph.tx().getConnection();
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                for (Map.Entry<SqlgVertex, Map<String, Object>> rowEntry : rows.entrySet()) {
                    int i = 1;
                    SqlgVertex sqlgVertex = rowEntry.getKey();
                    if (!columns.isEmpty()) {
                        Map<String, Object> parameterValueMap = rowEntry.getValue();
                        List<Pair<PropertyType, Object>> typeAndValues = new ArrayList<>();
                        for (String column : columns) {
                            PropertyColumn propertyColumn = propertyColumns.get(column);
                            Object value = parameterValueMap.get(column);
                            if (value == null) {
                                //if the value is not present update it to what is currently is.
                                if (sqlgVertex.property(column).isPresent()) {
                                    value = sqlgVertex.value(column);
                                } else {
                                    value = null;
                                }
                            }
                            typeAndValues.add(Pair.of(propertyColumn.getPropertyType(), value));
                        }
                        i = SqlgUtil.setKeyValuesAsParameterUsingPropertyColumn(sqlgGraph, true, i, preparedStatement, typeAndValues);
                        RecordId recordId = ((RecordId) sqlgVertex.id());
                        if (recordId.hasSequenceId()) {
                            preparedStatement.setLong(i, ((RecordId) sqlgVertex.id()).sequenceId());
                        } else {
                            for (Comparable identifierValue : recordId.getIdentifiers()) {
                                preparedStatement.setObject(i, identifierValue);
                            }
                        }
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void flushEdgePropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>>> edgePropertyCache) {
        for (Map.Entry<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>>> entry : edgePropertyCache.entrySet()) {
            SchemaTable schemaTable = entry.getKey();
            Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>> edges = entry.getValue();
            SortedSet<String> columns = edges.getLeft();
            Map<SqlgEdge, Map<String, Object>> rows = edges.getRight();

            StringBuilder sql = new StringBuilder();
            sql.append("UPDATE ");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
            sql.append(".");
            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(EDGE_PREFIX + schemaTable.getTable()));
            sql.append(" SET ");

            EdgeLabel edgeLabel = sqlgGraph.getTopology()
                    .getSchema(schemaTable.getSchema()).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", schemaTable.getSchema())))
                    .getEdgeLabel(schemaTable.getTable()).orElseThrow(() -> new IllegalStateException(String.format("EdgeLabel %s not found", schemaTable.getTable())));
            Map<String, PropertyColumn> propertyColumns = edgeLabel.getProperties();
            if (!columns.isEmpty()) {
                Map<String, PropertyType> propertyTypeMap = new HashMap<>();
                for (String column : columns) {
                    PropertyColumn propertyColumn = propertyColumns.get(column);
                    propertyTypeMap.put(column, propertyColumn.getPropertyType());
                }
                sql.append(" ");
                int i = 1;
                //noinspection Duplicates
                for (String column : columns) {
                    PropertyType propertyType = propertyTypeMap.get(column);
                    String[] sqlDefinitions = sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
                    int count = 1;
                    for (@SuppressWarnings("unused") String sqlDefinition : sqlDefinitions) {
                        if (count > 1) {
                            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column + propertyType.getPostFixes()[count - 2]));
                            sql.append(" = ?");
                        } else {
                            sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
                            sql.append(" = ?");
                        }
                        if (count++ < sqlDefinitions.length) {
                            sql.append(",");
                        }
                    }
                    if (i++ < columns.size()) {
                        sql.append(", ");
                    }
                }
            }
            sql.append(" WHERE ");
            if (edgeLabel.hasIDPrimaryKey()) {
                sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Topology.ID));
                sql.append(" = ?");
            } else {
                for (String identifier : edgeLabel.getIdentifiers()) {
                    sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                    sql.append(" = ?");
                }
            }
            if (sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            Connection conn = sqlgGraph.tx().getConnection();
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                for (Map.Entry<SqlgEdge, Map<String, Object>> rowEntry : rows.entrySet()) {
                    int i = 1;
                    SqlgEdge sqlgEdge = rowEntry.getKey();
                    if (!columns.isEmpty()) {
                        Map<String, Object> parameterValueMap = rowEntry.getValue();
                        List<Pair<PropertyType, Object>> typeAndValues = new ArrayList<>();
                        for (String column : columns) {
                            PropertyColumn propertyColumn = propertyColumns.get(column);
                            Object value = parameterValueMap.get(column);
                            if (value == null) {
                                //if the value is not present update it to what is currently is.
                                if (sqlgEdge.property(column).isPresent()) {
                                    value = sqlgEdge.value(column);
                                } else {
                                    value = null;
                                }
                            }
                            typeAndValues.add(Pair.of(propertyColumn.getPropertyType(), value));
                        }
                        i = SqlgUtil.setKeyValuesAsParameterUsingPropertyColumn(sqlgGraph, true, i, preparedStatement, typeAndValues);
                        RecordId recordId = (RecordId) sqlgEdge.id();
                        if (recordId.hasSequenceId()) {
                            preparedStatement.setLong(i, recordId.sequenceId());
                        } else {
                            for (Comparable identifierValue : recordId.getIdentifiers()) {
                                preparedStatement.setObject(i, identifierValue);
                            }
                        }
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void flushRemovedVertices(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgVertex>> removeVertexCache) {
        if (!removeVertexCache.isEmpty()) {
            //split the list of vertices, postgres existVertexLabel a 2 byte limit in the in clause
            for (Map.Entry<SchemaTable, List<SqlgVertex>> schemaVertices : removeVertexCache.entrySet()) {
                SchemaTable schemaTable = schemaVertices.getKey();

                VertexLabel vertexLabel = sqlgGraph.getTopology().getVertexLabel(schemaTable.getSchema(), schemaTable.getTable())
                        .orElseThrow(() -> new IllegalStateException(String.format("VertexLabel not found for %s.%s", schemaTable.getSchema(), schemaTable.getTable())));

                //TODO refacor to remove looping.
                List<RecordId.ID> ids = new ArrayList<>();
                for (SqlgVertex vertex : schemaVertices.getValue()) {
                    ids.add(((RecordId) vertex.id()).getID());
                }
                Map<String, EdgeLabel> outEdgeLabels = vertexLabel.getOutEdgeLabels();
                for (Map.Entry<String, EdgeLabel> stringEdgeLabelEntry : outEdgeLabels.entrySet()) {
                    EdgeLabel outEdgeLabel = stringEdgeLabelEntry.getValue();
                    String sql = dropWithForeignKey(true, outEdgeLabel, vertexLabel, ids, false);
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql);
                    }
                    Connection conn = sqlgGraph.tx().getConnection();
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                        preparedStatement.executeUpdate();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
                Map<String, EdgeLabel> inEdgeLabels = vertexLabel.getInEdgeLabels();
                for (Map.Entry<String, EdgeLabel> stringEdgeLabelEntry : inEdgeLabels.entrySet()) {
                    EdgeLabel inEdgeLabel = stringEdgeLabelEntry.getValue();
                    String sql = dropWithForeignKey(false, inEdgeLabel, vertexLabel, ids, false);
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql);
                    }
                    Connection conn = sqlgGraph.tx().getConnection();
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                        preparedStatement.executeUpdate();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
                String sql = drop(vertexLabel, ids);
                if (logger.isDebugEnabled()) {
                    logger.debug(sql);
                }
                Connection conn = sqlgGraph.tx().getConnection();
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                    preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

//                Pair<Set<SchemaTable>, Set<SchemaTable>> tableLabels = sqlgGraph.getTopology().getTableLabels(SchemaTable.of(schemaTable.getSchema(), VERTEX_PREFIX + schemaTable.getTable()));
//                List<SqlgVertex> vertices = schemaVertices.getValue();
//                int numberOfLoops = (vertices.size() / sqlInParameterLimit());
//                int previous = 0;
//                for (int i = 1; i <= numberOfLoops + 1; i++) {
//
//                    int subListTo = i * sqlInParameterLimit();
//                    List<SqlgVertex> subVertices;
//                    if (i <= numberOfLoops) {
//                        subVertices = vertices.subList(previous, subListTo);
//                    } else {
//                        subVertices = vertices.subList(previous, vertices.size());
//                    }
//
//                    previous = subListTo;
//
//                    if (!subVertices.isEmpty()) {
//
//                        Set<SchemaTable> inLabels = tableLabels.getLeft();
//                        Set<SchemaTable> outLabels = tableLabels.getRight();
//
//                        deleteEdges(sqlgGraph, schemaTable, subVertices, inLabels, true);
//                        deleteEdges(sqlgGraph, schemaTable, subVertices, outLabels, false);
//
//                        StringBuilder sql = new StringBuilder("DELETE FROM ");
//                        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
//                        sql.append(".");
//                        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes((VERTEX_PREFIX) + schemaTable.getTable()));
//                        sql.append(" WHERE ");
//                        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
//                        sql.append(" in (");
//                        int count = 1;
//                        for (SqlgVertex sqlgVertex : subVertices) {
//                            sql.append("?");
//                            if (count++ < subVertices.size()) {
//                                sql.append(",");
//                            }
//                        }
//                        sql.append(")");
//                        if (sqlgGraph.getSqlDialect().needsSemicolon()) {
//                            sql.append(";");
//                        }
//                        if (logger.isDebugEnabled()) {
//                            logger.debug(sql);
//                        }
//                        Connection conn = sqlgGraph.tx().getConnection();
//                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
//                            preparedStatement.executeUpdate();
//                        } catch (SQLException e) {
//                            throw new RuntimeException(e);
//                        }
//                    }
//                }
            }
        }
    }

    @Override
    public void flushRemovedEdges(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgEdge>> removeEdgeCache) {

        if (!removeEdgeCache.isEmpty()) {

            //split the list of edges, postgres existVertexLabel a 2 byte limit in the in clause
            for (Map.Entry<SchemaTable, List<SqlgEdge>> schemaEdges : removeEdgeCache.entrySet()) {
                SchemaTable schemaTable = schemaEdges.getKey();
                EdgeLabel edgeLabel = sqlgGraph.getTopology().getEdgeLabel(schemaTable.getSchema(), schemaTable.getTable())
                        .orElseThrow(() -> new IllegalStateException(String.format("EdgeLabel not found for %s.%s", schemaTable.getSchema(), schemaTable.getTable())));

                //TODO refacor to remove looping.
                List<RecordId.ID> ids = new ArrayList<>();
                for (SqlgEdge edge : schemaEdges.getValue()) {
                    ids.add(((RecordId) edge.id()).getID());
                }
                String sql = drop(edgeLabel, ids);
                if (logger.isDebugEnabled()) {
                    logger.debug(sql);
                }
                Connection conn = sqlgGraph.tx().getConnection();
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                    preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            }
        }
    }

    @Override
    public int sqlInParameterLimit() {
        return 1000;
    }

    @Override
    public List<Triple<String, Integer, String>> getTableColumns(DatabaseMetaData metaData, String catalog, String schemaPattern,
                                                                 String tableNamePattern, String columnNamePattern) {

        List<Triple<String, Integer, String>> columns = new ArrayList<>();
        try (ResultSet rs = metaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern)) {
            while (rs.next()) {
                String columnName = rs.getString(4);
                int columnType = rs.getInt(5);
                String typeName = rs.getString("TYPE_NAME");
                columns.add(Triple.of(columnName, columnType, typeName));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return columns;
    }

    @Override
    public List<String> getPrimaryKeys(DatabaseMetaData metaData, String catalog, String schemaPattern, String tableNamePattern) {
        List<String> primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(catalog, schemaPattern, tableNamePattern)) {
            while (rs.next()) {
                String columnName = rs.getString(4);
                int index = rs.getShort(5);
                primaryKeys.add(index - 1, columnName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return primaryKeys;
    }

    @Override
    public List<Triple<String, Boolean, String>> getIndexInfo(DatabaseMetaData metaData, String catalog,
                                                              String schema, String table, boolean unique, boolean approximate) {
        List<Triple<String, Boolean, String>> indexes = new ArrayList<>();
        try (ResultSet indexRs = metaData.getIndexInfo(null, schema, table, false, true)) {
            while (indexRs.next()) {
                String indexName = indexRs.getString("INDEX_NAME");
                boolean nonUnique = indexRs.getBoolean("NON_UNIQUE");
                String columnName = indexRs.getString("COLUMN_NAME");
                indexes.add(Triple.of(indexName, nonUnique, columnName));
            }
            return indexes;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setJson(PreparedStatement preparedStatement, int parameterStartIndex, JsonNode right) {
        try {
            preparedStatement.setString(parameterStartIndex, right.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handleOther(Map<String, Object> properties, String columnName, Object o, PropertyType propertyType) {
        switch (propertyType.ordinal()) {
            case JSON_ORDINAL:
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    JsonNode jsonNode = objectMapper.readTree(o.toString());
                    properties.put(columnName, jsonNode);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                break;
            default:
                throw new IllegalStateException("sqlgDialect.handleOther does not handle " + propertyType.name());
        }
    }

    /**
     * escape quotes by doubling them when we need a string inside quotes
     *
     * @param o
     * @return
     */
    protected String escapeQuotes(Object o) {
        if (o != null) {
            return o.toString().replace("'", "''");
        }
        return null;
    }
}
