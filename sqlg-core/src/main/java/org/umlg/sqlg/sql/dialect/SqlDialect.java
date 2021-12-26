package org.umlg.sqlg.sql.dialect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.umlg.sqlg.predicate.FullText;
import org.umlg.sqlg.sql.parse.ColumnList;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.strategy.SqlgSqlExecutor;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.*;

import javax.annotation.Nullable;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.umlg.sqlg.structure.PropertyType.*;

public interface SqlDialect {


    String INDEX_POSTFIX = "_sqlgIdx";

    default boolean supportsDistribution() {
        return false;
    }

    String dialectName();

    Set<String> getInternalSchemas();

    PropertyType sqlTypeToPropertyType(SqlgGraph sqlgGraph, String schema, String table, String column, int sqlType, String typeName, ListIterator<Triple<String, Integer, String>> metaDataIter);

    /**
     * "TYPE_NAME" is column meta data returned by the jdbc driver.
     * This method returns the TYPE_NAME for the sql {@link Types} constant.
     * This method is only called for array types.
     *
     * @return the TYPE_NAME for the given Types constant.
     */
    PropertyType sqlArrayTypeNameToPropertyType(String typeName, SqlgGraph sqlgGraph, String schema, String table, String columnName, ListIterator<Triple<String, Integer, String>> metaDataIter);

    void validateProperty(Object key, Object value);

    default boolean needsSemicolon() {
        return true;
    }

    default boolean supportsCascade() {
        return true;
    }

    default boolean supportsIfExists() {
        return true;
    }

    default boolean needsSchemaDropCascade() {
        return supportsCascade();
    }

    String getColumnEscapeKey();

    String getPrimaryKeyType();

    String getAutoIncrementPrimaryKeyConstruct();

    default String getAutoIncrement() {
        throw new RuntimeException("Not yet implemented.");
    }

    String[] propertyTypeToSqlDefinition(PropertyType propertyType);

    int[] propertyTypeToJavaSqlType(PropertyType propertyType);

    String getForeignKeyTypeDefinition();

    default String maybeWrapInQoutes(String field) {
        return getColumnEscapeKey() + field.replace(getColumnEscapeKey(), "\"" + getColumnEscapeKey()) + getColumnEscapeKey();
    }

    default boolean supportsFloatValues() {
        return true;
    }

    default boolean supportsByteValues() {
        return false;
    }

    default boolean supportsTransactionalSchema() {
        return true;
    }

    default boolean supportsBooleanArrayValues() {
        return true;
    }

    default boolean supportsByteArrayValues() {
        return true;
    }

    default boolean supportsDoubleArrayValues() {
        return true;
    }

    default boolean supportsFloatArrayValues() {
        return true;
    }

    default boolean supportsIntegerArrayValues() {
        return true;
    }

    default boolean supportsShortArrayValues() {
        return true;
    }

    default boolean supportsLongArrayValues() {
        return true;
    }

    default boolean supportsStringArrayValues() {
        return true;
    }

    default boolean supportsZonedDateTimeArrayValues() {
        return true;
    }

    default boolean supportsLocalTimeArrayValues() {
        return true;
    }

    default boolean supportsLocalDateArrayValues() {
        return true;
    }

    default boolean supportsLocalDateTimeArrayValues() {
        return true;
    }

    default boolean supportsPeriodArrayValues() {
        return true;
    }

    default boolean supportsJsonArrayValues() {
        return false;
    }

    default boolean supportsUUID() {
        return true;
    }

    default boolean supportsDurationArrayValues() {
        return true;
    }

    default void assertTableName(String tableName) {
    }

    default void putJsonObject(ObjectNode obj, String columnName, int sqlType, Object o) {
        try {
            switch (sqlType) {
                case Types.BIT:
                    obj.put(columnName, (Boolean) o);
                    break;
                case Types.SMALLINT:
                    Short v = o instanceof Short ? (Short) o : ((Integer) o).shortValue();
                    obj.put(columnName, v);
                    break;
                case Types.INTEGER:
                    obj.put(columnName, (Integer) o);
                    break;
                case Types.BIGINT:
                    obj.put(columnName, (Long) o);
                    break;
                case Types.REAL:
                    obj.put(columnName, (Float) o);
                    break;
                case Types.DOUBLE:
                    obj.put(columnName, (Double) o);
                    break;
                case Types.VARCHAR:
                    obj.put(columnName, (String) o);
                    break;
                case Types.ARRAY:
                    ArrayNode arrayNode = obj.putArray(columnName);
                    Array array = (Array) o;
                    int baseType = array.getBaseType();
                    Object[] objectArray = (Object[]) array.getArray();
                    switch (baseType) {
                        case Types.BIT:
                            for (Object arrayElement : objectArray) {
                                arrayNode.add((Boolean) arrayElement);
                            }
                            break;
                        case Types.SMALLINT:
                            for (Object arrayElement : objectArray) {
                                arrayNode.add((Short) arrayElement);
                            }
                            break;
                        case Types.INTEGER:
                            for (Object arrayElement : objectArray) {
                                arrayNode.add((Integer) arrayElement);
                            }
                            break;
                        case Types.BIGINT:
                            for (Object arrayElement : objectArray) {
                                arrayNode.add((Long) arrayElement);
                            }
                            break;
                        case Types.REAL:
                            for (Object arrayElement : objectArray) {
                                arrayNode.add((Float) arrayElement);
                            }
                            break;
                        case Types.DOUBLE:
                            for (Object arrayElement : objectArray) {
                                arrayNode.add((Double) arrayElement);
                            }
                            break;
                        case Types.VARCHAR:
                            for (Object arrayElement : objectArray) {
                                arrayNode.add((String) arrayElement);
                            }
                            break;
                        default:
                            throw new IllegalStateException("Unknown array sqlType " + sqlType);
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown sqlType " + sqlType);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    default void putJsonMetaObject(ObjectMapper mapper, ArrayNode metaNodeArray, String columnName, int sqlType, Object o) {
        try {
            ObjectNode metaNode = mapper.createObjectNode();
            metaNode.put("name", columnName);
            metaNodeArray.add(metaNode);
            switch (sqlType) {
                case Types.BIT:
                    metaNode.put("type", BOOLEAN.name());
                    break;
                case Types.SMALLINT:
                    metaNode.put("type", PropertyType.SHORT.name());
                    break;
                case Types.INTEGER:
                    metaNode.put("type", PropertyType.INTEGER.name());
                    break;
                case Types.BIGINT:
                    metaNode.put("type", PropertyType.LONG.name());
                    break;
                case Types.REAL:
                    metaNode.put("type", PropertyType.FLOAT.name());
                    break;
                case Types.DOUBLE:
                    metaNode.put("type", PropertyType.DOUBLE.name());
                    break;
                case Types.VARCHAR:
                    metaNode.put("type", PropertyType.STRING.name());
                    break;
                case Types.ARRAY:
                    Array array = (Array) o;
                    int baseType = array.getBaseType();
                    switch (baseType) {
                        case Types.BIT:
                            metaNode.put("type", PropertyType.boolean_ARRAY.name());
                            break;
                        case Types.SMALLINT:
                            metaNode.put("type", PropertyType.short_ARRAY.name());
                            break;
                        case Types.INTEGER:
                            metaNode.put("type", PropertyType.int_ARRAY.name());
                            break;
                        case Types.BIGINT:
                            metaNode.put("type", PropertyType.long_ARRAY.name());
                            break;
                        case Types.REAL:
                            metaNode.put("type", PropertyType.float_ARRAY.name());
                            break;
                        case Types.DOUBLE:
                            metaNode.put("type", PropertyType.double_ARRAY.name());
                            break;
                        case Types.VARCHAR:
                            metaNode.put("type", PropertyType.STRING_ARRAY.name());
                            break;
                        default:
                            throw new IllegalStateException("Unknown array sqlType " + sqlType);
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown sqlType " + sqlType);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    String getArrayDriverType(PropertyType booleanArray);

    default String createTableStatement() {
        return "CREATE TABLE ";
    }

    default String createTemporaryTableStatement() {
        return "CREATE TEMPORARY TABLE ";
    }

    /**
     * @return the statement head to create a schema
     */
    default String createSchemaStatement(String schemaName) {
        return "CREATE SCHEMA " + maybeWrapInQoutes(schemaName);
    }

    /**
     * Builds an add column statement.
     *
     * @param schema         schema name
     * @param table          table name
     * @param column         new column name
     * @param typeDefinition column definition
     * @return the statement to add the column
     */
    default String addColumnStatement(String schema, String table, String column, String typeDefinition) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ");
        sql.append(maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(maybeWrapInQoutes(table));
        sql.append(" ADD COLUMN ");
        sql.append(maybeWrapInQoutes(column));
        sql.append(" ");
        sql.append(typeDefinition);
        if (needsSemicolon()) {
            sql.append(";");
        }
        return sql.toString();
    }

    /**
     * @return the statement head to drop a schema
     */
    default String dropSchemaStatement(String schema) {
        return "DROP SCHEMA IF EXISTS " + maybeWrapInQoutes(schema) +
                (supportsCascade() ? " CASCADE" : "") +
                (needsSemicolon() ? ";" : "");
    }

    default void prepareDB(Connection conn) {
    }

    /**
     * A getter to return the "public" schema for the database. For postgresql it is "public" and for HSQLDB it is "PUBLIC"
     *
     * @return the database's public schema.
     */
    default String getPublicSchema() {
        return "public";
    }

    default boolean requiresIndexName() {
        return false;
    }

    default String indexName(SchemaTable schemaTable, String prefix, List<String> columns) {
        return indexName(schemaTable, prefix, INDEX_POSTFIX, columns);
    }

    default String indexName(SchemaTable schemaTable, String prefix, String postfix, List<String> columns) {
        Preconditions.checkState(!columns.isEmpty(), "SqlDialect.indexName may not be called with an empty list of columns");
        String sb = schemaTable.getSchema() +
                "_" +
                prefix +
                schemaTable.getTable() +
                "_" +
                //noinspection OptionalGetWithoutIsPresent
                columns.stream().reduce((a, b) -> a + "_" + b).orElseThrow() +
                postfix;
        return sb;
    }


    /**
     * This indicates whether a unique index considers mull values as equal or not.
     * Mssql server is the only db so far that considers nulls equals.
     *
     * @return true is multiple null values are equal and thus not allowed.
     */
    default boolean uniqueIndexConsidersNullValuesEqual() {
        return false;
    }

    String existIndexQuery(SchemaTable schemaTable, String prefix, String indexName);

    //This is needed for mariadb, which does not support schemas, so need to drop the database instead
    default boolean supportsSchemas() {
        return true;
    }

    default boolean supportsBatchMode() {
        return false;
    }

    /**
     * This is primarily for Postgresql's copy command.
     *
     * @return true if data can be streamed in via a socket.
     */
    default boolean supportsStreamingBatchMode() {
        return false;
    }

    default boolean supportsJsonType() {
        return false;
    }

    default String hasContainerKeyToColumn(String key) {
        if (key.equals(T.id.getAccessor()))
            return "ID";
        else
            return key;
    }

    default boolean needForeignKeyIndex() {
        return false;
    }

    default boolean supportsClientInfo() {
        return false;
    }

    default void validateSchemaName(String schema) {
    }

    default void validateTableName(String table) {
    }

    default void validateColumnName(String column) {
    }

    default int getMaximumSchemaNameLength() {
        return Integer.MAX_VALUE;
    }

    default int getMaximumTableNameLength() {
        return Integer.MAX_VALUE;
    }

    default int getMaximumColumnNameLength() {
        return Integer.MAX_VALUE;
    }

    default int getMaximumIndexNameLength() {
        return Integer.MAX_VALUE;
    }

    default boolean supportsILike() {
        return Boolean.FALSE;
    }

    default boolean needsTimeZone() {
        return Boolean.FALSE;
    }

    Set<String> getSpacialRefTable();

    List<String> getGisSchemas();

    void setJson(PreparedStatement preparedStatement, int parameterStartIndex, JsonNode right);

    void handleOther(Map<String, Object> properties, String columnName, Object o, PropertyType propertyType);

    default void setPoint(PreparedStatement preparedStatement, int parameterStartIndex, Object point) {
        throw SqlgExceptions.gisNotSupportedException(PropertyType.POINT);
    }

    default void setLineString(PreparedStatement preparedStatement, int parameterStartIndex, Object lineString) {
        throw SqlgExceptions.gisNotSupportedException(PropertyType.LINESTRING);
    }

    default void setPolygon(PreparedStatement preparedStatement, int parameterStartIndex, Object point) {
        throw SqlgExceptions.gisNotSupportedException(PropertyType.POLYGON);
    }

    default void setGeographyPoint(PreparedStatement preparedStatement, int parameterStartIndex, Object point) {
        throw SqlgExceptions.gisNotSupportedException(PropertyType.GEOGRAPHY_POINT);
    }

    default boolean isPostgresql() {
        return false;
    }

    default boolean isMariaDb() {
        return false;
    }

    default boolean isMysql() {
        return false;
    }

    default boolean isMssqlServer() {
        return false;
    }

    default boolean isHsqldb() {
        return false;
    }

    default boolean isH2() {
        return false;
    }

    default <T> T getGis(SqlgGraph sqlgGraph) {
        throw SqlgExceptions.gisNotSupportedException();
    }

    void lockTable(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix);

    void alterSequenceCacheSize(SqlgGraph sqlgGraph, SchemaTable schemaTable, String sequence, int batchSize);

    long nextSequenceVal(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix);

    long currSequenceVal(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix);

    String sequenceName(SqlgGraph sqlgGraph, SchemaTable outSchemaTable, String prefix);

    boolean supportsBulkWithinOut();

    String afterCreateTemporaryTableStatement();

    /**
     * For Postgresql/Hsqldb and H2 temporary tables have no schema.
     * For Mariadb the schema/database must be specified.
     *
     * @return true is a schema/database must be specified.
     */
    default boolean needsTemporaryTableSchema() {
        return false;
    }

    /**
     * Mssql server identifies temporary table by prepending it wirh a '#'
     *
     * @return true if a prefix is needed.
     */
    default boolean needsTemporaryTablePrefix() {
        return false;
    }

    /**
     * Mssql server's # prefix for temporary tables.
     *
     * @return The prefix.
     */
    default String temporaryTablePrefix() {
        Preconditions.checkState(!needsTemporaryTablePrefix());
        return "";
    }

    /**
     * MariaDb does not drop the temporary table after a commit. It only drops it when the session ends.
     * Sqlg will manually drop the temporary table for Mariadb as we need the same semantics across all dialects.
     *
     * @return true if temporary tables are dropped on commit.
     */
    default boolean supportsTemporaryTableOnCommitDrop() {
        return true;
    }

    /**
     * These are internal columns used by sqlg that must be ignored when loading elements.
     * eg. '_copy_dummy' when doing using the copy command on postgresql.
     *
     * @return The columns to ignore.
     */
    default List<String> columnsToIgnore() {
        return Collections.emptyList();
    }

    default String sqlgSqlgSchemaCreationScript() {
        return this.createSchemaStatement(Schema.SQLG_SCHEMA) + (needsSemicolon() ? ";" : "");
    }

    List<String> sqlgTopologyCreationScripts();

    String sqlgAddIndexEdgeSequenceColumn();

    default Long getPrimaryKeyStartValue() {
        return 1L;
    }

    Object convertArray(PropertyType propertyType, Array array) throws SQLException;

    void setArray(PreparedStatement statement, int index, PropertyType type, Object[] values) throws SQLException;

    /**
     * range condition
     *
     * @param r range
     * @return the range clause.
     */
    default String getRangeClause(Range<Long> r) {
        return "LIMIT " + (r.getMaximum() - r.getMinimum()) + " OFFSET " + r.getMinimum();
    }

    default String getSkipClause(long skip) {
        return " OFFSET " + skip;
    }

    /**
     * get the full text query for the given predicate and column
     *
     * @param fullText
     * @param column
     * @return
     */
    default String getFullTextQueryText(FullText fullText, String column) {
        throw new UnsupportedOperationException("FullText search is not supported on this database");
    }

    default String getArrayContainsQueryText(String column) {
        throw new UnsupportedOperationException("Array Contains is not supported on this database");
    }

    default String getArrayOverlapsQueryText(String column) {
        throw new UnsupportedOperationException("Array Overlaps is not supported on this database");
    }

    default boolean schemaExists(DatabaseMetaData metadata, String schema) throws SQLException {
//        ResultSet schemas = metadata.getSchemas();
//        while (schemas.next()) {
//            System.out.println(schemas.getString(1));
//        }
        ResultSet schemaRs = metadata.getSchemas(null, schema);
        return schemaRs.next();
    }

    /**
     * Returns all schemas. For some RDBMSes, like Cockroachdb and MariaDb, this is the database/catalog.
     *
     * @return The list of schema names.
     */
    List<String> getSchemaNames(DatabaseMetaData metaData);

    /**
     * Get all the Vertex tables. i.e. all tables starting with 'V_'
     *
     * @param metaData JDBC meta data.
     * @return A triple holding the catalog, schema and table.
     */
    List<Triple<String, String, String>> getVertexTables(DatabaseMetaData metaData);

    /**
     * Get all the Edge tables. i.e. all tables starting with 'E_'
     *
     * @param metaData JDBC meta data.
     * @return A triple holding the catalog, thea schema and the table.
     */
    List<Triple<String, String, String>> getEdgeTables(DatabaseMetaData metaData);

    /**
     * Get the columns for a table.
     *
     * @param metaData JDBC meta data.
     * @return The columns.
     */
    List<Triple<String, Integer, String>> getTableColumns(DatabaseMetaData metaData, String catalog, String schemaPattern,
                                                          String tableNamePattern, String columnNamePattern);

    /**
     * Return the table's primary keys.
     *
     * @param metaData         JDBC meta data.
     * @param catalog          The db catalog.
     * @param schemaPattern    The schema name.
     * @param tableNamePattern The table name.
     * @return A list of primary key column names.
     */
    List<String> getPrimaryKeys(DatabaseMetaData metaData, String catalog, String schemaPattern, String tableNamePattern);

    List<Triple<String, Boolean, String>> getIndexInfo(DatabaseMetaData metaData, String catalog,
                                                       String schema, String table, boolean unique, boolean approximate);

    /**
     * extract all indices in one go
     *
     * @param conn
     * @param catalog
     * @param schema
     * @return a map of indices references by key, the key being cat+schema+table
     * @throws SQLException
     */
    default Map<String, Set<IndexRef>> extractIndices(Connection conn, String catalog, String schema) throws SQLException {
        return null;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    boolean isSystemIndex(String indexName);

    /**
     * This is needed for H2 that does not support the standard <code>select * from values((1,1),(2,2)) as tmp("field1", "field2")</code>
     * Instead the columns are hardcoded as "C1", "C2"
     *
     * @return true if the valueExpression is similar to Postgresql. i.e. <code>select * from values((1,1),(2,2)) as tmp("field1", "field2")</code>
     * H2 returns false and has some special code for it.
     */
    default boolean supportsFullValueExpression() {
        return true;
    }

    /**
     * Indicates if the rdbms supports 'VALUES (x,y)" table expressions.
     * This is needed because Mariadb does not.
     *
     * @return true is 'VALUES' expression is supported else false.
     */
    default boolean supportsValuesExpression() {
        return true;
    }

    /**
     * This is needed for Cockroachdb where the index needs to be specified as a part of the 'CREATE TABLE' statement.
     *
     * @return true if the indices must be specified together with the 'CREATE TABLE' sql, else false.
     */
    default boolean isIndexPartOfCreateTable() {
        return false;
    }


    default String sqlInsertEmptyValues() {
        return " DEFAULT VALUES";
    }

    /**
     * MariaDb can not index the LONGTEXT type. It needs to know how many characters to index.
     *
     * @return Return true is the number of characters to index needs to be specified.
     */
    default boolean requiresIndexLengthLimit() {
        return false;
    }

    /**
     * Convert a value to insert into the db so that it can be used in a 'values' sql clause.
     *
     * @param propertyType The type of the property.
     * @param value        The value of the property.
     * @return The value that can be used in a sql 'from' clause.
     */
    String valueToValuesString(PropertyType propertyType, Object value);

    /**
     * An easy way to see if a dialect supports the given type of not.
     *
     * @param propertyType A {@link PropertyType} representing the type of the property.
     * @return true if the PropertyType is supported else false.
     */
    boolean supportsType(PropertyType propertyType);

    /**
     * Returns the number of parameters that can be passed into a sql 'IN' statement.
     *
     * @return
     */
    int sqlInParameterLimit();

    /**
     * This is for Cockroachdb that only allows partial transactional schema creation.
     * It to create schema elements if the transtion has already been written to.
     *
     * @return false if there is no need to force a commit before schema creation.
     */
    default boolean needsSchemaCreationPrecommit() {
        return false;
    }


    /**
     * If true it means a labels (tables) can be created in existing schemas.
     *
     * @return true if 'CREATE SCHEMA IF NOT EXISTS' works.
     */
    default boolean supportsSchemaIfNotExists() {
        return false;
    }

    String sqlgCreateTopologyGraph();

    /**
     * if the query traverses edges then the deletion logic is non trivial.
     * The edges can not be deleted upfront as then we will not be able to travers to the leaf vertices anymore
     * because the edges are no longer there to travers. In this case we need to drop foreign key constraint checking.
     * Delete the vertices and then the edges using the same query.
     * The edge query is the same as the vertex query with the last SchemaTableTree removed from the distinctQueryStack;
     *
     * @param sqlgGraph            The graph.
     * @param leafElementsToDelete The leaf elements of the query. eg. g.V().out().out() The last vertices returned by the gremlin query.
     * @param edgesToDelete
     * @param distinctQueryStack   The query's SchemaTableTree stack as constructed by parsing.
     * @return
     */
    @SuppressWarnings("Duplicates")
    default List<Triple<SqlgSqlExecutor.DROP_QUERY, String, Boolean>> drop(SqlgGraph sqlgGraph, String leafElementsToDelete, @Nullable String edgesToDelete, LinkedList<SchemaTableTree> distinctQueryStack) {

        List<Triple<SqlgSqlExecutor.DROP_QUERY, String, Boolean>> sqls = new ArrayList<>();
        SchemaTableTree last = distinctQueryStack.getLast();

        SchemaTableTree lastEdge = null;
        //if the leaf elements are vertices then we need to delete its in and out edges.
        boolean isVertex = last.getSchemaTable().isVertexTable();
        VertexLabel lastVertexLabel = null;
        if (isVertex) {
            Optional<Schema> schemaOptional = sqlgGraph.getTopology().getSchema(last.getSchemaTable().getSchema());
            Preconditions.checkState(schemaOptional.isPresent(), "BUG: %s not found in the topology.", last.getSchemaTable().getSchema());
            Schema schema = schemaOptional.get();
            Optional<VertexLabel> vertexLabelOptional = schema.getVertexLabel(last.getSchemaTable().withOutPrefix().getTable());
            Preconditions.checkState(vertexLabelOptional.isPresent(), "BUG: %s not found in the topology.", last.getSchemaTable().withOutPrefix().getTable());
            lastVertexLabel = vertexLabelOptional.get();
        }
        boolean queryTraversesEdge = isVertex && (distinctQueryStack.size() > 1);
        EdgeLabel lastEdgeLabel = null;
        if (queryTraversesEdge) {
            lastEdge = distinctQueryStack.get(distinctQueryStack.size() - 2);
            Optional<Schema> edgeSchema = sqlgGraph.getTopology().getSchema(lastEdge.getSchemaTable().getSchema());
            Preconditions.checkState(edgeSchema.isPresent(), "BUG: %s not found in the topology.", lastEdge.getSchemaTable().getSchema());
            Optional<EdgeLabel> edgeLabelOptional = edgeSchema.get().getEdgeLabel(lastEdge.getSchemaTable().withOutPrefix().getTable());
            Preconditions.checkState(edgeLabelOptional.isPresent(), "BUG: %s not found in the topology.", lastEdge.getSchemaTable().getTable());
            lastEdgeLabel = edgeLabelOptional.get();
        }

        if (isVertex) {
            //First delete all edges except for this edge traversed to get to the vertices.
            StringBuilder sb;
            for (Map.Entry<String, EdgeLabel> edgeLabelEntry : lastVertexLabel.getOutEdgeLabels().entrySet()) {
                EdgeLabel edgeLabel = edgeLabelEntry.getValue();
                if (!edgeLabel.equals(lastEdgeLabel)) {
                    //Delete
                    sb = new StringBuilder();
                    sb.append("DELETE FROM ");
                    sb.append(maybeWrapInQoutes(edgeLabel.getSchema().getName()));
                    sb.append(".");
                    sb.append(maybeWrapInQoutes(Topology.EDGE_PREFIX + edgeLabel.getName()));
                    sb.append("\nWHERE ");
                    if (lastVertexLabel.hasIDPrimaryKey()) {
                        sb.append(maybeWrapInQoutes(lastVertexLabel.getSchema().getName() + "." + lastVertexLabel.getName() + Topology.OUT_VERTEX_COLUMN_END));
                    } else {
                        int count = 1;
                        sb.append("(");
                        for (String identifier : lastVertexLabel.getIdentifiers()) {
                            sb.append(maybeWrapInQoutes(lastVertexLabel.getSchema().getName() + "." + lastVertexLabel.getName() + "." + identifier + Topology.OUT_VERTEX_COLUMN_END));
                            if (count++ < lastVertexLabel.getIdentifiers().size()) {
                                sb.append(", ");
                            }
                        }
                        sb.append(")");
                    }
                    sb.append(" IN\n\t(");
                    sb.append(leafElementsToDelete);
                    sb.append(")");
                    sqls.add(Triple.of(SqlgSqlExecutor.DROP_QUERY.NORMAL, sb.toString(), false));
                }
            }
            for (Map.Entry<String, EdgeLabel> edgeLabelEntry : lastVertexLabel.getInEdgeLabels().entrySet()) {
                EdgeLabel edgeLabel = edgeLabelEntry.getValue();
                if (!edgeLabel.equals(lastEdgeLabel)) {
                    //Delete
                    sb = new StringBuilder();
                    sb.append("DELETE FROM ");
                    sb.append(maybeWrapInQoutes(edgeLabel.getSchema().getName()));
                    sb.append(".");
                    sb.append(maybeWrapInQoutes(Topology.EDGE_PREFIX + edgeLabel.getName()));
                    sb.append("\nWHERE ");
                    if (lastVertexLabel.hasIDPrimaryKey()) {
                        sb.append(maybeWrapInQoutes(lastVertexLabel.getSchema().getName() + "." + lastVertexLabel.getName() + Topology.IN_VERTEX_COLUMN_END));
                    } else {
                        sb.append("(");
                        int count = 1;
                        for (String identifier : lastVertexLabel.getIdentifiers()) {
                            sb.append(maybeWrapInQoutes(lastVertexLabel.getSchema().getName() + "." + lastVertexLabel.getName() + "." + identifier + Topology.IN_VERTEX_COLUMN_END));
                            if (count++ < lastVertexLabel.getIdentifiers().size()) {
                                sb.append(", ");
                            }
                        }
                        sb.append(")");
                    }
                    sb.append(" IN\n\t(");
                    sb.append(leafElementsToDelete);
                    sb.append(")");
                    sqls.add(Triple.of(SqlgSqlExecutor.DROP_QUERY.NORMAL, sb.toString(), false));
                }
            }
        }

        //Need to defer foreign key constraint checks.
        if (queryTraversesEdge) {
            String edgeTableName = (maybeWrapInQoutes(lastEdge.getSchemaTable().getSchema())) + "." + maybeWrapInQoutes(lastEdge.getSchemaTable().getTable());
            sqls.add(Triple.of(SqlgSqlExecutor.DROP_QUERY.ALTER, this.sqlToTurnOffReferentialConstraintCheck(edgeTableName), false));
        }
        //Delete the leaf vertices, if there are foreign keys then its been deferred.
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ");
        sb.append(maybeWrapInQoutes(last.getSchemaTable().getSchema()));
        sb.append(".");
        sb.append(maybeWrapInQoutes(last.getSchemaTable().getTable()));
        if (last.isHasIDPrimaryKey()) {
            sb.append("\nWHERE \"ID\" IN (\n\t");
        } else {
            sb.append("\nWHERE (");
            int count = 1;
            for (String identifier : last.getIdentifiers()) {
                sb.append(maybeWrapInQoutes(identifier));
                if (count++ < last.getIdentifiers().size()) {
                    sb.append(", ");
                }
            }
            sb.append(") IN (\n\t");
        }
        sb.append(leafElementsToDelete);
        sb.append(")");
        sqls.add(Triple.of(SqlgSqlExecutor.DROP_QUERY.NORMAL, sb.toString(), false));

        if (queryTraversesEdge) {
            sb = new StringBuilder();
            sb.append("DELETE FROM ");
            sb.append(maybeWrapInQoutes(lastEdge.getSchemaTable().getSchema()));
            sb.append(".");
            sb.append(maybeWrapInQoutes(lastEdge.getSchemaTable().getTable()));
            if (lastEdge.isHasIDPrimaryKey()) {
                sb.append("\nWHERE \"ID\" IN (\n\t");
            } else {
                sb.append("\nWHERE (");
                int count = 1;
                for (String identifier : lastEdge.getIdentifiers()) {
                    sb.append(maybeWrapInQoutes(identifier));
                    if (count++ < lastEdge.getIdentifiers().size()) {
                        sb.append(", ");
                    }
                }
                sb.append(") IN (\n\t");
            }
            sb.append(edgesToDelete);
            sb.append(")");
            sqls.add(Triple.of(SqlgSqlExecutor.DROP_QUERY.EDGE, sb.toString(), false));
        }
        //Enable the foreign key constraint
        if (queryTraversesEdge) {
            String edgeTableName = (maybeWrapInQoutes(lastEdge.getSchemaTable().getSchema())) + "." + maybeWrapInQoutes(lastEdge.getSchemaTable().getTable());
            sqls.add(Triple.of(SqlgSqlExecutor.DROP_QUERY.ALTER, this.sqlToTurnOnReferentialConstraintCheck(edgeTableName), false));
        }
        return sqls;
    }


    default String drop(VertexLabel vertexLabel, Collection<RecordId.ID> ids) {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM\n\t");
        sql.append(maybeWrapInQoutes(vertexLabel.getSchema().getName()));
        sql.append(".");
        sql.append(maybeWrapInQoutes(Topology.VERTEX_PREFIX + vertexLabel.getName()));
        sql.append(" WHERE ");
        if (vertexLabel.hasIDPrimaryKey()) {
            sql.append(maybeWrapInQoutes("ID"));
        } else {
            int cnt = 1;
            sql.append("(");
            for (String identifier : vertexLabel.getIdentifiers()) {
                sql.append(maybeWrapInQoutes(identifier));
                if (cnt++ < vertexLabel.getIdentifiers().size()) {
                    sql.append(",");
                }
            }
            sql.append(")");
        }
        sql.append(" IN (\n");
        int count = 1;
        for (RecordId.ID id : ids) {
            if (vertexLabel.hasIDPrimaryKey()) {
                sql.append(id.getSequenceId());
                if (count++ < ids.size()) {
                    sql.append(",");
                }
            } else {
                int cnt = 1;
                sql.append("(");
                for (Comparable identifierValue : id.getIdentifiers()) {
                    sql.append(toRDBSStringLiteral(identifierValue));
                    if (cnt++ < id.getIdentifiers().size()) {
                        sql.append(",");
                    }
                }
                sql.append(")");
                if (count++ < ids.size()) {
                    sql.append(",");
                }
            }
        }
        sql.append(")");

        return sql.toString();
    }

    default String drop(EdgeLabel edgeLabel, Collection<RecordId.ID> ids) {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM\n\t");
        sql.append(maybeWrapInQoutes(edgeLabel.getSchema().getName()));
        sql.append(".");
        sql.append(maybeWrapInQoutes(Topology.EDGE_PREFIX + edgeLabel.getName()));
        sql.append(" WHERE ");
        if (edgeLabel.hasIDPrimaryKey()) {
            sql.append(maybeWrapInQoutes("ID"));
        } else {
            int cnt = 1;
            sql.append("(");
            for (String identifier : edgeLabel.getIdentifiers()) {
                sql.append(maybeWrapInQoutes(identifier));
                if (cnt++ < edgeLabel.getIdentifiers().size()) {
                    sql.append(",");
                }
            }
            sql.append(")");
        }
        sql.append(" IN (\n");
        int count = 1;
        for (RecordId.ID id : ids) {
            if (edgeLabel.hasIDPrimaryKey()) {
                sql.append(id.getSequenceId());
                if (count++ < ids.size()) {
                    sql.append(",");
                }
            } else {
                int cnt = 1;
                sql.append("(");
                for (Comparable identifierValue : id.getIdentifiers()) {
                    sql.append(toRDBSStringLiteral(identifierValue));
                    if (cnt++ < id.getIdentifiers().size()) {
                        sql.append(",");
                    }
                }
                sql.append(")");
                if (count++ < ids.size()) {
                    sql.append(",");
                }
            }
        }
        sql.append(")");
        return sql.toString();
    }

    default String dropWithForeignKey(boolean out, EdgeLabel edgeLabel, VertexLabel vertexLabel, Collection<RecordId.ID> ids, boolean mutatingCallbacks) {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM\n\t");
        sql.append(maybeWrapInQoutes(edgeLabel.getSchema().getName()));
        sql.append(".");
        sql.append(maybeWrapInQoutes(Topology.EDGE_PREFIX + edgeLabel.getName()));
        sql.append(" WHERE ");
        if (vertexLabel.hasIDPrimaryKey()) {
            sql.append(maybeWrapInQoutes(
                    vertexLabel.getSchema().getName() + "." + vertexLabel.getName()
                            + (out ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END)));
        } else {
            sql.append("(");
            int count = 1;
            for (String identifier : vertexLabel.getIdentifiers()) {
                sql.append(maybeWrapInQoutes(
                        vertexLabel.getSchema().getName() + "." + vertexLabel.getName() + "." + identifier
                                + (out ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END)));
                if (count++ < vertexLabel.getIdentifiers().size()) {
                    sql.append(",");
                }
            }
            sql.append(")");

        }
        sql.append(" IN (\n");
        int count = 1;
        for (RecordId.ID id : ids) {
            if (vertexLabel.hasIDPrimaryKey()) {
                sql.append(id.getSequenceId());
            } else {
                int cnt = 1;
                sql.append("(");
                for (Comparable identifierValue : id.getIdentifiers()) {
                    sql.append(toRDBSStringLiteral(identifierValue));
                    if (cnt++ < id.getIdentifiers().size()) {
                        sql.append(",");
                    }
                }
                sql.append(")");
            }
            if (count++ < ids.size()) {
                sql.append(",");
            }
        }
        sql.append(")");
        if (mutatingCallbacks) {
            sql.append(" RETURNING *");
        }
        return sql.toString();
    }

    default boolean supportsDeferrableForeignKey() {
        return false;
    }

    default String sqlToTurnOffReferentialConstraintCheck(String tableName) {
        throw new UnsupportedOperationException("Turning of foreign key constraint check is not supported.");
    }

    default String sqlToTurnOnReferentialConstraintCheck(String tableName) {
        throw new UnsupportedOperationException("Turning of foreign key constraint check is not supported.");
    }

    /**
     * This is only relevant to Postgresql for now.
     *
     * @return The sql string that will return all the foreign keys.
     */
    default String sqlToGetAllForeignKeys() {
        throw new IllegalStateException("sqlToGetAllForeignKeys is not supported.");
    }

    /**
     * Only used by Postgresql
     *
     * @param schema
     * @param table
     * @param foreignKeyName
     * @return The sql statement to alter the foreign key to be deferrable.
     */
    default String alterForeignKeyToDeferrable(String schema, String table, String foreignKeyName) {
        throw new IllegalStateException("alterForeignKeyToDeferrable is not supported.");
    }

    default List<Triple<SqlgSqlExecutor.DROP_QUERY, String, Boolean>> sqlTruncate(SqlgGraph sqlgGraph, SchemaTable schemaTable) {
        Preconditions.checkState(schemaTable.isWithPrefix(), "SqlDialect.sqlTruncate' schemaTable must start with a prefix %s or %s", Topology.VERTEX_PREFIX, Topology.EDGE_PREFIX);
        return Collections.singletonList(
                Triple.of(
                        SqlgSqlExecutor.DROP_QUERY.TRUNCATE,
                        "TRUNCATE TABLE " + maybeWrapInQoutes(schemaTable.getSchema()) + "." + maybeWrapInQoutes(schemaTable.getTable()),
                        false
                )
        );
    }

    default boolean supportsTruncateMultipleTablesTogether() {
        return false;
    }

    default boolean supportsPartitioning() {
        return false;
    }

    default List<Map<String, String>> getPartitions(Connection connection) {
        throw new IllegalStateException("Partitioning is not supported.");
    }

    default List<String> addPartitionTables() {
        throw new IllegalStateException("Partitioning is not supported.");
    }

    default List<String> addHashPartitionColumns() {
        throw new IllegalStateException("Partitioning is not supported.");
    }

    default String addDbVersionToGraph(DatabaseMetaData metadata) {
        try {
            return "ALTER TABLE \"sqlg_schema\".\"V_graph\" ADD COLUMN \"dbVersion\" TEXT DEFAULT '" + metadata.getDatabaseProductVersion() + "';";
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * get the default fetch size
     *
     * @return the default fetch size, maybe null if we want to use the default from the driver
     */
    default Integer getDefaultFetchSize() {
        return null;
    }

    default int getShardCount(SqlgGraph sqlgGraph, AbstractLabel label) {
        throw new IllegalStateException("Sharding is not supported.");
    }

    default boolean supportsSharding() {
        return false;
    }

    default String toRDBSStringLiteral(Object value) {
        PropertyType propertyType = PropertyType.from(value);
        return toRDBSStringLiteral(propertyType, value);
    }

    //TODO this is very lazy do properly
    default String toRDBSStringLiteral(PropertyType propertyType, Object value) {
        switch (propertyType.ordinal()) {
            case BOOLEAN_ORDINAL:
                Boolean b = (Boolean) value;
                return b.toString();
            case BYTE_ORDINAL:
                Byte byteValue = (Byte) value;
                return byteValue.toString();
            case SHORT_ORDINAL:
                Short shortValue = (Short) value;
                return shortValue.toString();
            case INTEGER_ORDINAL:
                Integer intValue = (Integer) value;
                return intValue.toString();
            case LONG_ORDINAL:
                Long longValue = (Long) value;
                return longValue.toString();
            case FLOAT_ORDINAL:
                Float floatValue = (Float) value;
                return floatValue.toString();
            case DOUBLE_ORDINAL:
                Double doubleValue = (Double) value;
                return doubleValue.toString();
            case STRING_ORDINAL:
                return "'" + value.toString() + "'";
            case LOCALDATE_ORDINAL:
                LocalDate localDateValue = (LocalDate) value;
                return "'" + localDateValue.toString() + "'";
            case LOCALDATETIME_ORDINAL:
                LocalDateTime localDateTimeValue = (LocalDateTime) value;
                return "'" + localDateTimeValue.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "'";
            case LOCALTIME_ORDINAL:
                LocalTime localTimeValue = (LocalTime) value;
                return "'" + localTimeValue.toString() + "'";
            case ZONEDDATETIME_ORDINAL:
                break;
            case PERIOD_ORDINAL:
                break;
            case DURATION_ORDINAL:
                break;
            case JSON_ORDINAL:
                break;
            case POINT_ORDINAL:
                break;
            case LINESTRING_ORDINAL:
                break;
            case POLYGON_ORDINAL:
                break;
            case GEOGRAPHY_POINT_ORDINAL:
                break;
            case GEOGRAPHY_POLYGON_ORDINAL:
                break;
            case boolean_ARRAY_ORDINAL:
                break;
            case BOOLEAN_ARRAY_ORDINAL:
                break;
            case byte_ARRAY_ORDINAL:
                break;
            case BYTE_ARRAY_ORDINAL:
                break;
            case short_ARRAY_ORDINAL:
                break;
            case SHORT_ARRAY_ORDINAL:
                break;
            case int_ARRAY_ORDINAL:
                break;
            case INTEGER_ARRAY_ORDINAL:
                break;
            case long_ARRAY_ORDINAL:
                break;
            case LONG_ARRAY_ORDINAL:
                break;
            case float_ARRAY_ORDINAL:
                break;
            case FLOAT_ARRAY_ORDINAL:
                break;
            case double_ARRAY_ORDINAL:
                break;
            case DOUBLE_ARRAY_ORDINAL:
                break;
            case STRING_ARRAY_ORDINAL:
                break;
            case LOCALDATETIME_ARRAY_ORDINAL:
                break;
            case LOCALDATE_ARRAY_ORDINAL:
                break;
            case LOCALTIME_ARRAY_ORDINAL:
                break;
            case ZONEDDATETIME_ARRAY_ORDINAL:
                break;
            case DURATION_ARRAY_ORDINAL:
                break;
            case PERIOD_ARRAY_ORDINAL:
                break;
            case JSON_ARRAY_ORDINAL:
                break;
        }
        return "'" + value.toString() + "'";
    }

    default void grantReadOnlyUserPrivilegesToSqlgSchemas(SqlgGraph sqlgGraph) {
        throw new RuntimeException("Not yet implemented!");
    }

    default Pair<Boolean, String> getBlocked(int pid, Connection connection) {
        return Pair.of(false, "");
    }

    default int getConnectionBackendPid(Connection connection) {
        return -1;
    }

    default String toSelectString(boolean partOfDuplicateQuery, ColumnList.Column column, String alias) {
        StringBuilder sb = new StringBuilder();
        if (!partOfDuplicateQuery && column.getAggregateFunction() != null) {
            sb.append(column.getAggregateFunction().toUpperCase());
            sb.append("(");
        }
        if (!partOfDuplicateQuery && column.getAggregateFunction() != null && column.getAggregateFunction().equals(GraphTraversal.Symbols.count)) {
            sb.append("1");
        } else {
            sb.append(maybeWrapInQoutes(column.getSchema()));
            sb.append(".");
            sb.append(maybeWrapInQoutes(column.getTable()));
            sb.append(".");
            sb.append(maybeWrapInQoutes(column.getColumn()));
        }
        if (!partOfDuplicateQuery && column.getAggregateFunction() != null) {
            sb.append(") AS ").append(maybeWrapInQoutes(alias));
            if (column.getAggregateFunction().equalsIgnoreCase("avg")) {
                sb.append(", COUNT(1) AS ").append(maybeWrapInQoutes(alias + "_weight"));
            }
        } else {
            sb.append(" AS ").append(maybeWrapInQoutes(alias));
        }
        return sb.toString();
    }

    default boolean isTimestampz(String typeName) {
        return false;
    }

    default String dropIndex(SqlgGraph sqlgGraph, AbstractLabel parentLabel, String name) {
        StringBuilder sql = new StringBuilder("DROP INDEX IF EXISTS ");
        SqlDialect sqlDialect = sqlgGraph.getSqlDialect();
        sql.append(sqlDialect.maybeWrapInQoutes(parentLabel.getSchema().getName()));
        sql.append(".");
        sql.append(sqlDialect.maybeWrapInQoutes(name));
        if (sqlDialect.needsSemicolon()) {
            sql.append(";");
        }
        return sql.toString();
    }

    /**
     * This is only needed for Hsqldb where we are unable to check for the existence of Sqlg's schemas
     * @return
     */
    default boolean canUserCreateSchemas(SqlgGraph sqlgGraph) {
        return true;
    }

    default String renameColumn(String schema, String table, String column, String newName) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        sql.append(maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(maybeWrapInQoutes(table));
        sql.append(" RENAME COLUMN ");
        sql.append(maybeWrapInQoutes(column));
        sql.append(" TO ");
        sql.append(maybeWrapInQoutes(newName));
        if (needsSemicolon()) {
            sql.append(";");
        }
        return sql.toString();
    }

    default String renameTable(String schema, String table, String newName) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        sql.append(maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(maybeWrapInQoutes(table));
        sql.append(" RENAME TO ");
        sql.append(maybeWrapInQoutes(newName));
        if (needsSemicolon()) {
            sql.append(";");
        }
        return sql.toString();
    }
}
