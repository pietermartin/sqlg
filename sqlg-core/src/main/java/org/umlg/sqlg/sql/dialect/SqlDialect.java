package org.umlg.sqlg.sql.dialect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.T;
import org.umlg.sqlg.structure.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.*;
import java.util.*;

public interface SqlDialect {

    void setConfiguration(Configuration configuration);

    Configuration getConfiguration();

    Set<String> getDefaultSchemas();

    PropertyType sqlTypeToPropertyType(SqlgGraph sqlgGraph, String schema, String table, String column, int sqlType, String typeName);

    void validateProperty(Object key, Object value);

    default boolean needsSemicolon() {
        return true;
    }

    default boolean supportsCascade() {
        return true;
    }

    default boolean needsSchemaDropCascade() {
        return supportsCascade();
    }

    String getColumnEscapeKey();

    String getPrimaryKeyType();

    String getAutoIncrementPrimaryKeyConstruct();

    String[] propertyTypeToSqlDefinition(PropertyType propertyType);

    int propertyTypeToJavaSqlType(PropertyType propertyType);

    String getForeignKeyTypeDefinition();

    default String maybeWrapInQoutes(String field) {
        StringBuilder sb = new StringBuilder(getColumnEscapeKey());
        sb.append(field);
        sb.append(getColumnEscapeKey());
        return sb.toString();
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
                    metaNode.put("type", PropertyType.BOOLEAN.name());
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

    default void prepareDB(Connection conn) {

    }

    default String getPublicSchema() {
        return "public";
    }


    default String indexName(SchemaTable schemaTable, String prefix, String column) {
        StringBuilder sb = new StringBuilder();
        sb.append(schemaTable.getSchema());
        sb.append("_");
        sb.append(prefix);
        sb.append(schemaTable.getTable());
        sb.append("_");
        sb.append(column);
        sb.append("Idx");
        return sb.toString();
    }

    String existIndexQuery(SchemaTable schemaTable, String prefix, String indexName);

    //This is needed for mariadb, which does not support schemas, so need to drop the database instead
    default boolean supportSchemas() {
        return true;
    }

    Map<SchemaTable, Pair<Long, Long>> flushVertexCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexCache);

//    void flushEdgeCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>>> edgeCache);
    void flushEdgeCache(SqlgGraph sqlgGraph, Map<MetaEdge, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>>> edgeCache);

    default boolean supportsBatchMode() {
        return false;
    }

    default boolean supportsJson() {
        return false;
    }

    String getBatchNull();

    void flushVertexPropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexPropertyCache);

    void flushEdgePropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>>> edgePropertyCache);

    default String hasContainerKeyToColumn(String key) {

        if (key.equals(T.id.getAccessor()))
            return "ID";
        else
            return key;
    }

    void flushRemovedVertices(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgVertex>> removeVertexCache);

    void flushRemovedEdges(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgEdge>> removeEdgeCache);

    String temporaryTableCopyCommandSqlVertex(SqlgGraph sqlgGraph, SchemaTable schemaTable, Map<String, Object> keyValueMap);

    String constructCompleteCopyCommandTemporarySqlVertex(SqlgGraph sqlgGraph, SqlgVertex vertex, Map<String, Object> keyValueMap);

    String constructCompleteCopyCommandSqlVertex(SqlgGraph sqlgGraph, SqlgVertex vertex, Map<String, Object> keyValueMap);

    String constructCompleteCopyCommandSqlEdge(SqlgGraph sqlgGraph, SqlgEdge sqlgEdge, SqlgVertex outVertex, SqlgVertex inVertex, Map<String, Object> keyValueMap);

    void writeStreamingVertex(OutputStream out, Map<String, Object> keyValueMap);

    void writeStreamingEdge(OutputStream out, SqlgEdge sqlgEdge, SqlgVertex outVertex, SqlgVertex inVertex, Map<String, Object> keyValueMap) throws IOException;

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

    default int getMinimumSchemaNameLength() {
        return Integer.MAX_VALUE;
    }

    default int getMinimumTableNameLength() {
        return Integer.MAX_VALUE;
    }

    default int getMinimumColumnNameLength() {
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

    void setPoint(PreparedStatement preparedStatement, int parameterStartIndex, Object point);

    void setLineString(PreparedStatement preparedStatement, int parameterStartIndex, Object lineString);

    void setPolygon(PreparedStatement preparedStatement, int parameterStartIndex, Object point);

    void setGeographyPoint(PreparedStatement preparedStatement, int parameterStartIndex, Object point);

    default boolean isPostgresql() {
        return false;
    }

    default void registerGisDataTypes(Connection connection) {
        //do nothing
    }

    <T> T getGis(SqlgGraph sqlgGraph);

    OutputStream streamSql(SqlgGraph sqlgGraph, String sql);

    InputStream inputStreamSql(SqlgGraph sqlgGraph, String sql);

    <L, R> void bulkAddEdges(SqlgGraph sqlgGraph, SchemaTable in, SchemaTable out, String edgeLabel, Pair<String, String> idFields, List<Pair<L, R>> uids);

    void lockTable(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix);

    void alterSequenceCacheSize(SqlgGraph sqlgGraph, SchemaTable schemaTable, String sequence, int batchSize);

    long nextSequenceVal(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix);

    long currSequenceVal(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix);

    String sequenceName(SqlgGraph sqlgGraph, SchemaTable outSchemaTable, String prefix);

    boolean supportsBulkWithinOut();

    String afterCreateTemporaryTableStatement();

    /**
     * These are internal columns used by sqlg that must be ignored when loading elements.
     * eg. '_copy_dummy' when doing using the copy command on postgresql.
     * @return
     */
    default List<String> columnsToIgnore() {
        return Collections.emptyList();
    }

    List<String> sqlgTopologyCreationScripts();

    default Long getPrimaryKeyStartValue() {
        return 1L;
    }

    Object convertArray(PropertyType propertyType, java.sql.Array array) throws SQLException;

    void setArray(PreparedStatement statement, int index, PropertyType type, Object[] values) throws SQLException;
}
