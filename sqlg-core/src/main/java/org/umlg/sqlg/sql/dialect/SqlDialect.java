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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

public interface SqlDialect {

    public void setConfiguration(Configuration configuration);

    public Configuration getConfiguration();

    Set<String> getDefaultSchemas();

    PropertyType sqlTypeToPropertyType(int sqlType, String typeName);

    String getJdbcDriver();

    void validateProperty(Object key, Object value);

    public default boolean needsSemicolon() {
        return true;
    }

    public default boolean supportsCascade() {
        return true;
    }

    String getColumnEscapeKey();

    String getPrimaryKeyType();

    String getAutoIncrementPrimaryKeyConstruct();

    String[] propertyTypeToSqlDefinition(PropertyType propertyType);

    int propertyTypeToJavaSqlType(PropertyType propertyType);

    String getForeignKeyTypeDefinition();

    public default String maybeWrapInQoutes(String field) {
        StringBuilder sb = new StringBuilder(getColumnEscapeKey());
        sb.append(field);
        sb.append(getColumnEscapeKey());
        return sb.toString();
    }

    public default boolean supportsFloatValues() {
        return true;
    }

    public default boolean supportsByteValues() {
        return false;
    }

    public default boolean supportsTransactionalSchema() {
        return true;
    }

    public default boolean supportsBooleanArrayValues() {
        return true;
    }

    public default boolean supportsByteArrayValues() {
        return true;
    }

    public default boolean supportsDoubleArrayValues() {
        return true;
    }

    public default boolean supportsFloatArrayValues() {
        return true;
    }

    public default boolean supportsIntegerArrayValues() {
        return true;
    }

    public default boolean supportsShortArrayValues() {
        return true;
    }

    public default boolean supportsLongArrayValues() {
        return true;
    }

    public default boolean supportsStringArrayValues() {
        return true;
    }

    public default void assertTableName(String tableName) {
    }

    public default void putJsonObject(ObjectNode obj, String columnName, int sqlType, Object o) {
        try {
            switch (sqlType) {
                case Types.BIT:
                    obj.put(columnName, (Boolean) o);
                    break;
                case Types.SMALLINT:
                    obj.put(columnName, ((Integer) o).shortValue());
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

    public default void putJsonMetaObject(ObjectMapper mapper, ArrayNode metaNodeArray, String columnName, int sqlType, Object o) {
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
                            metaNode.put("type", PropertyType.BOOLEAN_ARRAY.name());
                            break;
                        case Types.SMALLINT:
                            metaNode.put("type", PropertyType.SHORT_ARRAY.name());
                            break;
                        case Types.INTEGER:
                            metaNode.put("type", PropertyType.INTEGER_ARRAY.name());
                            break;
                        case Types.BIGINT:
                            metaNode.put("type", PropertyType.LONG_ARRAY.name());
                            break;
                        case Types.REAL:
                            metaNode.put("type", PropertyType.FLOAT_ARRAY.name());
                            break;
                        case Types.DOUBLE:
                            metaNode.put("type", PropertyType.DOUBLE_ARRAY.name());
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

    public default String createTableStatement() {
        return "CREATE TABLE ";
    }

    public default String createTemporaryTableStatement() {
        return "CREATE TEMPORARY TABLE ";
    }

    public default void prepareDB(Connection conn) {

    }

    public default String getPublicSchema() {
        return "public";
    }

    public default String indexName(SchemaTable schemaTable, String prefix, String column) {
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
    public default boolean supportSchemas() {
        return true;
    }

    Map<SchemaTable, Pair<Long, Long>> flushVertexCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexCache);

    void flushEdgeCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>>> edgeCache);

    public default boolean supportsBatchMode() {
        return false;
    }

    public default boolean supportsJson() {
        return false;
    }

    void flushVertexLabelCache(SqlgGraph sqlgGraph, Map<SqlgVertex, Pair<String, String>> vertexOutInLabelMap);

    String getBatchNull();

    void flushVertexPropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexPropertyCache);

    void flushEdgePropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>>> edgePropertyCache);

    public default String hasContainerKeyToColumn(String key) {

        if (key.equals(T.id.getAccessor()))
            return "ID";
        else
            return key;
    }

    void flushRemovedVertices(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgVertex>> removeVertexCache);

    void flushRemovedEdges(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgEdge>> removeEdgeCache);

    String constructManualCopyCommandSqlVertex(SqlgGraph sqlgGraph, SchemaTable schemaTable, Map<String, Object> keyValueMap);

    public String constructCompleteCopyCommandSqlVertex(SqlgGraph sqlgGraph, SqlgVertex vertex, Map<String, Object> keyValueMap);

    public String constructCompleteCopyCommandSqlEdge(SqlgGraph sqlgGraph, SqlgEdge sqlgEdge, SqlgVertex outVertex, SqlgVertex inVertex, Map<String, Object> keyValueMap);

    void flushStreamingVertex(OutputStream out, Map<String, Object> keyValueMap);

    void flushCompleteEdge(OutputStream out, SqlgEdge sqlgEdge, SqlgVertex outVertex, SqlgVertex inVertex, Map<String, Object> keyValueMap) throws IOException;

    public default boolean needForeignKeyIndex() {
        return false;
    }

    public default boolean supportsClientInfo() {
        return false;
    }

    public default void validateSchemaName(String schema) {
    }

    public default void validateTableName(String table) {
    }

    public default void validateColumnName(String column) {
    }

    public default int getMinimumSchemaNameLength() {
        return Integer.MAX_VALUE;
    }

    public default int getMinimumTableNameLength() {
        return Integer.MAX_VALUE;
    }

    public default int getMinimumColumnNameLength() {
        return Integer.MAX_VALUE;
    }

    public default boolean supportsILike() {
        return Boolean.FALSE;
    }

    public default boolean needsTimeZone() {
        return Boolean.FALSE;
    }

    List<String> getSpacialRefTable();

    List<String> getGisSchemas();

    void setJson(PreparedStatement preparedStatement, int parameterStartIndex, JsonNode right);

    void handleOther(Map<String, Object> properties, String columnName, Object o);

    void setPoint(PreparedStatement preparedStatement, int parameterStartIndex, Object point);

    void setPolygon(PreparedStatement preparedStatement, int parameterStartIndex, Object point);

    void setGeographyPoint(PreparedStatement preparedStatement, int parameterStartIndex, Object point);

    public default boolean isPostgresql() {
        return false;
    }

    public default void registerGisDataTypes(Connection connection) {
        //do nothing
    }

    <T> T getGis(SqlgGraph sqlgGraph);

    OutputStream streamSql(SqlgGraph sqlgGraph, String sql);

    InputStream inputStreamSql(SqlgGraph sqlgGraph, String sql);

    void copyInBulkTempEdges(SqlgGraph sqlgGraph, SchemaTable schemaTable, List<Pair<String, String>> uids);

    void bulkAddEdges(SqlgGraph sqlgGraph, SchemaTable in, SchemaTable out, String edgeLabel, Pair<String, String> idFields, List<Pair<String, String>> uids);

    void lockTable(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix);

    void alterSequenceCacheSize(SqlgGraph sqlgGraph, SchemaTable schemaTable, String sequence, int batchSize);

    long nextSequenceVal(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix);

    long currSequenceVal(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix);

    String sequenceName(SqlgGraph sqlgGraph, SchemaTable outSchemaTable, String prefix);

    boolean supportsBulkWithinOut();
}
