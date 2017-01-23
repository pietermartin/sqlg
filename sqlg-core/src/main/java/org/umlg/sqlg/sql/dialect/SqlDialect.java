package org.umlg.sqlg.sql.dialect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.T;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;

import java.sql.*;
import java.util.*;

public interface SqlDialect {

    String dialectName();

    Set<String> getDefaultSchemas();

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
        return getColumnEscapeKey() + field + getColumnEscapeKey();
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

    /**
     * @return the statement head to create a schema
     */
    default String createSchemaStatement() {
        return "CREATE SCHEMA ";
    }

    default void prepareDB(Connection conn) {
    }

    /**
     * A getter to return the "public" schema for the database. For postgresql it is "public" and for HSQLDB it is "PUBLIC"
     * @return the database's public schema.
     */
    default String getPublicSchema() {
        return "public";
    }

    default String indexName(SchemaTable schemaTable, String prefix, List<String> columns) {
        Preconditions.checkState(!columns.isEmpty(), "SqlDialect.indexName may not be called with an empty list of columns");
        StringBuilder sb = new StringBuilder();
        sb.append(schemaTable.getSchema());
        sb.append("_");
        sb.append(prefix);
        sb.append(schemaTable.getTable());
        sb.append("_");
        //noinspection OptionalGetWithoutIsPresent
        sb.append(columns.stream().reduce((a, b) -> a + "_" + b).get());
        sb.append("Idx");
        return sb.toString();
    }

    String existIndexQuery(SchemaTable schemaTable, String prefix, String indexName);

    //This is needed for mariadb, which does not support schemas, so need to drop the database instead
    default boolean supportSchemas() {
        return true;
    }

    default boolean supportsBatchMode() {
        return false;
    }

    default boolean supportsJson() {
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

    <T> T getGis(SqlgGraph sqlgGraph);

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
     *
     * @return
     */
    default List<String> columnsToIgnore() {
        return Collections.emptyList();
    }

    default String sqlgSqlgSchemaCreationScript() {
        return "CREATE SCHEMA \"sqlg_schema\";";
    }

    List<String> sqlgTopologyCreationScripts();

    String sqlgAddPropertyIndexTypeColumn();

    default Long getPrimaryKeyStartValue() {
        return 1L;
    }

    Object convertArray(PropertyType propertyType, java.sql.Array array) throws SQLException;

    void setArray(PreparedStatement statement, int index, PropertyType type, Object[] values) throws SQLException;

    /**
     * range condition
     *
     * @return
     */
    default String getRangeClause(Range<Long> r) {
        return "LIMIT " + (r.getMaximum() - r.getMinimum()) + " OFFSET " + r.getMinimum();
    }

    default boolean requiredPreparedStatementDeallocate() {
        return false;
    }

}
