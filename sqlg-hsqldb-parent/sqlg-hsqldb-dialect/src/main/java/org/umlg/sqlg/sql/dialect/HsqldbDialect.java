package org.umlg.sqlg.sql.dialect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.hsqldb.jdbc.JDBCArrayBasic;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.types.Type;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgExceptions;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.io.IOException;
import java.sql.*;
import java.time.*;
import java.util.*;

import static org.umlg.sqlg.structure.PropertyType.*;

/**
 * Date: 2014/07/16
 * Time: 3:09 PM
 */
public class HsqldbDialect extends BaseSqlDialect implements SqlBulkDialect {

    public HsqldbDialect() {
        super();
    }

    @Override
    public String dialectName() {
        return "HsqldbDialect";
    }

    @Override
    public Set<String> getInternalSchemas() {
        return new HashSet<>(Arrays.asList("INFORMATION_SCHEMA", "SYSTEM_LOBS"));
    }

    @Override
    public String valueToValuesString(PropertyType propertyType, Object value) {
        Preconditions.checkState(supportsType(propertyType), "PropertyType %s is not supported", propertyType.name());
        switch (propertyType.ordinal()) {
            case STRING_ORDINAL:
                return "'" + escapeQuotes(value.toString()) + "'";
            case STRING_ARRAY_ORDINAL:
                return toValuesArray(true, value).toString();
            case BYTE_ORDINAL:
                return value.toString();
            case byte_ARRAY_ORDINAL:
                return StringConverter.byteArrayToSQLHexString((byte[]) value);
            case BYTE_ARRAY_ORDINAL:
                return StringConverter.byteArrayToSQLHexString(
                        SqlgUtil.convertObjectArrayToBytePrimitiveArray((Byte[]) value)
                );
            case BOOLEAN_ORDINAL:
                return value.toString();
            case boolean_ARRAY_ORDINAL:
                return toValuesArray(false, value).toString();
            case BOOLEAN_ARRAY_ORDINAL:
                return toValuesArray(false, value).toString();
            case SHORT_ORDINAL:
                return value.toString();
            case short_ARRAY_ORDINAL:
                return toValuesArray(false, value).toString();
            case SHORT_ARRAY_ORDINAL:
                return toValuesArray(false, value).toString();
            case INTEGER_ORDINAL:
                return value.toString();
            case int_ARRAY_ORDINAL:
                return toValuesArray(false, value).toString();
            case INTEGER_ARRAY_ORDINAL:
                return toValuesArray(false, value).toString();
            case LONG_ORDINAL:
                return value.toString();
            case long_ARRAY_ORDINAL:
                return toValuesArray(false, value).toString();
            case LONG_ARRAY_ORDINAL:
                return toValuesArray(false, value).toString();
            case DOUBLE_ORDINAL:
                return value.toString();
            case double_ARRAY_ORDINAL:
                return toValuesArray(false, value).toString();
            case DOUBLE_ARRAY_ORDINAL:
                return toValuesArray(false, value).toString();
            case LOCALDATE_ORDINAL:
                return "'" + value.toString() + "'";
            case LOCALDATE_ARRAY_ORDINAL:
                return toValuesArray(true, getArrayDriverType(propertyType), value).toString();
            case LOCALDATETIME_ORDINAL:
                return "TIMESTAMP '" + Timestamp.valueOf((LocalDateTime) value) + "'";
            case LOCALDATETIME_ARRAY_ORDINAL:
                return toLocalDateTimeArray(true, getArrayDriverType(propertyType), value).toString();
            case LOCALTIME_ORDINAL:
                return "TIME '" + Time.valueOf((LocalTime) value) + "'";
            case LOCALTIME_ARRAY_ORDINAL:
                return toLocalTimeArray(true, getArrayDriverType(propertyType), value).toString();
            case JSON_ORDINAL:
                return "'" + value.toString() + "'";
            case JSON_ARRAY_ORDINAL:
                return toValuesArray(true, value).toString();
            default:
                throw SqlgExceptions.invalidPropertyType(propertyType);
        }
    }

    private StringBuilder toValuesArray(boolean quote, Object value) {
        return toValuesArray(quote, "", value);
    }

    private StringBuilder toValuesArray(boolean quote, String type, Object value) {
        StringBuilder sb;
        int length;
        sb = new StringBuilder();
        sb.append("ARRAY [");
        length = java.lang.reflect.Array.getLength(value);
        for (int i = 0; i < length; i++) {
            String valueOfArray = java.lang.reflect.Array.get(value, i).toString();
            sb.append(type);
            sb.append(" ");
            if (quote) {
                sb.append("'");
            }
            sb.append(valueOfArray);
            if (quote) {
                sb.append("'");
            }
            if (i < length - 1) {
                sb.append(",");
            }
        }
        sb.append("]");
        return sb;
    }

    private StringBuilder toLocalDateTimeArray(boolean quote, String type, Object value) {
        StringBuilder sb;
        int length;
        sb = new StringBuilder();
        sb.append("ARRAY [");
        length = java.lang.reflect.Array.getLength(value);
        for (int i = 0; i < length; i++) {
            LocalDateTime valueOfArray = (LocalDateTime) java.lang.reflect.Array.get(value, i);
            sb.append(type);
            sb.append(" ");
            if (quote) {
                sb.append("'");
            }
            sb.append(Timestamp.valueOf(valueOfArray));
            sb.append("+0:00");
            if (quote) {
                sb.append("'");
            }
            if (i < length - 1) {
                sb.append(",");
            }
        }
        sb.append("]");
        return sb;
    }

    private StringBuilder toLocalTimeArray(boolean quote, String type, Object value) {
        StringBuilder sb;
        int length;
        sb = new StringBuilder();
        sb.append("ARRAY [");
        length = java.lang.reflect.Array.getLength(value);
        for (int i = 0; i < length; i++) {
            LocalTime valueOfArray = (LocalTime) java.lang.reflect.Array.get(value, i);
            sb.append(type);
            sb.append(" ");
            if (quote) {
                sb.append("'");
            }
            sb.append(Time.valueOf(valueOfArray));
            sb.append("+0:00");
            if (quote) {
                sb.append("'");
            }
            if (i < length - 1) {
                sb.append(",");
            }
        }
        sb.append("]");
        return sb;
    }

    @Override
    public String getPublicSchema() {
        return "PUBLIC";
    }

    @Override
    public String existIndexQuery(SchemaTable schemaTable, String prefix, String indexName) {
        return "SELECT * FROM INFORMATION_SCHEMA.SYSTEM_INDEXINFO WHERE TABLE_SCHEM = '" + schemaTable.getSchema() +
                "' AND  TABLE_NAME = '" +
                prefix +
                schemaTable.getTable() +
                "' AND INDEX_NAME = '" +
                indexName +
                "'";
    }

    @Override
    public boolean supportsTransactionalSchema() {
        return false;
    }

    @Override
    public boolean supportsIfExists() {
        return false;
    }

    @Override
    public void validateProperty(Object key, Object value) {
        if (value instanceof String) {
            return;
        }
        if (value instanceof Character) {
            return;
        }
        if (value instanceof Boolean) {
            return;
        }
        if (value instanceof Byte) {
            return;
        }
        if (value instanceof Short) {
            return;
        }
        if (value instanceof Integer) {
            return;
        }
        if (value instanceof Long) {
            return;
        }
        if (value instanceof Double) {
            return;
        }
        if (value instanceof LocalDate) {
            return;
        }
        if (value instanceof LocalDateTime) {
            return;
        }
        if (value instanceof ZonedDateTime) {
            return;
        }
        if (value instanceof LocalTime) {
            return;
        }
        if (value instanceof Period) {
            return;
        }
        if (value instanceof Duration) {
            return;
        }
        if (value instanceof JsonNode) {
            return;
        }
        if (value instanceof byte[]) {
            return;
        }
        if (value instanceof boolean[]) {
            return;
        }
        if (value instanceof char[]) {
            return;
        }
        if (value instanceof short[]) {
            return;
        }
        if (value instanceof int[]) {
            return;
        }
        if (value instanceof long[]) {
            return;
        }
        if (value instanceof double[]) {
            return;
        }
        if (value instanceof String[]) {
            return;
        }
        if (value instanceof Character[]) {
            return;
        }
        if (value instanceof Boolean[]) {
            return;
        }
        if (value instanceof Byte[]) {
            return;
        }
        if (value instanceof Short[]) {
            return;
        }
        if (value instanceof Integer[]) {
            return;
        }
        if (value instanceof Long[]) {
            return;
        }
        if (value instanceof Double[]) {
            return;
        }
        if (value instanceof LocalDateTime[]) {
            return;
        }
        if (value instanceof LocalDate[]) {
            return;
        }
        if (value instanceof LocalTime[]) {
            return;
        }
        if (value instanceof ZonedDateTime[]) {
            return;
        }
        if (value instanceof Duration[]) {
            return;
        }
        if (value instanceof Period[]) {
            return;
        }
        if (value instanceof JsonNode[]) {
            return;
        }
        throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
    }

    @Override
    public String getColumnEscapeKey() {
        return "\"";
    }

    @Override
    public String getPrimaryKeyType() {
        return "BIGINT NOT NULL PRIMARY KEY";
    }

    @Override
    public String getAutoIncrementPrimaryKeyConstruct() {
        return "BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY";
    }

    @Override
    public String[] propertyTypeToSqlDefinition(PropertyType propertyType) {
        switch (propertyType.ordinal()) {
            case BOOLEAN_ORDINAL:
                return new String[]{"BOOLEAN"};
            case BYTE_ORDINAL:
                return new String[]{"TINYINT"};
            case SHORT_ORDINAL:
                return new String[]{"SMALLINT"};
            case INTEGER_ORDINAL:
                return new String[]{"INTEGER"};
            case LONG_ORDINAL:
                return new String[]{"BIGINT"};
            case DOUBLE_ORDINAL:
                return new String[]{"DOUBLE"};
            case LOCALDATE_ORDINAL:
                return new String[]{"DATE"};
            case LOCALDATETIME_ORDINAL:
                return new String[]{"TIMESTAMP"};
            case ZONEDDATETIME_ORDINAL:
                return new String[]{"TIMESTAMP", "LONGVARCHAR"};
            case LOCALTIME_ORDINAL:
                return new String[]{"TIME"};
            case PERIOD_ORDINAL:
                return new String[]{"INTEGER", "INTEGER", "INTEGER"};
            case DURATION_ORDINAL:
                return new String[]{"BIGINT", "INTEGER"};
            case STRING_ORDINAL:
                return new String[]{"LONGVARCHAR"};
            case VARCHAR_ORDINAL:
                return new String[]{"VARCHAR(" + propertyType.getLength() + ")"};
            case JSON_ORDINAL:
                return new String[]{"LONGVARCHAR"};
            case POINT_ORDINAL:
                throw new IllegalStateException("HSQLDB does not support gis types!");
            case POLYGON_ORDINAL:
                throw new IllegalStateException("HSQLDB does not support gis types!");
            case GEOGRAPHY_POINT_ORDINAL:
                throw new IllegalStateException("HSQLDB does not support gis types!");
            case GEOGRAPHY_POLYGON_ORDINAL:
                throw new IllegalStateException("HSQLDB does not support gis types!");
            case BYTE_ARRAY_ORDINAL:
                return new String[]{"LONGVARBINARY"};
            case byte_ARRAY_ORDINAL:
                return new String[]{"LONGVARBINARY"};
            case boolean_ARRAY_ORDINAL:
                return new String[]{"BOOLEAN ARRAY DEFAULT ARRAY[]"};
            case BOOLEAN_ARRAY_ORDINAL:
                return new String[]{"BOOLEAN ARRAY DEFAULT ARRAY[]"};
            case SHORT_ARRAY_ORDINAL:
                return new String[]{"SMALLINT ARRAY DEFAULT ARRAY[]"};
            case short_ARRAY_ORDINAL:
                return new String[]{"SMALLINT ARRAY DEFAULT ARRAY[]"};
            case int_ARRAY_ORDINAL:
                return new String[]{"INTEGER ARRAY DEFAULT ARRAY[]"};
            case INTEGER_ARRAY_ORDINAL:
                return new String[]{"INTEGER ARRAY DEFAULT ARRAY[]"};
            case LONG_ARRAY_ORDINAL:
                return new String[]{"BIGINT ARRAY DEFAULT ARRAY[]"};
            case long_ARRAY_ORDINAL:
                return new String[]{"BIGINT ARRAY DEFAULT ARRAY[]"};
            case float_ARRAY_ORDINAL:
                return new String[]{"REAL ARRAY DEFAULT ARRAY[]"};
            case DOUBLE_ARRAY_ORDINAL:
                return new String[]{"DOUBLE ARRAY DEFAULT ARRAY[]"};
            case double_ARRAY_ORDINAL:
                return new String[]{"DOUBLE ARRAY DEFAULT ARRAY[]"};
            case STRING_ARRAY_ORDINAL:
                return new String[]{"LONGVARCHAR ARRAY DEFAULT ARRAY[]"};
            case LOCALDATETIME_ARRAY_ORDINAL:
                return new String[]{"TIMESTAMP ARRAY DEFAULT ARRAY[]"};
            case LOCALDATE_ARRAY_ORDINAL:
                return new String[]{"DATE ARRAY DEFAULT ARRAY[]"};
            case LOCALTIME_ARRAY_ORDINAL:
                return new String[]{"TIME ARRAY DEFAULT ARRAY[]"};
            case ZONEDDATETIME_ARRAY_ORDINAL:
                return new String[]{"TIMESTAMP ARRAY DEFAULT ARRAY[]", "LONGVARCHAR ARRAY DEFAULT ARRAY[]"};
            case DURATION_ARRAY_ORDINAL:
                return new String[]{"BIGINT ARRAY DEFAULT ARRAY[]", "INTEGER ARRAY DEFAULT ARRAY[]"};
            case PERIOD_ARRAY_ORDINAL:
                return new String[]{"INTEGER ARRAY DEFAULT ARRAY[]", "INTEGER ARRAY DEFAULT ARRAY[]", "INTEGER ARRAY DEFAULT ARRAY[]"};
            case JSON_ARRAY_ORDINAL:
                return new String[]{"LONGVARCHAR ARRAY DEFAULT ARRAY[]"};
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    @Override
    public int[] propertyTypeToJavaSqlType(PropertyType propertyType) {
        switch (propertyType.ordinal()) {
            case BOOLEAN_ORDINAL:
                return new int[]{Types.BOOLEAN};
            case BYTE_ORDINAL:
                return new int[]{Types.TINYINT};
            case SHORT_ORDINAL:
                return new int[]{Types.SMALLINT};
            case INTEGER_ORDINAL:
                return new int[]{Types.INTEGER};
            case LONG_ORDINAL:
                return new int[]{Types.BIGINT};
            case DOUBLE_ORDINAL:
                return new int[]{Types.DOUBLE};
            case STRING_ORDINAL:
                return new int[]{Types.CLOB};
            case LOCALDATETIME_ORDINAL:
                return new int[]{Types.TIMESTAMP};
            case LOCALDATE_ORDINAL:
                return new int[]{Types.DATE};
            case LOCALTIME_ORDINAL:
                return new int[]{Types.TIME};
            case ZONEDDATETIME_ORDINAL:
                return new int[]{Types.TIMESTAMP, Types.CLOB};
            case PERIOD_ORDINAL:
                return new int[]{Types.INTEGER, Types.INTEGER, Types.INTEGER};
            case DURATION_ORDINAL:
                return new int[]{Types.BIGINT, Types.INTEGER};
            case JSON_ORDINAL:
                return new int[]{Types.CLOB};
            case BYTE_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case byte_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case boolean_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case BOOLEAN_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case short_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case SHORT_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case int_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case INTEGER_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case long_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case LONG_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case float_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case FLOAT_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case double_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case DOUBLE_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case STRING_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case LOCALDATETIME_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case LOCALDATE_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case LOCALTIME_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            case ZONEDDATETIME_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY, Types.ARRAY};
            case DURATION_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY, Types.ARRAY};
            case PERIOD_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY, Types.ARRAY, Types.ARRAY};
            case JSON_ARRAY_ORDINAL:
                return new int[]{Types.ARRAY};
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    @Override
    public PropertyType sqlTypeToPropertyType(SqlgGraph sqlgGraph, String schema, String table, String column, int sqlType, String typeName, ListIterator<Triple<String, Integer, String>> metaDataIter) {
        switch (sqlType) {
            case Types.BOOLEAN:
                return PropertyType.BOOLEAN;
            case Types.SMALLINT:
                return PropertyType.SHORT;
            case Types.INTEGER:
                return PropertyType.INTEGER;
            case Types.BIGINT:
                return PropertyType.LONG;
            case Types.REAL:
                return PropertyType.FLOAT;
            case Types.DOUBLE:
                return PropertyType.DOUBLE;
            case Types.VARCHAR:
                return PropertyType.STRING;
            case Types.TIMESTAMP:
                return PropertyType.LOCALDATETIME;
            case Types.DATE:
                return PropertyType.LOCALDATE;
            case Types.TIME:
                return PropertyType.LOCALTIME;
            case Types.VARBINARY:
                return PropertyType.BYTE_ARRAY;
            case Types.ARRAY:
                return sqlArrayTypeNameToPropertyType(typeName, sqlgGraph, schema, table, column, metaDataIter);
            default:
                throw new IllegalStateException("Unknown sqlType " + sqlType);
        }
    }

    @Override
    public PropertyType sqlArrayTypeNameToPropertyType(String typeName, SqlgGraph sqlgGraph, String schema, String table, String columnName, ListIterator<Triple<String, Integer, String>> metaDataIter) {
        switch (typeName) {
            case "BOOLEAN ARRAY":
                return PropertyType.BOOLEAN_ARRAY;
            case "SMALLINT ARRAY":
                return PropertyType.SHORT_ARRAY;
            case "INTEGER ARRAY":
                return PropertyType.INTEGER_ARRAY;
            case "BIGINT ARRAY":
                return PropertyType.LONG_ARRAY;
            case "DOUBLE ARRAY":
                return PropertyType.DOUBLE_ARRAY;
            case "DATE ARRAY":
                return PropertyType.LOCALDATE_ARRAY;
            case "TIME ARRAY":
                return PropertyType.LOCALTIME_ARRAY;
            case "TIMESTAMP ARRAY":
                //need to check the next column to know if its a LocalDateTime or ZonedDateTime array
                Triple<String, Integer, String> metaData = metaDataIter.next();
                metaDataIter.previous();
                if (metaData.getLeft().startsWith(columnName + "~~~")) {
                    return PropertyType.ZONEDDATETIME_ARRAY;
                } else {
                    return PropertyType.LOCALDATETIME_ARRAY;
                }
            default:
                if (typeName.contains("VARCHAR") && typeName.contains("ARRAY")) {
                    return PropertyType.STRING_ARRAY;
                } else {
                    throw new RuntimeException(String.format("Array type not supported typeName = %s", typeName));
                }
        }
    }

    @Override
    public String getForeignKeyTypeDefinition() {
        return "BIGINT";
    }

    @Override
    public boolean supportsFloatValues() {
        return false;
    }

    @Override
    public boolean supportsByteValues() {
        return true;
    }

    @Override
    public boolean supportsFloatArrayValues() {
        return false;
    }

    @Override
    public String getArrayDriverType(PropertyType propertyType) {
        switch (propertyType.ordinal()) {
            case boolean_ARRAY_ORDINAL:
                return "BOOLEAN";
            case BOOLEAN_ARRAY_ORDINAL:
                return "BOOLEAN";
            case SHORT_ARRAY_ORDINAL:
                return "SMALLINT";
            case short_ARRAY_ORDINAL:
                return "SMALLINT";
            case INTEGER_ARRAY_ORDINAL:
                return "INTEGER";
            case int_ARRAY_ORDINAL:
                return "INTEGER";
            case LONG_ARRAY_ORDINAL:
                return "BIGINT";
            case long_ARRAY_ORDINAL:
                return "BIGINT";
            case DOUBLE_ARRAY_ORDINAL:
                return "DOUBLE";
            case double_ARRAY_ORDINAL:
                return "DOUBLE";
            case STRING_ARRAY_ORDINAL:
                return "VARCHAR";
            case LOCALDATETIME_ARRAY_ORDINAL:
                return "TIMESTAMP";
            case LOCALDATE_ARRAY_ORDINAL:
                return "DATE";
            case LOCALTIME_ARRAY_ORDINAL:
                return "TIME";
            default:
                throw new IllegalStateException("propertyType " + propertyType.name() + " unknown!");
        }
    }

    @Override
    public String createTableStatement() {
        return "CREATE TABLE ";
    }

    @Override
    public void prepareDB(Connection conn) {
        StringBuilder sql = new StringBuilder("SET DATABASE TRANSACTION CONTROL MVCC;");
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = new StringBuilder("SET DATABASE DEFAULT TABLE TYPE CACHED;");
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void validateColumnName(String column) {
        super.validateColumnName(column);
    }

    @Override
    public Set<String> getSpacialRefTable() {
        return Collections.emptySet();
    }

    @Override
    public List<String> getGisSchemas() {
        return Collections.emptyList();
    }

    @Override
    public void setPoint(PreparedStatement preparedStatement, int parameterStartIndex, Object point) {
        throw new IllegalStateException("Hsqldb does not support gis types, this should not have happened!");
    }

    @Override
    public void setLineString(PreparedStatement preparedStatement, int parameterStartIndex, Object lineString) {
        throw new IllegalStateException("Hsqldb does not support gis types, this should not have happened!");
    }

    @Override
    public void setPolygon(PreparedStatement preparedStatement, int parameterStartIndex, Object point) {
        throw new IllegalStateException("Hsqldb does not support gis types, this should not have happened!");
    }

    @Override
    public void setGeographyPoint(PreparedStatement preparedStatement, int parameterStartIndex, Object point) {
        throw new IllegalStateException("Hsqldb does not support gis types, this should not have happened!");
    }

    @Override
    public <T> T getGis(SqlgGraph sqlgGraph) {
        throw new IllegalStateException("Hsqldb does not support other types, this should not have happened!");
    }

    @Override
    public void lockTable(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix) {
        throw new UnsupportedOperationException("Hsqldb does not support table locking!");
    }

    @Override
    public void alterSequenceCacheSize(SqlgGraph sqlgGraph, SchemaTable schemaTable, String sequence, int batchSize) {
        throw new UnsupportedOperationException("Hsqldb does not support alterSequenceCacheSize!");
    }

    @Override
    public long nextSequenceVal(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix) {
        throw new UnsupportedOperationException("Hsqldb does not support nextSequenceVal!");
    }

    @Override
    public long currSequenceVal(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix) {
        throw new UnsupportedOperationException("Hsqldb does not support currSequenceVal!");
    }

    @Override
    public String sequenceName(SqlgGraph sqlgGraph, SchemaTable outSchemaTable, String prefix) {
        throw new UnsupportedOperationException("Hsqldb does not support sequenceName!");
    }

    @Override
    public boolean supportsBatchMode() {
        return true;
    }

    @Override
    public boolean supportsBulkWithinOut() {
        return true;
    }

    @Override
    public String createTemporaryTableStatement() {
        return "DECLARE LOCAL TEMPORARY TABLE ";
    }

    @Override
    public String afterCreateTemporaryTableStatement() {
        return "";
    }

    @Override
    public List<String> sqlgTopologyCreationScripts() {
        List<String> result = new ArrayList<>();

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_graph\" (" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP, " +
                "\"updatedOn\" TIMESTAMP, " +
                "\"version\" LONGVARCHAR, " +
                "\"dbVersion\" LONGVARCHAR);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_schema\" (" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP, " +
                "\"name\" LONGVARCHAR);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_vertex\" (" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP, " +
                "\"name\" LONGVARCHAR, " +
                "\"schemaVertex\" LONGVARCHAR," +
                "\"partitionType\" LONGVARCHAR, " +
                "\"partitionExpression\" LONGVARCHAR, " +
                "\"shardCount\" INTEGER);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_edge\" (" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP, " +
                "\"name\" LONGVARCHAR, " +
                "\"partitionType\" LONGVARCHAR, " +
                "\"partitionExpression\" LONGVARCHAR, " +
                "\"shardCount\" INTEGER);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_partition\" (" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP, " +
                "\"name\" LONGVARCHAR, " +
                "\"from\" LONGVARCHAR, " +
                "\"to\" LONGVARCHAR, " +
                "\"in\" LONGVARCHAR, " +
                "\"modulus\" INTEGER, " +
                "\"remainder\" INTEGER, " +
                "\"partitionType\" LONGVARCHAR, " +
                "\"partitionExpression\" LONGVARCHAR);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_property\" (" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP, " +
                "\"name\" LONGVARCHAR, " +
                "\"type\" LONGVARCHAR);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_index\" (" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP, " +
                "\"name\" LONGVARCHAR, " +
                "\"index_type\" LONGVARCHAR);");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_schema_vertex\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.vertex__I\" BIGINT, " +
                "\"sqlg_schema.schema__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.schema__O\") REFERENCES \"sqlg_schema\".\"V_schema\" (\"ID\") ON DELETE CASCADE);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_in_edges\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.edge__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.edge__I\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_out_edges\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.edge__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.edge__I\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_property\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_property\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\") ON DELETE CASCADE, FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\") ON DELETE CASCADE);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_identifier\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "\"identifier_index\" INTEGER, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_identifier\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "\"identifier_index\" INTEGER, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\") ON DELETE CASCADE);");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_partition\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.partition__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE);"
        );
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_partition\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.partition__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\") ON DELETE CASCADE);"
        );
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_partition_partition\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.partition__I\" BIGINT, " +
                "\"sqlg_schema.partition__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.partition__O\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\") ON DELETE CASCADE);"
        );
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_distribution\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE);"
        );
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_colocate\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.vertex__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE);"
        );
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_distribution\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\") ON DELETE CASCADE);"
        );

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_colocate\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.vertex__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\") ON DELETE CASCADE);"
        );

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_index\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.index__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.index__I\") REFERENCES \"sqlg_schema\".\"V_index\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_index\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.index__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.index__I\") REFERENCES \"sqlg_schema\".\"V_index\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\") ON DELETE CASCADE);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_index_property\"(" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.index__O\" BIGINT, " +
                "\"sequence\" INTEGER, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\") ON DELETE CASCADE, " +
                "FOREIGN KEY (\"sqlg_schema.index__O\") REFERENCES \"sqlg_schema\".\"V_index\" (\"ID\") ON DELETE CASCADE);");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_log\" (" +
                "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                "\"timestamp\" TIMESTAMP, " +
                "\"pid\" INTEGER, " +
                "\"log\" LONGVARCHAR);");

        return result;
    }

    @Override
    public String sqlgCreateTopologyGraph() {
        return null;
    }

    @Override
    public String sqlgAddIndexEdgeSequenceColumn() {
        return "ALTER TABLE \"sqlg_schema\".\"E_index_property\" ADD COLUMN \"sequence\" INTEGER DEFAULT 0;";
    }


    @Override
    public Long getPrimaryKeyStartValue() {
        return 0L;
    }

    private Array createArrayOf(Connection conn, PropertyType propertyType, Object[] data) {
        org.hsqldb.types.Type type;
        switch (propertyType.ordinal()) {
            case STRING_ARRAY_ORDINAL:
                type = Type.SQL_VARCHAR;
                break;
            case long_ARRAY_ORDINAL:
                type = Type.SQL_BIGINT;
                break;
            case LONG_ARRAY_ORDINAL:
                type = Type.SQL_BIGINT;
                break;
            case int_ARRAY_ORDINAL:
                type = Type.SQL_INTEGER;
                break;
            case INTEGER_ARRAY_ORDINAL:
                type = Type.SQL_INTEGER;
                break;
            case SHORT_ARRAY_ORDINAL:
                type = Type.SQL_SMALLINT;
                break;
            case short_ARRAY_ORDINAL:
                type = Type.SQL_SMALLINT;
                break;
            case FLOAT_ARRAY_ORDINAL:
                type = Type.SQL_DOUBLE;
                break;
            case float_ARRAY_ORDINAL:
                type = Type.SQL_DOUBLE;
                break;
            case DOUBLE_ARRAY_ORDINAL:
                type = Type.SQL_DOUBLE;
                break;
            case double_ARRAY_ORDINAL:
                type = Type.SQL_DOUBLE;
                break;
            case BOOLEAN_ARRAY_ORDINAL:
                type = Type.SQL_BIT;
                break;
            case boolean_ARRAY_ORDINAL:
                type = Type.SQL_BIT;
                break;
            case LOCALDATETIME_ARRAY_ORDINAL:
                type = Type.SQL_TIMESTAMP_WITH_TIME_ZONE;
                break;
            case LOCALDATE_ARRAY_ORDINAL:
                type = Type.SQL_DATE;
                break;
            case LOCALTIME_ARRAY_ORDINAL:
                type = Type.SQL_TIME;
                break;
            case ZONEDDATETIME_ARRAY_ORDINAL:
                type = Type.SQL_TIMESTAMP_WITH_TIME_ZONE;
                break;
            case JSON_ARRAY_ORDINAL:
                type = Type.SQL_VARCHAR;
                break;
            default:
                throw new IllegalStateException("Unhandled array type " + propertyType.name());
        }
        return new JDBCArrayBasic(data, type);
    }

    @Override
    public Object convertArray(PropertyType propertyType, java.sql.Array array) throws SQLException {
        switch (propertyType.ordinal()) {
            case BOOLEAN_ARRAY_ORDINAL:
                return SqlgUtil.convertObjectArrayToBooleanArray((Object[]) array.getArray());
            case boolean_ARRAY_ORDINAL:
                return SqlgUtil.convertObjectArrayToBooleanPrimitiveArray((Object[]) array.getArray());
            case SHORT_ARRAY_ORDINAL:
                return SqlgUtil.convertObjectOfIntegersArrayToShortArray((Object[]) array.getArray());
            case short_ARRAY_ORDINAL:
                return SqlgUtil.convertObjectOfIntegersArrayToShortPrimitiveArray((Object[]) array.getArray());
            case INTEGER_ARRAY_ORDINAL:
                return SqlgUtil.convertObjectOfIntegersArrayToIntegerArray((Object[]) array.getArray());
            case int_ARRAY_ORDINAL:
                return SqlgUtil.convertObjectOfIntegersArrayToIntegerPrimitiveArray((Object[]) array.getArray());
            case LONG_ARRAY_ORDINAL:
                return SqlgUtil.convertObjectOfLongsArrayToLongArray((Object[]) array.getArray());
            case long_ARRAY_ORDINAL:
                return SqlgUtil.convertObjectOfLongsArrayToLongPrimitiveArray((Object[]) array.getArray());
            case DOUBLE_ARRAY_ORDINAL:
                return SqlgUtil.convertObjectOfDoublesArrayToDoubleArray((Object[]) array.getArray());
            case double_ARRAY_ORDINAL:
                return SqlgUtil.convertObjectOfDoublesArrayToDoublePrimitiveArray((Object[]) array.getArray());
            case FLOAT_ARRAY_ORDINAL:
                return SqlgUtil.convertObjectOfFloatsArrayToFloatArray((Object[]) array.getArray());
            case float_ARRAY_ORDINAL:
                return SqlgUtil.convertObjectOfFloatsArrayToFloatPrimitiveArray((Object[]) array.getArray());
            case STRING_ARRAY_ORDINAL:
                return SqlgUtil.convertObjectOfStringsArrayToStringArray((Object[]) array.getArray());
            case LOCALDATETIME_ARRAY_ORDINAL:
                Object[] timestamps = (Object[]) array.getArray();
                return SqlgUtil.copyObjectArrayOfTimestampToLocalDateTime(timestamps, new LocalDateTime[(timestamps).length]);
            case LOCALDATE_ARRAY_ORDINAL:
                Object[] dates = (Object[]) array.getArray();
                return SqlgUtil.copyObjectArrayOfDateToLocalDate(dates, new LocalDate[dates.length]);
            case LOCALTIME_ARRAY_ORDINAL:
                Object[] times = (Object[]) array.getArray();
                return SqlgUtil.copyObjectArrayOfTimeToLocalTime(times, new LocalTime[times.length]);
            case JSON_ARRAY_ORDINAL:
                String[] jsons = SqlgUtil.convertObjectOfStringsArrayToStringArray((Object[]) array.getArray());
                JsonNode[] jsonNodes = new JsonNode[jsons.length];
                ObjectMapper objectMapper = new ObjectMapper();
                int count = 0;
                for (String json : jsons) {
                    try {
                        JsonNode jsonNode = objectMapper.readTree(json);
                        jsonNodes[count++] = jsonNode;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return jsonNodes;
            default:
                throw new IllegalStateException("Unhandled property type " + propertyType.name());
        }
    }

    @Override
    public void setArray(PreparedStatement statement, int index, PropertyType type,
                         Object[] values) throws SQLException {
        statement.setArray(index, createArrayOf(statement.getConnection(), type, values));
    }

    @Override
    public boolean isSystemIndex(String indexName) {
        return indexName.startsWith("SYS_IDX_") || indexName.startsWith("SYS_PK") || indexName.endsWith("SYS_FK");
    }

    @Override
    public boolean supportsJsonArrayValues() {
        return true;
    }

    @Override
    public boolean supportsType(PropertyType propertyType) {
        switch (propertyType.ordinal()) {
            case BOOLEAN_ORDINAL:
                return true;
            case BOOLEAN_ARRAY_ORDINAL:
                return true;
            case boolean_ARRAY_ORDINAL:
                return true;
            case BYTE_ORDINAL:
                return true;
            case BYTE_ARRAY_ORDINAL:
                return true;
            case byte_ARRAY_ORDINAL:
                return true;
            case SHORT_ORDINAL:
                return true;
            case short_ARRAY_ORDINAL:
                return true;
            case SHORT_ARRAY_ORDINAL:
                return true;
            case INTEGER_ORDINAL:
                return true;
            case int_ARRAY_ORDINAL:
                return true;
            case INTEGER_ARRAY_ORDINAL:
                return true;
            case LONG_ORDINAL:
                return true;
            case long_ARRAY_ORDINAL:
                return true;
            case LONG_ARRAY_ORDINAL:
                return true;
            case DOUBLE_ORDINAL:
                return true;
            case DOUBLE_ARRAY_ORDINAL:
                return true;
            case double_ARRAY_ORDINAL:
                return true;
            case STRING_ORDINAL:
                return true;
            case LOCALDATE_ORDINAL:
                return true;
            case LOCALDATE_ARRAY_ORDINAL:
                return true;
            case LOCALDATETIME_ORDINAL:
                return true;
            case LOCALDATETIME_ARRAY_ORDINAL:
                return true;
            case LOCALTIME_ORDINAL:
                return true;
            case LOCALTIME_ARRAY_ORDINAL:
                return true;
            case JSON_ORDINAL:
                return true;
            case STRING_ARRAY_ORDINAL:
                return true;
            case JSON_ARRAY_ORDINAL:
                return true;
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }


    @Override
    public String createSchemaStatement(String schemaName) {
        // if ever schema is created outside of sqlg while the graph is already instantiated
        return "CREATE SCHEMA IF NOT EXISTS " + maybeWrapInQoutes(schemaName);
    }

    @Override
    public boolean isHsqldb() {
        return true;
    }

    @Override
    public String sqlToTurnOffReferentialConstraintCheck(String tableName) {
        return "SET DATABASE REFERENTIAL INTEGRITY FALSE";
    }

    @Override
    public String sqlToTurnOnReferentialConstraintCheck(String tableName) {
        return "SET DATABASE REFERENTIAL INTEGRITY TRUE";
    }

    @Override
    public List<String> addPartitionTables() {
        return Arrays.asList(
                "ALTER TABLE \"sqlg_schema\".\"V_vertex\" ADD COLUMN \"partitionType\" LONGVARCHAR DEFAULT 'NONE';",
                "ALTER TABLE \"sqlg_schema\".\"V_vertex\" ADD COLUMN \"partitionExpression\" LONGVARCHAR;",
                "ALTER TABLE \"sqlg_schema\".\"V_vertex\" ADD COLUMN \"shardCount\" INTEGER;",
                "ALTER TABLE \"sqlg_schema\".\"V_edge\" ADD COLUMN \"partitionType\" LONGVARCHAR DEFAULT 'NONE';",
                "ALTER TABLE \"sqlg_schema\".\"V_edge\" ADD COLUMN \"partitionExpression\" LONGVARCHAR;",
                "ALTER TABLE \"sqlg_schema\".\"V_edge\" ADD COLUMN \"shardCount\" INTEGER;",
                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_partition\" (" +
                        "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                        "\"createdOn\" TIMESTAMP, " +
                        "\"name\" LONGVARCHAR, " +
                        "\"from\" LONGVARCHAR, " +
                        "\"to\" LONGVARCHAR, " +
                        "\"in\" LONGVARCHAR, " +
                        "\"partitionType\" LONGVARCHAR, " +
                        "\"partitionExpression\" LONGVARCHAR);",
                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_partition\"(" +
                        "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.partition__I\" BIGINT, " +
                        "\"sqlg_schema.vertex__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"), " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));",
                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_partition\"(" +
                        "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.partition__I\" BIGINT, " +
                        "\"sqlg_schema.edge__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"), " +
                        "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));",
                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_partition_partition\"(" +
                        "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.partition__I\" BIGINT, " +
                        "\"sqlg_schema.partition__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"), " +
                        "FOREIGN KEY (\"sqlg_schema.partition__O\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"));",

                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_identifier\"(" +
                        "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.property__I\" BIGINT, " +
                        "\"sqlg_schema.vertex__O\" BIGINT, " +
                        "\"identifier_index\" INTEGER, " +
                        "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\") ON DELETE CASCADE, " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE);",
                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_identifier\"(" +
                        "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.property__I\" BIGINT, " +
                        "\"sqlg_schema.edge__O\" BIGINT, " +
                        "\"identifier_index\" INTEGER, " +
                        "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\") ON DELETE CASCADE, " +
                        "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\") ON DELETE CASCADE);",

                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_distribution\"(" +
                        "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.property__I\" BIGINT, " +
                        "\"sqlg_schema.vertex__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\") ON DELETE CASCADE, " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE);",
                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_colocate\"(" +
                        "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.vertex__I\" BIGINT, " +
                        "\"sqlg_schema.vertex__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE, " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE);",
                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_distribution\"(" +
                        "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.property__I\" BIGINT, " +
                        "\"sqlg_schema.edge__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\") ON DELETE CASCADE, " +
                        "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\") ON DELETE CASCADE);",
                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_colocate\"(" +
                        "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.vertex__I\" BIGINT, " +
                        "\"sqlg_schema.edge__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\") ON DELETE CASCADE, " +
                        "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\") ON DELETE CASCADE);"
        );
    }

    @Override
    public List<String> addHashPartitionColumns() {
        return List.of(
                "ALTER TABLE \"sqlg_schema\".\"V_partition\" ADD COLUMN \"modulus\" INTEGER;",
                "ALTER TABLE \"sqlg_schema\".\"V_partition\" ADD COLUMN \"remainder\" INTEGER;"
        );
    }

    @Override
    public String addDbVersionToGraph(DatabaseMetaData metadata) {
        try {
            return "ALTER TABLE \"sqlg_schema\".\"V_graph\" ADD COLUMN \"dbVersion\" LONGVARCHAR DEFAULT '" + metadata.getDatabaseProductVersion() + "';";
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void grantReadOnlyUserPrivilegesToSqlgSchemas(SqlgGraph sqlgGraph) {
        Connection conn = sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute("CREATE USER \"sqlgReadOnly\" PASSWORD 'sqlgReadOnly'");
            statement.execute("CREATE ROLE \"READ_ONLY\"");
            statement.execute("GRANT READ_ONLY TO \"sqlgReadOnly\"");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"V_graph\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"V_schema\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"V_vertex\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"V_edge\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"V_partition\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"V_property\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"V_index\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_schema_vertex\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_in_edges\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_out_edges\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_vertex_property\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_edge_property\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_vertex_identifier\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_edge_identifier\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_vertex_partition\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_edge_partition\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_partition_partition\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_vertex_distribution\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_vertex_colocate\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_edge_distribution\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_edge_colocate\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_vertex_index\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_edge_index\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"E_index_property\" TO READ_ONLY");
            statement.execute("GRANT SELECT ON TABLE \"sqlg_schema\".\"V_log\" TO READ_ONLY");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isTimestampz(String typeName) {
        String localTypeName = typeName.toLowerCase();
        switch (localTypeName) {
            case "timestamp with time zone":
            case "timestamp with time zone array":
            case "time with time zone":
            case "time with time zone array":
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean canUserCreateSchemas(SqlgGraph sqlgGraph) {
        //this.deadlocks
//        Connection connection = sqlgGraph.tx().getConnection();
//        String testSchema = "TESTTHISANDTHAT";
//        try (Statement s = connection.createStatement()) {
//            s.execute(createSchemaStatement(testSchema));
//            s.execute("DROP SCHEMA " + maybeWrapInQoutes(testSchema));
//            return true;
//        } catch (SQLException e) {
//            return false;
//        }
        return true;
    }

    @Override
    public String renameColumn(String schema, String table, String column, String newName) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        sql.append(maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(maybeWrapInQoutes(table));
        sql.append(" ALTER COLUMN ");
        sql.append(maybeWrapInQoutes(column));
        sql.append(" RENAME TO ");
        sql.append(maybeWrapInQoutes(newName));
        if (needsSemicolon()) {
            sql.append(";");
        }
        return sql.toString();
    }
}
