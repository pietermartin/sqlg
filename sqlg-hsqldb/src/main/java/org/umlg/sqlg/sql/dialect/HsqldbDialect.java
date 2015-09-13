package org.umlg.sqlg.sql.dialect;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.umlg.sqlg.structure.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.*;
import java.util.*;

/**
 * Date: 2014/07/16
 * Time: 3:09 PM
 */
public class HsqldbDialect extends BaseSqlDialect implements SqlDialect {

    public HsqldbDialect(Configuration configurator) {
        super(configurator);
    }

    @Override
    public Set<String> getDefaultSchemas() {
        return new HashSet<>(Arrays.asList("PUBLIC", "INFORMATION_SCHEMA", "SYSTEM_LOBS"));
    }

    @Override
    public String getPublicSchema() {
        return "PUBLIC";
    }

    @Override
    public String existIndexQuery(SchemaTable schemaTable, String prefix, String indexName) {
        StringBuilder sb = new StringBuilder("SELECT * FROM INFORMATION_SCHEMA.SYSTEM_INDEXINFO WHERE TABLE_SCHEM = '");
        sb.append(schemaTable.getSchema());
        sb.append("' AND  TABLE_NAME = '");
        sb.append(prefix);
        sb.append(schemaTable.getTable());
        sb.append("' AND INDEX_NAME = '");
        sb.append(indexName);
        sb.append("'");
        return sb.toString();
    }

    @Override
    public Map<SchemaTable, Pair<Long, Long>> flushVertexCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Triple<String, String, Map<String, Object>>>>> vertexCache) {
        throw new UnsupportedOperationException("Batch processing is not supported by hsqldb.");
    }

    @Override
    public void flushEdgeCache(SqlgGraph sqlgGraph, Map<SchemaTable, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> edgeCache) {
        throw new UnsupportedOperationException("Batch processing is not supported by hsqldb.");
    }

    @Override
    public void flushVertexLabelCache(SqlgGraph sqlgGraph, Map<SqlgVertex, Pair<String, String>> vertexOutInLabelMap) {
        throw new UnsupportedOperationException("Batch processing is not supported by hsqldb.");
    }

    @Override
    public String getBatchNull() {
        throw new UnsupportedOperationException("Batch processing is not supported by hsqldb.");
    }

    @Override
    public void flushVertexPropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexPropertyCache) {
        throw new UnsupportedOperationException("Batch processing is not supported by hsqldb.");
    }

    @Override
    public void flushEdgePropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>>> edgePropertyCache) {
        throw new UnsupportedOperationException("Batch processing is not supported by hsqldb.");
    }

    @Override
    public void flushRemovedVertices(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgVertex>> removeVertexCache) {
        throw new UnsupportedOperationException("Batch processing is not supported by hsqldb.");
    }

    @Override
    public void flushRemovedEdges(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgEdge>> removeEdgeCache) {
        throw new UnsupportedOperationException("Batch processing is not supported by hsqldb.");
    }

    @Override
    public boolean supportsTransactionalSchema() {
        return false;
    }

    @Override
    public String getJdbcDriver() {
        return "org.hsqldb.jdbc.JDBCDriver";
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
        switch (propertyType) {
            case BOOLEAN:
                return new String[]{"BOOLEAN"};
            case BYTE:
                return new String[]{"TINYINT"};
            case SHORT:
                return new String[]{"SMALLINT"};
            case INTEGER:
                return new String[]{"INTEGER"};
            case LONG:
                return new String[]{"BIGINT"};
            case FLOAT:
                return new String[]{"REAL"};
            case DOUBLE:
                return new String[]{"DOUBLE"};
            case LOCALDATE:
                return new String[]{"DATE"};
            case LOCALDATETIME:
                return new String[]{"TIMESTAMP WITH TIME ZONE"};
            case ZONEDDATETIME:
                return new String[]{"TIMESTAMP WITH TIME ZONE", "LONGVARCHAR"};
            case LOCALTIME:
                return new String[]{"TIME WITH TIME ZONE"};
            case PERIOD:
                return new String[]{"INTEGER", "INTEGER", "INTEGER"};
            case DURATION:
                return new String[]{"BIGINT", "INTEGER"};
            case STRING:
                return new String[]{"LONGVARCHAR"};
            case JSON:
                throw new IllegalStateException("HSQLDB does not support json types, use good ol string instead!");
            case POINT:
                throw new IllegalStateException("HSQLDB does not support gis types!");
            case BYTE_ARRAY:
                return new String[]{"LONGVARBINARY"};
            case BOOLEAN_ARRAY:
                return new String[]{"BOOLEAN ARRAY DEFAULT ARRAY[]"};
            case SHORT_ARRAY:
                return new String[]{"SMALLINT ARRAY DEFAULT ARRAY[]"};
            case INTEGER_ARRAY:
                return new String[]{"INTEGER ARRAY DEFAULT ARRAY[]"};
            case LONG_ARRAY:
                return new String[]{"BIGINT ARRAY DEFAULT ARRAY[]"};
            case FLOAT_ARRAY:
                return new String[]{"REAL ARRAY DEFAULT ARRAY[]"};
            case DOUBLE_ARRAY:
                return new String[]{"DOUBLE ARRAY DEFAULT ARRAY[]"};
            case STRING_ARRAY:
                return new String[]{"LONGVARCHAR ARRAY DEFAULT ARRAY[]"};
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    @Override
    public int propertyTypeToJavaSqlType(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                return Types.BOOLEAN;
            case BYTE:
                return Types.TINYINT;
            case SHORT:
                return Types.SMALLINT;
            case INTEGER:
                return Types.INTEGER;
            case LONG:
                return Types.BIGINT;
            case FLOAT:
                return Types.REAL;
            case DOUBLE:
                return Types.DOUBLE;
            case STRING:
                return Types.CLOB;
            case BYTE_ARRAY:
                return Types.ARRAY;
            case BOOLEAN_ARRAY:
                return Types.ARRAY;
            case SHORT_ARRAY:
                return Types.ARRAY;
            case INTEGER_ARRAY:
                return Types.ARRAY;
            case LONG_ARRAY:
                return Types.ARRAY;
            case FLOAT_ARRAY:
                return Types.ARRAY;
            case DOUBLE_ARRAY:
                return Types.ARRAY;
            case STRING_ARRAY:
                return Types.ARRAY;
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    @Override
    public PropertyType sqlTypeToPropertyType(int sqlType, String typeName) {
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
            case Types.VARBINARY:
                return PropertyType.BYTE_ARRAY;
            case Types.ARRAY:
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
                    default:
                        if (typeName.contains("VARCHAR") && typeName.contains("ARRAY")) {
                            return PropertyType.STRING_ARRAY;
                        } else {
                            throw new RuntimeException(String.format("Array type not supported sqlType = %s and typeName = %s", new String[]{String.valueOf(sqlType), typeName}));
                        }
                }
            default:
                throw new IllegalStateException("Unknown sqlType " + sqlType);
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
        switch (propertyType) {
            case BOOLEAN_ARRAY:
                return "BOOLEAN";
            case SHORT_ARRAY:
                return "SMALLINT";
            case INTEGER_ARRAY:
                return "INTEGER";
            case LONG_ARRAY:
                return "BIGINT";
            case DOUBLE_ARRAY:
                return "DOUBLE";
            case STRING_ARRAY:
                return "VARCHAR";
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
    public List<String> getSpacialRefTable() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public List<String> getGisSchemas() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public void setJson(PreparedStatement preparedStatement, int parameterStartIndex, JsonNode right) {
        throw new IllegalStateException("Hsqldb does not support json types, this should not have happened!");
    }
    @Override
    public void setPoint(PreparedStatement preparedStatement, int parameterStartIndex, Object point) {
        throw new IllegalStateException("Hsqldb does not support gis types, this should not have happened!");
    }

    @Override
    public void handleOther(Map<String, Object> properties, String columnName, Object o) {
        throw new IllegalStateException("Hsqldb does not support other types, this should not have happened!");
    }
}
