package org.umlg.sqlg.mssqlserver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.umlg.sqlg.sql.dialect.BaseSqlDialect;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.*;
import java.util.*;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.tuple.Triple;

/**
 * @author Kevin Schmidt
 * @since 1.3.3
 */
public class MSSqlServerDialect extends BaseSqlDialect {

    public MSSqlServerDialect() {
        super();
    }

    @Override
    public String dialectName() {
        return "MSSqlServerDialect";
    }

    @Override
    public boolean needsSchemaDropCascade() {
        return false;
    }

    @Override
    public Set<String> getDefaultSchemas() {
        return ImmutableSet.of("GRAPH");
    }
    
    @Override
    public String createSchemaStatement() {
    	// if ever schema is created outside of sqlg while the graph is already instantiated
    	return "CREATE SCHEMA IF NOT EXISTS ";
    }

    @Override
    public boolean needForeignKeyIndex() {
        return true;
    }

    @Override
    public PropertyType sqlTypeToPropertyType(SqlgGraph sqlgGraph, String schema, String table, String column,
            int sqlType, String typeName, ListIterator<Triple<String, Integer, String>> metaDataIter) {
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
                return PropertyType.byte_ARRAY;
            case Types.ARRAY:
                //H2 supports just an array - we cannot specify the element type, so let's just pick
                //something...
                return PropertyType.JSON_ARRAY;
            default:
                throw new IllegalStateException("Unknown sqlType " + sqlType);
        }
    }

    @Override
    public PropertyType sqlArrayTypeNameToPropertyType(String typeName, SqlgGraph sqlgGraph, String schema, String table, String columnName, ListIterator<Triple<String, Integer, String>> metaDataIter) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void validateProperty(Object key, Object value) {
        if (value instanceof String || value instanceof String[]) {
            return;
        }
        if (value instanceof Character || value instanceof Character[]) {
            return;
        }
        if (value instanceof Boolean || value instanceof Boolean[] || value instanceof boolean[]) {
            return;
        }
        if (value instanceof Byte || value instanceof Byte[] || value instanceof byte[]) {
            return;
        }
        if (value instanceof Short || value instanceof Short[] || value instanceof short[]) {
            return;
        }
        if (value instanceof Integer || value instanceof Integer[] || value instanceof int[]) {
            return;
        }
        if (value instanceof Long || value instanceof Long[] || value instanceof long[]) {
            return;
        }
        if (value instanceof Double || value instanceof Double[] || value instanceof double[]) {
            return;
        }
        if (value instanceof Float || value instanceof Float[] || value instanceof float[]) {
            return;
        }
        if (value instanceof LocalDate || value instanceof LocalDate[]) {
            return;
        }
        if (value instanceof LocalDateTime || value instanceof LocalDateTime[]) {
            return;
        }
        if (value instanceof ZonedDateTime || value instanceof ZonedDateTime[]) {
            return;
        }
        if (value instanceof LocalTime || value instanceof LocalTime[]) {
            return;
        }
        if (value instanceof Period || value instanceof Period[]) {
            return;
        }
        if (value instanceof Duration || value instanceof Duration[]) {
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
        return "BIGINT IDENTITY NOT NULL PRIMARY KEY";
    }

    @Override
    public String[] propertyTypeToSqlDefinition(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                return new String[]{"BOOLEAN"};
            case BYTE:
                return new String[]{"TINYINT"};
            case byte_ARRAY:
                return new String[]{"BINARY"};
            case BYTE_ARRAY:
                return new String[]{"BINARY"};
            case DOUBLE:
                return new String[]{"DOUBLE"};
            case DURATION:
                return new String[]{"BIGINT", "INT"};
            case FLOAT:
                return new String[]{"REAL"};
            case INTEGER:
                return new String[]{"INT"};
            case LOCALDATE:
                return new String[]{"DATE"};
            case LOCALDATETIME:
                return new String[]{"DATETIME"};
            case LOCALTIME:
                return new String[]{"TIME"};
            case LONG:
                return new String[]{"BIGINT"};
            case PERIOD:
                return new String[]{"INT", "INT", "INT"};
            case SHORT:
                return new String[]{"SMALLINT"};
            case STRING:
                return new String[]{"VARCHAR(255)"};
            case ZONEDDATETIME:
                return new String[]{"DATETIME", "VARCHAR(255)"};
            case BOOLEAN_ARRAY: case boolean_ARRAY: case DOUBLE_ARRAY: case double_ARRAY:
            case FLOAT_ARRAY: case float_ARRAY: case int_ARRAY: case INTEGER_ARRAY:
            case LOCALDATE_ARRAY: case LOCALDATETIME_ARRAY: case LOCALTIME_ARRAY: case LONG_ARRAY:
            case long_ARRAY: case SHORT_ARRAY: case short_ARRAY: case STRING_ARRAY:
                return new String[]{"ARRAY"};
            case DURATION_ARRAY:
                return new String[]{"ARRAY", "ARRAY"};
            case PERIOD_ARRAY:
                return new String[]{"ARRAY", "ARRAY", "ARRAY"};
            case ZONEDDATETIME_ARRAY:
                return new String[]{"ARRAY", "ARRAY"};
            case JSON:
                throw new IllegalStateException("H2 does not support json types, use good ol string instead!");
            case POINT:
                throw new IllegalStateException("H2 does not support gis types!");
            case POLYGON:
                throw new IllegalStateException("H2 does not support gis types!");
            case GEOGRAPHY_POINT:
                throw new IllegalStateException("H2 does not support gis types!");
            case GEOGRAPHY_POLYGON:
                throw new IllegalStateException("H2 does not support gis types!");
            case LINESTRING:
                throw new IllegalStateException("H2 does not support gis types!");
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
            case LOCALDATETIME:
                return Types.TIMESTAMP;
            case LOCALDATE:
                return Types.DATE;
            case LOCALTIME:
                return Types.TIME;
            case JSON:
                return Types.VARCHAR;
            case byte_ARRAY:
                return Types.BINARY;
            case BYTE_ARRAY:
                return Types.BINARY;
            case BOOLEAN_ARRAY: case boolean_ARRAY: case DOUBLE_ARRAY: case double_ARRAY:
            case DURATION_ARRAY: case FLOAT_ARRAY: case float_ARRAY: case int_ARRAY: case INTEGER_ARRAY:
            case LOCALDATE_ARRAY: case LOCALDATETIME_ARRAY: case LOCALTIME_ARRAY: case LONG_ARRAY: case long_ARRAY:
            case SHORT_ARRAY: case short_ARRAY: case STRING_ARRAY:
                return Types.ARRAY;
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    @Override
    public String getForeignKeyTypeDefinition() {
        return "BIGINT";
    }

    @Override
    public String getArrayDriverType(PropertyType arrayType) {
        return "ARRAY";
    }

    @Override
    public void putJsonObject(ObjectNode obj, String columnName, int sqlType, Object o) {
        switch (sqlType) {
            case Types.ARRAY:
                try {
                    ArrayNode arrayNode = obj.putArray(columnName);
                    java.sql.Array sqlA = (java.sql.Array) o;
                    Object a = sqlA.getArray();

                    int len = Array.getLength(a);
                    if (len > 0) {
                        PropertyType pt = PropertyType.from(Array.get(a, 0));

                        for (int i = 0; i < len; ++i) {
                            Object v = Array.get(a, i);
                            switch (pt) {
                                case BOOLEAN:
                                    arrayNode.add((Boolean) v);
                                    break;
                                case BYTE:
                                    arrayNode.add((Byte) v);
                                    break;
                                case DOUBLE:
                                    arrayNode.add((Double) v);
                                    break;
                                case FLOAT:
                                    arrayNode.add((Float) v);
                                    break;
                                case INTEGER:
                                    arrayNode.add((Integer) v);
                                    break;
                                case LONG:
                                    arrayNode.add((Long) v);
                                    break;
                                case SHORT:
                                    arrayNode.add((Short) v);
                                    break;
                                case STRING:
                                    arrayNode.add((String) v);
                                    break;
                            }
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                break;
            default:
                super.putJsonObject(obj, columnName, sqlType, o);
        }
    }

    @Override
    public void putJsonMetaObject(ObjectMapper mapper, ArrayNode metaNodeArray, String columnName, int sqlType,
            Object o) {
        switch (sqlType) {
            case Types.ARRAY:
                try {
                    ObjectNode metaNode = mapper.createObjectNode();
                    metaNode.put("name", columnName);
                    metaNodeArray.add(metaNode);

                    java.sql.Array sqlA = (java.sql.Array) o;
                    Object a = sqlA.getArray();
                    if (Array.getLength(a) > 0) {
                        PropertyType pt = PropertyType.from(Array.get(a, 0));
                        switch (pt) {
                            case BOOLEAN:
                                metaNode.put("type", PropertyType.boolean_ARRAY.name());
                                break;
                            case SHORT:
                                metaNode.put("type", PropertyType.short_ARRAY.name());
                                break;
                            case INTEGER:
                                metaNode.put("type", PropertyType.int_ARRAY.name());
                                break;
                            case LONG:
                                metaNode.put("type", PropertyType.long_ARRAY.name());
                                break;
                            case FLOAT:
                                metaNode.put("type", PropertyType.float_ARRAY.name());
                                break;
                            case DOUBLE:
                                metaNode.put("type", PropertyType.double_ARRAY.name());
                                break;
                            case STRING:
                                metaNode.put("type", PropertyType.STRING_ARRAY.name());
                                break;
                            default:
                                throw new IllegalStateException("Unknown array sqlType " + sqlType);
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                break;
            default:
                super.putJsonMetaObject(mapper, metaNodeArray, columnName, sqlType, o);
        }
    }

    @Override
    public String existIndexQuery(SchemaTable schemaTable, String prefix, String indexName) {
        StringBuilder sb = new StringBuilder("SELECT * FROM sys.indexes i JOIN sys.tables t ON i.object_id = t.object_id JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = '");
        sb.append(schemaTable.getSchema());
        sb.append("' AND t.name = '");
        sb.append(prefix);
        sb.append(schemaTable.getTable());
        sb.append("' AND i.name = '");
        sb.append(indexName);
        sb.append("'");
        return sb.toString();
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
    public void setJson(PreparedStatement preparedStatement, int parameterStartIndex, JsonNode right) {
        throw new IllegalStateException("H2 doesn't support storing JSON.");
    }

    @Override
    public void handleOther(Map<String, Object> properties, String columnName, Object o, PropertyType propertyType) {
        throw new IllegalStateException("H2 doesn't support other types.");
    }

    @Override
    public void setPoint(PreparedStatement preparedStatement, int parameterStartIndex, Object point) {
        throw new IllegalStateException("H2 does not support gis types, this should not have happened!");
    }

    @Override
    public void setLineString(PreparedStatement preparedStatement, int parameterStartIndex, Object lineString) {
        throw new IllegalStateException("H2 does not support gis types, this should not have happened!");
    }

    @Override
    public void setPolygon(PreparedStatement preparedStatement, int parameterStartIndex, Object point) {
        throw new IllegalStateException("H2 does not support gis types, this should not have happened!");
    }

    @Override
    public void setGeographyPoint(PreparedStatement preparedStatement, int parameterStartIndex, Object point) {
        throw new IllegalStateException("H2 does not support gis types, this should not have happened!");
    }

    @Override
    public <T> T getGis(SqlgGraph sqlgGraph) {
        throw new IllegalStateException("H2 does not support gis types, this should not have happened!");
    }

    @Override
    public void lockTable(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix) {
        throw new UnsupportedOperationException("H2 does not support table locking!");
    }

    @Override
    public void alterSequenceCacheSize(SqlgGraph sqlgGraph, SchemaTable schemaTable, String sequence, int batchSize) {
        throw new UnsupportedOperationException("Hsqldb does not support alterSequenceCacheSize!");
    }

    @Override
    public long nextSequenceVal(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix) {
        throw new UnsupportedOperationException("H2 does not support batch mode!");
    }

    @Override
    public long currSequenceVal(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix) {
        throw new UnsupportedOperationException("H2 does not support batch mode!");
    }

    @Override
    public String sequenceName(SqlgGraph sqlgGraph, SchemaTable outSchemaTable, String prefix) {
        throw new UnsupportedOperationException("H2 does not support sequenceName!");
    }

    @Override
    public boolean supportsBulkWithinOut() {
        return false;
    }

    @Override
    public boolean supportsTransactionalSchema() {
        return false;
    }

    @Override
    public String afterCreateTemporaryTableStatement() {
        return "";
    }

    @Override
    public List<String> sqlgTopologyCreationScripts() {
        List<String> result = new ArrayList<>();
        result.add("CREATE SCHEMA \"sqlg_schema\";");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_schema\" (\"ID\" BIGINT IDENTITY PRIMARY KEY, \"createdOn\" DATETIME, \"name\" VARCHAR(255));");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_vertex\" (\"ID\" BIGINT IDENTITY PRIMARY KEY, \"createdOn\" DATETIME, \"name\" VARCHAR(255), \"schemaVertex\" VARCHAR(255));");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_edge\" (\"ID\" BIGINT IDENTITY PRIMARY KEY, \"createdOn\" DATETIME, \"name\" VARCHAR(255));");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_property\" (\"ID\" BIGINT IDENTITY PRIMARY KEY, \"createdOn\" DATETIME, \"name\" VARCHAR(255), \"type\" VARCHAR(255));");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_schema_vertex\"(\"ID\" BIGINT IDENTITY PRIMARY KEY, \"sqlg_schema.vertex__I\" BIGINT, \"sqlg_schema.schema__O\" BIGINT, FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"),  FOREIGN KEY (\"sqlg_schema.schema__O\") REFERENCES \"sqlg_schema\".\"V_schema\" (\"ID\"));");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_in_edges\"(\"ID\" BIGINT IDENTITY PRIMARY KEY, \"sqlg_schema.edge__I\" BIGINT, \"sqlg_schema.vertex__O\" BIGINT, FOREIGN KEY (\"sqlg_schema.edge__I\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"),  FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_out_edges\"(\"ID\" BIGINT IDENTITY PRIMARY KEY, \"sqlg_schema.edge__I\" BIGINT, \"sqlg_schema.vertex__O\" BIGINT, FOREIGN KEY (\"sqlg_schema.edge__I\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"),  FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_vertex_property\"(\"ID\" BIGINT IDENTITY PRIMARY KEY, \"sqlg_schema.property__I\" BIGINT, \"sqlg_schema.vertex__O\" BIGINT, FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"),  FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_edge_property\"(\"ID\" BIGINT IDENTITY PRIMARY KEY, \"sqlg_schema.property__I\" BIGINT, \"sqlg_schema.edge__O\" BIGINT, FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"),  FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));");
        return result;
    }

    @Override
    public String sqlgAddPropertyIndexTypeColumn() {
        return "ALTER TABLE \"sqlg_schema\".\"V_property\" ADD COLUMN \"index_type\" TEXT DEFAULT 'NONE';";
    }

    @Override
    public Object convertArray(PropertyType propertyType, java.sql.Array array) throws SQLException {
        switch (propertyType) {
            case BOOLEAN_ARRAY:
                return SqlgUtil.convertObjectArrayToBooleanArray((Object[]) array.getArray());
            case boolean_ARRAY:
                return SqlgUtil.convertObjectArrayToBooleanPrimitiveArray((Object[]) array.getArray());
            case SHORT_ARRAY:
                return SqlgUtil.convertObjectOfShortsArrayToShortArray((Object[]) array.getArray());
            case short_ARRAY:
                return SqlgUtil.convertObjectOfShortsArrayToShortPrimitiveArray((Object[]) array.getArray());
            case INTEGER_ARRAY:
                return SqlgUtil.convertObjectOfIntegersArrayToIntegerArray((Object[]) array.getArray());
            case int_ARRAY:
                return SqlgUtil.convertObjectOfIntegersArrayToIntegerPrimitiveArray((Object[]) array.getArray());
            case LONG_ARRAY:
                return SqlgUtil.convertObjectOfLongsArrayToLongArray((Object[]) array.getArray());
            case long_ARRAY:
                return SqlgUtil.convertObjectOfLongsArrayToLongPrimitiveArray((Object[]) array.getArray());
            case DOUBLE_ARRAY:
                return SqlgUtil.convertObjectOfDoublesArrayToDoubleArray((Object[]) array.getArray());
            case double_ARRAY:
                return SqlgUtil.convertObjectOfDoublesArrayToDoublePrimitiveArray((Object[]) array.getArray());
            case FLOAT_ARRAY:
                return SqlgUtil.convertObjectOfFloatsArrayToFloatArray((Object[]) array.getArray());
            case float_ARRAY:
                return SqlgUtil.convertObjectOfFloatsArrayToFloatPrimitiveArray((Object[]) array.getArray());
            case STRING_ARRAY:
                return SqlgUtil.convertObjectOfStringsArrayToStringArray((Object[]) array.getArray());
            case LOCALDATETIME_ARRAY:
                Object[] timestamps = (Object[]) array.getArray();
                return SqlgUtil.copyObjectArrayOfTimestampToLocalDateTime(timestamps, new LocalDateTime[(timestamps).length]);
            case LOCALDATE_ARRAY:
                Object[] dates = (Object[]) array.getArray();
                if (dates != null && dates.length > 0 && dates[0] instanceof Timestamp) {
                    return SqlgUtil.copyObjectArrayOfTimestampToLocalDate(dates, new LocalDate[dates.length]);
                } else {
                    return SqlgUtil.copyObjectArrayOfDateToLocalDate(dates, new LocalDate[dates.length]);
                }
            case LOCALTIME_ARRAY:
                Object[] times = (Object[]) array.getArray();
                return SqlgUtil.copyObjectArrayOfTimeToLocalTime(times, new LocalTime[times.length]);
            default:
                throw new IllegalStateException("Unhandled property type " + propertyType.name());
        }
    }

    @Override
    public void setArray(PreparedStatement statement, int index, PropertyType type,
            Object[] values) throws SQLException {
        statement.setObject(index, values);
    }

    @Override
    public String getPublicSchema() {
        return "GRAPH";
    }

    @Override
    public String getRangeClause(Range<Long> r) {
        return "OFFSET " + r.getMinimum() + " ROWS FETCH NEXT " + (r.getMaximum() - r.getMinimum()) + " ROWS ONLY";
    }
}
