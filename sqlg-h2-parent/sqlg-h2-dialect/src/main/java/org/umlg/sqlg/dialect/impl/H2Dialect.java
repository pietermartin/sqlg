package org.umlg.sqlg.dialect.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.umlg.sqlg.sql.dialect.BaseSqlDialect;
import org.umlg.sqlg.sql.parse.ColumnList;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgExceptions;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.Preconditions;
import org.umlg.sqlg.util.SqlgUtil;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.*;
import java.time.*;
import java.util.*;

import static org.umlg.sqlg.structure.PropertyType.*;
import static org.umlg.sqlg.structure.topology.Topology.*;

/**
 * @author Lukas Krejci
 * @since 1.3.0
 */
public class H2Dialect extends BaseSqlDialect {

    public H2Dialect() {
        super();
    }

    @Override
    public boolean supportsCascade() {
        return true;
    }

    @Override
    public boolean isH2() {
        return true;
    }

    @Override
    public boolean supportsBatchMode() {
        return true;
    }

    @Override
    public String dialectName() {
        return "H2Dialect";
    }

    @Override
    public boolean needsSchemaDropCascade() {
        return true;
    }

    @Override
    public Set<String> getInternalSchemas() {
        return Set.of("INFORMATION_SCHEMA");
    }

    @Override
    public String createSchemaStatement(String schemaName) {
        // if ever schema is created outside of sqlg while the graph is already instantiated
        return "CREATE SCHEMA IF NOT EXISTS " + maybeWrapInQoutes(schemaName);
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
                return PropertyType.BYTE_ARRAY;
            case Types.ARRAY:
                return sqlArrayTypeNameToPropertyType(typeName, sqlgGraph, schema, table, column, metaDataIter);
            default:
                throw new IllegalStateException("Unknown sqlType " + sqlType);
        }
    }

    /**
     * All this is because H2 does not return the TYPE_NAME for column meta data.
     * The strategy is to actualy query the table get the column's value and interrogate it to get its type.
     * If the column has no data then we are stuffed and an exception is thrown.
     *
     * @param typeName
     * @param sqlgGraph
     * @param schema
     * @param table
     * @param columnName
     * @param metaDataIter
     * @return
     */
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
            case "DOUBLE PRECISION ARRAY":
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
            case "CHARACTER VARYING ARRAY":
                return PropertyType.STRING_ARRAY;
            case "REAL ARRAY":
                return FLOAT_ARRAY;
            default:
                throw new RuntimeException(String.format("Array type not supported typeName = %s", typeName));
        }
    }


    @Override
    public void validateProperty(Object key, Object value) {
        if (value == null) {
            return;
        }
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
        if (value instanceof BigDecimal || value instanceof BigDecimal[]) {
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
        if (value instanceof JsonNode || value instanceof JsonNode[]) {
            return;
        }
        if (value instanceof UUID) {
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
        return "IDENTITY NOT NULL PRIMARY KEY";
    }

    @Override
    public String[] propertyTypeToSqlDefinition(PropertyType propertyType) {
        return switch (propertyType.ordinal()) {
            case BOOLEAN_ORDINAL -> new String[]{"BOOLEAN"};
            case BYTE_ORDINAL -> new String[]{"TINYINT"};
            case byte_ARRAY_ORDINAL -> new String[]{"VARBINARY"};
            case BYTE_ARRAY_ORDINAL -> new String[]{"VARBINARY"};
            case DOUBLE_ORDINAL -> new String[]{"DOUBLE"};
            case BIG_DECIMAL_ORDINAL -> new String[]{"DOUBLE"};
            case DURATION_ORDINAL -> new String[]{"BIGINT", "INT"};
            case FLOAT_ORDINAL -> new String[]{"REAL"};
            case INTEGER_ORDINAL -> new String[]{"INT"};
            case LOCALDATE_ORDINAL -> new String[]{"DATE"};
            case LOCALDATETIME_ORDINAL -> new String[]{"TIMESTAMP"};
            case LOCALTIME_ORDINAL -> new String[]{"TIME"};
            case LONG_ORDINAL -> new String[]{"BIGINT"};
            case PERIOD_ORDINAL -> new String[]{"INT", "INT", "INT"};
            case SHORT_ORDINAL -> new String[]{"SMALLINT"};
            case STRING_ORDINAL -> new String[]{"VARCHAR"};
            case VARCHAR_ORDINAL -> new String[]{"VARCHAR(" + propertyType.getLength() + ")"};
            case UUID_ORDINAL -> new String[]{"UUID"};
            case ZONEDDATETIME_ORDINAL -> new String[]{"TIMESTAMP", "VARCHAR"};
            case BOOLEAN_ARRAY_ORDINAL -> new String[]{"BOOLEAN ARRAY"};
            case boolean_ARRAY_ORDINAL -> new String[]{"BOOLEAN ARRAY"};
            case DOUBLE_ARRAY_ORDINAL -> new String[]{"DOUBLE ARRAY"};
            case BIG_DECIMAL_ARRAY_ORDINAL -> new String[]{"DOUBLE ARRAY"};
            case double_ARRAY_ORDINAL -> new String[]{"DOUBLE ARRAY"};
            case FLOAT_ARRAY_ORDINAL -> new String[]{"REAL ARRAY"};
            case float_ARRAY_ORDINAL -> new String[]{"REAL ARRAY"};
            case int_ARRAY_ORDINAL -> new String[]{"INT ARRAY"};
            case INTEGER_ARRAY_ORDINAL -> new String[]{"INT ARRAY"};
            case LOCALDATE_ARRAY_ORDINAL -> new String[]{"DATE ARRAY"};
            case LOCALDATETIME_ARRAY_ORDINAL -> new String[]{"TIMESTAMP ARRAY"};
            case LOCALTIME_ARRAY_ORDINAL -> new String[]{"TIME ARRAY"};
            case LONG_ARRAY_ORDINAL -> new String[]{"BIGINT ARRAY"};
            case long_ARRAY_ORDINAL -> new String[]{"BIGINT ARRAY"};
            case SHORT_ARRAY_ORDINAL -> new String[]{"SMALLINT ARRAY"};
            case short_ARRAY_ORDINAL -> new String[]{"SMALLINT ARRAY"};
            case STRING_ARRAY_ORDINAL -> new String[]{"VARCHAR ARRAY"};
            case DURATION_ARRAY_ORDINAL -> new String[]{"BIGINT ARRAY", "INT ARRAY"};
            case PERIOD_ARRAY_ORDINAL -> new String[]{"INT ARRAY", "INT ARRAY", "INT ARRAY"};
            case ZONEDDATETIME_ARRAY_ORDINAL -> new String[]{"TIMESTAMP ARRAY", "VARCHAR ARRAY"};
            case JSON_ORDINAL -> new String[]{"VARCHAR"};
            case JSON_ARRAY_ORDINAL -> new String[]{"VARCHAR ARRAY"};
            case POINT_ORDINAL -> throw new IllegalStateException("H2 does not support gis types!");
            case POLYGON_ORDINAL -> throw new IllegalStateException("H2 does not support gis types!");
            case GEOGRAPHY_POINT_ORDINAL -> throw new IllegalStateException("H2 does not support gis types!");
            case GEOGRAPHY_POLYGON_ORDINAL -> throw new IllegalStateException("H2 does not support gis types!");
            case LINESTRING_ORDINAL -> throw new IllegalStateException("H2 does not support gis types!");
            default -> throw SqlgExceptions.invalidPropertyType(propertyType);
        };
    }

    @Override
    public int[] propertyTypeToJavaSqlType(PropertyType propertyType) {
        return switch (propertyType.ordinal()) {
            case BOOLEAN_ORDINAL -> new int[]{Types.BOOLEAN};
            case BYTE_ORDINAL -> new int[]{Types.TINYINT};
            case SHORT_ORDINAL -> new int[]{Types.SMALLINT};
            case INTEGER_ORDINAL -> new int[]{Types.INTEGER};
            case LONG_ORDINAL -> new int[]{Types.BIGINT};
            case FLOAT_ORDINAL -> new int[]{Types.REAL};
            case DOUBLE_ORDINAL -> new int[]{Types.DOUBLE};
            case BIG_DECIMAL_ORDINAL -> new int[]{Types.DOUBLE};
            case STRING_ORDINAL -> new int[]{Types.CLOB};
            case LOCALDATETIME_ORDINAL -> new int[]{Types.TIMESTAMP};
            case LOCALDATE_ORDINAL -> new int[]{Types.DATE};
            case LOCALTIME_ORDINAL -> new int[]{Types.TIME};
            case ZONEDDATETIME_ORDINAL -> new int[]{Types.TIMESTAMP, Types.CLOB};
            case DURATION_ORDINAL -> new int[]{Types.BIGINT, Types.INTEGER};
            case PERIOD_ORDINAL -> new int[]{Types.INTEGER, Types.INTEGER, Types.INTEGER};
            case JSON_ORDINAL -> new int[]{Types.VARCHAR};
            case byte_ARRAY_ORDINAL -> new int[]{Types.BINARY};
            case BYTE_ARRAY_ORDINAL -> new int[]{Types.BINARY};
            case BOOLEAN_ARRAY_ORDINAL, boolean_ARRAY_ORDINAL, BIG_DECIMAL_ARRAY_ORDINAL, DOUBLE_ARRAY_ORDINAL, double_ARRAY_ORDINAL, FLOAT_ARRAY_ORDINAL, float_ARRAY_ORDINAL, int_ARRAY_ORDINAL, INTEGER_ARRAY_ORDINAL, LOCALDATE_ARRAY_ORDINAL, LOCALDATETIME_ARRAY_ORDINAL, LOCALTIME_ARRAY_ORDINAL, LONG_ARRAY_ORDINAL, long_ARRAY_ORDINAL, SHORT_ARRAY_ORDINAL, short_ARRAY_ORDINAL, STRING_ARRAY_ORDINAL ->
                    new int[]{Types.ARRAY};
            case ZONEDDATETIME_ARRAY_ORDINAL -> new int[]{Types.ARRAY, Types.ARRAY};
            case DURATION_ARRAY_ORDINAL -> new int[]{Types.ARRAY, Types.ARRAY};
            case PERIOD_ARRAY_ORDINAL -> new int[]{Types.ARRAY, Types.ARRAY, Types.ARRAY};
            case JSON_ARRAY_ORDINAL -> new int[]{Types.ARRAY};
            default -> throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        };
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
                            switch (pt.ordinal()) {
                                case BOOLEAN_ORDINAL:
                                    arrayNode.add((Boolean) v);
                                    break;
                                case BYTE_ORDINAL:
                                    arrayNode.add((Byte) v);
                                    break;
                                case DOUBLE_ORDINAL:
                                    arrayNode.add((Double) v);
                                    break;
                                case FLOAT_ORDINAL:
                                    arrayNode.add((Float) v);
                                    break;
                                case INTEGER_ORDINAL:
                                    arrayNode.add((Integer) v);
                                    break;
                                case LONG_ORDINAL:
                                    arrayNode.add((Long) v);
                                    break;
                                case SHORT_ORDINAL:
                                    arrayNode.add((Short) v);
                                    break;
                                case STRING_ORDINAL:
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
                        switch (pt.ordinal()) {
                            case BOOLEAN_ORDINAL:
                                metaNode.put("type", PropertyType.boolean_ARRAY.name());
                                break;
                            case SHORT_ORDINAL:
                                metaNode.put("type", PropertyType.short_ARRAY.name());
                                break;
                            case INTEGER_ORDINAL:
                                metaNode.put("type", PropertyType.int_ARRAY.name());
                                break;
                            case LONG_ORDINAL:
                                metaNode.put("type", PropertyType.long_ARRAY.name());
                                break;
                            case FLOAT_ORDINAL:
                                metaNode.put("type", PropertyType.float_ARRAY.name());
                                break;
                            case DOUBLE_ORDINAL:
                                metaNode.put("type", PropertyType.double_ARRAY.name());
                                break;
                            case STRING_ORDINAL:
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
        StringBuilder sb = new StringBuilder("SELECT * FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_SCHEMA = '");
        sb.append(schemaTable.getSchema());
        sb.append("' AND TABLE_NAME = '");
        sb.append(prefix);
        sb.append(schemaTable.getTable());
        sb.append("' AND INDEX_NAME = '");
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

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_graph\" (" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP, " +
                "\"updatedOn\" TIMESTAMP, " +
                "\"version\" VARCHAR, " +
                "\"dbVersion\" VARCHAR);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_schema\" (" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP, " +
                "\"name\" VARCHAR);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_vertex\" (" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP, " +
                "\"name\" VARCHAR, " +
                "\"schemaVertex\" VARCHAR," +
                "\"partitionType\" VARCHAR, " +
                "\"partitionExpression\" VARCHAR, " +
                "\"shardCount\" INTEGER);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_edge\" (" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP, " +
                "\"name\" VARCHAR, " +
                "\"partitionType\" VARCHAR, " +
                "\"partitionExpression\" VARCHAR, " +
                "\"shardCount\" INTEGER);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_partition\" (" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP, " +
                "\"schemaName\" VARCHAR NOT NULL, " +
                "\"abstractLabelName\" VARCHAR NOT NULL, " +
                "\"name\" VARCHAR NOT NULL, " +
                "\"from\" VARCHAR, " +
                "\"to\" VARCHAR, " +
                "\"in\" VARCHAR, " +
                "\"modulus\" INTEGER, " +
                "\"remainder\" INTEGER, " +
                "\"partitionType\" VARCHAR, " +
                "\"partitionExpression\" VARCHAR);");
        result.add("CREATE UNIQUE INDEX IF NOT EXISTS \"V_schema_abstractLabel_name_idx\" ON \"sqlg_schema\".\"V_partition\" (\"schemaName\", \"abstractLabelName\", \"name\");");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_property\" (" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP, " +
                "\"name\" VARCHAR, " +
                "\"type\" VARCHAR," +
                "\"lowerMultiplicity\" INTEGER NOT NULL," +
                "\"upperMultiplicity\" INTEGER NOT NULL," +
                "\"defaultLiteral\" VARCHAR," +
                "\"checkConstraint\" VARCHAR" +
                ");");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_index\" (" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP, " +
                "\"name\" VARCHAR, " +
                "\"index_type\" VARCHAR);");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_schema_vertex\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.vertex__I\" BIGINT, " +
                "\"sqlg_schema.schema__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.schema__O\") REFERENCES \"sqlg_schema\".\"V_schema\" (\"ID\"));");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_in_edges\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"lowerMultiplicity\" BIGINT, " +
                "\"upperMultiplicity\" BIGINT, " +
                "\"unique\" BOOLEAN, " +
                "\"ordered\" BOOLEAN, " +
                "\"sqlg_schema.edge__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.edge__I\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"),  " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_out_edges\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"lowerMultiplicity\" BIGINT, " +
                "\"upperMultiplicity\" BIGINT, " +
                "\"unique\" BOOLEAN, " +
                "\"ordered\" BOOLEAN, " +
                "\"sqlg_schema.edge__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.edge__I\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"),  " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_property\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"),  " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_property\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"),  " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_identifier\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "\"identifier_index\" INTEGER, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"),  " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_identifier\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "\"identifier_index\" INTEGER, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"),  " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_partition\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.partition__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_partition\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.partition__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_partition_partition\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.partition__I\" BIGINT, " +
                "\"sqlg_schema.partition__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.partition__O\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"));");


        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_distribution\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_colocate\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.vertex__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_distribution\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_colocate\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.vertex__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_index\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.index__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.index__I\") REFERENCES \"sqlg_schema\".\"V_index\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_index\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.index__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.index__I\") REFERENCES \"sqlg_schema\".\"V_index\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_index_property\"(" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.index__O\" BIGINT, " +
                "\"sequence\" INTEGER, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.index__O\") REFERENCES \"sqlg_schema\".\"V_index\" (\"ID\"));");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_log\" (" +
                "\"ID\" IDENTITY PRIMARY KEY, " +
                "\"timestamp\" TIMESTAMP, " +
                "\"pid\" INTEGER, " +
                "\"log\" VARCHAR);");

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
            case BIG_DECIMAL_ARRAY_ORDINAL:
                return SqlgUtil.convertObjectOfDoublesArrayToBigDecimalArray((Object[]) array.getArray());
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
                if (dates != null && dates.length > 0 && dates[0] instanceof Timestamp) {
                    return SqlgUtil.copyObjectArrayOfTimestampToLocalDate(dates, new LocalDate[dates.length]);
                } else {
                    return SqlgUtil.copyObjectArrayOfDateToLocalDate(dates, new LocalDate[dates.length]);
                }
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
        statement.setObject(index, values);
    }

    @Override
    public String getPublicSchema() {
        return "PUBLIC";
    }

    @Override
    public boolean isSystemIndex(String indexName) {
        return indexName.startsWith("PRIMARY_KEY_") || indexName.startsWith("CONSTRAINT_INDEX_");
    }

    @Override
    public boolean supportsFullValueExpression() {
        return false;
    }

    @Override
    public String valueToValuesString(PropertyType propertyType, Object value) {
        throw new RuntimeException("Not yet implemented");
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
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }


    @Override
    public boolean supportsJsonArrayValues() {
        return true;
    }

    @Override
    public String sqlToTurnOffReferentialConstraintCheck(String tableName) {
        return "SET REFERENTIAL_INTEGRITY FALSE";
    }

    @Override
    public String sqlToTurnOnReferentialConstraintCheck(String tableName) {
        return "SET REFERENTIAL_INTEGRITY TRUE";
    }

    @Override
    public List<String> addPartitionTables() {
        return Arrays.asList(
                "ALTER TABLE \"sqlg_schema\".\"V_vertex\" ADD COLUMN \"partitionType\" VARCHAR DEFAULT 'NONE';",
                "ALTER TABLE \"sqlg_schema\".\"V_vertex\" ADD COLUMN \"partitionExpression\" VARCHAR;",
                "ALTER TABLE \"sqlg_schema\".\"V_vertex\" ADD COLUMN \"shardCount\" INTEGER;",
                "ALTER TABLE \"sqlg_schema\".\"V_edge\" ADD COLUMN \"partitionType\" VARCHAR DEFAULT 'NONE';",
                "ALTER TABLE \"sqlg_schema\".\"V_edge\" ADD COLUMN \"partitionExpression\" VARCHAR;",
                "ALTER TABLE \"sqlg_schema\".\"V_edge\" ADD COLUMN \"shardCount\" INTEGER;",
                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_partition\" (" +
                        "\"ID\" BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, " +
                        "\"createdOn\" TIMESTAMP, " +
                        "\"name\" VARCHAR, " +
                        "\"from\" VARCHAR, " +
                        "\"to\" VARCHAR, " +
                        "\"in\" VARCHAR, " +
                        "\"partitionType\" VARCHAR, " +
                        "\"partitionExpression\" VARCHAR);",
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
                        "\"ID\" IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.property__I\" BIGINT, " +
                        "\"sqlg_schema.vertex__O\" BIGINT, " +
                        "\"identifier_index\" INTEGER, " +
                        "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"),  " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));",
                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_identifier\"(" +
                        "\"ID\" IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.property__I\" BIGINT, " +
                        "\"sqlg_schema.edge__O\" BIGINT, " +
                        "\"identifier_index\" INTEGER, " +
                        "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"),  " +
                        "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));",
                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_distribution\"(" +
                        "\"ID\" IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.property__I\" BIGINT, " +
                        "\"sqlg_schema.vertex__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"), " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));",
                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_colocate\"(" +
                        "\"ID\" IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.vertex__I\" BIGINT, " +
                        "\"sqlg_schema.vertex__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"), " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));",
                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_distribution\"(" +
                        "\"ID\" IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.property__I\" BIGINT, " +
                        "\"sqlg_schema.edge__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"), " +
                        "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));",
                "CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_colocate\"(" +
                        "\"ID\" IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.vertex__I\" BIGINT, " +
                        "\"sqlg_schema.edge__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"), " +
                        "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));"
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
            return "ALTER TABLE \"sqlg_schema\".\"V_graph\" ADD COLUMN \"dbVersion\" VARCHAR DEFAULT '" + metadata.getDatabaseProductVersion() + "';";
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void grantReadOnlyUserPrivilegesToSqlgSchemas(SqlgGraph sqlgGraph) {
        //Do nothing, we are not testing readOnly on H2
    }

    @Override
    public String toSelectString(boolean partOfDuplicateQuery, ColumnList.Column column, String alias) {
        StringBuilder sb = new StringBuilder();
        if (!partOfDuplicateQuery && column.getAggregateFunction() != null) {
            if (column.getAggregateFunction().equals("avg")) {
                sb.append(column.getAggregateFunction().toUpperCase());
                sb.append("(CAST(");
            } else {
                sb.append(column.getAggregateFunction().toUpperCase());
                sb.append("(");
            }
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
            if (column.getAggregateFunction().equals("avg")) {
                sb.append(" as DOUBLE PRECISION)) AS ").append(maybeWrapInQoutes(alias));
            } else {
                sb.append(" ) AS ").append(maybeWrapInQoutes(alias));
            }
            if (column.getAggregateFunction().equals("avg")) {
                sb.append(", COUNT(1) AS ").append(maybeWrapInQoutes(alias + "_weight"));
            }
        } else {
            sb.append(" AS ").append(maybeWrapInQoutes(alias));
        }
        return sb.toString();
    }

    @Override
    public boolean isTimestampz(String typeName) {
        //H2 is not using timestamps with zones
        return false;
    }

    @Override
    public List<String> addPropertyDefinitions() {
        return List.of(
//                "ALTER TABLE \"sqlg_schema\".\"V_" + SQLG_SCHEMA_PROPERTY + "\" ADD COLUMN \"" + SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER + "\" INTEGER DEFAULT -1 NOT NULL;",
//                "UPDATE \"sqlg_schema\".\"V_" + SQLG_SCHEMA_PROPERTY + "\" set \"" + SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER + "\" = \n" +
//                        "CASE\n" +
//                        "  WHEN \"type\" like '%_ARRAY' THEN -1\n" +
//                        "  ELSE 0\n" +
//                        "END;",
//                "ALTER TABLE \"sqlg_schema\".\"V_" + SQLG_SCHEMA_PROPERTY + "\" ALTER COLUMN \"" + SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER + "\" DROP DEFAULT;",
                "ALTER TABLE \"sqlg_schema\".\"V_" + SQLG_SCHEMA_PROPERTY + "\" ADD COLUMN \"" + SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER + "\" INTEGER DEFAULT 0 NOT NULL;",
                "ALTER TABLE \"sqlg_schema\".\"V_" + SQLG_SCHEMA_PROPERTY + "\" ALTER COLUMN \"" + SQLG_SCHEMA_PROPERTY_MULTIPLICITY_LOWER + "\" DROP DEFAULT;",

                "ALTER TABLE \"sqlg_schema\".\"V_" + SQLG_SCHEMA_PROPERTY + "\" ADD COLUMN \"" + SQLG_SCHEMA_PROPERTY_MULTIPLICITY_UPPER + "\" INTEGER DEFAULT -1 NOT NULL;",
//                "UPDATE \"sqlg_schema\".\"V_" + SQLG_SCHEMA_PROPERTY + "\" set \"" + SQLG_SCHEMA_PROPERTY_MULTIPLICITY_UPPER + "\" = \n" +
//                        "CASE\n" +
//                        "  WHEN \"type\" like '%_ARRAY' THEN -1\n" +
//                        "  ELSE 0\n" +
//                        "END;",
                "UPDATE \"sqlg_schema\".\"V_" + SQLG_SCHEMA_PROPERTY + "\" set \"" + SQLG_SCHEMA_PROPERTY_MULTIPLICITY_UPPER + "\" = \n" +
                        "CASE\n" +
                        "  WHEN \"type\" like '%_ARRAY' THEN -1\n" +
                        "  ELSE 1\n" +
                        "END;",
                "ALTER TABLE \"sqlg_schema\".\"V_" + SQLG_SCHEMA_PROPERTY + "\" ALTER COLUMN \"" + SQLG_SCHEMA_PROPERTY_MULTIPLICITY_UPPER + "\" DROP DEFAULT;",

                "ALTER TABLE \"sqlg_schema\".\"V_" + SQLG_SCHEMA_PROPERTY + "\" ADD COLUMN \"" + SQLG_SCHEMA_PROPERTY_DEFAULT_LITERAL + "\" VARCHAR;",
                "ALTER TABLE \"sqlg_schema\".\"V_" + SQLG_SCHEMA_PROPERTY + "\" ADD COLUMN \"" + SQLG_SCHEMA_PROPERTY_CHECK_CONSTRAINT + "\" VARCHAR;"
        );
    }

    @Override
    public List<String> addOutEdgeDefinitions() {
        return List.of(
                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_OUT_EDGES_EDGE + "\" ADD COLUMN \"" + SQLG_SCHEMA_OUT_EDGES_LOWER_MULTIPLICITY + "\" INTEGER DEFAULT 0 NOT NULL;",
                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_OUT_EDGES_EDGE + "\" ALTER COLUMN \"" + SQLG_SCHEMA_OUT_EDGES_LOWER_MULTIPLICITY + "\" DROP DEFAULT;",

                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_OUT_EDGES_EDGE + "\" ADD COLUMN \"" + SQLG_SCHEMA_OUT_EDGES_UPPER_MULTIPLICITY + "\" INTEGER DEFAULT -1 NOT NULL;",
                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_OUT_EDGES_EDGE + "\" ALTER COLUMN \"" + SQLG_SCHEMA_OUT_EDGES_UPPER_MULTIPLICITY + "\" DROP DEFAULT;",

                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_OUT_EDGES_EDGE + "\" ADD COLUMN \"" + SQLG_SCHEMA_OUT_EDGES_UNIQUE + "\" BOOLEAN DEFAULT FALSE NOT NULL;",
                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_OUT_EDGES_EDGE + "\" ALTER COLUMN \"" + SQLG_SCHEMA_OUT_EDGES_UNIQUE + "\" DROP DEFAULT;",
                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_OUT_EDGES_EDGE + "\" ADD COLUMN \"" + SQLG_SCHEMA_OUT_EDGES_ORDERED + "\" BOOLEAN DEFAULT FALSE NOT NULL;",
                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_OUT_EDGES_EDGE + "\" ALTER COLUMN \"" + SQLG_SCHEMA_OUT_EDGES_ORDERED + "\" DROP DEFAULT;"
        );
    }

    @Override
    public List<String> addInEdgeDefinitions() {
        return List.of(
                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_IN_EDGES_EDGE + "\" ADD COLUMN \"" + SQLG_SCHEMA_IN_EDGES_LOWER_MULTIPLICITY + "\" INTEGER DEFAULT 0 NOT NULL;",
                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_IN_EDGES_EDGE + "\" ALTER COLUMN \"" + SQLG_SCHEMA_IN_EDGES_LOWER_MULTIPLICITY + "\" DROP DEFAULT;",

                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_IN_EDGES_EDGE + "\" ADD COLUMN \"" + SQLG_SCHEMA_IN_EDGES_UPPER_MULTIPLICITY + "\" INTEGER DEFAULT -1 NOT NULL;",
                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_IN_EDGES_EDGE + "\" ALTER COLUMN \"" + SQLG_SCHEMA_IN_EDGES_UPPER_MULTIPLICITY + "\" DROP DEFAULT;",

                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_IN_EDGES_EDGE + "\" ADD COLUMN \"" + SQLG_SCHEMA_IN_EDGES_UNIQUE + "\" BOOLEAN DEFAULT FALSE NOT NULL;",
                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_IN_EDGES_EDGE + "\" ALTER COLUMN \"" + SQLG_SCHEMA_IN_EDGES_UNIQUE + "\" DROP DEFAULT;",
                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_IN_EDGES_EDGE + "\" ADD COLUMN \"" + SQLG_SCHEMA_IN_EDGES_ORDERED + "\" BOOLEAN DEFAULT FALSE NOT NULL;",
                "ALTER TABLE \"sqlg_schema\".\"E_" + SQLG_SCHEMA_IN_EDGES_EDGE + "\" ALTER COLUMN \"" + SQLG_SCHEMA_IN_EDGES_ORDERED + "\" DROP DEFAULT;"
        );
    }

    @Override
    public String checkConstraintName(SqlgGraph sqlgGraph, String schema, String table, String column, String constraint) {
        Connection conn = sqlgGraph.tx().getConnection();
        String sql = "SELECT a.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE a JOIN\n" +
                "INFORMATION_SCHEMA.CHECK_CONSTRAINTS b ON a.CONSTRAINT_NAME = b.CONSTRAINT_NAME\n" +
                "WHERE a.TABLE_SCHEMA = ? and a.TABLE_NAME = ? AND a.COLUMN_NAME = ? AND b.CHECK_CLAUSE NOT LIKE '%NOT NULL%';";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, schema);
            statement.setString(2, table);
            statement.setString(3, column);
            ResultSet rs = statement.executeQuery();
            if (rs.next()) {
                String checkConstraintName = rs.getString(1);
                if (rs.next()) {
                    String _checkConstraintName = rs.getString(1);
                    System.out.println(_checkConstraintName);
                }
                Preconditions.checkState(!rs.next(), "Column '%s.%s' has more than one check constraint.", table, column);
                return checkConstraintName;
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
