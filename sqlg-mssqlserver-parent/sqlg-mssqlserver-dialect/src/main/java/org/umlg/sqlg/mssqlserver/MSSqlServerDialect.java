package org.umlg.sqlg.mssqlserver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.umlg.sqlg.sql.dialect.BaseSqlDialect;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.strategy.SqlgSqlExecutor;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.util.SqlgUtil;

import javax.annotation.Nullable;
import javax.xml.bind.DatatypeConverter;
import java.lang.reflect.Array;
import java.sql.*;
import java.time.*;
import java.util.*;

import static org.umlg.sqlg.structure.topology.Topology.EDGE_PREFIX;
import static org.umlg.sqlg.structure.topology.Topology.VERTEX_PREFIX;

/**
 * @author Kevin Schmidt
 * @since 1.3.3
 */
public class MSSqlServerDialect extends BaseSqlDialect {

    MSSqlServerDialect() {
        super();
    }

    @Override
    public boolean requiresIndexName() {
        return true;
    }

    @Override
    public String dialectName() {
        return "MSSqlServerDialect";
    }

    @Override
    public Set<String> getInternalSchemas() {
        return ImmutableSet.copyOf(Arrays.asList("db_accessadmin", "db_backupoperator", "db_datareader",
                "db_ddladmin", "db_debydatareader", "db_denydatawriter", "db_owner", "db_scurityadmin",
                "dbo", "guest", "INFORMATION_SCHEMA", "sys"));
    }

    @Override
    public boolean needsSchemaDropCascade() {
        return false;
    }

    @Override
    public boolean supportsCascade() {
        return false;
    }

    @Override
    public String addColumnStatement(String schema, String table, String column, String typeDefinition) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ");
        sql.append(maybeWrapInQoutes(schema));
        sql.append(".");
        sql.append(maybeWrapInQoutes(table));
        sql.append(" ADD ");
        sql.append(maybeWrapInQoutes(column));
        sql.append(" ");
        sql.append(typeDefinition);
        if (needsSemicolon()) {
            sql.append(";");
        }
        return sql.toString();
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
        if (value instanceof String) {
            return;
        }
        if (value instanceof Character) {
            return;
        }
        if (value instanceof Boolean) {
            return;
        }
        if (value instanceof Byte || value instanceof Byte[] || value instanceof byte[]) {
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
                return new String[]{"BIT"};
            case BYTE:
                return new String[]{"TINYINT"};
            case byte_ARRAY:
                return new String[]{"VARBINARY(max)"};
            case BYTE_ARRAY:
                return new String[]{"VARBINARY(max)"};
            case DOUBLE:
                return new String[]{"DOUBLE PRECISION"};
            case DURATION:
                return new String[]{"BIGINT", "INT"};
            case FLOAT:
                return new String[]{"REAL"};
            case INTEGER:
                return new String[]{"INT"};
            case LOCALDATE:
                return new String[]{"DATE"};
            case LOCALDATETIME:
                return new String[]{"DATETIME2(3)"};
            case LOCALTIME:
                return new String[]{"TIME"};
            case LONG:
                return new String[]{"BIGINT"};
            case PERIOD:
                return new String[]{"INT", "INT", "INT"};
            case SHORT:
                return new String[]{"SMALLINT"};
            case STRING:
                return new String[]{"VARCHAR(2000)"};
            case ZONEDDATETIME:
                return new String[]{"DATETIME2(3)", "VARCHAR(255)"};
            case STRING_ARRAY:
                return new String[]{"ARRAY"};
            case DURATION_ARRAY:
                return new String[]{"ARRAY", "ARRAY"};
            case PERIOD_ARRAY:
                return new String[]{"ARRAY", "ARRAY", "ARRAY"};
            case ZONEDDATETIME_ARRAY:
                return new String[]{"ARRAY", "ARRAY"};
            case JSON:
                return new String[]{"VARCHAR(max)"};
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    @Override
    public int[] propertyTypeToJavaSqlType(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                return new int[]{Types.BIT};
            case BYTE:
                return new int[]{Types.TINYINT};
            case SHORT:
                return new int[]{Types.SMALLINT};
            case INTEGER:
                return new int[]{Types.INTEGER};
            case LONG:
                return new int[]{Types.BIGINT};
            case FLOAT:
                return new int[]{Types.REAL};
            case DOUBLE:
                return new int[]{Types.DOUBLE};
            case STRING:
                return new int[]{Types.LONGVARCHAR};
            case LOCALDATETIME:
                return new int[]{Types.TIMESTAMP};
            case LOCALDATE:
                return new int[]{Types.DATE};
            case LOCALTIME:
                return new int[]{Types.TIME};
            case ZONEDDATETIME:
                return new int[]{Types.TIMESTAMP, Types.CLOB};
            case DURATION:
                return new int[]{Types.BIGINT, Types.INTEGER};
            case PERIOD:
                return new int[]{Types.INTEGER, Types.INTEGER, Types.INTEGER};
            case JSON:
                return new int[]{Types.VARCHAR};
            case byte_ARRAY:
                return new int[]{Types.VARBINARY};
            case BYTE_ARRAY:
                return new int[]{Types.VARBINARY};
            case BOOLEAN_ARRAY:
            case boolean_ARRAY:
            case DOUBLE_ARRAY:
            case double_ARRAY:
            case FLOAT_ARRAY:
            case float_ARRAY:
            case int_ARRAY:
            case INTEGER_ARRAY:
            case LOCALDATE_ARRAY:
            case LOCALDATETIME_ARRAY:
            case LOCALTIME_ARRAY:
            case LONG_ARRAY:
            case long_ARRAY:
            case SHORT_ARRAY:
            case short_ARRAY:
            case STRING_ARRAY:
                return new int[]{Types.ARRAY};
            case ZONEDDATETIME_ARRAY:
                return new int[]{Types.ARRAY, Types.ARRAY};
            case DURATION_ARRAY:
                return new int[]{Types.ARRAY, Types.ARRAY};
            case PERIOD_ARRAY:
                return new int[]{Types.ARRAY, Types.ARRAY, Types.ARRAY};
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

    @SuppressWarnings("Duplicates")
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

    @SuppressWarnings("Duplicates")
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
        return "SELECT * FROM sys.indexes i JOIN sys.tables t ON i.object_id = t.object_id JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = '" + schemaTable.getSchema() +
                "' AND t.name = '" +
                prefix +
                schemaTable.getTable() +
                "' AND i.name = '" +
                indexName +
                "'";
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
        return true;
    }

    @Override
    public boolean supportsTransactionalSchema() {
        return true;
    }

    @Override
    public String createTemporaryTableStatement() {
        return "CREATE TABLE ";
    }

    @Override
    public String afterCreateTemporaryTableStatement() {
        return "";
    }

    @Override
    public List<String> sqlgTopologyCreationScripts() {
        List<String> result = new ArrayList<>();
        result.add("CREATE TABLE \"sqlg_schema\".\"V_graph\" (" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"createdOn\" DATETIME, " +
                "\"updatedOn\" DATETIME, " +
                "\"version\" VARCHAR(255), " +
                "\"dbVersion\" VARCHAR(255));");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_schema\" (" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"createdOn\" DATETIME, " +
                "\"name\" VARCHAR(255));");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_vertex\" (" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"createdOn\" DATETIME, " +
                "\"name\" VARCHAR(255), " +
                "\"schemaVertex\" VARCHAR(255), " +
                "\"partitionType\" VARCHAR(255), " +
                "\"partitionExpression\" VARCHAR(255), " +
                "\"shardCount\" INTEGER);");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_edge\" (" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"createdOn\" DATETIME, " +
                "\"name\" VARCHAR(255), " +
                "\"partitionType\" VARCHAR(255), " +
                "\"partitionExpression\" VARCHAR(255), " +
                "\"shardCount\" INTEGER);");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_partition\" (" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"createdOn\" DATETIME, " +
                "\"name\" VARCHAR(255), " +
                "\"from\" VARCHAR(255), " +
                "\"to\" VARCHAR(255), " +
                "\"in\" VARCHAR(255), " +
                "\"partitionType\" VARCHAR(255), " +
                "\"partitionExpression\" VARCHAR(255)" +
                ");");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_property\" (" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"createdOn\" DATETIME, " +
                "\"name\" VARCHAR(255), " +
                "\"type\" VARCHAR(255));");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_index\" (" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"createdOn\" DATETIME, " +
                "\"name\" VARCHAR(255), " +
                "\"index_type\" VARCHAR(255));");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_globalUniqueIndex\" (" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"createdOn\" DATETIME, " +
                "\"name\" VARCHAR(255));");

        result.add("CREATE TABLE \"sqlg_schema\".\"E_schema_vertex\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.vertex__I\" BIGINT, " +
                "\"sqlg_schema.schema__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"),  " +
                "FOREIGN KEY (\"sqlg_schema.schema__O\") REFERENCES \"sqlg_schema\".\"V_schema\" (\"ID\"));");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_in_edges\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.edge__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.edge__I\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"),  " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_out_edges\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.edge__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.edge__I\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"),  " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_vertex_property\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"),  " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_edge_property\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"),  " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));");

        result.add("CREATE TABLE \"sqlg_schema\".\"" + Topology.EDGE_PREFIX + "vertex_identifier\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "\"identifier_index\" INTEGER, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "property\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "vertex\" (\"ID\"));");
        result.add("CREATE TABLE \"sqlg_schema\".\"" + Topology.EDGE_PREFIX + "edge_identifier\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "\"identifier_index\" INTEGER, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "property\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "edge\" (\"ID\"));");

        result.add("CREATE TABLE \"sqlg_schema\".\"E_vertex_partition\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.partition__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"),  " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_edge_partition\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.partition__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"),  " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_partition_partition\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.partition__I\" BIGINT, " +
                "\"sqlg_schema.partition__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"),  " +
                "FOREIGN KEY (\"sqlg_schema.partition__O\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"));");

        result.add("CREATE TABLE \"sqlg_schema\".\"" + Topology.EDGE_PREFIX + "vertex_distribution\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "property\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "vertex\" (\"ID\"));");

        result.add("CREATE TABLE \"sqlg_schema\".\"" + Topology.EDGE_PREFIX + "vertex_colocate\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.vertex__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "vertex\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "vertex\" (\"ID\"));");

        result.add("CREATE TABLE \"sqlg_schema\".\"" + Topology.EDGE_PREFIX + "edge_distribution\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "property\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "edge\" (\"ID\"));");

        result.add("CREATE TABLE \"sqlg_schema\".\"" + Topology.EDGE_PREFIX + "edge_colocate\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.vertex__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "vertex\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "edge\" (\"ID\"));");

        result.add("CREATE TABLE \"sqlg_schema\".\"E_vertex_index\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.index__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.index__I\") REFERENCES \"sqlg_schema\".\"V_index\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_edge_index\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.index__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.index__I\") REFERENCES \"sqlg_schema\".\"V_index\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_index_property\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.index__O\" BIGINT, " +
                "\"sequence\" INTEGER, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.index__O\") REFERENCES \"sqlg_schema\".\"V_index\" (\"ID\"));");

        result.add("CREATE TABLE \"sqlg_schema\".\"V_log\" (" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"timestamp\" TIMESTAMP, " +
                "\"pid\" INTEGER, " +
                "\"log\" VARCHAR);");

        result.add("CREATE TABLE \"sqlg_schema\".\"E_globalUniqueIndex_property\"(" +
                "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.globalUniqueIndex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.globalUniqueIndex__O\") REFERENCES \"sqlg_schema\".\"V_globalUniqueIndex\" (\"ID\"));");
        return result;
    }

    @Override
    public String sqlgCreateTopologyGraph() {
        return "CREATE TABLE \"sqlg_schema\".\"V_graph\" (\"ID\" BIGINT IDENTITY PRIMARY KEY, \"createdOn\" DATETIME, \"updatedOn\" DATETIME, \"version\" VARCHAR(255), \"dbVersion\" VARCHAR(255));";
    }

    @Override
    public String sqlgAddIndexEdgeSequenceColumn() {
        return "ALTER TABLE \"sqlg_schema\".\"E_index_property\" ADD \"sequence\" INTEGER DEFAULT 0;";

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
                if (dates.length > 0 && dates[0] instanceof Timestamp) {
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
        return "graph";
    }

    @Override
    public String getRangeClause(Range<Long> r) {
        return "OFFSET " + r.getMinimum() + " ROWS FETCH NEXT " + (r.getMaximum() - r.getMinimum()) + " ROWS ONLY";
    }

    @Override
    public String getSkipClause(long skip) {
        return " OFFSET " + skip + " ROWS";
    }

    @Override
    public boolean isSystemIndex(String indexName) {
        return indexName.startsWith("PK_") || indexName.startsWith("FK_") || indexName.endsWith("_idx");
    }

    @Override
    public boolean supportsType(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                return true;
            case BYTE:
                return true;
            case BYTE_ARRAY:
                return true;
            case byte_ARRAY:
                return true;
            case SHORT:
                return true;
            case INTEGER:
                return true;
            case LONG:
                return true;
            case DOUBLE:
                return true;
            case STRING:
                return true;
            case LOCALDATE:
                return true;
            case LOCALDATETIME:
                return true;
            case LOCALTIME:
                return true;
            case JSON:
                return true;
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    @Override
    public boolean supportsBooleanArrayValues() {
        return false;
    }

    @Override
    public boolean supportsDoubleArrayValues() {
        return false;
    }

    @Override
    public boolean supportsFloatArrayValues() {
        return false;
    }

    @Override
    public boolean supportsIntegerArrayValues() {
        return false;
    }

    @Override
    public boolean supportsShortArrayValues() {
        return false;
    }

    @Override
    public boolean supportsLongArrayValues() {
        return false;
    }

    @Override
    public boolean supportsStringArrayValues() {
        return false;
    }

    @Override
    public boolean supportsFloatValues() {
        return false;
    }

    @Override
    public boolean supportsByteValues() {
        return false;
    }

    @Override
    public boolean supportsLocalDateTimeArrayValues() {
        return false;
    }

    @Override
    public boolean supportsLocalTimeArrayValues() {
        return false;
    }

    @Override
    public boolean supportsLocalDateArrayValues() {
        return false;
    }

    @Override
    public boolean supportsZonedDateTimeArrayValues() {
        return false;
    }

    @Override
    public boolean supportsPeriodArrayValues() {
        return false;
    }

    @Override
    public boolean supportsDurationArrayValues() {
        return false;
    }

    @Override
    public boolean needsTemporaryTablePrefix() {
        return true;
    }

    @Override
    public String temporaryTablePrefix() {
        return "#";
    }

    @Override
    public boolean supportsBatchMode() {
        return true;
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void flushVertexCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexCache) {
        Connection connection = sqlgGraph.tx().getConnection();
        for (Map.Entry<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> entry : vertexCache.entrySet()) {
            SchemaTable schemaTable = entry.getKey();
            VertexLabel vertexLabel = null;
            if (!schemaTable.isTemporary()) {
                vertexLabel = sqlgGraph.getTopology().getVertexLabel(schemaTable.getSchema(), schemaTable.getTable()).orElseThrow(
                        () -> new IllegalStateException(String.format("VertexLabel %s not found.", schemaTable.toString())));
            }
            Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> vertices = entry.getValue();
            if (vertices.getLeft().isEmpty()) {
                Map<String, PropertyType> columns = new HashMap<>();
                columns.put("dummy", PropertyType.from(0));
                sqlgGraph.getTopology().ensureVertexLabelPropertiesExist(
                        schemaTable.getSchema(),
                        schemaTable.getTable(),
                        columns);
            }
            try {
                SQLServerConnection sqlServerConnection = connection.unwrap(SQLServerConnection.class);
                try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlServerConnection)) {
                    SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                    //locking the table guarantee that the sequences will be in order.
                    options.setTableLock(true);
                    bulkCopy.setBulkCopyOptions(options);
                    if (schemaTable.isTemporary()) {
                        bulkCopy.setDestinationTableName(
                                sqlgGraph.getSqlDialect().maybeWrapInQoutes(sqlgGraph.getSqlDialect().temporaryTablePrefix() + VERTEX_PREFIX + schemaTable.getTable())
                        );
                    } else {
                        bulkCopy.setDestinationTableName(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()) + "." +
                                sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + schemaTable.getTable())
                        );
                    }
                    bulkCopy.writeToServer(new SQLServerVertexCacheBulkRecord(bulkCopy, sqlgGraph, schemaTable, vertices));
                }
                if (vertexLabel != null && vertexLabel.hasIDPrimaryKey()) {
                    int numberInserted = vertices.getRight().size();
                    if (!schemaTable.isTemporary() && numberInserted > 0) {
                        long endHigh;
                        try (PreparedStatement preparedStatement = connection.prepareStatement(
                                "SELECT IDENT_CURRENT('" + schemaTable.getSchema() + "." +
                                        VERTEX_PREFIX + schemaTable.getTable() + "')")) {
                            ResultSet resultSet = preparedStatement.executeQuery();
                            resultSet.next();
                            endHigh = resultSet.getLong(1);
                            resultSet.close();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        //set the id on the vertex
                        long id = endHigh - numberInserted + 1;
                        for (SqlgVertex sqlgVertex : vertices.getRight().keySet()) {
                            sqlgVertex.setInternalPrimaryKey(RecordId.from(schemaTable, id++));
                        }
                    }
                } else if (vertexLabel != null) {
                    for (Map.Entry<SqlgVertex, Map<String, Object>> sqlgVertexMapEntry : vertices.getRight().entrySet()) {
                        SqlgVertex sqlgVertex = sqlgVertexMapEntry.getKey();
                        Map<String, Object> values = sqlgVertexMapEntry.getValue();
                        ListOrderedSet<Comparable> identifiers = new ListOrderedSet<>();
                        for (String identifier : vertexLabel.getIdentifiers()) {
                            identifiers.add((Comparable) values.get(identifier));
                        }
                        sqlgVertex.setInternalPrimaryKey(RecordId.from(schemaTable, identifiers));
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void flushEdgeCache(SqlgGraph sqlgGraph, Map<MetaEdge, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>>> edgeCache) {
        Connection connection = sqlgGraph.tx().getConnection();
        try {
            for (MetaEdge metaEdge : edgeCache.keySet()) {
                SchemaTable schemaTable = metaEdge.getSchemaTable();
                EdgeLabel edgeLabel = sqlgGraph.getTopology().getEdgeLabel(schemaTable.getSchema(), schemaTable.getTable()).orElseThrow(() -> new IllegalStateException(String.format("EdgeLabel not found for %s.%s", schemaTable.getSchema(), schemaTable.getTable())));
                Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> triples = edgeCache.get(metaEdge);
                try {
                    SQLServerConnection sqlServerConnection = connection.unwrap(SQLServerConnection.class);
                    try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlServerConnection)) {
                        SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                        //locking the table guarantee that the sequences will be in order.
                        options.setTableLock(true);
                        bulkCopy.setBulkCopyOptions(options);
                        bulkCopy.setDestinationTableName(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()) + "." +
                                sqlgGraph.getSqlDialect().maybeWrapInQoutes(EDGE_PREFIX + schemaTable.getTable())
                        );
                        bulkCopy.writeToServer(new SQLServerEdgeCacheBulkRecord(bulkCopy, sqlgGraph, metaEdge, schemaTable, triples));
                    }

                    if (edgeLabel.hasIDPrimaryKey()) {
                        int numberInserted = triples.getRight().size();
                        if (!schemaTable.isTemporary() && numberInserted > 0) {
                            long endHigh;
                            try (PreparedStatement preparedStatement = connection.prepareStatement(
                                    "SELECT IDENT_CURRENT('" + schemaTable.getSchema() + "." +
                                            EDGE_PREFIX + schemaTable.getTable() + "')")) {
                                ResultSet resultSet = preparedStatement.executeQuery();
                                resultSet.next();
                                endHigh = resultSet.getLong(1);
                                resultSet.close();
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                            //set the id on the vertex
                            long id = endHigh - numberInserted + 1;
                            for (SqlgEdge sqlgEdge : triples.getRight().keySet()) {
                                sqlgEdge.setInternalPrimaryKey(RecordId.from(schemaTable, id++));
                            }
                        }
                    } else {
                        for (Map.Entry<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> sqlgEdgeTripleEntry : triples.getRight().entrySet()) {
                            SqlgEdge sqlgEdge = sqlgEdgeTripleEntry.getKey();
                            Map<String, Object> values = sqlgEdgeTripleEntry.getValue().getRight();
                            ListOrderedSet<Comparable> identifiers = new ListOrderedSet<>();
                            for (String identifier : edgeLabel.getIdentifiers()) {
                                identifiers.add((Comparable) values.get(identifier));
                            }
                            sqlgEdge.setInternalPrimaryKey(RecordId.from(schemaTable, identifiers));
                        }
                    }

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flushVertexGlobalUniqueIndexes(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexCache) {
        for (SchemaTable schemaTable : vertexCache.keySet()) {
            Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> vertices = vertexCache.get(schemaTable);
            Map<String, PropertyColumn> propertyColumnMap = sqlgGraph.getTopology().getPropertiesFor(schemaTable.withPrefix(VERTEX_PREFIX));
            for (Map.Entry<String, PropertyColumn> propertyColumnEntry : propertyColumnMap.entrySet()) {
                PropertyColumn propertyColumn = propertyColumnEntry.getValue();
                for (GlobalUniqueIndex globalUniqueIndex : propertyColumn.getGlobalUniqueIndices()) {
                    try {
                        Connection connection = sqlgGraph.tx().getConnection();
                        SQLServerConnection sqlServerConnection = connection.unwrap(SQLServerConnection.class);
                        try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlServerConnection)) {
                            bulkCopy.setDestinationTableName(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA) + "." +
                                    sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + globalUniqueIndex.getName())
                            );
                            bulkCopy.writeToServer(new SQLServerVertexGlobalUniqueIndexBulkRecord(bulkCopy, sqlgGraph, vertices, propertyColumn));
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    @Override
    public void flushEdgeGlobalUniqueIndexes(SqlgGraph sqlgGraph,
                                             Map<MetaEdge, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>>> edgeCache) {
        for (MetaEdge metaEdge : edgeCache.keySet()) {

            Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> triples = edgeCache.get(metaEdge);
            Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>> edgeMap = triples.getRight();
            Map<String, PropertyColumn> propertyColumnMap = sqlgGraph.getTopology().getPropertiesFor(metaEdge.getSchemaTable().withPrefix(EDGE_PREFIX));

            for (Map.Entry<String, PropertyColumn> propertyColumnEntry : propertyColumnMap.entrySet()) {
                PropertyColumn propertyColumn = propertyColumnEntry.getValue();
                for (GlobalUniqueIndex globalUniqueIndex : propertyColumn.getGlobalUniqueIndices()) {
                    try {
                        Connection connection = sqlgGraph.tx().getConnection();
                        SQLServerConnection sqlServerConnection = connection.unwrap(SQLServerConnection.class);
                        try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(sqlServerConnection)) {
                            bulkCopy.setDestinationTableName(sqlgGraph.getSqlDialect().maybeWrapInQoutes(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA) + "." +
                                    sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + globalUniqueIndex.getName())
                            );
                            bulkCopy.writeToServer(new SQLServerEdgeGlobalUniqueIndexBulkRecord(bulkCopy, sqlgGraph, edgeMap, propertyColumn));
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    @Override
    public boolean supportsSchemaIfNotExists() {
        return false;
    }


    @Override
    public boolean isMssqlServer() {
        return true;
    }

    @Override
    public boolean uniqueIndexConsidersNullValuesEqual() {
        return true;
    }

    @Override
    public String dropSchemaStatement(String schema) {
        return "DROP SCHEMA " + maybeWrapInQoutes(schema) + (needsSemicolon() ? ";" : "");
    }

    @Override
    public String valueToValuesString(PropertyType propertyType, Object value) {
        Preconditions.checkState(supportsType(propertyType), "PropertyType %s is not supported", propertyType.name());
        switch (propertyType) {
            case STRING:
                return "'" + escapeQuotes(value) + "'";
            case BYTE:
                return value.toString();
            case byte_ARRAY:
                //Mssql likes to have 0x prefix for binary literal
                //not compiling with java 10
                return "0x" + DatatypeConverter.printHexBinary((byte[]) value);
            case BYTE_ARRAY:
                //Mssql likes to have 0x prefix for binary literal
                byte[] bytes = SqlgUtil.convertObjectArrayToBytePrimitiveArray((Byte[]) value);
                //not compiling with java 10
                return "0x" + DatatypeConverter.printHexBinary(bytes);
            case BOOLEAN:
                Boolean b = (Boolean) value;
                if (b) {
                    return Integer.valueOf(1).toString();
                } else {
                    return Integer.valueOf(0).toString();
                }
            case SHORT:
                return value.toString();
            case INTEGER:
                return value.toString();
            case LONG:
                return value.toString();
            case DOUBLE:
                return value.toString();
            case LOCALDATE:
                return "'" + value.toString() + "'";
            case LOCALDATETIME:
                return "'" + Timestamp.valueOf((LocalDateTime) value).toString() + "'";
            case LOCALTIME:
                return "'" + Time.valueOf((LocalTime) value).toString() + "'";
            case JSON:
                return "'" + value.toString() + "'";
            default:
                throw SqlgExceptions.invalidPropertyType(propertyType);
        }
    }

    @Override
    public String sqlToTurnOffReferentialConstraintCheck(String tableName) {
        return "ALTER TABLE " + tableName + " NOCHECK CONSTRAINT ALL";
    }

    @Override
    public String sqlToTurnOnReferentialConstraintCheck(String tableName) {
        return "ALTER TABLE " + tableName + " CHECK CONSTRAINT ALL";
    }

    @SuppressWarnings("Duplicates")
    @Override
    public List<Triple<SqlgSqlExecutor.DROP_QUERY, String, SchemaTable>> drop(SqlgGraph sqlgGraph, String leafElementsToDelete, @Nullable  String edgesToDelete, LinkedList<SchemaTableTree> distinctQueryStack) {

        List<Triple<SqlgSqlExecutor.DROP_QUERY, String, SchemaTable>> sqls = new ArrayList<>();
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
                    sb.append(maybeWrapInQoutes(lastVertexLabel.getSchema().getName() + "." + lastVertexLabel.getName() + Topology.OUT_VERTEX_COLUMN_END));
                    sb.append(" IN(");
                    sb.append(leafElementsToDelete);
                    sb.append(")");
                    sqls.add(Triple.of(SqlgSqlExecutor.DROP_QUERY.NORMAL, sb.toString(), SchemaTable.of(edgeLabel.getSchema().getName(), Topology.EDGE_PREFIX + edgeLabel.getName())));
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
                    sb.append(maybeWrapInQoutes(lastVertexLabel.getSchema().getName() + "." + lastVertexLabel.getName() + Topology.IN_VERTEX_COLUMN_END));
                    sb.append(" IN\n\t(");
                    sb.append(leafElementsToDelete);
                    sb.append(")");
                    sqls.add(Triple.of(SqlgSqlExecutor.DROP_QUERY.NORMAL, sb.toString(), SchemaTable.of(edgeLabel.getSchema().getName(), Topology.EDGE_PREFIX + edgeLabel.getName())));
                }
            }
        }

        //Need to defer foreign key constraint checks.
        if (queryTraversesEdge) {
            String edgeTableName = (maybeWrapInQoutes(lastEdge.getSchemaTable().getSchema())) + "." + maybeWrapInQoutes(lastEdge.getSchemaTable().getTable());
            sqls.add(Triple.of(SqlgSqlExecutor.DROP_QUERY.ALTER, this.sqlToTurnOffReferentialConstraintCheck(edgeTableName), lastEdge.getSchemaTable()));
        }
        //Delete the leaf vertices, if there are foreign keys then its been deferred.
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ");
        sb.append(maybeWrapInQoutes(last.getSchemaTable().getSchema()));
        sb.append(".");
        sb.append(maybeWrapInQoutes(last.getSchemaTable().getTable()));
        sb.append("\nWHERE \"ID\" IN (");
        sb.append(leafElementsToDelete);
        sb.append(")");
        sqls.add(Triple.of(SqlgSqlExecutor.DROP_QUERY.NORMAL, sb.toString(), last.getSchemaTable()));

        if (queryTraversesEdge) {
            sb = new StringBuilder();
            sb.append("DELETE FROM ");
            sb.append(maybeWrapInQoutes(lastEdge.getSchemaTable().getSchema()));
            sb.append(".");
            sb.append(maybeWrapInQoutes(lastEdge.getSchemaTable().getTable()));
            sb.append("\nWHERE \"ID\" IN (\n\t");
            sb.append(edgesToDelete);
            sb.append(")");
            sqls.add(Triple.of(SqlgSqlExecutor.DROP_QUERY.EDGE, sb.toString(), lastEdge.getSchemaTable()));
        }
        //Enable the foreign key constraint
        if (queryTraversesEdge) {
            String edgeTableName = (maybeWrapInQoutes(lastEdge.getSchemaTable().getSchema())) + "." + maybeWrapInQoutes(lastEdge.getSchemaTable().getTable());
            sqls.add(Triple.of(SqlgSqlExecutor.DROP_QUERY.ALTER, this.sqlToTurnOnReferentialConstraintCheck(edgeTableName), null));
        }
        return sqls;
    }

    @SuppressWarnings("Duplicates")
    @Override
    public String dropWithForeignKey(boolean out, EdgeLabel edgeLabel, VertexLabel vertexLabel, Collection<RecordId.ID> ids, boolean mutatingCallbacks) {
        StringBuilder sql = new StringBuilder();
        sql.append("WITH todelete(");
        if (vertexLabel.hasIDPrimaryKey()) {
            sql.append(maybeWrapInQoutes("ID"));
        } else {
            int count = 1;
            for (String identifier : vertexLabel.getIdentifiers()) {
                sql.append(maybeWrapInQoutes(identifier));
                if (count++ < vertexLabel.getIdentifiers().size()) {
                    sql.append(",");
                }
            }
        }
        sql.append(") as (\nSELECT * FROM (VALUES");
        if (vertexLabel.hasIDPrimaryKey()) {
            int count = 1;
            for (RecordId.ID id : ids) {
                sql.append("(");
                sql.append(id.getSequenceId());
                sql.append(")");
                if (count++ < ids.size()) {
                    sql.append(",");
                }
            }
        } else {
            int cnt = 1;
            for (RecordId.ID id : ids) {
                sql.append("(");
                int count = 1;
                for (Comparable identifierValue : id.getIdentifiers()) {
                    sql.append(toRDBSStringLiteral(identifierValue));
                    if (count++ < id.getIdentifiers().size()) {
                        sql.append(",");
                    }
                }
                sql.append(")");
                if (cnt++ < ids.size()) {
                    sql.append(",");
                }
            }
        }
        sql.append(") as tmp(");
        if (vertexLabel.hasIDPrimaryKey()) {
            sql.append(maybeWrapInQoutes("ID"));
        } else {
            int count = 1;
            for (String identifier : vertexLabel.getIdentifiers()) {
                sql.append(maybeWrapInQoutes(identifier));
                if (count++ < vertexLabel.getIdentifiers().size()) {
                    sql.append(",");
                }
            }
        }
        sql.append("))\n");

        sql.append("DELETE a FROM\n\t");
        sql.append(maybeWrapInQoutes(edgeLabel.getSchema().getName()));
        sql.append(".");
        sql.append(maybeWrapInQoutes(Topology.EDGE_PREFIX + edgeLabel.getName()));
        sql.append(" a ");
        if (mutatingCallbacks) {
            sql.append("\nOUTPUT DELETED.");
            sql.append(maybeWrapInQoutes("ID"));
        }
        sql.append("JOIN todelete on ");
        if (vertexLabel.hasIDPrimaryKey()) {
            sql.append("todelete.");
            sql.append(maybeWrapInQoutes("ID"));
            sql.append(" = a.");
            sql.append(maybeWrapInQoutes(
                    vertexLabel.getSchema().getName() + "." + vertexLabel.getName()
                            + (out ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END)));
        } else {
            int count = 1;
            for (String identifier : vertexLabel.getIdentifiers()) {
                sql.append("todelete.");
                sql.append(maybeWrapInQoutes(identifier));
                sql.append(" = a.");
                sql.append(maybeWrapInQoutes(
                        vertexLabel.getSchema().getName() + "." + vertexLabel.getName() + "." + identifier
                                + (out ? Topology.OUT_VERTEX_COLUMN_END : Topology.IN_VERTEX_COLUMN_END)));
                if (count++ < vertexLabel.getIdentifiers().size()) {
                    sql.append(" AND ");
                }
            }
        }
        return sql.toString();
    }

    @Override
    public List<String> addPartitionTables() {
        return Arrays.asList(
                "ALTER TABLE \"sqlg_schema\".\"V_vertex\" ADD \"partitionType\" VARCHAR(255) DEFAULT 'NONE' WITH VALUES;",
                "ALTER TABLE \"sqlg_schema\".\"V_vertex\" ADD \"partitionExpression\" VARCHAR(255);",
                "ALTER TABLE \"sqlg_schema\".\"V_vertex\" ADD \"shardCount\" INTEGER;",
                "ALTER TABLE \"sqlg_schema\".\"V_edge\" ADD \"partitionType\" VARCHAR(255) DEFAULT 'NONE' WITH VALUES;",
                "ALTER TABLE \"sqlg_schema\".\"V_edge\" ADD \"partitionExpression\" VARCHAR(255);",
                "ALTER TABLE \"sqlg_schema\".\"V_edge\" ADD \"shardCount\" INTEGER;",
                "CREATE TABLE \"sqlg_schema\".\"V_partition\" (" +
                        "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                        "\"createdOn\" DATETIME, " +
                        "\"name\" VARCHAR(255), " +
                        "\"from\" VARCHAR(255), " +
                        "\"to\" VARCHAR(255), " +
                        "\"in\" VARCHAR(255), " +
                        "\"partitionType\" VARCHAR(255), " +
                        "\"partitionExpression\" VARCHAR(255)" +
                        ");",
                "CREATE TABLE \"sqlg_schema\".\"E_vertex_partition\"(" +
                        "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.partition__I\" BIGINT, " +
                        "\"sqlg_schema.vertex__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"),  " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));",
                "CREATE TABLE \"sqlg_schema\".\"E_edge_partition\"(" +
                        "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.partition__I\" BIGINT, " +
                        "\"sqlg_schema.edge__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"),  " +
                        "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));",
                "CREATE TABLE \"sqlg_schema\".\"E_partition_partition\"(" +
                        "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.partition__I\" BIGINT, " +
                        "\"sqlg_schema.partition__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.partition__I\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"),  " +
                        "FOREIGN KEY (\"sqlg_schema.partition__O\") REFERENCES \"sqlg_schema\".\"V_partition\" (\"ID\"));",


                "CREATE TABLE \"sqlg_schema\".\"" + Topology.EDGE_PREFIX + "vertex_identifier\"(" +
                        "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.property__I\" BIGINT, " +
                        "\"sqlg_schema.vertex__O\" BIGINT, " +
                        "\"identifier_index\" INTEGER, " +
                        "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "property\" (\"ID\"), " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "vertex\" (\"ID\"));",
                "CREATE TABLE \"sqlg_schema\".\"" + Topology.EDGE_PREFIX + "edge_identifier\"(" +
                        "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.property__I\" BIGINT, " +
                        "\"sqlg_schema.edge__O\" BIGINT, " +
                        "\"identifier_index\" INTEGER, " +
                        "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "property\" (\"ID\"), " +
                        "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "edge\" (\"ID\"));",
                "CREATE TABLE \"sqlg_schema\".\"" + Topology.EDGE_PREFIX + "vertex_distribution\"(" +
                        "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.property__I\" BIGINT, " +
                        "\"sqlg_schema.vertex__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "property\" (\"ID\"), " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "vertex\" (\"ID\"));",

                "CREATE TABLE \"sqlg_schema\".\"" + Topology.EDGE_PREFIX + "vertex_colocate\"(" +
                        "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.vertex__I\" BIGINT, " +
                        "\"sqlg_schema.vertex__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "vertex\" (\"ID\"), " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "vertex\" (\"ID\"));",

                "CREATE TABLE \"sqlg_schema\".\"" + Topology.EDGE_PREFIX + "edge_distribution\"(" +
                        "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.property__I\" BIGINT, " +
                        "\"sqlg_schema.edge__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "property\" (\"ID\"), " +
                        "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "edge\" (\"ID\"));",

                "CREATE TABLE \"sqlg_schema\".\"" + Topology.EDGE_PREFIX + "edge_colocate\"(" +
                        "\"ID\" BIGINT IDENTITY PRIMARY KEY, " +
                        "\"sqlg_schema.vertex__I\" BIGINT, " +
                        "\"sqlg_schema.edge__O\" BIGINT, " +
                        "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "vertex\" (\"ID\"), " +
                        "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"" + Topology.VERTEX_PREFIX + "edge\" (\"ID\"));"

        );
    }

    @Override
    public String addDbVersionToGraph(DatabaseMetaData metadata) {
        try {
            return "ALTER TABLE \"sqlg_schema\".\"V_graph\" ADD \"dbVersion\" VARCHAR(255) DEFAULT '" + metadata.getDatabaseProductVersion() + "';";
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("Duplicates")
    @Override
    public String drop(VertexLabel vertexLabel, Collection<RecordId.ID> ids) {
        StringBuilder sql = new StringBuilder();

        sql.append("WITH todelete(");
        if (vertexLabel.hasIDPrimaryKey()) {
            sql.append(maybeWrapInQoutes("ID"));
        } else {
            int count = 1;
            for (String identifier : vertexLabel.getIdentifiers()) {
                sql.append(maybeWrapInQoutes(identifier));
                if (count++ < vertexLabel.getIdentifiers().size()) {
                    sql.append(",");
                }
            }
        }
        sql.append(") as (\nSELECT * FROM (VALUES");
        if (vertexLabel.hasIDPrimaryKey()) {
            int count = 1;
            for (RecordId.ID id : ids) {
                sql.append("(");
                sql.append(id.getSequenceId());
                sql.append(")");
                if (count++ < ids.size()) {
                    sql.append(",");
                }
            }
        } else {
            int cnt = 1;
            for (RecordId.ID id : ids) {
                sql.append("(");
                int count = 1;
                for (Comparable identifierValue : id.getIdentifiers()) {
                    sql.append(toRDBSStringLiteral(identifierValue));
                    if (count++ < id.getIdentifiers().size()) {
                        sql.append(",");
                    }
                }
                sql.append(")");
                if (cnt++ < ids.size()) {
                    sql.append(",");
                }
            }
        }
        sql.append(") as tmp(");
        if (vertexLabel.hasIDPrimaryKey()) {
            sql.append(maybeWrapInQoutes("ID"));
        } else {
            int count = 1;
            for (String identifier : vertexLabel.getIdentifiers()) {
                sql.append(maybeWrapInQoutes(identifier));
                if (count++ < vertexLabel.getIdentifiers().size()) {
                    sql.append(",");
                }
            }
        }
        sql.append("))\n");
        sql.append("DELETE a FROM\n\t");
        sql.append(maybeWrapInQoutes(vertexLabel.getSchema().getName()));
        sql.append(".");
        sql.append(maybeWrapInQoutes(Topology.VERTEX_PREFIX + vertexLabel.getName()));
        sql.append(" a JOIN todelete on ");
        if (vertexLabel.hasIDPrimaryKey()) {
            sql.append("todelete.");
            sql.append(maybeWrapInQoutes("ID"));
            sql.append(" = a.");
            sql.append(maybeWrapInQoutes("ID"));
        } else {
            int count = 1;
            for (String identifier : vertexLabel.getIdentifiers()) {
                sql.append("todelete.");
                sql.append(maybeWrapInQoutes(identifier));
                sql.append(" = a.");
                sql.append(maybeWrapInQoutes(identifier));
                if (count++ < vertexLabel.getIdentifiers().size()) {
                    sql.append(" AND ");
                }
            }
        }
        return sql.toString();
    }

    @SuppressWarnings("Duplicates")
    @Override
    public String drop(EdgeLabel edgeLabel, Collection<RecordId.ID> ids) {
        StringBuilder sql = new StringBuilder();

        sql.append("WITH todelete(");
        if (edgeLabel.hasIDPrimaryKey()) {
            sql.append(maybeWrapInQoutes("ID"));
        } else {
            int count = 1;
            for (String identifier : edgeLabel.getIdentifiers()) {
                sql.append(maybeWrapInQoutes(identifier));
                if (count++ < edgeLabel.getIdentifiers().size()) {
                    sql.append(",");
                }
            }
        }
        sql.append(") as (\nSELECT * FROM (VALUES");
        if (edgeLabel.hasIDPrimaryKey()) {
            int count = 1;
            for (RecordId.ID id : ids) {
                sql.append("(");
                sql.append(id.getSequenceId());
                sql.append(")");
                if (count++ < ids.size()) {
                    sql.append(",");
                }
            }
        } else {
            int cnt = 1;
            for (RecordId.ID id : ids) {
                sql.append("(");
                int count = 1;
                for (Comparable identifierValue : id.getIdentifiers()) {
                    sql.append(toRDBSStringLiteral(identifierValue));
                    if (count++ < id.getIdentifiers().size()) {
                        sql.append(",");
                    }
                }
                sql.append(")");
                if (cnt++ < ids.size()) {
                    sql.append(",");
                }
            }
        }
        sql.append(") as tmp(");
        if (edgeLabel.hasIDPrimaryKey()) {
            sql.append(maybeWrapInQoutes("ID"));
        } else {
            int count = 1;
            for (String identifier : edgeLabel.getIdentifiers()) {
                sql.append(maybeWrapInQoutes(identifier));
                if (count++ < edgeLabel.getIdentifiers().size()) {
                    sql.append(",");
                }
            }
        }
        sql.append("))\n");
        sql.append("DELETE a FROM\n\t");
        sql.append(maybeWrapInQoutes(edgeLabel.getSchema().getName()));
        sql.append(".");
        sql.append(maybeWrapInQoutes(Topology.EDGE_PREFIX + edgeLabel.getName()));
        sql.append(" a JOIN todelete on ");
        if (edgeLabel.hasIDPrimaryKey()) {
            sql.append("todelete.");
            sql.append(maybeWrapInQoutes("ID"));
            sql.append(" = a.");
            sql.append(maybeWrapInQoutes("ID"));
        } else {
            int count = 1;
            for (String identifier : edgeLabel.getIdentifiers()) {
                sql.append("todelete.");
                sql.append(maybeWrapInQoutes(identifier));
                sql.append(" = a.");
                sql.append(maybeWrapInQoutes(identifier));
                if (count++ < edgeLabel.getIdentifiers().size()) {
                    sql.append(" AND ");
                }
            }
        }
        return sql.toString();
    }
}
