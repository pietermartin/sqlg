package org.umlg.sqlg.sql.dialect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.mchange.v2.c3p0.C3P0ProxyConnection;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.postgis.*;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.PGCopyInputStream;
import org.postgresql.copy.PGCopyOutputStream;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.PGbytea;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.gis.GeographyPoint;
import org.umlg.sqlg.gis.GeographyPolygon;
import org.umlg.sqlg.gis.Gis;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.util.SqlgUtil;

import java.io.*;
import java.lang.reflect.Method;
import java.security.SecureRandom;
import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.util.*;

import static org.umlg.sqlg.structure.PropertyType.*;

/**
 * Date: 2014/07/16
 * Time: 1:42 PM
 */
@SuppressWarnings("unused")
public class PostgresDialect extends BaseSqlDialect {

    private static final String BATCH_NULL = "";
    private static final String COPY_COMMAND_DELIMITER = "\t";
    //this strange character is apparently an illegal json char so its good as a quote
    private static final String COPY_COMMAND_QUOTE = "e'\\x01'";
    private static final char QUOTE = 0x01;
    private static final char ESCAPE = '\\';
    private static final int PARAMETER_LIMIT = 32767;
    private static final String COPY_DUMMY = "_copy_dummy";
    private Logger logger = LoggerFactory.getLogger(SqlgGraph.class.getName());
    private PropertyType postGisType;

    @SuppressWarnings("unused")
    public PostgresDialect() {
        super();
    }

    @Override
    public String dialectName() {
        return "Postgresql";
    }

    @Override
    public String createSchemaStatement() {
        // if ever schema is created outside of sqlg while the graph is already instantiated
        return "CREATE SCHEMA IF NOT EXISTS ";
    }

    @Override
    public boolean supportsBatchMode() {
        return true;
    }

    @Override
    public Set<String> getDefaultSchemas() {
        return ImmutableSet.copyOf(Arrays.asList("pg_catalog", "public", "information_schema", "tiger", "tiger_data", "topology"));
    }

    @Override
    public Set<String> getSpacialRefTable() {
        return ImmutableSet.copyOf(Collections.singletonList("spatial_ref_sys"));
    }

    @Override
    public List<String> getGisSchemas() {
        return Arrays.asList("tiger", "tiger_data", "topology");
    }

    @Override
    public String getForeignKeyTypeDefinition() {
        return "BIGINT";
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
        return "BIGSERIAL PRIMARY KEY";
    }

    public void assertTableName(String tableName) {
        if (!StringUtils.isEmpty(tableName) && tableName.length() > 63) {
            throw new IllegalStateException(String.format("Postgres table names must be 63 characters or less! Given table name is %s", tableName));
        }
    }

    @Override
    public String getArrayDriverType(PropertyType propertyType) {
        switch (propertyType) {
            case BYTE_ARRAY:
                return "bytea";
            case byte_ARRAY:
                return "bytea";
            case boolean_ARRAY:
                return "bool";
            case BOOLEAN_ARRAY:
                return "bool";
            case SHORT_ARRAY:
                return "smallint";
            case short_ARRAY:
                return "smallint";
            case INTEGER_ARRAY:
                return "integer";
            case int_ARRAY:
                return "integer";
            case LONG_ARRAY:
                return "bigint";
            case long_ARRAY:
                return "bigint";
            case FLOAT_ARRAY:
                return "float";
            case float_ARRAY:
                return "float";
            case DOUBLE_ARRAY:
                return "float";
            case double_ARRAY:
                return "float";
            case STRING_ARRAY:
                return "varchar";
            case LOCALDATETIME_ARRAY:
                return "timestamptz";
            case LOCALDATE_ARRAY:
                return "date";
            case LOCALTIME_ARRAY:
                return "timetz";
            case ZONEDDATETIME_ARRAY:
                return "timestamptz";
            case JSON_ARRAY:
                return "jsonb";
            default:
                throw new IllegalStateException("propertyType " + propertyType.name() + " unknown!");
        }
    }

    @Override
    public String existIndexQuery(SchemaTable schemaTable, String prefix, String indexName) {
        StringBuilder sb = new StringBuilder("SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace");
        sb.append(" WHERE  c.relname = '");
        sb.append(indexName);
        sb.append("' AND n.nspname = '");
        sb.append(schemaTable.getSchema());
        sb.append("'");
        return sb.toString();
    }

    /**
     * flushes the cache via the copy command.
     *
     * @param vertexCache A rather complex object.
     *                    The map's key is the vertex being cached.
     *                    The Triple holds,
     *                    1) The in labels
     *                    2) The out labels
     *                    3) The properties as a map of key values
     */
    @Override
    public Map<SchemaTable, Pair<Long, Long>> flushVertexCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> vertexCache) {
        Map<SchemaTable, Pair<Long, Long>> verticesRanges = new LinkedHashMap<>();
        C3P0ProxyConnection con = (C3P0ProxyConnection) sqlgGraph.tx().getConnection();
        try {
            Method m = BaseConnection.class.getMethod("getCopyAPI", new Class[]{});
            Object[] arg = new Object[]{};
            CopyManager copyManager = (CopyManager) con.rawConnectionOperation(m, C3P0ProxyConnection.RAW_CONNECTION, arg);
            for (SchemaTable schemaTable : vertexCache.keySet()) {
                Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> vertices = vertexCache.get(schemaTable);
                Map<String, PropertyType> propertyTypeMap = sqlgGraph.getSchemaManager().getAllTables().get(schemaTable.getSchema() + "." + SchemaManager.VERTEX_PREFIX + schemaTable.getTable());

                //insert the labeled vertices
                long endHigh = 0;
                long numberInserted = 0;
                try (InputStream is = mapVertexToInputStream(propertyTypeMap, vertices)) {
                    StringBuilder sql = new StringBuilder();
                    sql.append("COPY ");
                    sql.append(maybeWrapInQoutes(schemaTable.getSchema()));
                    sql.append(".");
                    sql.append(maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + schemaTable.getTable()));
                    sql.append(" (");
                    if (vertices.getLeft().isEmpty()) {
                        //copy command needs at least one field.
                        //check if the dummy field exist, if not create it
                        sqlgGraph.getSchemaManager().ensureColumnExist(
                                schemaTable.getSchema(),
                                SchemaManager.VERTEX_PREFIX + schemaTable.getTable(),
                                ImmutablePair.of(COPY_DUMMY, PropertyType.from(0)));
                        sql.append(maybeWrapInQoutes(COPY_DUMMY));
                    } else {
                        int count = 1;
                        for (String key : vertices.getLeft()) {
                            if (count > 1 && count <= vertices.getLeft().size()) {
                                sql.append(", ");
                            }
                            count++;
                            appendKeyForStream(propertyTypeMap.get(key), sql, key);
                        }
                    }
                    sql.append(")");

                    sql.append(" FROM stdin CSV DELIMITER '");
                    sql.append(COPY_COMMAND_DELIMITER);
                    sql.append("' ");
                    sql.append("QUOTE ");
                    sql.append(COPY_COMMAND_QUOTE);
                    sql.append(" ESCAPE '");
                    sql.append(ESCAPE);
                    sql.append("'");
                    sql.append(" NULL '");
                    sql.append(BATCH_NULL);
                    sql.append("';");
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql.toString());
                    }
                    numberInserted = copyManager.copyIn(sql.toString(), is);
                    if (numberInserted > 0) {
                        try (PreparedStatement preparedStatement = con.prepareStatement("SELECT CURRVAL('\"" + schemaTable.getSchema() + "\".\"" + SchemaManager.VERTEX_PREFIX + schemaTable.getTable() + "_ID_seq\"');")) {
                            ResultSet resultSet = preparedStatement.executeQuery();
                            resultSet.next();
                            endHigh = resultSet.getLong(1);
                            resultSet.close();
                        }
                        //set the id on the vertex
                        long id = endHigh - numberInserted + 1;
                        for (SqlgVertex sqlgVertex : vertices.getRight().keySet()) {
                            sqlgVertex.setInternalPrimaryKey(RecordId.from(schemaTable, id++));
                        }
                        verticesRanges.put(schemaTable, Pair.of(endHigh - numberInserted + 1, endHigh));
                    }
                }

            }
            return verticesRanges;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
//    public void flushEdgeCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>>> edgeCache) {
    public void flushEdgeCache(SqlgGraph sqlgGraph, Map<MetaEdge, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>>> edgeCache) {
        C3P0ProxyConnection con = (C3P0ProxyConnection) sqlgGraph.tx().getConnection();
        try {
            Method m = BaseConnection.class.getMethod("getCopyAPI", new Class[]{});
            Object[] arg = new Object[]{};
            CopyManager copyManager = (CopyManager) con.rawConnectionOperation(m, C3P0ProxyConnection.RAW_CONNECTION, arg);

            for (MetaEdge metaEdge : edgeCache.keySet()) {
                Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> triples = edgeCache.get(metaEdge);

                Map<String, PropertyType> propertyTypeMap = sqlgGraph.getSchemaManager().getAllTables()
                        .get(metaEdge.getSchemaTable().getSchema() + "." + SchemaManager.EDGE_PREFIX + metaEdge.getSchemaTable().getTable());
                long endHigh;
                long numberInserted;
                try (InputStream is = mapEdgeToInputStream(propertyTypeMap, triples)) {
                    StringBuilder sql = new StringBuilder();
                    sql.append("COPY ");
                    sql.append(maybeWrapInQoutes(metaEdge.getSchemaTable().getSchema()));
                    sql.append(".");
                    sql.append(maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + metaEdge.getSchemaTable().getTable()));
                    sql.append(" (");
                    for (Triple<SqlgVertex, SqlgVertex, Map<String, Object>> triple : triples.getRight().values()) {
                        int count = 1;
                        sql.append(maybeWrapInQoutes(triple.getLeft().getSchema() + "." + triple.getLeft().getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
                        sql.append(", ");
                        sql.append(maybeWrapInQoutes(triple.getMiddle().getSchema() + "." + triple.getMiddle().getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
                        for (String key : triples.getLeft()) {
                            if (count <= triples.getLeft().size()) {
                                sql.append(", ");
                            }
                            count++;
                            appendKeyForStream(propertyTypeMap.get(key), sql, key);
                        }
                        break;
                    }
                    sql.append(") ");

                    sql.append(" FROM stdin CSV DELIMITER '");
                    sql.append(COPY_COMMAND_DELIMITER);
                    sql.append("' ");
                    sql.append("QUOTE ");
                    sql.append(COPY_COMMAND_QUOTE);
                    sql.append(" ESCAPE '");
                    sql.append(ESCAPE);
                    sql.append("';");
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql.toString());
                    }
                    numberInserted = copyManager.copyIn(sql.toString(), is);
                    try (PreparedStatement preparedStatement = con.prepareStatement(
                            "SELECT CURRVAL('\"" + metaEdge.getSchemaTable().getSchema() + "\".\"" +
                                    SchemaManager.EDGE_PREFIX + metaEdge.getSchemaTable().getTable() + "_ID_seq\"');")) {

                        ResultSet resultSet = preparedStatement.executeQuery();
                        resultSet.next();
                        endHigh = resultSet.getLong(1);
                        resultSet.close();
                    }
                    //set the id on the vertex
                    long id = endHigh - numberInserted + 1;
                    for (SqlgEdge sqlgEdge : triples.getRight().keySet()) {
                        sqlgEdge.setInternalPrimaryKey(RecordId.from(metaEdge.getSchemaTable(), id++));
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flushVertexPropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>>> schemaVertexPropertyCache) {
        flushElementPropertyCache(sqlgGraph, true, schemaVertexPropertyCache);
    }

    @Override
    public void flushEdgePropertyCache(SqlgGraph sqlgGraph, Map<SchemaTable, Pair<SortedSet<String>, Map<SqlgEdge, Map<String, Object>>>> edgePropertyCache) {
        flushElementPropertyCache(sqlgGraph, false, edgePropertyCache);
    }

    public <T extends SqlgElement> void flushElementPropertyCache(SqlgGraph sqlgGraph, boolean forVertices, Map<SchemaTable, Pair<SortedSet<String>, Map<T, Map<String, Object>>>> schemaVertexPropertyCache) {

        Connection conn = sqlgGraph.tx().getConnection();
        Map<String, Map<String, PropertyType>> allTables = sqlgGraph.getSchemaManager().getAllTables();
        for (SchemaTable schemaTable : schemaVertexPropertyCache.keySet()) {

            Pair<SortedSet<String>, Map<T, Map<String, Object>>> vertexKeysPropertyCache = schemaVertexPropertyCache.get(schemaTable);
            SortedSet<String> keys = vertexKeysPropertyCache.getLeft();
            Map<? extends SqlgElement, Map<String, Object>> vertexPropertyCache = vertexKeysPropertyCache.getRight();

            StringBuilder sql = new StringBuilder();
            sql.append("UPDATE ");
            sql.append(maybeWrapInQoutes(schemaTable.getSchema()));
            sql.append(".");
            sql.append(maybeWrapInQoutes((forVertices ? SchemaManager.VERTEX_PREFIX : SchemaManager.EDGE_PREFIX) + schemaTable.getTable()));
            sql.append(" a \nSET\n\t(");
            int count = 1;
            //this map is for optimizations reason to not look up the property via all tables within the loop
            Map<String, PropertyType> keyPropertyTypeMap = new HashMap<>();
            for (String key : keys) {
                PropertyType propertyType = allTables.get(schemaTable.getSchema() + "." + (forVertices ? SchemaManager.VERTEX_PREFIX : SchemaManager.EDGE_PREFIX) + schemaTable.getTable()).get(key);
                keyPropertyTypeMap.put(key, propertyType);
                appendKeyForBatchUpdate(propertyType, sql, key, false);
                if (count++ < keys.size()) {
                    sql.append(", ");
                }
            }
            sql.append(") = \n\t(");
            count = 1;
            for (String key : keys) {
                sql.append("v.");
                PropertyType propertyType = keyPropertyTypeMap.get(key);
                appendKeyForBatchUpdate(propertyType, sql, key, true);
                switch (propertyType) {
                    case boolean_ARRAY:
                        sql.append("::boolean[]");
                        break;
                    case byte_ARRAY:
                        sql.append("::bytea");
                        break;
                    case short_ARRAY:
                        sql.append("::smallint[]");
                        break;
                    case int_ARRAY:
                        sql.append("::int[]");
                        break;
                    case long_ARRAY:
                        sql.append("::bigint[]");
                        break;
                    case float_ARRAY:
                        sql.append("::real[]");
                        break;
                    case double_ARRAY:
                        sql.append("::double precision[]");
                        break;
                    case STRING_ARRAY:
                        sql.append("::text[]");
                        break;
                    case BOOLEAN_ARRAY:
                        sql.append("::boolean[]");
                        break;
                    case BYTE_ARRAY:
                        sql.append("::bytea");
                        break;
                    case SHORT_ARRAY:
                        sql.append("::smallint[]");
                        break;
                    case INTEGER_ARRAY:
                        sql.append("::int[]");
                        break;
                    case LONG_ARRAY:
                        sql.append("::bigint[]");
                        break;
                    case FLOAT_ARRAY:
                        sql.append("::real[]");
                        break;
                    case DOUBLE_ARRAY:
                        sql.append("::double precision[]");
                        break;
                }
                if (count++ < keys.size()) {
                    sql.append(", ");
                }
            }
            sql.append(")\nFROM (\nVALUES\n\t");
            count = 1;
            for (SqlgElement sqlgVertex : vertexPropertyCache.keySet()) {
                Map<String, Object> properties = vertexPropertyCache.get(sqlgVertex);
                sql.append("(");
                sql.append(((RecordId) sqlgVertex.id()).getId());
                sql.append(", ");
                int countProperties = 1;
                for (String key : keys) {
                    Object value = properties.get(key);
                    if (value == null) {
                        if (sqlgVertex.property(key).isPresent()) {
                            value = sqlgVertex.value(key);
                        } else {
                            value = "null";
                        }
                    }
                    PropertyType propertyType = keyPropertyTypeMap.get(key);
                    switch (propertyType) {
                        case BOOLEAN:
                            sql.append(value);
                            break;
                        case BYTE:
                            sql.append(value);
                            break;
                        case SHORT:
                            sql.append(value);
                            break;
                        case INTEGER:
                            sql.append(value);
                            break;
                        case LONG:
                            sql.append(value);
                            break;
                        case FLOAT:
                            sql.append(value);
                            break;
                        case DOUBLE:
                            sql.append(value);
                            break;
                        case STRING:
                            //Postgres supports custom quoted strings using the 'with token' clause
                            sql.append("$token$");
                            sql.append(value);
                            sql.append("$token$");
                            break;
                        case LOCALDATETIME:
                            sql.append("'");
                            sql.append(value.toString());
                            sql.append("'::TIMESTAMP");
                            break;
                        case LOCALDATE:
                            sql.append("'");
                            sql.append(value.toString());
                            sql.append("'::DATE");
                            break;
                        case LOCALTIME:
                            sql.append("'");
                            sql.append(shiftDST((LocalTime) value).toString());
                            sql.append("'::TIME");
                            break;
                        case ZONEDDATETIME:
                            ZonedDateTime zonedDateTime = (ZonedDateTime) value;
                            LocalDateTime localDateTime = zonedDateTime.toLocalDateTime();
                            TimeZone timeZone = TimeZone.getTimeZone(zonedDateTime.getZone().getId());
                            sql.append("'");
                            sql.append(localDateTime.toString());
                            sql.append("'::TIMESTAMP");
                            sql.append(",'");
                            sql.append(timeZone.getID());
                            sql.append("'");
                            break;
                        case DURATION:
                            Duration duration = (Duration) value;
                            sql.append("'");
                            sql.append(duration.getSeconds());
                            sql.append("'::BIGINT");
                            sql.append(",'");
                            sql.append(duration.getNano());
                            sql.append("'::INTEGER");
                            break;
                        case PERIOD:
                            Period period = (Period) value;
                            sql.append("'");
                            sql.append(period.getYears());
                            sql.append("'::INTEGER");
                            sql.append(",'");
                            sql.append(period.getMonths());
                            sql.append("'::INTEGER");
                            sql.append(",'");
                            sql.append(period.getDays());
                            sql.append("'::INTEGER");
                            break;
                        case JSON:
                            sql.append("'");
                            sql.append(value.toString());
                            sql.append("'::JSONB");
                            break;
                        case boolean_ARRAY:
                            sql.append("'{");
                            boolean[] booleanArray = (boolean[]) value;
                            int countBooleanArray = 1;
                            for (Boolean b : booleanArray) {
                                sql.append(b);
                                if (countBooleanArray++ < booleanArray.length) {
                                    sql.append(",");
                                }
                            }
                            sql.append("}'");
                            break;
                        case BOOLEAN_ARRAY:
                            sql.append("'{");
                            Boolean[] BooleanArray = (Boolean[]) value;
                            int countBOOLEANArray = 1;
                            for (Boolean b : BooleanArray) {
                                sql.append(b);
                                if (countBOOLEANArray++ < BooleanArray.length) {
                                    sql.append(",");
                                }
                            }
                            sql.append("}'");
                            break;
                        case byte_ARRAY:
                            try {
                                sql.append("'");
                                sql.append(PGbytea.toPGString((byte[]) value));
                                sql.append("'");
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                            break;
                        case BYTE_ARRAY:
                            try {
                                sql.append("'");
                                sql.append(PGbytea.toPGString((byte[]) SqlgUtil.convertByteArrayToPrimitiveArray((Byte[]) value)));
                                sql.append("'");
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                            break;
                        case short_ARRAY:
                            sql.append("'{");
                            short[] sortArray = (short[]) value;
                            int countShortArray = 1;
                            for (Short s : sortArray) {
                                sql.append(s);
                                if (countShortArray++ < sortArray.length) {
                                    sql.append(",");
                                }
                            }
                            sql.append("}'");
                            break;
                        case SHORT_ARRAY:
                            sql.append("'{");
                            Short[] shortObjectArray = (Short[]) value;
                            for (int i = 0; i < shortObjectArray.length; i++) {
                                Short s = shortObjectArray[i];
                                sql.append(s);
                                if (i < shortObjectArray.length - 1) {
                                    sql.append(",");
                                }
                            }
                            sql.append("}'");
                            break;
                        case int_ARRAY:
                            sql.append("'{");
                            int[] intArray = (int[]) value;
                            int countIntArray = 1;
                            for (Integer i : intArray) {
                                sql.append(i);
                                if (countIntArray++ < intArray.length) {
                                    sql.append(",");
                                }
                            }
                            sql.append("}'");
                            break;
                        case INTEGER_ARRAY:
                            sql.append("'{");
                            Integer[] integerArray = (Integer[]) value;
                            int countIntegerArray = 1;
                            for (Integer i : integerArray) {
                                sql.append(i);
                                if (countIntegerArray++ < integerArray.length) {
                                    sql.append(",");
                                }
                            }
                            sql.append("}'");
                            break;
                        case LONG_ARRAY:
                            sql.append("'{");
                            Long[] longArray = (Long[]) value;
                            int countLongArray = 1;
                            for (Long l : longArray) {
                                sql.append(l);
                                if (countLongArray++ < longArray.length) {
                                    sql.append(",");
                                }
                            }
                            sql.append("}'");
                            break;
                        case long_ARRAY:
                            sql.append("'{");
                            long[] longPrimitiveArray = (long[]) value;
                            int countLongPrimitiveArray = 1;
                            for (Long l : longPrimitiveArray) {
                                sql.append(l);
                                if (countLongPrimitiveArray++ < longPrimitiveArray.length) {
                                    sql.append(",");
                                }
                            }
                            sql.append("}'");
                            break;
                        case FLOAT_ARRAY:
                            sql.append("'{");
                            Float[] floatArray = (Float[]) value;
                            int countFloatArray = 1;
                            for (Float f : floatArray) {
                                sql.append(f);
                                if (countFloatArray++ < floatArray.length) {
                                    sql.append(",");
                                }
                            }
                            sql.append("}'");
                            break;
                        case float_ARRAY:
                            sql.append("'{");
                            float[] floatPrimitiveArray = (float[]) value;
                            int countFloatPrimitiveArray = 1;
                            for (Float f : floatPrimitiveArray) {
                                sql.append(f);
                                if (countFloatPrimitiveArray++ < floatPrimitiveArray.length) {
                                    sql.append(",");
                                }
                            }
                            sql.append("}'");
                            break;
                        case DOUBLE_ARRAY:
                            sql.append("'{");
                            Double[] doubleArray = (Double[]) value;
                            int countDoubleArray = 1;
                            for (Double d : doubleArray) {
                                sql.append(d);
                                if (countDoubleArray++ < doubleArray.length) {
                                    sql.append(",");
                                }
                            }
                            sql.append("}'");
                            break;
                        case double_ARRAY:
                            sql.append("'{");
                            double[] doublePrimitiveArray = (double[]) value;
                            int countDoublePrimitiveArray = 1;
                            for (Double d : doublePrimitiveArray) {
                                sql.append(d);
                                if (countDoublePrimitiveArray++ < doublePrimitiveArray.length) {
                                    sql.append(",");
                                }
                            }
                            sql.append("}'");
                            break;
                        case STRING_ARRAY:
                            sql.append("'{");
                            String[] stringArray = (String[]) value;
                            int countStringArray = 1;
                            for (String s : stringArray) {
                                sql.append("\"");
                                sql.append(s);
                                sql.append("\"");
                                if (countStringArray++ < stringArray.length) {
                                    sql.append(",");
                                }
                            }
                            sql.append("}'");
                            break;
                        default:
                            throw new IllegalStateException("Unknown propertyType " + propertyType.name());
                    }
                    if (countProperties++ < keys.size()) {
                        sql.append(", ");
                    }
                }
                sql.append(")");
                if (count++ < vertexPropertyCache.size()) {
                    sql.append(",\n\t");
                }
            }

            sql.append("\n) AS v(id, ");
            count = 1;
            for (String key : keys) {
                PropertyType propertyType = keyPropertyTypeMap.get(key);
                appendKeyForBatchUpdate(propertyType, sql, key, false);
                if (count++ < keys.size()) {
                    sql.append(", ");
                }
            }
            sql.append(")");
            sql.append("\nWHERE a.\"ID\" = v.id");
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            try (Statement statement = conn.createStatement()) {
                statement.execute(sql.toString());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public String constructCompleteCopyCommandTemporarySqlVertex(SqlgGraph sqlgGraph, SqlgVertex vertex, Map<String, Object> keyValueMap) {
        return internalConstructCompleteCopyCommandSqlVertex(sqlgGraph, true, vertex, keyValueMap);
    }

    @Override
    public String constructCompleteCopyCommandSqlVertex(SqlgGraph sqlgGraph, SqlgVertex vertex, Map<String, Object> keyValueMap) {
        return internalConstructCompleteCopyCommandSqlVertex(sqlgGraph, false, vertex, keyValueMap);
    }

    private String internalConstructCompleteCopyCommandSqlVertex(SqlgGraph sqlgGraph, boolean isTemp, SqlgVertex vertex, Map<String, Object> keyValueMap) {
        Map<String, PropertyType> propertyTypeMap = sqlgGraph.getSchemaManager().getAllTables().get((isTemp == false ? vertex.getSchema() + "." : "") + SchemaManager.VERTEX_PREFIX + vertex.getTable());
        StringBuilder sql = new StringBuilder();
        sql.append("COPY ");
        if (!isTemp) {
            sql.append(maybeWrapInQoutes(vertex.getSchema()));
            sql.append(".");
        }
        sql.append(maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + vertex.getTable()));
        sql.append(" (");
        if (keyValueMap.isEmpty()) {
            //copy command needs at least one field.
            //check if the dummy field exist, if not create it
            sqlgGraph.getSchemaManager().ensureColumnExist(
                    vertex.getSchema(),
                    SchemaManager.VERTEX_PREFIX + vertex.getTable(),
                    ImmutablePair.of(COPY_DUMMY, PropertyType.from(0)));
            sql.append(maybeWrapInQoutes(COPY_DUMMY));
        } else {
            int count = 1;
            for (String key : keyValueMap.keySet()) {
                if (count > 1 && count <= keyValueMap.size()) {
                    sql.append(", ");
                }
                count++;
                appendKeyForStream(propertyTypeMap.get(key), sql, key);
            }
        }
        sql.append(")");
        sql.append(" FROM stdin CSV DELIMITER '");
        sql.append(COPY_COMMAND_DELIMITER);
        sql.append("' ");
        sql.append("QUOTE ");
        sql.append(COPY_COMMAND_QUOTE);
        sql.append(" ESCAPE '");
        sql.append(ESCAPE);
        sql.append("'");
        sql.append(" NULL'");
        sql.append(BATCH_NULL);
        sql.append("';");
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        return sql.toString();
    }

    @Override
    public String constructCompleteCopyCommandSqlEdge(SqlgGraph sqlgGraph, SqlgEdge sqlgEdge, SqlgVertex outVertex, SqlgVertex inVertex, Map<String, Object> keyValueMap) {
        Map<String, PropertyType> propertyTypeMap = sqlgGraph.getSchemaManager().getAllTables().get(sqlgEdge.getSchema() + "." + SchemaManager.EDGE_PREFIX + sqlgEdge.getTable());
        StringBuilder sql = new StringBuilder();
        sql.append("COPY ");
        sql.append(maybeWrapInQoutes(sqlgEdge.getSchema()));
        sql.append(".");
        sql.append(maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + sqlgEdge.getTable()));
        sql.append(" (");
        sql.append(maybeWrapInQoutes(outVertex.getSchema() + "." + outVertex.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
        sql.append(", ");
        sql.append(maybeWrapInQoutes(inVertex.getSchema() + "." + inVertex.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
        int count = 1;
        for (String key : keyValueMap.keySet()) {
            if (count <= keyValueMap.size()) {
                sql.append(", ");
            }
            count++;
            appendKeyForStream(propertyTypeMap.get(key), sql, key);
        }
        sql.append(") ");

        sql.append(" FROM stdin CSV DELIMITER '");
        sql.append(COPY_COMMAND_DELIMITER);
        sql.append("' ");
        sql.append("QUOTE ");
        sql.append(COPY_COMMAND_QUOTE);
        sql.append(";");
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        return sql.toString();
    }

    private void appendKeyForStream(PropertyType propertyType, StringBuilder sql, String key) {
        String[] sqlDefinitions = propertyTypeToSqlDefinition(propertyType);
        int countPerKey = 1;
        for (@SuppressWarnings("unused") String sqlDefinition : sqlDefinitions) {
            if (countPerKey > 1) {
                sql.append(maybeWrapInQoutes(key + propertyType.getPostFixes()[countPerKey - 2]));
            } else {
                sql.append(maybeWrapInQoutes(key));
            }
            if (countPerKey++ < sqlDefinitions.length) {
                sql.append(",");
            }
        }
    }

    private void appendKeyForBatchUpdate(PropertyType propertyType, StringBuilder sql, String key, boolean withV) {
        String[] sqlDefinitions = propertyTypeToSqlDefinition(propertyType);
        int countPerKey = 1;
        for (@SuppressWarnings("unused") String sqlDefinition : sqlDefinitions) {
            if (countPerKey > 1) {
                if (withV) {
                    sql.append("v.");
                }
                sql.append(maybeWrapInQoutes(key + propertyType.getPostFixes()[countPerKey - 2]));
            } else {
                sql.append(maybeWrapInQoutes(key));
            }
            if (countPerKey++ < sqlDefinitions.length) {
                sql.append(",");
            }
        }
    }


    @Override
    public String temporaryTableCopyCommandSqlVertex(SqlgGraph sqlgGraph, SchemaTable schemaTable, Map<String, Object> keyValueMap) {
        StringBuilder sql = new StringBuilder();
        sql.append("COPY ");
        //Temp tables only
        sql.append(maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + schemaTable.getTable()));
        sql.append(" (");
        if (keyValueMap.isEmpty()) {
            //copy command needs at least one field.
            //check if the dummy field exist, if not create it
            sqlgGraph.getSchemaManager().ensureColumnExist(
                    schemaTable.getSchema(),
                    SchemaManager.VERTEX_PREFIX + schemaTable.getTable(),
                    ImmutablePair.of(COPY_DUMMY, PropertyType.from(0)));
            sql.append(maybeWrapInQoutes(COPY_DUMMY));
        } else {
            int count = 1;
            for (String key : keyValueMap.keySet()) {
                if (count > 1 && count <= keyValueMap.size()) {
                    sql.append(", ");
                }
                count++;
                sql.append(maybeWrapInQoutes(key));
            }
        }
        sql.append(")");
        sql.append(" FROM stdin CSV DELIMITER '");
        sql.append(COPY_COMMAND_DELIMITER);
        sql.append("' ");
        sql.append("QUOTE ");
        sql.append(COPY_COMMAND_QUOTE);
        sql.append(" ESCAPE '");
        sql.append(ESCAPE);
        sql.append("';");
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        return sql.toString();
    }

    @Override
    public void writeStreamingVertex(OutputStream out, Map<String, Object> keyValueMap) {
        try {
            int countKeys = 1;
            if (keyValueMap.isEmpty()) {
                out.write(Integer.toString(1).getBytes());
            } else {
                for (Map.Entry<String, Object> entry : keyValueMap.entrySet()) {
                    if (countKeys > 1 && countKeys <= keyValueMap.size()) {
                        out.write(COPY_COMMAND_DELIMITER.getBytes());
                    }
                    countKeys++;
                    Object value = entry.getValue();
                    PropertyType propertyType;
                    if (value == null) {
                        propertyType = PropertyType.STRING;
                    } else {
                        propertyType = PropertyType.from(value);
                    }
                    if (JSON_ARRAY == propertyType) {
                        throw SqlgExceptions.invalidPropertyType(propertyType);
                    }
                    out.write(valueToStreamBytes(propertyType, value));
                }
            }
            out.write("\n".getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeStreamingEdge(OutputStream out, SqlgEdge sqlgEdge, SqlgVertex outVertex, SqlgVertex inVertex, Map<String, Object> keyValueMap) {
        try {
            String encoding = "UTF-8";
            out.write(((RecordId) outVertex.id()).getId().toString().getBytes(encoding));
            out.write(COPY_COMMAND_DELIMITER.getBytes(encoding));
            out.write(((RecordId) inVertex.id()).getId().toString().getBytes(encoding));
            for (Map.Entry<String, Object> entry : keyValueMap.entrySet()) {
                out.write(COPY_COMMAND_DELIMITER.getBytes(encoding));
                Object value = entry.getValue();
                PropertyType propertyType = PropertyType.from(value);
                out.write(valueToStreamBytes(propertyType, value));
            }
            out.write("\n".getBytes(encoding));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] valueToStreamBytes(PropertyType propertyType, Object value) throws UnsupportedEncodingException {
        String encoding = "UTF-8";
        return valueToStreamString(propertyType, value).getBytes(encoding);
    }

    private String valueToStreamString(PropertyType propertyType, Object value) {
        String result;
        if (value == null) {
            result = getBatchNull();
        } else {
            switch (propertyType) {
                case ZONEDDATETIME:
                    ZonedDateTime zonedDateTime = (ZonedDateTime) value;
                    LocalDateTime localDateTime = zonedDateTime.toLocalDateTime();
                    TimeZone timeZone = TimeZone.getTimeZone(zonedDateTime.getZone().getId());
                    result = localDateTime.toString() + COPY_COMMAND_DELIMITER + timeZone.getID();
                    break;
                case PERIOD:
                    Period period = (Period) value;
                    result = period.getYears() + COPY_COMMAND_DELIMITER + period.getMonths() + COPY_COMMAND_DELIMITER + period.getDays();
                    break;
                case DURATION:
                    Duration duration = (Duration) value;
                    result = duration.getSeconds() + COPY_COMMAND_DELIMITER + duration.getNano();
                    break;
                case LOCALTIME:
                    LocalTime lt = (LocalTime) value;
                    result = shiftDST(lt).toString();
                    break;
                case ZONEDDATETIME_ARRAY:
                    ZonedDateTime[] zonedDateTimes = (ZonedDateTime[]) value;
                    StringBuilder sb = new StringBuilder();
                    sb.append("{");
                    int length = java.lang.reflect.Array.getLength(value);
                    for (int i = 0; i < length; i++) {
                        zonedDateTime = zonedDateTimes[i];
                        localDateTime = zonedDateTime.toLocalDateTime();
                        result = localDateTime.toString();
                        sb.append(result);
                        if (i < length - 1) {
                            sb.append(",");
                        }
                    }
                    sb.append("}");
                    sb.append(COPY_COMMAND_DELIMITER);
                    sb.append("{");
                    for (int i = 0; i < length; i++) {
                        zonedDateTime = zonedDateTimes[i];
                        timeZone = TimeZone.getTimeZone(zonedDateTime.getZone().getId());
                        result = timeZone.getID();
                        sb.append(result);
                        if (i < length - 1) {
                            sb.append(",");
                        }
                    }
                    sb.append("}");
                    return sb.toString();
                case DURATION_ARRAY:
                    Duration[] durations = (Duration[]) value;
                    sb = new StringBuilder();
                    sb.append("{");
                    length = java.lang.reflect.Array.getLength(value);
                    for (int i = 0; i < length; i++) {
                        duration = durations[i];
                        sb.append(duration.getSeconds());
                        if (i < length - 1) {
                            sb.append(",");
                        }
                    }
                    sb.append("}");
                    sb.append(COPY_COMMAND_DELIMITER);
                    sb.append("{");
                    for (int i = 0; i < length; i++) {
                        duration = durations[i];
                        sb.append(duration.getNano());
                        if (i < length - 1) {
                            sb.append(",");
                        }
                    }
                    sb.append("}");
                    return sb.toString();
                case PERIOD_ARRAY:
                    Period[] periods = (Period[]) value;
                    sb = new StringBuilder();
                    sb.append("{");
                    length = java.lang.reflect.Array.getLength(value);
                    for (int i = 0; i < length; i++) {
                        period = periods[i];
                        sb.append(period.getYears());
                        if (i < length - 1) {
                            sb.append(",");
                        }
                    }
                    sb.append("}");
                    sb.append(COPY_COMMAND_DELIMITER);
                    sb.append("{");
                    for (int i = 0; i < length; i++) {
                        period = periods[i];
                        sb.append(period.getMonths());
                        if (i < length - 1) {
                            sb.append(",");
                        }
                    }
                    sb.append("}");
                    sb.append(COPY_COMMAND_DELIMITER);
                    sb.append("{");
                    for (int i = 0; i < length; i++) {
                        period = periods[i];
                        sb.append(period.getDays());
                        if (i < length - 1) {
                            sb.append(",");
                        }
                    }
                    sb.append("}");
                    return sb.toString();
                case LOCALTIME_ARRAY:
                    LocalTime[] localTimes = (LocalTime[]) value;
                    sb = new StringBuilder();
                    sb.append("{");
                    length = java.lang.reflect.Array.getLength(value);
                    for (int i = 0; i < length; i++) {
                        LocalTime localTime = localTimes[i];
                        result = shiftDST(localTime).toString();
                        sb.append(result);
                        if (i < length - 1) {
                            sb.append(",");
                        }
                    }
                    sb.append("}");
                    return sb.toString();
//                case JSON_ARRAY:
//                    throw new IllegalStateException("arrays of json just does not wanna behave");
//                    JsonNode[] jsons = (JsonNode[]) value;
//                    sb = new StringBuilder();
//                    sb.append("{");
//                    length = java.lang.reflect.Array.getLength(value);
//                    for (int i = 0; i < length; i++) {
//                        JsonNode json = jsons[i];
////                        sb.append(escapeSpecialCharacters("{" + COPY_COMMAND_QUOTE + "\"" + COPY_COMMAND_QUOTE + "username" + COPY_COMMAND_QUOTE + "\"" +
////                                COPY_COMMAND_QUOTE + ":" + COPY_COMMAND_QUOTE + "\"" + COPY_COMMAND_QUOTE + "asd" + COPY_COMMAND_QUOTE + "\"}"));
//                        sb.append(escapeSpecialCharacters("e'\\x01'{e'\\x01'" + getColumnEscapeKey() + "username" + getColumnEscapeKey() + ":1e'\\x01'}e'\\x01'"));
//                        if (i < length - 1) {
//                            sb.append(",");
//                        }
//                    }
//                    sb.append("}");
//                    return sb.toString();
                default:
                    if (value.getClass().isArray()) {
                        if (value.getClass().getName().equals("[B")) {
                            try {
                                String valueOfArrayAsString = PGbytea.toPGString((byte[]) value);
                                return (valueOfArrayAsString);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        } else {
                            sb = new StringBuilder();
                            sb.append("{");
                            length = java.lang.reflect.Array.getLength(value);
                            for (int i = 0; i < length; i++) {
                                String valueOfArray = java.lang.reflect.Array.get(value, i).toString();
                                sb.append(escapeSpecialCharacters(valueOfArray));
                                if (i < length - 1) {
                                    sb.append(",");
                                }
                            }
                            sb.append("}");
                            return sb.toString();
                        }
                    }
                    result = escapeSpecialCharacters(value.toString());
            }
        }
        return result;
    }


    @Override
    public void flushRemovedVertices(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgVertex>> removeVertexCache) {

        if (!removeVertexCache.isEmpty()) {


            //split the list of vertices, postgres has a 2 byte limit in the in clause
            for (Map.Entry<SchemaTable, List<SqlgVertex>> schemaVertices : removeVertexCache.entrySet()) {

                SchemaTable schemaTable = schemaVertices.getKey();

                Pair<Set<SchemaTable>, Set<SchemaTable>> tableLabels = sqlgGraph.getSchemaManager().getTableLabels(SchemaTable.of(schemaTable.getSchema(), SchemaManager.VERTEX_PREFIX + schemaTable.getTable()));

                //This is causing dead locks under load
//                dropForeignKeys(sqlgGraph, schemaTable);

                List<SqlgVertex> vertices = schemaVertices.getValue();
                int numberOfLoops = (vertices.size() / PARAMETER_LIMIT);
                int previous = 0;
                for (int i = 1; i <= numberOfLoops + 1; i++) {

                    int subListTo = i * PARAMETER_LIMIT;
                    List<SqlgVertex> subVertices;
                    if (i <= numberOfLoops) {
                        subVertices = vertices.subList(previous, subListTo);
                    } else {
                        subVertices = vertices.subList(previous, vertices.size());
                    }

                    previous = subListTo;

                    if (!subVertices.isEmpty()) {

                        Set<SchemaTable> inLabels = tableLabels.getLeft();
                        Set<SchemaTable> outLabels = tableLabels.getRight();

                        deleteEdges(sqlgGraph, schemaTable, subVertices, inLabels, true);
                        deleteEdges(sqlgGraph, schemaTable, subVertices, outLabels, false);

//                        Pair<Set<Long>, Set<SchemaTable>> outLabels = Pair.of(new HashSet<>(), new HashSet<>());
//                        Pair<Set<Long>, Set<SchemaTable>> inLabels = Pair.of(new HashSet<>(), new HashSet<>());
                        //get all the in and out labels for each vertex
                        //then for all in and out edges
                        //then remove the edges
//                        getInAndOutEdgesToRemove(sqlgGraph, subVertices, outLabels, inLabels);
//                        deleteEdges(sqlgGraph, schemaTable, outLabels, true);
//                        deleteEdges(sqlgGraph, schemaTable, inLabels, false);

                        StringBuilder sql = new StringBuilder("DELETE FROM ");
                        sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                        sql.append(".");
                        sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes((SchemaManager.VERTEX_PREFIX) + schemaTable.getTable()));
                        sql.append(" WHERE ");
                        sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
                        sql.append(" in (");
                        int count = 1;
                        for (SqlgVertex sqlgVertex : subVertices) {
                            sql.append("?");
                            if (count++ < subVertices.size()) {
                                sql.append(",");
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
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                            count = 1;
                            for (SqlgVertex sqlgVertex : subVertices) {
                                preparedStatement.setLong(count++, ((RecordId) sqlgVertex.id()).getId());
                            }
                            preparedStatement.executeUpdate();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }

//                        sql = new StringBuilder("DELETE FROM ");
//                        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(sqlgGraph.getSqlDialect().getPublicSchema()));
//                        sql.append(".");
//                        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(SchemaManager.VERTICES));
//                        sql.append(" WHERE ");
//                        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
//                        sql.append(" in (");
//
//                        count = 1;
//                        for (SqlgVertex vertex : subVertices) {
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
//                            logger.debug(sql.toString());
//                        }
//                        conn = sqlgGraph.tx().getConnection();
//                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
//                            count = 1;
//                            for (SqlgVertex vertex : subVertices) {
//                                preparedStatement.setLong(count++, (Long) vertex.id());
//                            }
//                            preparedStatement.executeUpdate();
//                        } catch (SQLException e) {
//                            throw new RuntimeException(e);
//                        }
                    }

                }

//                createForeignKeys(sqlgGraph, schemaTable);

            }
        }
    }


    private void dropForeignKeys(SqlgGraph sqlgGraph, SchemaTable schemaTable) {

        SchemaManager schemaManager = sqlgGraph.getSchemaManager();
        Map<String, Set<String>> edgeForeignKeys = schemaManager.getEdgeForeignKeys();

        for (Map.Entry<String, Set<String>> edgeForeignKey : edgeForeignKeys.entrySet()) {
            String edgeTable = edgeForeignKey.getKey();
            Set<String> foreignKeys = edgeForeignKey.getValue();
            String[] schemaTableArray = edgeTable.split("\\.");

            for (String foreignKey : foreignKeys) {
                if (foreignKey.startsWith(schemaTable.toString() + "_")) {

                    Set<String> foreignKeyNames = getForeignKeyConstraintNames(sqlgGraph, schemaTableArray[0], schemaTableArray[1]);
                    for (String foreignKeyName : foreignKeyNames) {

                        StringBuilder sql = new StringBuilder();
                        sql.append("ALTER TABLE ");
                        sql.append(maybeWrapInQoutes(schemaTableArray[0]));
                        sql.append(".");
                        sql.append(maybeWrapInQoutes(schemaTableArray[1]));
                        sql.append(" DROP CONSTRAINT ");
                        sql.append(maybeWrapInQoutes(foreignKeyName));
                        if (needsSemicolon()) {
                            sql.append(";");
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug(sql.toString());
                        }
                        Connection conn = sqlgGraph.tx().getConnection();
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                            preparedStatement.executeUpdate();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }

                    }
                }
            }
        }
    }

    private void createForeignKeys(SqlgGraph sqlgGraph, SchemaTable schemaTable) {
        SchemaManager schemaManager = sqlgGraph.getSchemaManager();
        Map<String, Set<String>> edgeForeignKeys = schemaManager.getEdgeForeignKeys();

        for (Map.Entry<String, Set<String>> edgeForeignKey : edgeForeignKeys.entrySet()) {
            String edgeTable = edgeForeignKey.getKey();
            Set<String> foreignKeys = edgeForeignKey.getValue();
            for (String foreignKey : foreignKeys) {
                if (foreignKey.startsWith(schemaTable.toString() + "_")) {
                    String[] schemaTableArray = edgeTable.split("\\.");
                    StringBuilder sql = new StringBuilder();
                    sql.append("ALTER TABLE ");
                    sql.append(maybeWrapInQoutes(schemaTableArray[0]));
                    sql.append(".");
                    sql.append(maybeWrapInQoutes(schemaTableArray[1]));
                    sql.append(" ADD FOREIGN KEY (");
                    sql.append(maybeWrapInQoutes(foreignKey));
                    sql.append(") REFERENCES ");
                    sql.append(maybeWrapInQoutes(schemaTable.getSchema()));
                    sql.append(".");
                    sql.append(maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + schemaTable.getTable()));
                    sql.append(" MATCH SIMPLE");
                    if (needsSemicolon()) {
                        sql.append(";");
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql.toString());
                    }
                    Connection conn = sqlgGraph.tx().getConnection();
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                        preparedStatement.executeUpdate();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private void deleteEdges(SqlgGraph sqlgGraph, SchemaTable schemaTable, List<SqlgVertex> subVertices, Set<SchemaTable> labels, boolean inDirection) {
        for (SchemaTable inLabel : labels) {

            StringBuilder sql = new StringBuilder();
            sql.append("DELETE FROM ");
            sql.append(maybeWrapInQoutes(inLabel.getSchema()));
            sql.append(".");
            sql.append(maybeWrapInQoutes(inLabel.getTable()));
            sql.append(" WHERE ");
            sql.append(maybeWrapInQoutes(schemaTable.toString() + (inDirection ? SchemaManager.IN_VERTEX_COLUMN_END : SchemaManager.OUT_VERTEX_COLUMN_END)));
            sql.append(" IN (");
            int count = 1;
            for (Vertex vertexToDelete : subVertices) {
                sql.append("?");
                if (count++ < subVertices.size()) {
                    sql.append(",");
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
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                count = 1;
                for (Vertex vertexToDelete : subVertices) {
                    preparedStatement.setLong(count++, ((RecordId) vertexToDelete.id()).getId());
                }
                int deleted = preparedStatement.executeUpdate();
                if (logger.isDebugEnabled()) {
                    logger.debug("Deleted " + deleted + " edges from " + inLabel.toString());
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void flushRemovedEdges(SqlgGraph sqlgGraph, Map<SchemaTable, List<SqlgEdge>> removeEdgeCache) {

        if (!removeEdgeCache.isEmpty()) {

            //split the list of edges, postgres has a 2 byte limit in the in clause
            for (Map.Entry<SchemaTable, List<SqlgEdge>> schemaEdges : removeEdgeCache.entrySet()) {

                List<SqlgEdge> edges = schemaEdges.getValue();
                int numberOfLoops = (edges.size() / PARAMETER_LIMIT);
                int previous = 0;
                for (int i = 1; i <= numberOfLoops + 1; i++) {

                    List<SqlgEdge> flattenedEdges = new ArrayList<>();
                    int subListTo = i * PARAMETER_LIMIT;
                    List<SqlgEdge> subEdges;
                    if (i <= numberOfLoops) {
                        subEdges = edges.subList(previous, subListTo);
                    } else {
                        subEdges = edges.subList(previous, edges.size());
                    }
                    previous = subListTo;

                    for (SchemaTable schemaTable : removeEdgeCache.keySet()) {
                        StringBuilder sql = new StringBuilder("DELETE FROM ");
                        sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
                        sql.append(".");
                        sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes((SchemaManager.EDGE_PREFIX) + schemaTable.getTable()));
                        sql.append(" WHERE ");
                        sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
                        sql.append(" in (");
                        int count = 1;
                        for (SqlgEdge sqlgEdge : subEdges) {
                            flattenedEdges.add(sqlgEdge);
                            sql.append("?");
                            if (count++ < subEdges.size()) {
                                sql.append(",");
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
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                            count = 1;
                            for (SqlgEdge sqlgEdge : subEdges) {
                                preparedStatement.setLong(count++, ((RecordId) sqlgEdge.id()).getId());
                            }
                            preparedStatement.executeUpdate();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
    }

    @Override
    public String getBatchNull() {
        return BATCH_NULL;
    }


    private InputStream mapVertexToInputStream(Map<String, PropertyType> propertyTypeMap, Pair<SortedSet<String>, Map<SqlgVertex, Map<String, Object>>> vertexCache) throws SQLException {
        //String str = "2,peter\n3,john";
        StringBuilder sb = new StringBuilder();
        int count = 1;
        for (SqlgVertex sqlgVertex : vertexCache.getRight().keySet()) {
            Map<String, Object> triple = vertexCache.getRight().get(sqlgVertex);
            //set the internal batch id to be used with inserting batch edges
            if (!vertexCache.getLeft().isEmpty()) {
                int countKeys = 1;
                for (String key : vertexCache.getLeft()) {
                    PropertyType propertyType = propertyTypeMap.get(key);
                    if (countKeys > 1 && countKeys <= vertexCache.getLeft().size()) {
                        sb.append(COPY_COMMAND_DELIMITER);
                    }
                    countKeys++;
                    Object value = triple.get(key);
//                    if (value == null) {
//                        sb.append(getBatchNull());
//                    }
                    switch (propertyType) {
                        case BYTE_ARRAY:
                            String valueOfArrayAsString = PGbytea.toPGString((byte[]) SqlgUtil.convertByteArrayToPrimitiveArray((Byte[]) value));
                            sb.append(valueOfArrayAsString);
                            break;
                        case byte_ARRAY:
                            valueOfArrayAsString = PGbytea.toPGString((byte[]) value);
                            sb.append(valueOfArrayAsString);
                            break;
                        default:
                            sb.append(valueToStreamString(propertyType, value));
                    }
                }
            } else {
                sb.append("0");
            }
            if (count++ < vertexCache.getRight().size()) {
                sb.append("\n");
            }
        }
        return new ByteArrayInputStream(sb.toString().getBytes());
    }

    private InputStream mapEdgeToInputStream(Map<String, PropertyType> propertyTypeMap, Pair<SortedSet<String>, Map<SqlgEdge, Triple<SqlgVertex, SqlgVertex, Map<String, Object>>>> edgeCache) throws SQLException {
        StringBuilder sb = new StringBuilder();
        int count = 1;
        for (Triple<SqlgVertex, SqlgVertex, Map<String, Object>> triple : edgeCache.getRight().values()) {
            sb.append(((RecordId) triple.getLeft().id()).getId());
            sb.append(COPY_COMMAND_DELIMITER);
            sb.append(((RecordId) triple.getMiddle().id()).getId());
            if (!edgeCache.getLeft().isEmpty()) {
                sb.append(COPY_COMMAND_DELIMITER);
            }
            int countKeys = 1;
            for (String key : edgeCache.getLeft()) {
                PropertyType propertyType = propertyTypeMap.get(key);
                Object value = triple.getRight().get(key);
//                if (value == null) {
//                    sb.append(getBatchNull());
//                }
                switch (propertyType) {
                    case BYTE_ARRAY:
                        String valueOfArrayAsString = PGbytea.toPGString((byte[]) SqlgUtil.convertByteArrayToPrimitiveArray((Byte[]) value));
                        sb.append(valueOfArrayAsString);
                        break;
                    case byte_ARRAY:
                        valueOfArrayAsString = PGbytea.toPGString((byte[]) value);
                        sb.append(valueOfArrayAsString);
                        break;
                    default:
                        sb.append(valueToStreamString(propertyType, value));
                }
                if (countKeys < edgeCache.getLeft().size()) {
                    sb.append(COPY_COMMAND_DELIMITER);
                }
                countKeys++;
            }
            if (count++ < edgeCache.getRight().size()) {
                sb.append("\n");
            }
        }
        return new ByteArrayInputStream(sb.toString().getBytes());
    }

    /**
     * this follows the PostgreSQL rules at https://www.postgresql.org/docs/current/static/sql-copy.html#AEN77663
     * "If the value contains the delimiter character, the QUOTE character, the NULL string, a carriage return,
     * or line feed character, then the whole value is prefixed and suffixed by the QUOTE character,
     * and any occurrence within the value of a QUOTE character or the ESCAPE character is preceded
     * by the escape character."
     *
     * @param s
     * @return
     */
    private String escapeSpecialCharacters(String s) {
        StringBuilder sb = new StringBuilder();
        boolean needEscape = s.length() == 0; // escape empty strings
        for (int a = 0; a < s.length(); a++) {
            char c = s.charAt(a);
            if (c == '\n' || c == '\r' || c == 0 || c == COPY_COMMAND_DELIMITER.charAt(0)) {
                needEscape = true;
            }
            if (c == ESCAPE || c == QUOTE) {
                needEscape = true;
                sb.append(ESCAPE);
            }
            sb.append(c);
        }
        if (needEscape) {
            return QUOTE + sb.toString() + QUOTE;
        }
        return s;
    }

    @Override
    public String[] propertyTypeToSqlDefinition(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                return new String[]{"BOOLEAN"};
            case SHORT:
                return new String[]{"SMALLINT"};
            case INTEGER:
                return new String[]{"INTEGER"};
            case LONG:
                return new String[]{"BIGINT"};
            case FLOAT:
                return new String[]{"REAL"};
            case DOUBLE:
                return new String[]{"DOUBLE PRECISION"};
            case LOCALDATE:
                return new String[]{"DATE"};
            case LOCALDATETIME:
                return new String[]{"TIMESTAMP WITH TIME ZONE"};
            case ZONEDDATETIME:
                return new String[]{"TIMESTAMP WITH TIME ZONE", "TEXT"};
            case LOCALTIME:
                return new String[]{"TIME WITH TIME ZONE"};
            case PERIOD:
                return new String[]{"INTEGER", "INTEGER", "INTEGER"};
            case DURATION:
                return new String[]{"BIGINT", "INTEGER"};
            case STRING:
                return new String[]{"TEXT"};
            case JSON:
                return new String[]{"JSONB"};
            case POINT:
                return new String[]{"geometry(POINT)"};
            case LINESTRING:
                return new String[]{"geometry(LINESTRING)"};
            case POLYGON:
                return new String[]{"geometry(POLYGON)"};
            case GEOGRAPHY_POINT:
                return new String[]{"geography(POINT, 4326)"};
            case GEOGRAPHY_POLYGON:
                return new String[]{"geography(POLYGON, 4326)"};
            case byte_ARRAY:
                return new String[]{"BYTEA"};
            case boolean_ARRAY:
                return new String[]{"BOOLEAN[]"};
            case short_ARRAY:
                return new String[]{"SMALLINT[]"};
            case int_ARRAY:
                return new String[]{"INTEGER[]"};
            case long_ARRAY:
                return new String[]{"BIGINT[]"};
            case float_ARRAY:
                return new String[]{"REAL[]"};
            case double_ARRAY:
                return new String[]{"DOUBLE PRECISION[]"};
            case STRING_ARRAY:
                return new String[]{"TEXT[]"};
            case LOCALDATETIME_ARRAY:
                return new String[]{"TIMESTAMP WITH TIME ZONE[]"};
            case LOCALDATE_ARRAY:
                return new String[]{"DATE[]"};
            case LOCALTIME_ARRAY:
                return new String[]{"TIME WITH TIME ZONE[]"};
            case ZONEDDATETIME_ARRAY:
                return new String[]{"TIMESTAMP WITH TIME ZONE[]", "TEXT[]"};
            case DURATION_ARRAY:
                return new String[]{"BIGINT[]", "INTEGER[]"};
            case PERIOD_ARRAY:
                return new String[]{"INTEGER[]", "INTEGER[]", "INTEGER[]"};
            case INTEGER_ARRAY:
                return new String[]{"INTEGER[]"};
            case BOOLEAN_ARRAY:
                return new String[]{"BOOLEAN[]"};
            case BYTE_ARRAY:
                return new String[]{"BYTEA"};
            case SHORT_ARRAY:
                return new String[]{"SMALLINT[]"};
            case LONG_ARRAY:
                return new String[]{"BIGINT[]"};
            case FLOAT_ARRAY:
                return new String[]{"REAL[]"};
            case DOUBLE_ARRAY:
                return new String[]{"DOUBLE PRECISION[]"};
            case JSON_ARRAY:
                return new String[]{"JSONB[]"};
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    /**
     * This is only used for upgrading from pre sqlg_schema sqlg to a sqlg_schema
     *
     * @param sqlType
     * @param typeName
     * @return
     */
    @Override
    public PropertyType sqlTypeToPropertyType(SqlgGraph sqlgGraph, String schema, String table, String column, int sqlType, String typeName) {
        switch (sqlType) {
            case Types.BIT:
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
            case Types.OTHER:
                //this is a f up as only JSON can be used for other.
                //means all the gis data types which are also OTHER are not supported
                switch (typeName) {
                    case "JSON":
                        return PropertyType.JSON;
                    case "geometry":
                        return getPostGisGeometryType(sqlgGraph, schema, table, column);
                    case "geography":
                        return getPostGisGeographyType(sqlgGraph, schema, table, column);
                    default:
                        throw new RuntimeException("Other type not supported " + typeName);

                }
            case Types.BINARY:
                return byte_ARRAY;
            case Types.ARRAY:
                switch (typeName) {
                    case "_bool":
                        return boolean_ARRAY;
                    case "_int2":
                        return short_ARRAY;
                    case "_int4":
                        return PropertyType.int_ARRAY;
                    case "_int8":
                        return PropertyType.long_ARRAY;
                    case "_float4":
                        return PropertyType.float_ARRAY;
                    case "_float8":
                        return PropertyType.double_ARRAY;
                    case "_text":
                        return PropertyType.STRING_ARRAY;
                    default:
                        throw new RuntimeException("Array type not supported " + typeName);
                }
            default:
                throw new IllegalStateException("Unknown sqlType " + sqlType);
        }
    }

    @Override
    public int propertyTypeToJavaSqlType(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                return Types.BOOLEAN;
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
            case byte_ARRAY:
                return Types.ARRAY;
            case LOCALDATETIME:
                return Types.TIMESTAMP;
            case LOCALDATE:
                return Types.DATE;
            case LOCALTIME:
                return Types.TIME;
            case JSON:
                //TODO support other others like Geometry...
                return Types.OTHER;
            case boolean_ARRAY:
                return Types.ARRAY;
            case short_ARRAY:
                return Types.ARRAY;
            case int_ARRAY:
                return Types.ARRAY;
            case long_ARRAY:
                return Types.ARRAY;
            case float_ARRAY:
                return Types.ARRAY;
            case double_ARRAY:
                return Types.ARRAY;
            case STRING_ARRAY:
                return Types.ARRAY;
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    @Override
    public void validateProperty(Object key, Object value) {
        if (key instanceof String && ((String) key).length() > 63) {
            validateColumnName((String) key);
        }
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
        if (value instanceof Float) {
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
        if (value instanceof Point) {
            return;
        }
        if (value instanceof LineString) {
            return;
        }
        if (value instanceof Polygon) {
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
        if (value instanceof float[]) {
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
        if (value instanceof Float[]) {
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
    public boolean needForeignKeyIndex() {
        return true;
    }

    private Set<String> getForeignKeyConstraintNames(SqlgGraph sqlgGraph, String foreignKeySchema, String foreignKeyTable) {
        Set<String> result = new HashSet<>();
        Connection conn = sqlgGraph.tx().getConnection();
        DatabaseMetaData metadata;
        try {
            metadata = conn.getMetaData();
            String childCatalog = null;
            String childSchemaPattern = foreignKeySchema;
            String childTableNamePattern = foreignKeyTable;
            ResultSet resultSet = metadata.getImportedKeys(childCatalog, childSchemaPattern, childTableNamePattern);
            while (resultSet.next()) {
                result.add(resultSet.getString("FK_NAME"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public boolean supportsClientInfo() {
        return true;
    }

    public void validateSchemaName(String schema) {
        if (schema.length() > getMinimumSchemaNameLength()) {
            throw SqlgExceptions.invalidSchemaName("Postgresql schema names can only be 63 characters. " + schema + " exceeds that");
        }
    }

    public void validateTableName(String table) {
        if (table.length() > getMinimumTableNameLength()) {
            throw SqlgExceptions.invalidTableName("Postgresql table names can only be 63 characters. " + table + " exceeds that");
        }
    }

    @Override
    public void validateColumnName(String column) {
        super.validateColumnName(column);
        if (column.length() > getMinimumColumnNameLength()) {
            throw SqlgExceptions.invalidColumnName("Postgresql column names can only be 63 characters. " + column + " exceeds that");
        }
    }

    public int getMinimumSchemaNameLength() {
        return 63;
    }

    public int getMinimumTableNameLength() {
        return 63;
    }

    public int getMinimumColumnNameLength() {
        return 63;
    }

    @Override
    public boolean supportsILike() {
        return Boolean.TRUE;
    }

    @Override
    public boolean needsTimeZone() {
        return Boolean.TRUE;
    }


    @Override
    public void setJson(PreparedStatement preparedStatement, int parameterStartIndex, JsonNode json) {
        PGobject jsonObject = new PGobject();
        jsonObject.setType("jsonb");
        try {
            jsonObject.setValue(json.toString());
            preparedStatement.setObject(parameterStartIndex, jsonObject);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setPoint(PreparedStatement preparedStatement, int parameterStartIndex, Object point) {
        Preconditions.checkArgument(point instanceof Point, "point must be an instance of " + Point.class.getName());
        try {
            preparedStatement.setObject(parameterStartIndex, new PGgeometry((Point) point));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setLineString(PreparedStatement preparedStatement, int parameterStartIndex, Object lineString) {
        Preconditions.checkArgument(lineString instanceof LineString, "lineString must be an instance of " + LineString.class.getName());
        try {
            preparedStatement.setObject(parameterStartIndex, new PGgeometry((LineString) lineString));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setPolygon(PreparedStatement preparedStatement, int parameterStartIndex, Object polygon) {
        Preconditions.checkArgument(polygon instanceof Polygon, "polygon must be an instance of " + Polygon.class.getName());
        try {
            preparedStatement.setObject(parameterStartIndex, new PGgeometry((Polygon) polygon));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setGeographyPoint(PreparedStatement preparedStatement, int parameterStartIndex, Object point) {
        Preconditions.checkArgument(point instanceof GeographyPoint, "point must be an instance of " + GeographyPoint.class.getName());
        try {
            preparedStatement.setObject(parameterStartIndex, new PGgeometry((GeographyPoint) point));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handleOther(Map<String, Object> properties, String columnName, Object o, PropertyType propertyType) {
        switch (propertyType) {
            case POINT:
                properties.put(columnName, ((PGgeometry) o).getGeometry());
                break;
            case LINESTRING:
                properties.put(columnName, ((PGgeometry) o).getGeometry());
                break;
            case GEOGRAPHY_POINT:
                try {
                    Geometry geometry = PGgeometry.geomFromString(((PGobject) o).getValue());
                    properties.put(columnName, new GeographyPoint((Point) geometry));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                break;
            case GEOGRAPHY_POLYGON:
                try {
                    Geometry geometry = PGgeometry.geomFromString(((PGobject) o).getValue());
                    properties.put(columnName, new GeographyPolygon((Polygon) geometry));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                break;
            case POLYGON:
                properties.put(columnName, ((PGgeometry) o).getGeometry());
                break;
            case JSON:
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    JsonNode jsonNode = objectMapper.readTree(((PGobject) o).getValue());
                    properties.put(columnName, jsonNode);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                break;
            case BYTE_ARRAY:
                java.sql.Array array = (java.sql.Array) o;
                String arrayAsString = array.toString();
                //remove the wrapping curly brackets
                arrayAsString = arrayAsString.substring(1);
                arrayAsString = arrayAsString.substring(0, arrayAsString.length() - 1);
                String[] byteAsString = arrayAsString.split(",");
//                PGbytea.toBytes();
                Byte[] result = new Byte[byteAsString.length];
                int count = 0;
                for (String s : byteAsString) {
                    Integer byteAsInteger = Integer.parseUnsignedInt(s.replace("\"", ""));
                    result[count++] = new Byte("");
                }
                properties.put(columnName, result);
                break;
            default:
                throw new IllegalStateException("sqlgDialect.handleOther does not handle " + propertyType.name());
        }
//        if (o instanceof PGgeometry) {
//            properties.put(columnName, ((PGgeometry) o).getGeometry());
//        } else if ((o instanceof PGobject) && ((PGobject) o).getType().equals("geography")) {
//            try {
//                Geometry geometry = PGgeometry.geomFromString(((PGobject) o).getValue());
//                if (geometry instanceof Point) {
//                    properties.put(columnName, new GeographyPoint((Point) geometry));
//                } else if (geometry instanceof Polygon) {
//                    properties.put(columnName, new GeographyPolygon((Polygon) geometry));
//                } else {
//                    throw new IllegalStateException("Gis type " + geometry.getClass().getName() + " is not supported.");
//                }
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        } else {
//            //Assume json for now
//            if (o instanceof java.sql.Array) {
//                java.sql.Array array = (java.sql.Array) o;
//                String arrayAsString = array.toString();
//                //remove the wrapping curly brackets
//                arrayAsString = arrayAsString.substring(1);
//                arrayAsString = arrayAsString.substring(0, arrayAsString.length() - 1);
//                arrayAsString = StringEscapeUtils.unescapeJava(arrayAsString);
//                //remove the wrapping qoutes
//                arrayAsString = arrayAsString.substring(1);
//                arrayAsString = arrayAsString.substring(0, arrayAsString.length() - 1);
//                String[] jsons = arrayAsString.split("\",\"");
//                JsonNode[] jsonNodes = new JsonNode[jsons.length];
//                ObjectMapper objectMapper = new ObjectMapper();
//                int count = 0;
//                for (String json : jsons) {
//                    try {
//                        JsonNode jsonNode = objectMapper.readTree(json);
//                        jsonNodes[count++] = jsonNode;
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//                properties.put(columnName, jsonNodes);
//            } else {
//                ObjectMapper objectMapper = new ObjectMapper();
//                try {
//                    JsonNode jsonNode = objectMapper.readTree(((PGobject) o).getValue());
//                    properties.put(columnName, jsonNode);
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        }
    }

    @Override
    public boolean supportsJson() {
        return true;
    }

    @Override
    public OutputStream streamSql(SqlgGraph sqlgGraph, String sql) {
        C3P0ProxyConnection conn = (C3P0ProxyConnection) sqlgGraph.tx().getConnection();
        PGConnection pgConnection;
        try {
            pgConnection = conn.unwrap(PGConnection.class);
            return new PGCopyOutputStream(pgConnection, sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream inputStreamSql(SqlgGraph sqlgGraph, String sql) {
        C3P0ProxyConnection conn = (C3P0ProxyConnection) sqlgGraph.tx().getConnection();
        PGConnection pgConnection;
        try {
            pgConnection = conn.unwrap(PGConnection.class);
            return new PGCopyInputStream(pgConnection, sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private <L, R> void copyInBulkTempEdges(SqlgGraph sqlgGraph, SchemaTable schemaTable, Collection<Pair<L, R>> uids, PropertyType inPropertyType, PropertyType outPropertyType) {
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("COPY ");
            sql.append(maybeWrapInQoutes(schemaTable.getTable()));
            sql.append(" (");
            int count = 1;
            for (String key : Arrays.asList("in", "out")) {
                if (count > 1 && count <= 2) {
                    sql.append(", ");
                }
                count++;
                sql.append(maybeWrapInQoutes(key));
            }
            sql.append(")");
            sql.append(" FROM stdin DELIMITER '");
            sql.append(COPY_COMMAND_DELIMITER);
            sql.append("';");
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            OutputStream out = streamSql(sqlgGraph, sql.toString());
            for (Pair<L, R> uid : uids) {
                out.write(valueToStreamBytes(inPropertyType, uid.getLeft()));
                out.write(COPY_COMMAND_DELIMITER.getBytes());
                out.write(valueToStreamBytes(outPropertyType, uid.getRight()));
                out.write("\n".getBytes());
            }
            out.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <L, R> void bulkAddEdges(SqlgGraph sqlgGraph, SchemaTable in, SchemaTable out, String edgeLabel, Pair<String, String> idFields, Collection<Pair<L, R>> uids) {
        if (!sqlgGraph.tx().isInStreamingBatchMode() && !sqlgGraph.tx().isInStreamingWithLockBatchMode()) {
            throw SqlgExceptions.invalidMode("Transaction must be in " + BatchManager.BatchModeType.STREAMING + " or " + BatchManager.BatchModeType.STREAMING_WITH_LOCK + " mode for bulkAddEdges");
        }
        if (!uids.isEmpty()) {
            //create temp table and copy the uids into it
            Map<String, PropertyType> columns = new HashMap<>();
            Map<String, PropertyType> inProperties = sqlgGraph.getSchemaManager().getTableFor(in.withPrefix(SchemaManager.VERTEX_PREFIX));
            Map<String, PropertyType> outProperties = sqlgGraph.getSchemaManager().getTableFor(out.withPrefix(SchemaManager.VERTEX_PREFIX));
            PropertyType inPropertyType;
            if (idFields.getLeft().equals(SchemaManager.ID)) {
                inPropertyType = PropertyType.INTEGER;
            } else {
                inPropertyType = inProperties.get(idFields.getLeft());
            }
            PropertyType outPropertyType;
            if (idFields.getRight().equals(SchemaManager.ID)) {
                outPropertyType = PropertyType.INTEGER;
            } else {
                outPropertyType = outProperties.get(idFields.getRight());
            }
            columns.put("out", outPropertyType);
            columns.put("in", inPropertyType);
            SecureRandom random = new SecureRandom();
            byte bytes[] = new byte[6];
            random.nextBytes(bytes);
            String tmpTableIdentified = Base64.getEncoder().encodeToString(bytes);
            tmpTableIdentified = SchemaManager.BULK_TEMP_EDGE + tmpTableIdentified;
            sqlgGraph.getSchemaManager().createTempTable(tmpTableIdentified, columns);
            this.copyInBulkTempEdges(sqlgGraph, SchemaTable.of(in.getSchema(), tmpTableIdentified), uids, inPropertyType, outPropertyType);
            //executeRegularQuery copy from select. select the edge ids to copy into the new table by joining on the temp table
            sqlgGraph.getSchemaManager().ensureEdgeTableExist(in.getSchema(), edgeLabel, out, in);

            StringBuilder sql = new StringBuilder("INSERT INTO \n");
            sql.append(this.maybeWrapInQoutes(in.getSchema()));
            sql.append(".");
            sql.append(this.maybeWrapInQoutes(SchemaManager.EDGE_PREFIX + edgeLabel));
            sql.append(" (");
            sql.append(this.maybeWrapInQoutes(in.getSchema() + "." + in.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END));
            sql.append(",");
            sql.append(this.maybeWrapInQoutes(out.getSchema() + "." + out.getTable() + SchemaManager.IN_VERTEX_COLUMN_END));
            sql.append(") \n");
            sql.append("select _in.\"ID\" as \"");
            sql.append(in.getSchema() + "." + in.getTable() + SchemaManager.OUT_VERTEX_COLUMN_END);
            sql.append("\", _out.\"ID\" as \"");
            sql.append(out.getSchema() + "." + out.getTable() + SchemaManager.IN_VERTEX_COLUMN_END);
            sql.append("\" FROM ");
            sql.append(this.maybeWrapInQoutes(in.getSchema()));
            sql.append(".");
            sql.append(this.maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + in.getTable()));
            sql.append(" _in join ");
            sql.append(this.maybeWrapInQoutes(tmpTableIdentified) + " ab on ab.in = _in." + this.maybeWrapInQoutes(idFields.getLeft()) + " join ");
            sql.append(this.maybeWrapInQoutes(out.getSchema()));
            sql.append(".");
            sql.append(this.maybeWrapInQoutes(SchemaManager.VERTEX_PREFIX + out.getTable()));
            sql.append(" _out on ab.out = _out." + this.maybeWrapInQoutes(idFields.getRight()));
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            Connection conn = sqlgGraph.tx().getConnection();
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void lockTable(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix) {
        Preconditions.checkArgument(prefix.equals(SchemaManager.VERTEX_PREFIX) || prefix.equals(SchemaManager.EDGE_PREFIX), "prefix must be " + SchemaManager.VERTEX_PREFIX + " or " + SchemaManager.EDGE_PREFIX);
        StringBuilder sql = new StringBuilder();
        sql.append("LOCK TABLE ");
        sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
        sql.append(".");
        sql.append(sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(prefix + schemaTable.getTable()));
        sql.append(" IN SHARE MODE");
        if (this.needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void alterSequenceCacheSize(SqlgGraph sqlgGraph, SchemaTable schemaTable, String sequence, int batchSize) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER SEQUENCE ");
        sql.append(sequence);
        sql.append(" CACHE ");
        sql.append(String.valueOf(batchSize));
        if (this.needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long nextSequenceVal(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix) {
        Preconditions.checkArgument(prefix.equals(SchemaManager.VERTEX_PREFIX) || prefix.equals(SchemaManager.EDGE_PREFIX), "prefix must be " + SchemaManager.VERTEX_PREFIX + " or " + SchemaManager.EDGE_PREFIX);
        long result;
        Connection conn = sqlgGraph.tx().getConnection();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT NEXTVAL('\"" + schemaTable.getSchema() + "\".\"" + prefix + schemaTable.getTable() + "_ID_seq\"');");
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            ResultSet resultSet = preparedStatement.executeQuery();
            resultSet.next();
            result = resultSet.getLong(1);
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public long currSequenceVal(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix) {
        Preconditions.checkArgument(prefix.equals(SchemaManager.VERTEX_PREFIX) || prefix.equals(SchemaManager.EDGE_PREFIX), "prefix must be " + SchemaManager.VERTEX_PREFIX + " or " + SchemaManager.EDGE_PREFIX);
        long result;
        Connection conn = sqlgGraph.tx().getConnection();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT CURRVAL('\"" + schemaTable.getSchema() + "\".\"" + prefix + schemaTable.getTable() + "_ID_seq\"');");
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            ResultSet resultSet = preparedStatement.executeQuery();
            resultSet.next();
            result = resultSet.getLong(1);
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public String sequenceName(SqlgGraph sqlgGraph, SchemaTable outSchemaTable, String prefix) {
        Preconditions.checkArgument(prefix.equals(SchemaManager.VERTEX_PREFIX) || prefix.equals(SchemaManager.EDGE_PREFIX), "prefix must be " + SchemaManager.VERTEX_PREFIX + " or " + SchemaManager.EDGE_PREFIX);
//        select pg_get_serial_sequence('public."V_Person"', 'ID')
        String result;
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT pg_get_serial_sequence('\"");
        sql.append(outSchemaTable.getSchema());
        sql.append("\".\"");
        sql.append(prefix).append(outSchemaTable.getTable()).append("\"', 'ID')");
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            ResultSet resultSet = preparedStatement.executeQuery();
            resultSet.next();
            result = resultSet.getString(1);
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public boolean supportsBulkWithinOut() {
        return true;
    }

    @Override
    public boolean isPostgresql() {
        return true;
    }

    //This is not being called but leaving it here for prosperity.
//    @Override
//    public void registerGisDataTypes(Connection connection) {
//        try {
//            ((Jdbc4Connection) ((com.mchange.v2.c3p0.impl.NewProxyConnection) connection).unwrap(Jdbc4Connection.class)).addDataType("geometry", "org.postgis.PGgeometry");
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }

    @Override
    public <T> T getGis(SqlgGraph sqlgGraph) {
        Gis gis = Gis.GIS;
        gis.setSqlgGraph(sqlgGraph);
        return (T) gis;
    }

    @Override
    public String afterCreateTemporaryTableStatement() {
        return "ON COMMIT DROP";
    }

    @Override
    public List<String> columnsToIgnore() {
        return Arrays.asList(COPY_DUMMY);
    }

    @Override
    public List<String> sqlgTopologyCreationScripts() {
        List<String> result = new ArrayList<>();
        result.add("CREATE SCHEMA \"sqlg_schema\";");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_schema\" (\"ID\" SERIAL PRIMARY KEY, \"createdOn\" TIMESTAMP WITH TIME ZONE, \"name\" TEXT);");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_vertex\" (\"ID\" SERIAL PRIMARY KEY, \"createdOn\" TIMESTAMP WITH TIME ZONE, \"name\" TEXT, \"schemaVertex\" TEXT);");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_edge\" (\"ID\" SERIAL PRIMARY KEY, \"createdOn\" TIMESTAMP WITH TIME ZONE, \"name\" TEXT);");
        result.add("CREATE TABLE \"sqlg_schema\".\"V_property\" (\"ID\" SERIAL PRIMARY KEY, \"createdOn\" TIMESTAMP WITH TIME ZONE, \"name\" TEXT, \"type\" TEXT);");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_schema_vertex\"(\"ID\" SERIAL PRIMARY KEY, \"sqlg_schema.vertex__I\" BIGINT, \"sqlg_schema.schema__O\" BIGINT, FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"),  FOREIGN KEY (\"sqlg_schema.schema__O\") REFERENCES \"sqlg_schema\".\"V_schema\" (\"ID\"));");
        result.add("CREATE INDEX ON \"sqlg_schema\".\"E_schema_vertex\" (\"sqlg_schema.vertex__I\");");
        result.add("CREATE INDEX ON \"sqlg_schema\".\"E_schema_vertex\" (\"sqlg_schema.schema__O\");");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_in_edges\"(\"ID\" SERIAL PRIMARY KEY, \"sqlg_schema.edge__I\" BIGINT, \"sqlg_schema.vertex__O\" BIGINT, FOREIGN KEY (\"sqlg_schema.edge__I\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"),  FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE INDEX ON \"sqlg_schema\".\"E_in_edges\" (\"sqlg_schema.edge__I\");");
        result.add("CREATE INDEX ON \"sqlg_schema\".\"E_in_edges\" (\"sqlg_schema.vertex__O\");");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_out_edges\"(\"ID\" SERIAL PRIMARY KEY, \"sqlg_schema.edge__I\" BIGINT, \"sqlg_schema.vertex__O\" BIGINT, FOREIGN KEY (\"sqlg_schema.edge__I\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"),  FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE INDEX ON \"sqlg_schema\".\"E_out_edges\" (\"sqlg_schema.edge__I\");");
        result.add("CREATE INDEX ON \"sqlg_schema\".\"E_out_edges\" (\"sqlg_schema.vertex__O\");");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_vertex_property\"(\"ID\" SERIAL PRIMARY KEY, \"sqlg_schema.property__I\" BIGINT, \"sqlg_schema.vertex__O\" BIGINT, FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"),  FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"));");
        result.add("CREATE INDEX ON \"sqlg_schema\".\"E_vertex_property\" (\"sqlg_schema.property__I\");");
        result.add("CREATE INDEX ON \"sqlg_schema\".\"E_vertex_property\" (\"sqlg_schema.vertex__O\");");
        result.add("CREATE TABLE \"sqlg_schema\".\"E_edge_property\"(\"ID\" SERIAL PRIMARY KEY, \"sqlg_schema.property__I\" BIGINT, \"sqlg_schema.edge__O\" BIGINT, FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"),  FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"));");
        result.add("CREATE INDEX ON \"sqlg_schema\".\"E_edge_property\" (\"sqlg_schema.property__I\");");
        result.add("CREATE INDEX ON \"sqlg_schema\".\"E_edge_property\" (\"sqlg_schema.edge__O\");");
        return result;
    }

    private Array createArrayOf(Connection conn, PropertyType propertyType, Object[] data) {
        try {
            switch (propertyType) {
                case STRING_ARRAY:
                case long_ARRAY:
                case LONG_ARRAY:
                case int_ARRAY:
                case INTEGER_ARRAY:
                case short_ARRAY:
                case SHORT_ARRAY:
                case float_ARRAY:
                case FLOAT_ARRAY:
                case double_ARRAY:
                case DOUBLE_ARRAY:
                case boolean_ARRAY:
                case BOOLEAN_ARRAY:
                case LOCALDATETIME_ARRAY:
                case LOCALDATE_ARRAY:
                case LOCALTIME_ARRAY:
                case ZONEDDATETIME_ARRAY:
                case JSON_ARRAY:
                    return conn.createArrayOf(getArrayDriverType(propertyType), data);
                default:
                    throw new IllegalStateException("Unhandled array type " + propertyType.name());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object convertArray(PropertyType propertyType, java.sql.Array array) throws SQLException {
        switch (propertyType) {
            case BOOLEAN_ARRAY:
                return array.getArray();
            case boolean_ARRAY:
                return SqlgUtil.convertObjectArrayToBooleanPrimitiveArray((Object[]) array.getArray());
            case SHORT_ARRAY:
                return SqlgUtil.convertObjectOfIntegersArrayToShortArray((Object[]) array.getArray());
            case short_ARRAY:
                return SqlgUtil.convertObjectOfIntegersArrayToShortPrimitiveArray((Object[]) array.getArray());
            case INTEGER_ARRAY:
                return array.getArray();
            case int_ARRAY:
                return SqlgUtil.convertObjectOfIntegersArrayToIntegerPrimitiveArray((Object[]) array.getArray());
            case LONG_ARRAY:
                return array.getArray();
            case long_ARRAY:
                return SqlgUtil.convertObjectOfLongsArrayToLongPrimitiveArray((Object[]) array.getArray());
            case DOUBLE_ARRAY:
                return array.getArray();
            case double_ARRAY:
                return SqlgUtil.convertObjectOfDoublesArrayToDoublePrimitiveArray((Object[]) array.getArray());
            case FLOAT_ARRAY:
                return array.getArray();
            case float_ARRAY:
                return SqlgUtil.convertObjectOfFloatsArrayToFloatPrimitiveArray((Object[]) array.getArray());
            case STRING_ARRAY:
                return array.getArray();
            case LOCALDATETIME_ARRAY:
                Timestamp[] timestamps = (Timestamp[]) array.getArray();
                return SqlgUtil.copyToLocalDateTime(timestamps, new LocalDateTime[timestamps.length]);
            case LOCALDATE_ARRAY:
                Date[] dates = (Date[]) array.getArray();
                return SqlgUtil.copyToLocalDate(dates, new LocalDate[dates.length]);
            case LOCALTIME_ARRAY:
                Time[] times = (Time[]) array.getArray();
                return SqlgUtil.copyToLocalTime(times, new LocalTime[times.length]);
            case JSON_ARRAY:
                String arrayAsString = array.toString();
                //remove the wrapping curly brackets
                arrayAsString = arrayAsString.substring(1);
                arrayAsString = arrayAsString.substring(0, arrayAsString.length() - 1);
                arrayAsString = StringEscapeUtils.unescapeJava(arrayAsString);
                //remove the wrapping qoutes
                arrayAsString = arrayAsString.substring(1);
                arrayAsString = arrayAsString.substring(0, arrayAsString.length() - 1);
                String[] jsons = arrayAsString.split("\",\"");
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

    private PropertyType getPostGisGeometryType(SqlgGraph sqlgGraph, String schema, String table, String column) {
        Connection connection = sqlgGraph.tx().getConnection();
        try (PreparedStatement statement = connection.prepareStatement("SELECT type FROM geometry_columns WHERE f_table_schema = ? and f_table_name = ? and f_geometry_column = ?")) {
            statement.setString(1, schema);
            statement.setString(2, table);
            statement.setString(3, column);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                String type = resultSet.getString(1);
                return PropertyType.valueOf(type);
            } else {
                throw new IllegalStateException("PostGis property type for column " + column + " not found");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private PropertyType getPostGisGeographyType(SqlgGraph sqlgGraph, String schema, String table, String column) {
        Connection connection = sqlgGraph.tx().getConnection();
        try (PreparedStatement statement = connection.prepareStatement("SELECT type FROM geography_columns WHERE f_table_schema = ? and f_table_name = ? and f_geography_column = ?")) {
            statement.setString(1, schema);
            statement.setString(2, table);
            statement.setString(3, column);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                String type = resultSet.getString(1);
                switch (type) {
                    case "Point":
                        return PropertyType.GEOGRAPHY_POINT;
                    case "Polygon":
                        return PropertyType.GEOGRAPHY_POLYGON;
                    default:
                        throw new IllegalStateException("Unhandled geography type " + type);
                }
            } else {
                throw new IllegalStateException("PostGis property type for column " + column + " not found");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Postgres gets confused by DST, it sets the timezone badly and then reads the wrong value out, so we convert the value to "winter time"
     *
     * @param lt the current time
     * @return the time in "winter time" if there is DST in effect today
     */
    @SuppressWarnings("deprecation")
    private static Time shiftDST(LocalTime lt) {
        Time t = Time.valueOf(lt);
        int offset = Calendar.getInstance().get(Calendar.DST_OFFSET) / 1000;
        // I know this are deprecated methods, but it's so much clearer than alternatives
        int m = t.getSeconds();
        t.setSeconds(m + offset);
        return t;
    }
}
