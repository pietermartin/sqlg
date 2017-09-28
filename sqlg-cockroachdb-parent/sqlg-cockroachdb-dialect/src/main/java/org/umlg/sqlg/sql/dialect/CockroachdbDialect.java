package org.umlg.sqlg.sql.dialect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.postgresql.util.PGbytea;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.predicate.FullText;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.util.SqlgUtil;

import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.util.*;

import static org.umlg.sqlg.structure.PropertyType.*;
import static org.umlg.sqlg.structure.Topology.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/06/30
 */
@SuppressWarnings("unused")
public class CockroachdbDialect extends BaseSqlDialect {

    private static final int PARAMETER_LIMIT = 32767;
    private Logger logger = LoggerFactory.getLogger(CockroachdbDialect.class.getName());

    public CockroachdbDialect() {
        super();
    }

    @Override
    public String sqlgSqlgSchemaCreationScript() {
        return "CREATE DATABASE " + this.maybeWrapInQoutes("sqlg_schema");
    }

    @Override
    public boolean supportsDistribution() {
        return true;
    }

    @Override
    public boolean supportsSchemas() {
        return false;
    }

    @Override
    public String dialectName() {
        return "Postgresql";
    }

    @Override
    public String createSchemaStatement(String schemaName) {
        return "CREATE DATABASE IF NOT EXISTS " + maybeWrapInQoutes(schemaName);
    }

    @Override
    public String dropSchemaStatement(String schema) {
        return "DROP DATABASE IF EXISTS " + maybeWrapInQoutes(schema);
    }

    @Override
    public boolean supportsCascade() {
        return false;
    }

    @Override
    public boolean supportsBatchMode() {
        return true;
    }

    @Override
    public Set<String> getInternalSchemas() {
        return ImmutableSet.copyOf(Arrays.asList("system", "crdb_internal","pg_catalog", "information_schema", "tiger", "tiger_data", "topology"));
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

    private void appendSqlValue(StringBuilder sql, Object value, PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                if (value != null) {
                    sql.append(value);
                } else {
                    sql.append("null");
                }
                break;
            case BYTE:
                if (value != null) {
                    sql.append(value);
                } else {
                    sql.append("null");
                }
                break;
            case SHORT:
                if (value != null) {
                    sql.append(value);
                } else {
                    sql.append("null");
                }
                break;
            case INTEGER:
                if (value != null) {
                    sql.append(value);
                } else {
                    sql.append("null");
                }
                break;
            case LONG:
                if (value != null) {
                    sql.append(value);
                } else {
                    sql.append("null");
                }
                break;
            case FLOAT:
                if (value != null) {
                    sql.append(value);
                } else {
                    sql.append("null");
                }
                break;
            case DOUBLE:
                if (value != null) {
                    sql.append(value);
                } else {
                    sql.append("null");
                }
                break;
            case STRING:
                if (value != null) {
                    sql.append("'");
                    sql.append(value.toString().replace("'", "''"));
                    sql.append("'");
                } else {
                    sql.append("null");
                }
                break;
            case LOCALDATETIME:
                if (value != null) {
                    sql.append("'");
                    sql.append(value.toString());
                    sql.append("'::TIMESTAMP");
                } else {
                    sql.append("null");
                }
                break;
            case LOCALDATE:
                if (value != null) {
                    sql.append("'");
                    sql.append(value.toString());
                    sql.append("'::DATE");
                } else {
                    sql.append("null");
                }
                break;
            case LOCALTIME:
                if (value != null) {
                    sql.append("'");
                    sql.append(shiftDST((LocalTime) value).toString());
                    sql.append("'::TIME");
                } else {
                    sql.append("null");
                }
                break;
            case ZONEDDATETIME:
                if (value != null) {
                    ZonedDateTime zonedDateTime = (ZonedDateTime) value;
                    LocalDateTime localDateTime = zonedDateTime.toLocalDateTime();
                    TimeZone timeZone = TimeZone.getTimeZone(zonedDateTime.getZone());
                    sql.append("'");
                    sql.append(localDateTime.toString());
                    sql.append("'::TIMESTAMP");
                    sql.append(",'");
                    sql.append(timeZone.getID());
                    sql.append("'");
                } else {
                    sql.append("null,null");
                }
                break;
            case DURATION:
                if (value != null) {
                    Duration duration = (Duration) value;
                    sql.append("'");
                    sql.append(duration.getSeconds());
                    sql.append("'::BIGINT");
                    sql.append(",'");
                    sql.append(duration.getNano());
                    sql.append("'::INTEGER");
                } else {
                    sql.append("null,null");
                }
                break;
            case PERIOD:
                if (value != null) {
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
                } else {
                    sql.append("null,null,null");
                }
                break;
            case JSON:
                if (value != null) {
                    sql.append("'");
                    sql.append(value.toString().replace("'", "''"));
                    sql.append("'::JSONB");
                } else {
                    sql.append("null");
                }
                break;
            case boolean_ARRAY:
                if (value != null) {
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
                } else {
                    sql.append("null");
                }
                break;
            case BOOLEAN_ARRAY:
                if (value != null) {
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
                } else {
                    sql.append("null");
                }
                break;
            case byte_ARRAY:
                if (value != null) {
                    sql.append("'");
                    sql.append(PGbytea.toPGString((byte[]) value).replace("'", "''"));
                    sql.append("'");
                } else {
                    sql.append("null");
                }
                break;
            case BYTE_ARRAY:
                if (value != null) {
                    sql.append("'");
                    sql.append(PGbytea.toPGString((byte[]) SqlgUtil.convertByteArrayToPrimitiveArray((Byte[]) value)).replace("'", "''"));
                    sql.append("'");
                } else {
                    sql.append("null");
                }
                break;
            case short_ARRAY:
                if (value != null) {
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
                } else {
                    sql.append("null");
                }
                break;
            case SHORT_ARRAY:
                if (value != null) {
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
                } else {
                    sql.append("null");
                }
                break;
            case int_ARRAY:
                if (value != null) {
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
                } else {
                    sql.append("null");
                }
                break;
            case INTEGER_ARRAY:
                if (value != null) {
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
                } else {
                    sql.append("null");
                }
                break;
            case LONG_ARRAY:
                if (value != null) {
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
                } else {
                    sql.append("null");
                }
                break;
            case long_ARRAY:
                if (value != null) {
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
                } else {
                    sql.append("null");
                }
                break;
            case FLOAT_ARRAY:
                if (value != null) {
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
                } else {
                    sql.append("null");
                }
                break;
            case float_ARRAY:
                if (value != null) {
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
                } else {
                    sql.append("null");
                }
                break;
            case DOUBLE_ARRAY:
                if (value != null) {
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
                } else {
                    sql.append("null");
                }
                break;
            case double_ARRAY:
                if (value != null) {
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
                } else {
                    sql.append("null");
                }
                break;
            case STRING_ARRAY:
                if (value != null) {
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
                } else {
                    sql.append("null");
                }
                break;
            case LOCALDATETIME_ARRAY:
                if (value != null) {
                    sql.append("ARRAY[");
                    LocalDateTime[] localDateTimeArray = (LocalDateTime[]) value;
                    int countStringArray = 1;
                    for (LocalDateTime s : localDateTimeArray) {
                        sql.append("'");
                        sql.append(s.toString());
                        sql.append("'::TIMESTAMP");
                        if (countStringArray++ < localDateTimeArray.length) {
                            sql.append(",");
                        }
                    }
                    sql.append("]");
                } else {
                    sql.append("null");
                }
                break;
            case LOCALDATE_ARRAY:
                if (value != null) {
                    sql.append("ARRAY[");
                    LocalDate[] localDateArray = (LocalDate[]) value;
                    int countStringArray = 1;
                    for (LocalDate s : localDateArray) {
                        sql.append("'");
                        sql.append(s.toString());
                        sql.append("'::DATE");
                        if (countStringArray++ < localDateArray.length) {
                            sql.append(",");
                        }
                    }
                    sql.append("]");
                } else {
                    sql.append("null");
                }
                break;
            case LOCALTIME_ARRAY:
                if (value != null) {
                    sql.append("ARRAY[");
                    LocalTime[] localTimeArray = (LocalTime[]) value;
                    int countStringArray = 1;
                    for (LocalTime s : localTimeArray) {
                        sql.append("'");
                        sql.append(shiftDST(s).toLocalTime().toString());
                        sql.append("'::TIME");
                        if (countStringArray++ < localTimeArray.length) {
                            sql.append(",");
                        }
                    }
                    sql.append("]");
                } else {
                    sql.append("null");
                }
                break;
            case ZONEDDATETIME_ARRAY:
                if (value != null) {
                    sql.append("ARRAY[");
                    ZonedDateTime[] localZonedDateTimeArray = (ZonedDateTime[]) value;
                    int countStringArray = 1;
                    for (ZonedDateTime zonedDateTime : localZonedDateTimeArray) {
                        LocalDateTime localDateTime = zonedDateTime.toLocalDateTime();
                        TimeZone timeZone = TimeZone.getTimeZone(zonedDateTime.getZone());
                        sql.append("'");
                        sql.append(localDateTime.toString());
                        sql.append("'::TIMESTAMP");
                        if (countStringArray++ < localZonedDateTimeArray.length) {
                            sql.append(",");
                        }
                    }
                    sql.append("],");
                    sql.append("ARRAY[");
                    countStringArray = 1;
                    for (ZonedDateTime zonedDateTime : localZonedDateTimeArray) {
                        LocalDateTime localDateTime = zonedDateTime.toLocalDateTime();
                        TimeZone timeZone = TimeZone.getTimeZone(zonedDateTime.getZone());
                        sql.append("'");
                        sql.append(timeZone.getID());
                        sql.append("'");
                        if (countStringArray++ < localZonedDateTimeArray.length) {
                            sql.append(",");
                        }
                    }
                    sql.append("]");
                } else {
                    sql.append("null,null");
                }
                break;
            case DURATION_ARRAY:
                if (value != null) {
                    sql.append("ARRAY[");
                    Duration[] durationArray = (Duration[]) value;
                    int countStringArray = 1;
                    for (Duration duration : durationArray) {
                        sql.append("'");
                        sql.append(duration.getSeconds());
                        sql.append("'::BIGINT");
                        if (countStringArray++ < durationArray.length) {
                            sql.append(",");
                        }
                    }
                    sql.append("],");
                    sql.append("ARRAY[");
                    countStringArray = 1;
                    for (Duration duration : durationArray) {
                        sql.append("'");
                        sql.append(duration.getNano());
                        sql.append("'::INTEGER");
                        if (countStringArray++ < durationArray.length) {
                            sql.append(",");
                        }
                    }
                    sql.append("]");
                } else {
                    sql.append("null,null");
                }
                break;
            case PERIOD_ARRAY:
                if (value != null) {
                    sql.append("ARRAY[");
                    Period[] periodArray = (Period[]) value;
                    int countStringArray = 1;
                    for (Period period : periodArray) {
                        sql.append("'");
                        sql.append(period.getYears());
                        sql.append("'::INTEGER");
                        if (countStringArray++ < periodArray.length) {
                            sql.append(",");
                        }
                    }
                    sql.append("],");
                    sql.append("ARRAY[");
                    countStringArray = 1;
                    for (Period period : periodArray) {
                        sql.append("'");
                        sql.append(period.getMonths());
                        sql.append("'::INTEGER");
                        if (countStringArray++ < periodArray.length) {
                            sql.append(",");
                        }
                    }
                    sql.append("],");
                    sql.append("ARRAY[");
                    countStringArray = 1;
                    for (Period period : periodArray) {
                        sql.append("'");
                        sql.append(period.getDays());
                        sql.append("'::INTEGER");
                        if (countStringArray++ < periodArray.length) {
                            sql.append(",");
                        }
                    }
                    sql.append("]");
                } else {
                    sql.append("null,null,null");
                }
                break;
            case POINT:
                throw new IllegalStateException("JSON Arrays are not supported.");
            case LINESTRING:
                throw new IllegalStateException("JSON Arrays are not supported.");
            case POLYGON:
                throw new IllegalStateException("JSON Arrays are not supported.");
            case GEOGRAPHY_POINT:
                throw new IllegalStateException("JSON Arrays are not supported.");
            case GEOGRAPHY_POLYGON:
                throw new IllegalStateException("JSON Arrays are not supported.");
            case JSON_ARRAY:
                throw new IllegalStateException("JSON Arrays are not supported.");
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    private void sqlCastArray(StringBuilder sql, PropertyType propertyType) {
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
            default:
                // noop
                break;
        }
    }


    private void dropForeignKeys(SqlgGraph sqlgGraph, SchemaTable schemaTable) {

        Map<String, Set<String>> edgeForeignKeys = sqlgGraph.getTopology().getAllEdgeForeignKeys();

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
        Map<String, Set<String>> edgeForeignKeys = sqlgGraph.getTopology().getAllEdgeForeignKeys();

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
                    sql.append(maybeWrapInQoutes(VERTEX_PREFIX + schemaTable.getTable()));
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
            sql.append(maybeWrapInQoutes(schemaTable.toString() + (inDirection ? Topology.IN_VERTEX_COLUMN_END : Topology.OUT_VERTEX_COLUMN_END)));
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
    public PropertyType sqlTypeToPropertyType(SqlgGraph sqlgGraph, String schema, String table, String column, int sqlType, String typeName, ListIterator<Triple<String, Integer, String>> metaDataIter) {
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
                    case "jsonb":
                        return PropertyType.JSON;
                    default:
                        throw new RuntimeException("Other type not supported " + typeName);

                }
            case Types.BINARY:
                return BYTE_ARRAY;
            case Types.ARRAY:
                return sqlArrayTypeNameToPropertyType(typeName, sqlgGraph, schema, table, column, metaDataIter);
            default:
                throw new IllegalStateException("Unknown sqlType " + sqlType);
        }
    }

    @Override
    public PropertyType sqlArrayTypeNameToPropertyType(String typeName, SqlgGraph sqlgGraph, String schema, String table, String columnName, ListIterator<Triple<String, Integer, String>> metaDataIter) {
        switch (typeName) {
            case "_bool":
                return BOOLEAN_ARRAY;
            case "_int2":
                return SHORT_ARRAY;
            case "_int4":
                return PropertyType.INTEGER_ARRAY;
            case "_int8":
                return PropertyType.LONG_ARRAY;
            case "_float4":
                return PropertyType.FLOAT_ARRAY;
            case "_float8":
                return PropertyType.DOUBLE_ARRAY;
            case "_text":
                return PropertyType.STRING_ARRAY;
            case "_date":
                return PropertyType.LOCALDATE_ARRAY;
            case "_timetz":
                return PropertyType.LOCALTIME_ARRAY;
            case "_timestamptz":
                //need to check the next column to know if its a LocalDateTime or ZonedDateTime array
                Triple<String, Integer, String> metaData = metaDataIter.next();
                metaDataIter.previous();
                if (metaData.getLeft().startsWith(columnName + "~~~")) {
                    return PropertyType.ZONEDDATETIME_ARRAY;
                } else {
                    return PropertyType.LOCALDATETIME_ARRAY;
                }
            case "_jsonb":
                return PropertyType.JSON_ARRAY;
            default:
                throw new RuntimeException("Array type not supported " + typeName);
        }
    }

    @Override
    public int[] propertyTypeToJavaSqlType(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                return new int[]{Types.BOOLEAN};
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
                return new int[]{Types.CLOB};
            case byte_ARRAY:
                return new int[]{Types.ARRAY};
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
                //TODO support other others like Geometry...
                return new int[]{Types.OTHER};
            case boolean_ARRAY:
            case BOOLEAN_ARRAY:
            case short_ARRAY:
            case SHORT_ARRAY:
            case int_ARRAY:
            case INTEGER_ARRAY:
            case long_ARRAY:
            case LONG_ARRAY:
            case float_ARRAY:
            case FLOAT_ARRAY:
            case double_ARRAY:
            case DOUBLE_ARRAY:
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
        if (schema.length() > getMaximumSchemaNameLength()) {
            throw SqlgExceptions.invalidSchemaName("Postgresql schema names can only be 63 characters. " + schema + " exceeds that");
        }
    }

    public void validateTableName(String table) {
        if (table.length() > getMaximumTableNameLength()) {
            throw SqlgExceptions.invalidTableName("Postgresql table names can only be 63 characters. " + table + " exceeds that");
        }
    }

    @Override
    public void validateColumnName(String column) {
        super.validateColumnName(column);
        if (column.length() > getMaximumColumnNameLength()) {
            throw SqlgExceptions.invalidColumnName("Postgresql column names can only be 63 characters. " + column + " exceeds that");
        }
    }

    public int getMaximumSchemaNameLength() {
        return 63;
    }

    public int getMaximumTableNameLength() {
        return 63;
    }

    public int getMaximumColumnNameLength() {
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
    public void handleOther(Map<String, Object> properties, String columnName, Object o, PropertyType propertyType) {
        switch (propertyType) {
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
                Array array = (Array) o;
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
    }

    @Override
    public void lockTable(SqlgGraph sqlgGraph, SchemaTable schemaTable, String prefix) {
        Preconditions.checkArgument(prefix.equals(VERTEX_PREFIX) || prefix.equals(EDGE_PREFIX), "prefix must be " + VERTEX_PREFIX + " or " + EDGE_PREFIX);
        StringBuilder sql = new StringBuilder();
        sql.append("LOCK TABLE ");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(schemaTable.getSchema()));
        sql.append(".");
        sql.append(sqlgGraph.getSqlDialect().maybeWrapInQoutes(prefix + schemaTable.getTable()));
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
        Preconditions.checkArgument(prefix.equals(VERTEX_PREFIX) || prefix.equals(EDGE_PREFIX), "prefix must be " + VERTEX_PREFIX + " or " + EDGE_PREFIX);
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
        Preconditions.checkArgument(prefix.equals(VERTEX_PREFIX) || prefix.equals(EDGE_PREFIX), "prefix must be " + VERTEX_PREFIX + " or " + EDGE_PREFIX);
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
        Preconditions.checkArgument(prefix.equals(VERTEX_PREFIX) || prefix.equals(EDGE_PREFIX), "prefix must be " + VERTEX_PREFIX + " or " + EDGE_PREFIX);
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

    @Override
    public String afterCreateTemporaryTableStatement() {
        return "ON COMMIT DROP";
    }

    @Override
    public List<String> sqlgTopologyCreationScripts() {
        List<String> result = new ArrayList<>();

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_schema\" (\"ID\" SERIAL PRIMARY KEY, \"createdOn\" TIMESTAMP WITH TIME ZONE, \"name\" TEXT);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_vertex\" (\"ID\" SERIAL PRIMARY KEY, \"createdOn\" TIMESTAMP WITH TIME ZONE, \"name\" TEXT, \"schemaVertex\" TEXT);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_edge\" (\"ID\" SERIAL PRIMARY KEY, \"createdOn\" TIMESTAMP WITH TIME ZONE, \"name\" TEXT);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_property\" (\"ID\" SERIAL PRIMARY KEY, \"createdOn\" TIMESTAMP WITH TIME ZONE, \"name\" TEXT, \"type\" TEXT);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_index\" (\"ID\" SERIAL PRIMARY KEY, \"createdOn\" TIMESTAMP WITH TIME ZONE, \"name\" TEXT, \"index_type\" TEXT);");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_globalUniqueIndex\" (" +
                "\"ID\" SERIAL PRIMARY KEY, " +
                "\"createdOn\" TIMESTAMP WITH TIME ZONE, " +
                "\"name\" TEXT, " +
                "CONSTRAINT propertyUniqueConstraint UNIQUE(name));");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_schema_vertex\"(" +
                "\"ID\" SERIAL PRIMARY KEY, " +
                "\"sqlg_schema.vertex__I\" BIGINT, " +
                "\"sqlg_schema.schema__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.vertex__I\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.schema__O\") REFERENCES \"sqlg_schema\".\"V_schema\" (\"ID\"), " +
                "INDEX (\"sqlg_schema.vertex__I\"), " +
                "INDEX (\"sqlg_schema.schema__O\"));");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_in_edges\"(" +
                "\"ID\" SERIAL PRIMARY KEY, " +
                "\"sqlg_schema.edge__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.edge__I\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"), " +
                "INDEX (\"sqlg_schema.edge__I\"), " +
                "INDEX (\"sqlg_schema.vertex__O\"));");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_out_edges\"(" +
                "\"ID\" SERIAL PRIMARY KEY, " +
                "\"sqlg_schema.edge__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.edge__I\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"), " +
                "INDEX (\"sqlg_schema.edge__I\"), " +
                "INDEX (\"sqlg_schema.vertex__O\"));");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_property\"(" +
                "\"ID\" SERIAL PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"), " +
                "INDEX (\"sqlg_schema.property__I\"), " +
                "INDEX (\"sqlg_schema.vertex__O\"));");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_property\"(" +
                "\"ID\" SERIAL PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"), " +
                "INDEX (\"sqlg_schema.property__I\"), " +
                "INDEX (\"sqlg_schema.edge__O\"));");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_vertex_index\"(" +
                "\"ID\" SERIAL PRIMARY KEY, " +
                "\"sqlg_schema.index__I\" BIGINT, " +
                "\"sqlg_schema.vertex__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.index__I\") REFERENCES \"sqlg_schema\".\"V_index\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.vertex__O\") REFERENCES \"sqlg_schema\".\"V_vertex\" (\"ID\"), " +
                "INDEX (\"sqlg_schema.index__I\"), " +
                "INDEX (\"sqlg_schema.vertex__O\"));");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_edge_index\"(" +
                "\"ID\" SERIAL PRIMARY KEY, " +
                "\"sqlg_schema.index__I\" BIGINT, " +
                "\"sqlg_schema.edge__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.index__I\") REFERENCES \"sqlg_schema\".\"V_index\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.edge__O\") REFERENCES \"sqlg_schema\".\"V_edge\" (\"ID\"), " +
                "INDEX (\"sqlg_schema.index__I\"), " +
                "INDEX (\"sqlg_schema.edge__O\"));");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_index_property\"(" +
                "\"ID\" SERIAL PRIMARY KEY, " +
                "\"sqlg_schema.property__I\" BIGINT, " +
                "\"sqlg_schema.index__O\" BIGINT, " +
                "FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"), " +
                "FOREIGN KEY (\"sqlg_schema.index__O\") REFERENCES \"sqlg_schema\".\"V_index\" (\"ID\"), " +
                "INDEX (\"sqlg_schema.property__I\"), " +
                "INDEX (\"sqlg_schema.index__O\"));");

        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"E_globalUniqueIndex_property\"(\"ID\" SERIAL PRIMARY KEY, \"sqlg_schema.property__I\" BIGINT, \"sqlg_schema.globalUniqueIndex__O\" BIGINT, FOREIGN KEY (\"sqlg_schema.property__I\") REFERENCES \"sqlg_schema\".\"V_property\" (\"ID\"), FOREIGN KEY (\"sqlg_schema.globalUniqueIndex__O\") REFERENCES \"sqlg_schema\".\"V_globalUniqueIndex\" (\"ID\"));");
        result.add("CREATE TABLE IF NOT EXISTS \"sqlg_schema\".\"V_log\"(\"ID\" SERIAL PRIMARY KEY, \"timestamp\" TIMESTAMP, \"pid\" INTEGER, \"log\" TEXT);");
        return result;
    }

    @Override
    public String sqlgAddIndexEdgeSequenceColumn() {
        return null;
    }

    @Override
    public boolean isIndexPartOfCreateTable() {
        return true;
    }

    @Override
    public String valueToValuesString(PropertyType propertyType, Object value) {
        throw new RuntimeException("Not yet implemented");
    }

    @Override
    public boolean supportsType(PropertyType propertyType) {
        return false;
    }

    private Array createArrayOf(Connection conn, PropertyType propertyType, Object[] data) {
        try {
            switch (propertyType) {
                case LOCALTIME_ARRAY:
                    // shit DST for local time
                    if (data != null) {
                        int a = 0;
                        for (Object o : data) {
                            data[a++] = shiftDST(((Time) o).toLocalTime());
                        }
                    }
                    // fall through
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
    public Object convertArray(PropertyType propertyType, Array array) throws SQLException {
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

    @Override
    public String getFullTextQueryText(FullText fullText, String column) {
        String toQuery = fullText.isPlain() ? "plainto_tsquery" : "to_tsquery";
        // either we provided the query expression...
        String leftHand = fullText.getQuery();
        // or we use the column
        if (leftHand == null) {
            leftHand = column;
        }
        return "to_tsvector('" + fullText.getConfiguration() + "', " + leftHand + ") @@ " + toQuery + "('" + fullText.getConfiguration() + "',?)";
    }

    @Override
    public boolean isSystemIndex(String indexName) {
        return indexName.endsWith("_pkey") || indexName.endsWith("_idx");
    }

    @Override
    public boolean needsSchemaCreationPrecommit() {
        return true;
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
}
