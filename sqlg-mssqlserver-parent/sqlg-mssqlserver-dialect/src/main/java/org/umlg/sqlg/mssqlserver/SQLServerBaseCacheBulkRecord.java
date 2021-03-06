package org.umlg.sqlg.mssqlserver;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgExceptions;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.umlg.sqlg.structure.PropertyType.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/08/09
 */
abstract class SQLServerBaseCacheBulkRecord implements ISQLServerBulkRecord {

    final Map<Integer, ColumnMetadata> columnMetadata = new HashMap<>();
    SortedSet<String> columns;
    Map<String, PropertyColumn> propertyColumns;
    //Used for temp tables.
    Map<String, PropertyType> properties;

    class ColumnMetadata {
        final String columnName;
        final int columnType;
        final int precision;
        final int scale;
        final DateTimeFormatter dateTimeFormatter;
        final PropertyType propertyType;

        ColumnMetadata(String name,
                       int type,
                       int precision,
                       int scale,
                       DateTimeFormatter dateTimeFormatter,
                       PropertyType propertyType) {
            columnName = name;
            columnType = type;
            this.precision = precision;
            this.scale = scale;
            this.dateTimeFormatter = dateTimeFormatter;
            this.propertyType = propertyType;
        }
    }

    int addMetaData(SQLServerBulkCopy bulkCopy, SqlgGraph sqlgGraph) throws SQLServerException {
        int i = 1;
        for (String column : this.columns) {
            PropertyType propertyType;
            if (this.propertyColumns != null) {
                PropertyColumn propertyColumn = propertyColumns.get(column);
                propertyType = propertyColumn.getPropertyType();
            } else {
                propertyType = this.properties.get(column);
            }
            switch (propertyType.ordinal()) {
                case BOOLEAN_ORDINAL:
                    //Add the column mappings, skipping the first identity column.
                    bulkCopy.addColumnMapping(i, column);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column,
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(propertyType)[0],
                            0,
                            0,
                            null,
                            propertyType
                    ));
                    break;
                case BYTE_ORDINAL:
                case SHORT_ORDINAL:
                case INTEGER_ORDINAL:
                case LONG_ORDINAL:
                case FLOAT_ORDINAL:
                case DOUBLE_ORDINAL:
                case JSON_ORDINAL:
                case STRING_ORDINAL:
                    //Add the column mappings, skipping the first identity column.
                    bulkCopy.addColumnMapping(i, column);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column,
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(propertyType)[0],
                            0,
                            0,
                            null,
                            propertyType
                    ));
                    break;
                case VARCHAR_ORDINAL:
                    //Add the column mappings, skipping the first identity column.
                    bulkCopy.addColumnMapping(i, column);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column,
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(propertyType)[0],
                            0,
                            0,
                            null,
                            propertyType
                    ));
                    break;
                case LOCALDATE_ORDINAL:
                    bulkCopy.addColumnMapping(i, column);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column,
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(propertyType)[0],
                            0,
                            0,
                            null,
                            propertyType
                    ));
                    break;
                case LOCALDATETIME_ORDINAL:
                    bulkCopy.addColumnMapping(i, column);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column,
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(propertyType)[0],
                            0,
                            0,
                            null,
                            propertyType
                    ));
                    break;
                case LOCALTIME_ORDINAL:
                    bulkCopy.addColumnMapping(i, column);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column,
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(propertyType)[0],
                            0,
                            0,
                            null,
                            propertyType
                    ));
                    break;
                case ZONEDDATETIME_ORDINAL:
                    bulkCopy.addColumnMapping(i, column);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column,
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.ZONEDDATETIME)[0],
                            0,
                            0,
                            null,
                            PropertyType.LOCALDATETIME
                    ));
                    bulkCopy.addColumnMapping(i, column + PropertyType.ZONEDDATETIME.getPostFixes()[0]);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column,
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.ZONEDDATETIME)[0],
                            0,
                            0,
                            null,
                            PropertyType.STRING
                    ));
                    break;
                case PERIOD_ORDINAL:
                    bulkCopy.addColumnMapping(i, column);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column,
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.PERIOD)[0],
                            0,
                            0,
                            null,
                            PropertyType.INTEGER
                    ));
                    bulkCopy.addColumnMapping(i, column + PropertyType.PERIOD.getPostFixes()[0]);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column + PropertyType.PERIOD.getPostFixes()[0],
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.PERIOD)[1],
                            0,
                            0,
                            null,
                            PropertyType.INTEGER
                    ));
                    bulkCopy.addColumnMapping(i, column + PropertyType.PERIOD.getPostFixes()[1]);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column + PropertyType.PERIOD.getPostFixes()[1],
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.PERIOD)[2],
                            0,
                            0,
                            null,
                            PropertyType.INTEGER
                    ));
                    break;
                case DURATION_ORDINAL:
                    bulkCopy.addColumnMapping(i, column);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column,
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.DURATION)[0],
                            0,
                            0,
                            null,
                            PropertyType.LONG
                    ));
                    bulkCopy.addColumnMapping(i, column + PropertyType.DURATION.getPostFixes()[0]);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column + PropertyType.DURATION.getPostFixes()[0],
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(PropertyType.DURATION)[1],
                            0,
                            0,
                            null,
                            PropertyType.INTEGER
                    ));
                    break;
                case byte_ARRAY_ORDINAL:
                    //Add the column mappings, skipping the first identity column.
                    bulkCopy.addColumnMapping(i, column);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column,
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(propertyType)[0],
                            8000,
                            0,
                            null,
                            propertyType
                    ));
                    break;
                case BYTE_ARRAY_ORDINAL:
                    //Add the column mappings, skipping the first identity column.
                    bulkCopy.addColumnMapping(i, column);
                    this.columnMetadata.put(i++, new ColumnMetadata(
                            column,
                            sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(propertyType)[0],
                            8000,
                            0,
                            null,
                            propertyType
                    ));
                    break;
                default:
                    throw SqlgExceptions.invalidPropertyType(propertyType);
            }
        }
        return i;
    }

    @Override
    public Set<Integer> getColumnOrdinals() {
        return this.columnMetadata.keySet();
    }

    @Override
    public String getColumnName(int column) {
        return this.columnMetadata.get(column).columnName;
    }

    @Override
    public int getColumnType(int column) {
        return this.columnMetadata.get(column).columnType;
    }

    @Override
    public int getPrecision(int column) {
        return this.columnMetadata.get(column).precision;
    }

    @Override
    public int getScale(int column) {
        return this.columnMetadata.get(column).scale;
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    abstract Object getValue(String column);

    void addValues(List<Object> values) {
        for (String column : this.columns) {
            PropertyType propertyType;
            if (this.propertyColumns != null) {
                propertyType = this.propertyColumns.get(column).getPropertyType();
            } else {
                propertyType = this.properties.get(column);
            }
            Object value = getValue(column);
            switch (propertyType.ordinal()) {
                case BOOLEAN_ORDINAL:
                    if (value != null) {
                        values.add(value);
                    } else {
                        values.add(null);
                    }
                    break;
                case BYTE_ORDINAL:
                case SHORT_ORDINAL:
                case INTEGER_ORDINAL:
                case LONG_ORDINAL:
                case FLOAT_ORDINAL:
                case DOUBLE_ORDINAL:
                case JSON_ORDINAL:
                case STRING_ORDINAL:
                    if (value != null) {
                        values.add(value);
                    } else {
                        values.add(null);
                    }
                    break;
                case VARCHAR_ORDINAL:
                    if (value != null) {
                        values.add(value);
                    } else {
                        values.add(null);
                    }
                    break;
                case LOCALDATE_ORDINAL:
                    if (value != null) {
                        values.add(value.toString());
                    } else {
                        values.add(null);
                    }
                    break;
                case LOCALDATETIME_ORDINAL:
                    if (value != null) {
                        Timestamp timestamp = Timestamp.valueOf((LocalDateTime) value);
                        values.add(timestamp.toString());
                    } else {
                        values.add(null);
                    }
                    break;
                case LOCALTIME_ORDINAL:
                    if (value != null) {
                        values.add(value.toString());
                    } else {
                        values.add(null);
                    }
                    break;
                case ZONEDDATETIME_ORDINAL:
                    if (value != null) {
                        values.add(((ZonedDateTime) value).toLocalDateTime());
                        TimeZone tz = TimeZone.getTimeZone(((ZonedDateTime) value).getZone());
                        values.add(tz.getID());
                    } else {
                        values.add(null);
                        values.add(null);
                    }
                    break;
                case PERIOD_ORDINAL:
                    if (value != null) {
                        Period period = (Period) value;
                        values.add(period.getYears());
                        values.add(period.getMonths());
                        values.add(period.getDays());
                    } else {
                        values.add(null);
                        values.add(null);
                        values.add(null);
                    }
                    break;
                case DURATION_ORDINAL:
                    if (value != null) {
                        Duration duration = (Duration) value;
                        values.add(duration.getSeconds());
                        values.add(duration.getNano());
                    } else {
                        values.add(null);
                        values.add(null);
                    }
                    break;
                case byte_ARRAY_ORDINAL:
                    if (value != null) {
                        values.add(value);
                    } else {
                        values.add(null);
                    }
                    break;
                case BYTE_ARRAY_ORDINAL:
                    if (value != null) {
                        byte[] byteArray = SqlgUtil.convertObjectArrayToBytePrimitiveArray((Object[]) value);
                        values.add(byteArray);
                    } else {
                        values.add(null);
                    }
                    break;
                default:
                    throw SqlgExceptions.invalidPropertyType(propertyType);

            }
        }
    }

}
