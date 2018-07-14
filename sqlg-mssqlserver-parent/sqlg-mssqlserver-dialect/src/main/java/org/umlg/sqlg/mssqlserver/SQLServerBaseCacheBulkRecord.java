package org.umlg.sqlg.mssqlserver;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgExceptions;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

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
            switch (propertyType) {
                case BOOLEAN:
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
                case BYTE:
                case SHORT:
                case INTEGER:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case JSON:
                case STRING:
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
                case LOCALDATE:
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
                case LOCALDATETIME:
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
                case LOCALTIME:
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
                case ZONEDDATETIME:
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
                case PERIOD:
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
                case DURATION:
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
                case byte_ARRAY:
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
                case BYTE_ARRAY:
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
            switch (propertyType) {
                case BOOLEAN:
                    values.add(value);
                    break;
                case BYTE:
                case SHORT:
                case INTEGER:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case JSON:
                case STRING:
                    values.add(value);
                    break;
                case LOCALDATE:
                    values.add(value.toString());
                    break;
                case LOCALDATETIME:
                    Timestamp timestamp = Timestamp.valueOf((LocalDateTime) value);
                    values.add(timestamp.toString());
                    break;
                case LOCALTIME:
                    values.add(value.toString());
                    break;
                case ZONEDDATETIME:
                    values.add(((ZonedDateTime) value).toLocalDateTime());
                    TimeZone tz = TimeZone.getTimeZone(((ZonedDateTime) value).getZone());
                    values.add(tz.getID());
                    break;
                case PERIOD:
                    Period period = (Period) value;
                    values.add(period.getYears());
                    values.add(period.getMonths());
                    values.add(period.getDays());
                    break;
                case DURATION:
                    Duration duration = (Duration) value;
                    values.add(duration.getSeconds());
                    values.add(duration.getNano());
                    break;
                case byte_ARRAY:
                    values.add(value);
                    break;
                case BYTE_ARRAY:
                    byte[] byteArray = SqlgUtil.convertObjectArrayToBytePrimitiveArray((Object[]) value);
                    values.add(byteArray);
                    break;
                default:
                    throw SqlgExceptions.invalidPropertyType(propertyType);

            }
        }
    }

}
