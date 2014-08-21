package org.umlg.sqlg.sql.dialect;

import com.tinkerpop.gremlin.structure.Property;
import org.umlg.sqlg.structure.PropertyType;

import java.sql.Types;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Date: 2014/07/16
 * Time: 1:42 PM
 */
public class MariaDbDialect implements SqlDialect {

    @Override
    public Set<String> getDefaultSchemas() {
        return new HashSet<>(Arrays.asList("information_schema", "performance_schema", "mysql"));
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
            default:
                throw new IllegalStateException("Unknown sqlType " + sqlType);
        }
    }

    @Override
    public String getJdbcDriver() {
        return "org.mariadb.jdbc.Driver";
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
        if (value instanceof Float) {
            return;
        }
        if (value instanceof Double) {
            return;
        }
        throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
    }


    @Override
    public String getColumnEscapeKey() {
        return "`";
    }

    @Override
    public String getPrimaryKeyType() {
        return "BIGINT NOT NULL PRIMARY KEY";
    }

    @Override
    public String getAutoIncrementPrimaryKeyConstruct() {
        return "BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY";
    }

    @Override
    public String propertyTypeToSqlDefinition(PropertyType propertyType) {

        switch (propertyType) {
            case BOOLEAN:
                return "BOOLEAN";
            case BYTE:
                return "TINYINT";
            case SHORT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case LONG:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case STRING:
                return "TEXT";
            default:
                throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(propertyType);
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
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case STRING:
                return Types.CLOB;
            default:
                throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(propertyType);
        }
    }

    @Override
    public String getForeignKeyTypeDefinition() {
        return "BIGINT NOT NULL";
    }

    @Override
    public boolean supportsTransactionalSchema() {
        return false;
    }

    @Override
    public boolean supportsBooleanArrayValues() {
        return false;
    }

    @Override
    public boolean supportsByteArrayValues() {
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
    public boolean supportsLongArrayValues() {
        return false;
    }

    @Override
    public boolean supportsStringArrayValues() {
        return false;
    }

    @Override
    public String getArrayDriverType(PropertyType booleanArray) {
        return null;
    }

}
