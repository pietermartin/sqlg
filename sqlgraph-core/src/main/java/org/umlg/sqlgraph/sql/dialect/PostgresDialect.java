package org.umlg.sqlgraph.sql.dialect;

import com.tinkerpop.gremlin.structure.Property;
import org.umlg.sqlgraph.structure.PropertyType;

import java.sql.Types;

/**
 * Date: 2014/07/16
 * Time: 1:42 PM
 */
public class PostgresDialect implements SqlDialect {

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
        return "SERIAL PRIMARY KEY";
    }

    @Override
    public String propertyTypeToSqlDefinition(PropertyType propertyType) {

        switch (propertyType) {
            case BOOLEAN:
                return "BOOLEAN" ;
            case SHORT:
                return "SMALLINT" ;
            case INTEGER:
                return "INTEGER" ;
            case LONG:
                return "BIGINT" ;
            case FLOAT:
                return "REAL" ;
            case DOUBLE:
                return "DOUBLE PRECISION" ;
            case STRING:
                return "TEXT" ;

            case BOOLEAN_ARRAY:
                return "BOOLEAN[]" ;
            case SHORT_ARRAY:
                return "SMALLINT[]" ;
            case INTEGER_ARRAY:
                return "INTEGER[]" ;
            case LONG_ARRAY:
                return "BIGINT[]" ;
            case FLOAT_ARRAY:
                return "REAL[]" ;
            case DOUBLE_ARRAY:
                return "DOUBLE PRECISION[]" ;
            case STRING_ARRAY:
                return "TEXT[]" ;
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    @Override
    public int propertyTypeToJavaSqlType(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                return Types.BOOLEAN ;
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
    public String getForeignKeyTypeDefinition() {
        return "BIGINT NOT NULL";
    }

}
