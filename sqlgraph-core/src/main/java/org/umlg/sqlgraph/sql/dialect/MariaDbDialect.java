package org.umlg.sqlgraph.sql.dialect;

import org.umlg.sqlgraph.structure.PropertyType;

import java.sql.Types;

/**
 * Date: 2014/07/16
 * Time: 1:42 PM
 */
public class MariaDbDialect implements SqlDialect {

    @Override
    public String getColumnEscapeKey() {
        return "`";
    }

    @Override
    public String getPrimaryKeyType() {
        return "BIGINT NOT NULL AUTO_INCREMENT";
    }

    @Override
    public String getAutoIncrementPrimaryKeyConstruct() {
        return "PRIMARY KEY";
    }

    @Override
    public String propertyTypeToSqlDefinition(PropertyType propertyType) {

        switch (propertyType) {
            case BOOLEAN:
                return "BOOLEAN";
            case BYTE:
                return "SMALLINT";
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
                return Types.TINYINT;
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
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    @Override
    public String getForeignKeyTypeDefinition() {
        return "BIGINT NOT NULL";
    }

    @Override
    public boolean supportsFloatValues() {
        return true;
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

}
