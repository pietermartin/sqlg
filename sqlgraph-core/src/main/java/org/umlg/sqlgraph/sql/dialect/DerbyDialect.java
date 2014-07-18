package org.umlg.sqlgraph.sql.dialect;

import org.umlg.sqlgraph.structure.PropertyType;

import java.sql.Types;

/**
 * Date: 2014/07/16
 * Time: 1:42 PM
 */
public class DerbyDialect implements SqlDialect {

    @Override
    public boolean needsSemicolon() {
        return false;
    }

    @Override
    public boolean supportsCascade() {
        return false;
    }

    @Override
    public String getColumnEscapeKey() {
        return "\"";
    }

    @Override
    public String getPrimaryKeyType() {
        return "BIGINT";
    }

    @Override
    public String getAutoIncrementPrimaryKeyConstruct() {
        return "GENERATED ALWAYS AS IDENTITY";
    }

    @Override
    public String propertyTypeToSqlDefinition(PropertyType propertyType) {

        switch (propertyType) {
            case BOOLEAN:
                return "BOOLEAN" ;
            case BYTE:
                return "SMALLINT" ;
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
                return "LONG VARCHAR" ;
            default:
                throw new IllegalStateException("Unknown propertyType " + propertyType.name());
        }
    }

    @Override
    public int propertyTypeToJavaSqlType(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN:
                return Types.BOOLEAN ;
            case BYTE:
                return Types.TINYINT;
            case SHORT:
                return Types.TINYINT;
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
}
