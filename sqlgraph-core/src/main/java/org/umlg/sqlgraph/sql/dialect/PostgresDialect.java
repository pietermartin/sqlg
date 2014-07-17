package org.umlg.sqlgraph.sql.dialect;

import org.umlg.sqlgraph.structure.PropertyType;

/**
 * Date: 2014/07/16
 * Time: 1:42 PM
 */
public class PostgresDialect implements SqlDialect {

    @Override
    public String getVarcharArray() {
        return "VARCHAR(255) ARRAY";
    }

    @Override
    public String getPrimaryKeyType() {
        return "SERIAL";
    }

    @Override
    public String getAutoIncrementPrimaryKeyConstruct() {
        return "PRIMARY KEY";
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
                return "FLOAT" ;
            case DOUBLE:
                return "REAL" ;
            case STRING:
                return "TEXT" ;
        }

        return null;
    }

    @Override
    public String getForeignKeyTypeDefinition() {
        return "BIGINT NOT NULL";
    }
}
