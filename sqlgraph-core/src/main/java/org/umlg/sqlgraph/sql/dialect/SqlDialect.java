package org.umlg.sqlgraph.sql.dialect;

import org.umlg.sqlgraph.structure.PropertyType;

/**
 * Created by pieter on 2014/07/16.
 */
public interface SqlDialect {
    String getVarcharArray();

    String getPrimaryKeyType();

    String getAutoIncrementPrimaryKeyConstruct();

    String propertyTypeToSqlDefinition(PropertyType propertyType);

    String getForeignKeyTypeDefinition();
}
