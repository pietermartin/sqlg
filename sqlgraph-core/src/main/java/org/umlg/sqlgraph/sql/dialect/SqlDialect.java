package org.umlg.sqlgraph.sql.dialect;

import org.umlg.sqlgraph.structure.PropertyType;

/**
 * Created by pieter on 2014/07/16.
 */
public interface SqlDialect {

    public void validateProperty(Object key, Object value);

    public default boolean needsSemicolon() {
        return true;
    }

    public default boolean supportsCascade() {
        return true;
    }

    String getColumnEscapeKey();

    String getPrimaryKeyType();

    String getAutoIncrementPrimaryKeyConstruct();

    String propertyTypeToSqlDefinition(PropertyType propertyType);

    int propertyTypeToJavaSqlType(PropertyType propertyType);

    String getForeignKeyTypeDefinition();

    public default String maybeWrapInQoutes(String field) {
        StringBuilder sb = new StringBuilder(getColumnEscapeKey());
        sb.append(field);
        sb.append(getColumnEscapeKey());
        return sb.toString();
    }

    public default boolean supportsFloatValues() {
        return true;
    }

    public default boolean supportsTransactionalSchema() {
        return true;
    }

    public default boolean supportsBooleanArrayValues() {
        return true;
    }

    public default boolean supportsByteArrayValues() {
        return true;
    }

    public default boolean supportsDoubleArrayValues() {
        return true;
    }

    public default boolean supportsFloatArrayValues() {
        return true;
    }

    public default boolean supportsIntegerArrayValues() {
        return true;
    }

    public default boolean supportsLongArrayValues() {
        return true;
    }

    public default boolean supportsStringArrayValues() {
        return true;
    }
}
