package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.umlg.sqlg.sql.dialect.SqlDialect;

import java.time.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by pieter on 2014/07/15.
 */
public enum PropertyType {

    BOOLEAN(Boolean.class.getName(), new String[]{}),
    BYTE(Byte.class.getName(), new String[]{}),
    SHORT(Short.class.getName(), new String[]{}),
    INTEGER(Integer.class.getName(), new String[]{}),
    LONG(Long.class.getName(), new String[]{}),
    FLOAT(Float.class.getName(), new String[]{}),
    DOUBLE(Double.class.getName(), new String[]{}),
    STRING(String.class.getName(), new String[]{}),
    LOCALDATE(LocalDate.class.getName(), new String[]{}),
    LOCALDATETIME(LocalDateTime.class.getName(), new String[]{}),
    ZONEDDATETIME(ZonedDateTime.class.getName(), new String[]{SchemaManager.ZONEID}),
    LOCALTIME(LocalTime.class.getName(), new String[]{}),
    //years is the first default column
    PERIOD(Period.class.getName(), new String[]{SchemaManager.MONTHS, SchemaManager.DAYS}),
    DURATION(Duration.class.getName(), new String[]{}),

    BOOLEAN_ARRAY(boolean[].class.getName(), new String[]{}),
    BYTE_ARRAY(byte[].class.getName(), new String[]{}),
    SHORT_ARRAY(short[].class.getName(), new String[]{}),
    INTEGER_ARRAY(int[].class.getName(), new String[]{}),
    LONG_ARRAY(long[].class.getName(), new String[]{}),
    FLOAT_ARRAY(float[].class.getName(), new String[]{}),
    DOUBLE_ARRAY(double[].class.getName(), new String[]{}),
    STRING_ARRAY(String[].class.getName(), new String[]{});

    private String javaClassName;
    //This postfix is for composite properties where one java type maps to multiple columns.
    //This columns' type is specified in the dialect, SqlDialect.propertyTypeToSqlDefinition.
    //The number of postfix must match the number of types - 1 as the first column as no postfix
    private String[] postFixes;
    private static final Map<String, PropertyType> javaClassNameToEnum = new HashMap<>();

    PropertyType(String javaClassName, String[] postFixes) {
        this.javaClassName = javaClassName;
        this.postFixes = postFixes;
    }

    static {
        for (PropertyType pt : values()) {
            javaClassNameToEnum.put(pt.javaClassName, pt);
        }
    }

    public static PropertyType from(Object o) {
        PropertyType propertyType = javaClassNameToEnum.get(o.getClass().getName());
        if (propertyType == null) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(o);
        }
        return propertyType;
    }

    public String[] getPostFixes() {
        return postFixes;
    }
}
