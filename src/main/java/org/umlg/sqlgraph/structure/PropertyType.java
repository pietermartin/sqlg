package org.umlg.sqlgraph.structure;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by pieter on 2014/07/15.
 */
public enum PropertyType {

    BOOLEAN("BOOLEAN", Boolean.class.getName()),
    BYTE("TINYINT", Byte.class.getName()),
    SHORT("SMALLINT", Short.class.getName()),
    INTEGER("INT", Integer.class.getName()),
    LONG("BIGINT", Long.class.getName()),
    FLOAT("REAL", Float.class.getName()),
    DOUBLE("DOUBLE", Double.class.getName()),
    STRING("VARCHAR", String.class.getName());
    //TODO this should go through a dialect alla hibernate
    private String dbName;
    private String javaClassName;
    private static final Map<String, PropertyType> javaClassNameToEnum = new HashMap<>();

    private PropertyType(String dbName, String javaClassName) {
        this.dbName = dbName;
        this.javaClassName = javaClassName;
    }

    static {
        for (PropertyType pt : values()) {
            javaClassNameToEnum.put(pt.javaClassName, pt);
        }
    }

    public String getDbName() {
        return dbName;
    }

    public static PropertyType from(Object o) {
        PropertyType propertyType = javaClassNameToEnum.get(o.getClass().getName());
        if (propertyType == null) {
            throw new UnsupportedOperationException("Unsupported type " + o.getClass().getName());
        }
        return propertyType;
    }

}
