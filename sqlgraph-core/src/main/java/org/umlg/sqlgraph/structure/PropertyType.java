package org.umlg.sqlgraph.structure;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by pieter on 2014/07/15.
 */
public enum PropertyType {

    BOOLEAN(Boolean.class.getName()),
    BYTE(Byte.class.getName()),
    SHORT(Short.class.getName()),
    INTEGER(Integer.class.getName()),
    LONG(Long.class.getName()),
    FLOAT(Float.class.getName()),
    DOUBLE(Double.class.getName()),
    STRING(String.class.getName());
    //TODO this should go through a dialect alla hibernate
    private String javaClassName;
    private static final Map<String, PropertyType> javaClassNameToEnum = new HashMap<>();

    private PropertyType(String javaClassName) {
        this.javaClassName = javaClassName;
    }

    static {
        for (PropertyType pt : values()) {
            javaClassNameToEnum.put(pt.javaClassName, pt);
        }
    }

    public static PropertyType from(Object o) {
        PropertyType propertyType = javaClassNameToEnum.get(o.getClass().getName());
        if (propertyType == null) {
            throw new UnsupportedOperationException("Unsupported type " + o.getClass().getName());
        }
        return propertyType;
    }

}
