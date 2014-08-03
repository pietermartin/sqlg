package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.structure.Property;

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
    STRING(String.class.getName()),

    BOOLEAN_ARRAY(boolean[].class.getName()),
    BYTE_ARRAY(byte[].class.getName()),
    SHORT_ARRAY(short[].class.getName()),
    INTEGER_ARRAY(int[].class.getName()),
    LONG_ARRAY(long[].class.getName()),
    FLOAT_ARRAY(float[].class.getName()),
    DOUBLE_ARRAY(double[].class.getName()),
    STRING_ARRAY(String[].class.getName());

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
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(o);
        }
        return propertyType;
    }

}
