package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.umlg.sqlg.structure.topology.Topology;

import java.time.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by pieter on 2014/07/15.
 */
public class PropertyType {

    public static final int BOOLEAN_ORDINAL = 0;
    public static final PropertyType BOOLEAN = new PropertyType("BOOLEAN", BOOLEAN_ORDINAL, Boolean.class.getName(), new String[]{});
    public static final int BYTE_ORDINAL = 1;
    @SuppressWarnings("WeakerAccess")
    public static final PropertyType BYTE = new PropertyType("BYTE", BYTE_ORDINAL, Byte.class.getName(), new String[]{});
    public static final int SHORT_ORDINAL = 2;
    public static final PropertyType SHORT = new PropertyType("SHORT", SHORT_ORDINAL, Short.class.getName(), new String[]{});
    public static final int INTEGER_ORDINAL = 3;
    public static final PropertyType INTEGER = new PropertyType("INTEGER", INTEGER_ORDINAL, Integer.class.getName(), new String[]{});
    public static final int LONG_ORDINAL = 4;
    public static final PropertyType LONG = new PropertyType("LONG", LONG_ORDINAL, Long.class.getName(), new String[]{});
    public static final int FLOAT_ORDINAL = 5;
    public static final PropertyType FLOAT = new PropertyType("FLOAT", FLOAT_ORDINAL, Float.class.getName(), new String[]{});
    public static final int DOUBLE_ORDINAL = 6;
    public static final PropertyType DOUBLE = new PropertyType("DOUBLE", DOUBLE_ORDINAL, Double.class.getName(), new String[]{});
    public static final int STRING_ORDINAL = 7;
    public static final PropertyType STRING = new PropertyType("STRING", STRING_ORDINAL, String.class.getName(), new String[]{});
    public static final int LOCALDATE_ORDINAL = 8;
    public static final PropertyType LOCALDATE = new PropertyType("LOCALDATE", LOCALDATE_ORDINAL, LocalDate.class.getName(), new String[]{});
    public static final int LOCALDATETIME_ORDINAL = 9;
    public static final PropertyType LOCALDATETIME = new PropertyType("LOCALDATETIME", LOCALDATETIME_ORDINAL, LocalDateTime.class.getName(), new String[]{});
    public static final int LOCALTIME_ORDINAL = 10;
    public static final PropertyType LOCALTIME = new PropertyType("LOCALTIME", LOCALTIME_ORDINAL, LocalTime.class.getName(), new String[]{});
    public static final int ZONEDDATETIME_ORDINAL = 11;
    public static final PropertyType ZONEDDATETIME = new PropertyType("ZONEDDATETIME", ZONEDDATETIME_ORDINAL, ZonedDateTime.class.getName(), new String[]{Topology.ZONEID});
    public static final int PERIOD_ORDINAL = 12;
    public static final PropertyType PERIOD = new PropertyType("PERIOD", PERIOD_ORDINAL, Period.class.getName(), new String[]{Topology.MONTHS, Topology.DAYS});
    public static final int DURATION_ORDINAL = 13;
    public static final PropertyType DURATION = new PropertyType("DURATION", DURATION_ORDINAL, Duration.class.getName(), new String[]{Topology.DURATION_NANOS});
    public static final int JSON_ORDINAL = 14;
    public static final PropertyType JSON = new PropertyType("JSON", JSON_ORDINAL, JsonNode.class.getName(), new String[]{});
    public static final int POINT_ORDINAL = 15;
    public static final PropertyType POINT = new PropertyType("POINT", POINT_ORDINAL, "org.postgis.Point", new String[]{});
    public static final int LINESTRING_ORDINAL = 16;
    public static final PropertyType LINESTRING = new PropertyType("LINESTRING", LINESTRING_ORDINAL, "org.postgis.LineString", new String[]{});
    public static final int POLYGON_ORDINAL = 17;
    public static final PropertyType POLYGON = new PropertyType("POLYGON", POLYGON_ORDINAL, "org.postgis.Polygon", new String[]{});
    public static final int GEOGRAPHY_POINT_ORDINAL = 18;
    public static final PropertyType GEOGRAPHY_POINT = new PropertyType("GEOGRAPHY_POINT", GEOGRAPHY_POINT_ORDINAL, "org.umlg.sqlg.gis.GeographyPoint", new String[]{});
    public static final int GEOGRAPHY_POLYGON_ORDINAL = 19;
    public static final PropertyType GEOGRAPHY_POLYGON = new PropertyType("GEOGRAPHY_POLYGON", GEOGRAPHY_POLYGON_ORDINAL, "org.umlg.sqlg.gis.GeographyPolygon", new String[]{});
    //arrays
    public static final int boolean_ARRAY_ORDINAL = 20;
    public static final PropertyType boolean_ARRAY = new PropertyType("boolean_ARRAY", boolean_ARRAY_ORDINAL, boolean[].class.getName(), new String[]{});
    public static final int BOOLEAN_ARRAY_ORDINAL = 21;
    public static final PropertyType BOOLEAN_ARRAY = new PropertyType("BOOLEAN_ARRAY", BOOLEAN_ARRAY_ORDINAL, Boolean[].class.getName(), new String[]{});
    public static final int byte_ARRAY_ORDINAL = 22;
    public static final PropertyType byte_ARRAY = new PropertyType("byte_ARRAY", byte_ARRAY_ORDINAL, byte[].class.getName(), new String[]{});
    public static final int BYTE_ARRAY_ORDINAL = 23;
    public static final PropertyType BYTE_ARRAY = new PropertyType("BYTE_ARRAY", BYTE_ARRAY_ORDINAL, Byte[].class.getName(), new String[]{});
    public static final int short_ARRAY_ORDINAL = 24;
    public static final PropertyType short_ARRAY = new PropertyType("short_ARRAY", short_ARRAY_ORDINAL, short[].class.getName(), new String[]{});
    public static final int SHORT_ARRAY_ORDINAL = 25;
    public static final PropertyType SHORT_ARRAY = new PropertyType("SHORT_ARRAY", SHORT_ARRAY_ORDINAL, Short[].class.getName(), new String[]{});
    public static final int int_ARRAY_ORDINAL = 26;
    public static final PropertyType int_ARRAY = new PropertyType("int_ARRAY", int_ARRAY_ORDINAL, int[].class.getName(), new String[]{});
    public static final int INTEGER_ARRAY_ORDINAL = 27;
    public static final PropertyType INTEGER_ARRAY = new PropertyType("INTEGER_ARRAY", INTEGER_ARRAY_ORDINAL, Integer[].class.getName(), new String[]{});
    public static final int long_ARRAY_ORDINAL = 28;
    public static final PropertyType long_ARRAY = new PropertyType("long_ARRAY", long_ARRAY_ORDINAL, long[].class.getName(), new String[]{});
    public static final int LONG_ARRAY_ORDINAL = 29;
    public static final PropertyType LONG_ARRAY = new PropertyType("LONG_ARRAY", LONG_ARRAY_ORDINAL, Long[].class.getName(), new String[]{});
    public static final int float_ARRAY_ORDINAL = 30;
    public static final PropertyType float_ARRAY = new PropertyType("float_ARRAY", float_ARRAY_ORDINAL, float[].class.getName(), new String[]{});
    public static final int FLOAT_ARRAY_ORDINAL = 31;
    public static final PropertyType FLOAT_ARRAY = new PropertyType("FLOAT_ARRAY", FLOAT_ARRAY_ORDINAL, Float[].class.getName(), new String[]{});
    public static final int double_ARRAY_ORDINAL = 32;
    public static final PropertyType double_ARRAY = new PropertyType("double_ARRAY", double_ARRAY_ORDINAL, double[].class.getName(), new String[]{});
    public static final int DOUBLE_ARRAY_ORDINAL = 33;
    public static final PropertyType DOUBLE_ARRAY = new PropertyType("DOUBLE_ARRAY", DOUBLE_ARRAY_ORDINAL, Double[].class.getName(), new String[]{});
    public static final int STRING_ARRAY_ORDINAL = 34;
    public static final PropertyType STRING_ARRAY = new PropertyType("STRING_ARRAY", STRING_ARRAY_ORDINAL, String[].class.getName(), new String[]{});
    public static final int LOCALDATETIME_ARRAY_ORDINAL = 35;
    public static final PropertyType LOCALDATETIME_ARRAY = new PropertyType("LOCALDATETIME_ARRAY", LOCALDATETIME_ARRAY_ORDINAL, LocalDateTime[].class.getName(), new String[]{});
    public static final int LOCALDATE_ARRAY_ORDINAL = 36;
    public static final PropertyType LOCALDATE_ARRAY = new PropertyType("LOCALDATE_ARRAY", LOCALDATE_ARRAY_ORDINAL, LocalDate[].class.getName(), new String[]{});
    public static final int LOCALTIME_ARRAY_ORDINAL = 37;
    public static final PropertyType LOCALTIME_ARRAY = new PropertyType("LOCALTIME_ARRAY", LOCALTIME_ARRAY_ORDINAL, LocalTime[].class.getName(), new String[]{});
    public static final int ZONEDDATETIME_ARRAY_ORDINAL = 38;
    public static final PropertyType ZONEDDATETIME_ARRAY = new PropertyType("ZONEDDATETIME_ARRAY", ZONEDDATETIME_ARRAY_ORDINAL, ZonedDateTime[].class.getName(), new String[]{Topology.ZONEID});
    public static final int DURATION_ARRAY_ORDINAL = 39;
    public static final PropertyType DURATION_ARRAY = new PropertyType("DURATION_ARRAY", DURATION_ARRAY_ORDINAL, Duration[].class.getName(), new String[]{Topology.DURATION_NANOS});
    public static final int PERIOD_ARRAY_ORDINAL = 40;
    public static final PropertyType PERIOD_ARRAY = new PropertyType("PERIOD_ARRAY", PERIOD_ARRAY_ORDINAL, Period[].class.getName(), new String[]{Topology.MONTHS, Topology.DAYS});
    public static final int JSON_ARRAY_ORDINAL = 41;
    public static final PropertyType JSON_ARRAY = new PropertyType("JSON_ARRAY", JSON_ARRAY_ORDINAL, JsonNode[].class.getName(), new String[]{});

    public static final int VARCHAR_ORDINAL = 42;
    public static final int UUID_ORDINAL = 43;
    public static final PropertyType UUID = new PropertyType("UUID", UUID_ORDINAL, java.util.UUID.class.getName(), new String[]{});

    public static PropertyType varChar(int length) {
        return new PropertyType(String.class.getName(), new String[]{}, length);
    }

    private String name;
    private final int ordinal;
    private String javaClassName;
    //This postfix is for composite properties where one java type maps to multiple columns.
    //This columns' type is specified in the dialect, SqlDialect.propertyTypeToSqlDefinition.
    //The number of postfix must match the number of types - 1 as the first column as no postfix
    private String[] postFixes;

    private int length = -1;
    private static final Map<String, PropertyType> JAVA_CLASS_NAME_TO_ENUM = new HashMap<>();
    private static final Map<String, PropertyType> NAME_TO_ENUM = new HashMap<>();

    private PropertyType(String javaClassName, String[] postFixes, int length) {
        this("VARCHAR", PropertyType.VARCHAR_ORDINAL, javaClassName, postFixes);
        this.length = length;
    }

    private PropertyType(String name, int ordinal, String javaClassName, String[] postFixes) {
        this.name = name;
        this.ordinal = ordinal;
        this.javaClassName = javaClassName;
        this.postFixes = postFixes;
    }

    static {
        for (PropertyType pt : values()) {
            NAME_TO_ENUM.put(pt.name, pt);
            JAVA_CLASS_NAME_TO_ENUM.put(pt.javaClassName, pt);
        }
    }

    public static PropertyType from(Object o) {
        PropertyType propertyType = JAVA_CLASS_NAME_TO_ENUM.get(o.getClass().getName());
        if (propertyType == null && (o instanceof JsonNode)) {
            propertyType = JAVA_CLASS_NAME_TO_ENUM.get(JsonNode.class.getName());
        } else if (propertyType == null && (o instanceof JsonNode[])) {
            propertyType = JAVA_CLASS_NAME_TO_ENUM.get(JsonNode[].class.getName());
        }
        if (propertyType == null) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(o);
        }
        return propertyType;
    }

    public String[] getPostFixes() {
        return postFixes;
    }

    public String name() {
        return this.name;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getLength() {
        return length;
    }

    public boolean isArray() {
        return name().endsWith("_ARRAY");
    }

    public boolean isString() {
        return this == PropertyType.STRING;
    }

    public static PropertyType[] values() {
        return new PropertyType[]{
                PropertyType.BOOLEAN,
                PropertyType.BYTE,
                PropertyType.SHORT,
                PropertyType.INTEGER,
                PropertyType.LONG,
                PropertyType.FLOAT,
                PropertyType.DOUBLE,
                PropertyType.STRING,
                PropertyType.LOCALDATE,
                PropertyType.LOCALDATETIME,
                PropertyType.LOCALTIME,
                PropertyType.ZONEDDATETIME,
                PropertyType.PERIOD,
                PropertyType.DURATION,
                PropertyType.JSON,
                PropertyType.POINT,
                PropertyType.LINESTRING,
                PropertyType.POLYGON,
                PropertyType.GEOGRAPHY_POINT,
                PropertyType.GEOGRAPHY_POLYGON,
                PropertyType.UUID,
                PropertyType.boolean_ARRAY,
                PropertyType.BOOLEAN_ARRAY,
                PropertyType.byte_ARRAY,
                PropertyType.BYTE_ARRAY,
                PropertyType.short_ARRAY,
                PropertyType.SHORT_ARRAY,
                PropertyType.int_ARRAY,
                PropertyType.INTEGER_ARRAY,
                PropertyType.long_ARRAY,
                PropertyType.LONG_ARRAY,
                PropertyType.float_ARRAY,
                PropertyType.FLOAT_ARRAY,
                PropertyType.double_ARRAY,
                PropertyType.DOUBLE_ARRAY,
                PropertyType.STRING_ARRAY,
                PropertyType.LOCALDATETIME_ARRAY,
                PropertyType.LOCALDATE_ARRAY,
                PropertyType.LOCALTIME_ARRAY,
                PropertyType.ZONEDDATETIME_ARRAY,
                PropertyType.DURATION_ARRAY,
                PropertyType.PERIOD_ARRAY,
                PropertyType.JSON_ARRAY
        };
    }

    public static PropertyType valueOf(String name) {
        return NAME_TO_ENUM.get(name);
    }

    public final int ordinal() {
        return this.ordinal;
    }

}
