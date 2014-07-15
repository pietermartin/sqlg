package org.umlg.sqlgraph.sql.impl;

import com.tinkerpop.gremlin.structure.Element;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.umlg.sqlgraph.structure.PropertyType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Date: 2014/07/12
 * Time: 3:13 PM
 */
public class SqlUtil {

    public static ConcurrentHashMap<String, PropertyType> transformToColumnDefinitionMap(Object ... keyValues) {
        ConcurrentHashMap<String, PropertyType> result = new ConcurrentHashMap<>();
        int i = 1;
        String key = "";
        for (Object keyValue : keyValues) {
            if (i++ % 2 != 0) {
                //key
                key = (String)keyValue;
            } else {
                //value
                //key
                //skip the label as that is not a property but the table
                if (key.equals(Element.LABEL)) {
                    continue;
                }
                result.put(key, PropertyType.from(keyValue));
            }
        }
        return result;
    }

    public static List<ImmutablePair<String, PropertyType>> transformToColumnDefinition(Object ... keyValues) {
        List<ImmutablePair<String, PropertyType>> result = new ArrayList<>();
        int i = 1;
        String key = "";
        for (Object keyValue : keyValues) {
            if (i++ % 2 != 0) {
                //key
                key = (String)keyValue;
            } else {
                //value
                //key
                //skip the label as that is not a property but the table
                if (key.equals(Element.LABEL)) {
                    continue;
                }
                result.add(ImmutablePair.of(key, PropertyType.from(keyValue)));
            }
        }
        return result;
    }

    public static List<String> transformToInsertColumns(Object ... keyValues) {
        List<String> result = new ArrayList<>();
        int i = 1;
        for (Object keyValue : keyValues) {
            if (i++ % 2 != 0) {
                //key
                //skip the label as that is not a property but the table
                if (keyValue.equals(Element.LABEL)) {
                    continue;
                }
                result.add((String)keyValue);
            } else {
                //value
            }
        }
        return result;
    }

    public static List<String> transformToInsertValues(Object ... keyValues) {
        List<String> result = new ArrayList<>();
        int i = 1;
        String key = "";
        for (Object keyValue : keyValues) {
            if (i++ % 2 != 0) {
                //key
                key = (String)keyValue;
            } else {
                //value
                //skip the label as that is not a property but the table
                if (key.equals(Element.LABEL)) {
                    continue;
                }
                result.add(keyValue.toString());
            }
        }
        return result;
    }

    public static List<ImmutablePair<PropertyType, Object>> transformToTypeAndValue(Object ... keyValues) {
        List<ImmutablePair<PropertyType, Object>> result = new ArrayList<>();
        int i = 1;
        String key = "";
        for (Object keyValue : keyValues) {
            if (i++ % 2 != 0) {
                //key
                key = (String)keyValue;
            } else {
                //value
                //skip the label as that is not a property but the table
                if (key.equals(Element.LABEL)) {
                    continue;
                }
                result.add(ImmutablePair.of(PropertyType.from(keyValue), keyValue));
            }
        }
        return result;
    }


}
