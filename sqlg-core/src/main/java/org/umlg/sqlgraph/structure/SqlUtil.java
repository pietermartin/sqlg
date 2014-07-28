package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.structure.Element;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Date: 2014/07/12
 * Time: 3:13 PM
 */
public class SqlUtil {

    public static Object[] mapTokeyValues(Map<String, Object> keyValues) {
        Object[] result = new Object[keyValues.size() * 2];
        int i = 0;
        for (String key : keyValues.keySet()) {
            result[i++] = key;
            result[i++] = keyValues.get(key);
        }
        return result;
    }

    public static ConcurrentHashMap<String, PropertyType> transformToColumnDefinitionMap(Object... keyValues) {
        //This is to ensure the keys are unique
        Set<String> keys = new HashSet<>();
        ConcurrentHashMap<String, PropertyType> result = new ConcurrentHashMap<>();
        int i = 1;
        String key = "";
        for (Object keyValue : keyValues) {
            if (i++ % 2 != 0) {
                //key
                key = (String) keyValue;
            } else {
                //value
                //key
                //skip the label as that is not a property but the table
                if (key.equals(Element.LABEL) || keys.contains(key)) {
                    continue;
                }
                keys.add(key);
                result.put(key, PropertyType.from(keyValue));
            }
        }
        return result;
    }

    public static List<String> transformToInsertColumns(Object... keyValues) {
        List<String> result = new ArrayList<>();
        int i = 1;
        for (Object keyValue : keyValues) {
            if (i++ % 2 != 0) {
                //key
                //skip the label as that is not a property but the table
                if (keyValue.equals(Element.LABEL) || keyValue.equals(Element.ID)) {
                    continue;
                }
                //if duplicate key are specified then only take the last one
                if (result.contains((String) keyValue)) {
                    result.remove((String) keyValue);
                }
                result.add((String) keyValue);
            } else {
                //value
            }
        }
        return result;
    }

    public static List<String> transformToInsertValues(Object... keyValues) {
        Map<String, Integer> keyResultIndexMap = new HashMap<>();
        List<String> result = new ArrayList<>();
        int listIndex = 0;
        int i = 1;
        String key = "";
        for (Object keyValue : keyValues) {
            if (i++ % 2 != 0) {
                //key
                key = (String) keyValue;
            } else {
                //value
                //skip the label as that is not a property but the table
                if (key.equals(Element.LABEL) || key.equals(Element.ID)) {
                    continue;
                }

                //Check it the key is already in the result.
                //If so remove it, as the last key/value pair wins
                if (keyResultIndexMap.containsKey(key)) {
                    result.remove(keyResultIndexMap.get(key).intValue());
                    listIndex--;
                }
                keyResultIndexMap.put(key, listIndex++);

                result.add(keyValue.toString());
            }
        }
        return result;
    }

    public static List<ImmutablePair<PropertyType, Object>> transformToTypeAndValue(Object... keyValues) {
        Map<String, Integer> keyResultIndexMap = new HashMap<>();
        List<ImmutablePair<PropertyType, Object>> result = new ArrayList<>();
        int listIndex = 0;
        int i = 1;
        String key = "";
        for (Object keyValue : keyValues) {
            if (i++ % 2 != 0) {
                //key
                key = (String) keyValue;
            } else {
                //value
                //skip the label as that is not a property but the table
                if (key.equals(Element.LABEL)) {
                    continue;
                }

                //Check it the key is already in the result.
                //If so remove it, as the last key/value pair wins
                if (keyResultIndexMap.containsKey(key)) {
                    result.remove(keyResultIndexMap.get(key).intValue());
                    listIndex--;
                }
                keyResultIndexMap.put(key, listIndex++);

                result.add(ImmutablePair.of(PropertyType.from(keyValue), keyValue));
            }
        }
        return result;
    }

    /**
     * This only gets called for array properties
     *
     * @param propertyType
     * @param value
     * @return
     */
    public static Object[] transformArrayToInsertValue(PropertyType propertyType, Object value) {
        return getArray(value);
    }

    private static Object[] getArray(Object val){
        int arrlength = Array.getLength(val);
        Object[] outputArray = new Object[arrlength];
        for(int i = 0; i < arrlength; ++i){
            outputArray[i] = Array.get(val, i);
        }
        return outputArray;
    }
}
