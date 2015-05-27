package org.umlg.sqlg.util;

import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SchemaManager;
import org.umlg.sqlg.structure.SchemaTable;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Date: 2014/07/12
 * Time: 3:13 PM
 */
public class SqlgUtil {

    public static Map<String, Object> toMap(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        int i = 0;
        String key = "";
        Object value;
        Map<String, Object> result = new HashMap<>();
        for (Object keyValue : keyValues) {
            if (i++ % 2 == 0) {
                key = (String) keyValue;
            } else {
                value = keyValue;
                if (!key.equals(T.label)) {
                    ElementHelper.validateProperty(key, value);
                }
                result.put(key, value);

            }
        }
        return result;
    }


    public static SchemaTable parseLabel(final String label) {
        Objects.requireNonNull(label, "label may not be null!");
        String[] schemaLabel = label.split("\\.");
        if (schemaLabel.length != 2) {
            throw new IllegalStateException(String.format("label must be if the format 'schema.table', %s", new Object[]{label}));
        }
        return SchemaTable.of(schemaLabel[0], schemaLabel[1]);
    }

    public static SchemaTable parseLabelMaybeNoSchema(final String label) {
        Objects.requireNonNull(label, "label may not be null!");
        String[] schemaLabel = label.split("\\.");
        if (schemaLabel.length == 2) {
            return SchemaTable.of(schemaLabel[0], schemaLabel[1]);
        } else if (schemaLabel.length == 1) {
            return SchemaTable.of(null, schemaLabel[0]);
        } else {
            throw new IllegalStateException("label must be if the format 'schema.table' or just 'table'");
        }
    }

    public static Object[] mapTokeyValues(Map<Object, Object> keyValues) {
        Object[] result = new Object[keyValues.size() * 2];
        int i = 0;
        for (Object key : keyValues.keySet()) {
            result[i++] = key;
            result[i++] = keyValues.get(key);
        }
        return result;
    }

    public static Object[] mapToStringKeyValues(Map<String, Object> keyValues) {
        Object[] result = new Object[keyValues.size() * 2];
        int i = 0;
        for (Object key : keyValues.keySet()) {
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
        Object key = null;
        for (Object keyValue : keyValues) {
            if (i++ % 2 != 0) {
                //key
                key = keyValue;
            } else {
                //value
                //key
                //skip the label as that is not a property but the table
                if (key.equals(T.label) || keys.contains(key)) {
                    continue;
                }
                keys.add((String) key);
                result.put((String) key, PropertyType.from(keyValue));
            }
        }
        return result;
    }

    public static Map<String, Object> transformToInsertValues(Object... keyValues) {
        Map<String, Object> result = new TreeMap<>();
        int i = 1;
        Object key = null;
        for (Object keyValue : keyValues) {
            if (i++ % 2 != 0) {
                //key
                key = keyValue;
            } else {
                //value
                //skip the label as that is not a property but the table
                if (key.equals(T.label) || key.equals(T.id)) {
                    continue;
                }
                result.put((String) key, keyValue);
            }
        }
        return result;
    }


    public static List<ImmutablePair<PropertyType, Object>> transformToTypeAndValue(Multimap<String, Object> keyValues) {
        List<ImmutablePair<PropertyType, Object>> result = new ArrayList<>();
        for (Map.Entry<String, Object> entry : keyValues.entries()) {
            Object value = entry.getValue();
            String key = entry.getKey();
            //value
            //skip the label as that is not a property but the table
            if (key.equals(T.label.getAccessor())) {
                continue;
            }
            if (key.equals(T.id.getAccessor())) {
                RecordId id;
                if (!(value instanceof RecordId)) {
                    id = RecordId.from(value);
                } else {
                    id = (RecordId)value;
                }
                result.add(ImmutablePair.of(PropertyType.LONG, id.getId()));
            } else {
                result.add(ImmutablePair.of(PropertyType.from(value), value));
            }
        }
        return result;
    }

    public static List<ImmutablePair<PropertyType, Object>> transformToTypeAndValue(Map<String, Object> keyValues) {
        List<ImmutablePair<PropertyType, Object>> result = new ArrayList<>();
        for (String key : keyValues.keySet()) {
            Object value = keyValues.get(key);
            //value
            //skip the label as that is not a property but the table
            if (key.equals(T.label)) {
                continue;
            }
            result.add(ImmutablePair.of(PropertyType.from(value), value));
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

    private static Object[] getArray(Object val) {
        int arrlength = Array.getLength(val);
        Object[] outputArray = new Object[arrlength];
        for (int i = 0; i < arrlength; ++i) {
            outputArray[i] = Array.get(val, i);
        }
        return outputArray;
    }

    public static String removeTrailingInId(String foreignKey) {
        if (foreignKey.endsWith(SchemaManager.IN_VERTEX_COLUMN_END)) {
            return foreignKey.substring(0, foreignKey.length() - SchemaManager.IN_VERTEX_COLUMN_END.length());
        } else {
            return foreignKey;
        }
    }

    public static String removeTrailingOutId(String foreignKey) {
        if (foreignKey.endsWith(SchemaManager.OUT_VERTEX_COLUMN_END)) {
            return foreignKey.substring(0, foreignKey.length() - SchemaManager.OUT_VERTEX_COLUMN_END.length());
        } else {
            return foreignKey;
        }
    }

}
