package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

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
                if (!key.equals(Element.LABEL)) {
                    ElementHelper.validateProperty(key, value);
                }
                result.put(key, value);

            }
        }
        return result;
    }

    public static SchemaTable parseLabel(final String label, String defaultSchema) {
        Objects.requireNonNull(label, "label may not be null!");
        String[] schemaLabel = label.split("\\.");
        final String schema;
        final String table;
        if (schemaLabel.length > 2) {
            throw new IllegalStateException("label may only contain one '.', indicating schema.table");
        }
        if (schemaLabel.length == 2) {
            schema = schemaLabel[0];
            table = schemaLabel[1];
        } else {
            schema = defaultSchema;
            table = label;
        }
        return SchemaTable.of(schema, table);
    }

    public static SchemaTable parseLabel(final String label) {
        Objects.requireNonNull(label, "label may not be null!");
        String[] schemaLabel = label.split("\\.");
        if (schemaLabel.length != 2) {
            throw new IllegalStateException("label must be if the format 'schema.table'");
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

    public static Map<String, Object> transformToInsertValues(Object... keyValues) {
        Map<String, Object> result = new TreeMap<>();
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
                result.put(key, keyValue);
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
            if (key.equals(Element.LABEL)) {
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
}
