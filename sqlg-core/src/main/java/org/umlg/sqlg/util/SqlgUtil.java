package org.umlg.sqlg.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.sql.parse.WhereClause;
import org.umlg.sqlg.structure.*;

import java.lang.reflect.Array;
import java.sql.*;
import java.time.*;
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

    public static <E> Pair<E, Multimap<String, Object>> loadElementsLabeledAndEndElements(SqlgGraph sqlgGraph, ResultSetMetaData resultSetMetaData, final ResultSet resultSet, LinkedList<SchemaTableTree> schemaTableTreeStack) throws SQLException {
        //First load all labeled entries from the resultSet
        Multimap<String, Integer> columnMap1 = ArrayListMultimap.create();
        Multimap<String, Integer> columnMap2 = ArrayListMultimap.create();
        //Translate the columns back from alias to meaningful column headings
        for (int columnCount = 1; columnCount <= resultSetMetaData.getColumnCount(); columnCount++) {
            String columnLabel = resultSetMetaData.getColumnLabel(columnCount);
            String unaliased = SchemaTableTree.threadLocalAliasColumnNameMap.get().get(columnLabel);
            columnMap1.put(unaliased != null ? unaliased : columnLabel, columnCount);
            columnMap2.put(unaliased != null ? unaliased : columnLabel, columnCount);
        }
        Multimap<String, Object> labeledResult = loadLabeledElements(sqlgGraph, columnMap1, resultSet, schemaTableTreeStack);
        E e = loadElements(sqlgGraph, columnMap2, resultSet, schemaTableTreeStack);
        return Pair.of(e, labeledResult);
    }

    private static <E> E loadElements(SqlgGraph sqlgGraph, Multimap<String, Integer> columnMap, ResultSet resultSet, LinkedList<SchemaTableTree> schemaTableTreeStack) throws SQLException {
        SchemaTable schemaTable = schemaTableTreeStack.getLast().getSchemaTable();
        String idProperty = schemaTable.getSchema() + "." + schemaTable.getTable() + "." + SchemaManager.ID;
        Collection<Integer> propertyColumnsCounts = columnMap.get(idProperty);
        Integer columnCount = propertyColumnsCounts.iterator().next();
        Long id = resultSet.getLong(columnCount);
        //Need to be removed so as not to load it again
        propertyColumnsCounts.remove(columnCount);
        SqlgElement sqlgElement;
        if (schemaTable.isVertexTable()) {
            String rawLabel = schemaTable.getTable().substring(SchemaManager.VERTEX_PREFIX.length());
            sqlgElement = SqlgVertex.of(sqlgGraph, id, schemaTable.getSchema(), rawLabel);
        } else {
            String rawLabel = schemaTable.getTable().substring(SchemaManager.EDGE_PREFIX.length());
            sqlgElement = new SqlgEdge(sqlgGraph, id, schemaTable.getSchema(), rawLabel);
        }
        sqlgElement.loadResultSet(resultSet, schemaTableTreeStack.getLast());
        return (E) sqlgElement;
    }

    private static Multimap<String, Object> loadLabeledElements(SqlgGraph sqlgGraph, Multimap<String, Integer> columnMap, ResultSet resultSet, LinkedList<SchemaTableTree> schemaTableTreeStack) throws SQLException {
        Multimap<String, Object> result = ArrayListMultimap.create();
        for (SchemaTableTree schemaTableTree : schemaTableTreeStack) {
            if (!schemaTableTree.getLabels().isEmpty()) {
                String idProperty = schemaTableTree.labeledAliasId();
                Collection<Integer> propertyColumnsCounts = columnMap.get(idProperty);
                Integer columnCount = propertyColumnsCounts.iterator().next();
                Long id = resultSet.getLong(columnCount);
                //Need to be removed so as not to load it again
                propertyColumnsCounts.remove(columnCount);
                SqlgElement sqlgElement;
                String rawLabel = schemaTableTree.getSchemaTable().getTable().substring(SchemaManager.VERTEX_PREFIX.length());
                if (schemaTableTree.getSchemaTable().isVertexTable()) {
                    sqlgElement = SqlgVertex.of(sqlgGraph, id, schemaTableTree.getSchemaTable().getSchema(), rawLabel);
                } else {
                    sqlgElement = new SqlgEdge(sqlgGraph, id, schemaTableTree.getSchemaTable().getSchema(), rawLabel);
                }
                sqlgElement.loadLabeledResultSet(resultSet, columnMap, schemaTableTree);
                schemaTableTree.getLabels().forEach(l -> result.put(l, sqlgElement));
            }
        }
        return result;
    }

    public static void setParametersOnStatement(SqlgGraph sqlgGraph, LinkedList<SchemaTableTree> schemaTableTreeStack, Connection conn, PreparedStatement preparedStatement, int parameterIndex) throws SQLException {
        //start the index at 2 as sql starts at 1 and the first is the id that is already set.
//        int parameterIndex = 2;
        Multimap<String, Object> keyValueMap = LinkedListMultimap.create();
        for (SchemaTableTree schemaTableTree : schemaTableTreeStack) {
            for (HasContainer hasContainer : schemaTableTree.getHasContainers()) {
                WhereClause whereClause = WhereClause.from(hasContainer.getPredicate());
                whereClause.putKeyValueMap(hasContainer, keyValueMap);
            }
        }
        List<ImmutablePair<PropertyType, Object>> typeAndValues = SqlgUtil.transformToTypeAndValue(keyValueMap);
        //This is for selects
        setKeyValueAsParameter(sqlgGraph, false, parameterIndex, conn, preparedStatement, typeAndValues);
    }

    //This is called for inserts
    public static int setKeyValuesAsParameter(SqlgGraph sqlgGraph, int i, Connection conn, PreparedStatement preparedStatement, Map<String, Object> keyValues) throws SQLException {
        List<ImmutablePair<PropertyType, Object>> typeAndValues = SqlgUtil.transformToTypeAndValue(keyValues);
        i = setKeyValueAsParameter(sqlgGraph, true, i, conn, preparedStatement, typeAndValues);
        return i;
    }

    private static int setKeyValueAsParameter(SqlgGraph sqlgGraph, boolean mod, int parameterStartIndex, Connection conn, PreparedStatement preparedStatement, List<ImmutablePair<PropertyType, Object>> typeAndValues) throws SQLException {
        for (ImmutablePair<PropertyType, Object> pair : typeAndValues) {
            switch (pair.left) {
                case BOOLEAN:
                    preparedStatement.setBoolean(parameterStartIndex++, (Boolean) pair.right);
                    break;
                case BYTE:
                    preparedStatement.setByte(parameterStartIndex++, (Byte) pair.right);
                    break;
                case SHORT:
                    preparedStatement.setShort(parameterStartIndex++, (Short) pair.right);
                    break;
                case INTEGER:
                    preparedStatement.setInt(parameterStartIndex++, (Integer) pair.right);
                    break;
                case LONG:
                    preparedStatement.setLong(parameterStartIndex++, (Long) pair.right);
                    break;
                case FLOAT:
                    preparedStatement.setFloat(parameterStartIndex++, (Float) pair.right);
                    break;
                case DOUBLE:
                    preparedStatement.setDouble(parameterStartIndex++, (Double) pair.right);
                    break;
                case STRING:
                    preparedStatement.setString(parameterStartIndex++, (String) pair.right);
                    break;
                case LOCALDATE:
                    preparedStatement.setTimestamp(parameterStartIndex++, Timestamp.valueOf(((LocalDate) pair.right).atStartOfDay()));
                    break;
                case LOCALDATETIME:
                    preparedStatement.setTimestamp(parameterStartIndex++, Timestamp.valueOf(((LocalDateTime) pair.right)));
                    break;
                case ZONEDDATETIME:
                    if (sqlgGraph.getSqlDialect().needsTimeZone()) {
                        //This is for postgresql that adjust the timestamp to the server's timezone
                        preparedStatement.setTimestamp(parameterStartIndex++, Timestamp.valueOf(((ZonedDateTime) pair.right).toLocalDateTime()),
                                Calendar.getInstance(TimeZone.getTimeZone(((ZonedDateTime) pair.right).getZone().getId())));
                    } else {
                        preparedStatement.setTimestamp(parameterStartIndex++, Timestamp.valueOf(((ZonedDateTime) pair.right).toLocalDateTime()));
                    }
                    if (mod)
                        preparedStatement.setString(parameterStartIndex++, ((ZonedDateTime) pair.right).getZone().getId());
                    break;
                case LOCALTIME:
                    //looses nanos
                    preparedStatement.setTime(parameterStartIndex++, Time.valueOf((LocalTime) pair.right));
                    break;
                case PERIOD:
                    preparedStatement.setInt(parameterStartIndex++, ((Period) pair.right).getYears());
                    preparedStatement.setInt(parameterStartIndex++, ((Period) pair.right).getMonths());
                    preparedStatement.setInt(parameterStartIndex++, ((Period) pair.right).getYears());
                    break;
                case DURATION:
                    preparedStatement.setLong(parameterStartIndex++, ((Duration) pair.right).getSeconds());
                    preparedStatement.setInt(parameterStartIndex++, ((Duration) pair.right).getNano());
                    break;
                case JSON:
                    sqlgGraph.getSqlDialect().setJson(preparedStatement, parameterStartIndex, (JsonNode) pair.getRight());
                    parameterStartIndex++;
                    break;
                case POINT:
                    sqlgGraph.getSqlDialect().setPoint(preparedStatement, parameterStartIndex, pair.getRight());
                    parameterStartIndex++;
                    break;
                case BOOLEAN_ARRAY:
                    java.sql.Array booleanArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.BOOLEAN_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, booleanArray);
                    break;
                case BYTE_ARRAY:
                    preparedStatement.setBytes(parameterStartIndex++, (byte[]) pair.right);
                    break;
                case SHORT_ARRAY:
                    java.sql.Array shortArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.SHORT_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, shortArray);
                    break;
                case INTEGER_ARRAY:
                    java.sql.Array intArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.INTEGER_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, intArray);
                    break;
                case LONG_ARRAY:
                    java.sql.Array longArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.LONG_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, longArray);
                    break;
                case FLOAT_ARRAY:
                    java.sql.Array floatArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.FLOAT_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, floatArray);
                    break;
                case DOUBLE_ARRAY:
                    java.sql.Array doubleArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.DOUBLE_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, doubleArray);
                    break;
                case STRING_ARRAY:
                    java.sql.Array stringArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.STRING_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, stringArray);
                    break;
                default:
                    throw new IllegalStateException("Unhandled type " + pair.left.name());
            }
        }
        return parameterStartIndex;
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
                    id = (RecordId) value;
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
