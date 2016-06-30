package org.umlg.sqlg.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.sql.parse.WhereClause;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.strategy.TopologyStrategy;
import org.umlg.sqlg.structure.*;

import java.lang.reflect.Array;
import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * Date: 2014/07/12
 * Time: 3:13 PM
 */
public class SqlgUtil {

    //This is the default count to indicate whether to use in statement or join onto a temp table.
    //As it happens postgres join to temp is always faster except for count = 1 when in is not used but '='
    private final static int BULK_WITHIN_COUNT = 1;
    public static final String PROPERTY_ARRAY_VALUE_ELEMENTS_MAY_NOT_BE_NULL = "Property array value elements may not be null.";

    private SqlgUtil() {
    }

    public static List<Pair<SqlgElement, Map<String, Emit<SqlgElement>>>> loadResultSetIntoResultIterator(
            SqlgGraph sqlgGraph,
            ResultSetMetaData resultSetMetaData,
            ResultSet resultSet,
            SchemaTableTree rootSchemaTableTree,
            List<LinkedList<SchemaTableTree>> subQueryStacks,
            boolean first,
            Map<String, Integer> labeledColumnNameCountMap,
            Map<String, Integer> lastElementIdCountMap
    ) throws SQLException {

        List<Pair<SqlgElement, Map<String, Emit<SqlgElement>>>> result = new ArrayList<>();
        if (resultSet.next()) {
            if (first) {
                for (LinkedList<SchemaTableTree> subQueryStack : subQueryStacks) {
                    subQueryStack.getFirst().clearColumnNamePropertNameMap();
                }
                populateMetaDataMaps(resultSetMetaData, rootSchemaTableTree, labeledColumnNameCountMap, lastElementIdCountMap);
            }
            int subQueryDepth = 0;
            Map<String, Emit<SqlgElement>> previousLabeledElements = null;
            for (LinkedList<SchemaTableTree> subQueryStack : subQueryStacks) {

                Map<String, Emit<SqlgElement>> labeledElements = SqlgUtil.loadLabeledElements(
                        sqlgGraph, resultSet, subQueryStack, lastElementIdCountMap
                );
                if (previousLabeledElements == null) {
                    previousLabeledElements = labeledElements;
                } else {
                    previousLabeledElements.putAll(labeledElements);
                }
                //The last subQuery
                if (subQueryDepth == subQueryStacks.size() - 1) {
                    SchemaTableTree lastSchemaTableTree = subQueryStack.getLast();
                    Optional<SqlgElement> e = SqlgUtil.loadElement(
                            sqlgGraph, lastElementIdCountMap, resultSet, lastSchemaTableTree
                    );
                    if (e.isPresent()) {
                        result.add(Pair.of(e.get(), previousLabeledElements));
                        //This is for when times is before and emit after, in this case the element is emitted twice.
                        if (lastSchemaTableTree.getReplacedStepDepth() == lastSchemaTableTree.getStepDepth() && lastSchemaTableTree.isEmit() && lastSchemaTableTree.isUntilFirst()) {
                            result.add(Pair.of(e.get(), previousLabeledElements));
                        }
                    } else {
                        throw new IllegalStateException("Element e must be present, BUG!!");
                    }
                }
                subQueryDepth++;
            }
        }
        return result;
    }

    private static void populateMetaDataMaps(ResultSetMetaData resultSetMetaData, SchemaTableTree rootSchemaTableTree, Map<String, Integer> labeledColumnNameCountMap, Map<String, Integer> lastElementIdCountMap) throws SQLException {
        lastElementIdCountMap.clear();
        labeledColumnNameCountMap.clear();
        //First load all labeled entries from the resultSet
        //Translate the columns back from alias to meaningful column headings
        for (int columnCount = 1; columnCount <= resultSetMetaData.getColumnCount(); columnCount++) {
            String columnLabel = resultSetMetaData.getColumnLabel(columnCount);
            String unAliased = rootSchemaTableTree.getThreadLocalAliasColumnNameMap().get(columnLabel);
            String mapKey = unAliased != null ? unAliased : columnLabel;
            if (mapKey.endsWith(SchemaTableTree.ALIAS_SEPARATOR + SchemaManager.ID)) {
                lastElementIdCountMap.put(mapKey, columnCount);
            }
        }
    }

    /**
     * Loads all labeled or emitted elements.
     * For emitted elements the edge id to the element is also returned as that is needed in the traverser to calculate whether the element should be emitted or not.
     *
     * @param sqlgGraph
     * @param resultSet
     * @param subQueryStack
     * @return
     * @throws SQLException
     */
    private static <E extends SqlgElement> Map<String, Emit<E>> loadLabeledElements(
            SqlgGraph sqlgGraph,
            final ResultSet resultSet,
            LinkedList<SchemaTableTree> subQueryStack,
            Map<String, Integer> lastElementIdCountMap) throws SQLException {

        Map<String, Emit<E>> result = new TreeMap<>();
        for (SchemaTableTree schemaTableTree : subQueryStack) {
            if (!schemaTableTree.getLabels().isEmpty()) {
                String idProperty = schemaTableTree.labeledAliasId();
                Integer columnCount = lastElementIdCountMap.get(idProperty);
                Long id = resultSet.getLong(columnCount);
                if (!resultSet.wasNull()) {
                    SqlgElement sqlgElement;
                    if (schemaTableTree.getSchemaTable().isVertexTable()) {
                        String rawLabel = schemaTableTree.getSchemaTable().getTable().substring(SchemaManager.VERTEX_PREFIX.length());
                        sqlgElement = SqlgVertex.of(sqlgGraph, id, schemaTableTree.getSchemaTable().getSchema(), rawLabel);
                    } else {
                        String rawLabel = schemaTableTree.getSchemaTable().getTable().substring(SchemaManager.EDGE_PREFIX.length());
                        sqlgElement = new SqlgEdge(sqlgGraph, id, schemaTableTree.getSchemaTable().getSchema(), rawLabel);
                    }
                    schemaTableTree.loadProperty(resultSet, sqlgElement);
                    for (String label : schemaTableTree.getLabels()) {
                        result.put(
                                label,
                                new Emit<>(
                                        (E) sqlgElement,
                                        schemaTableTree.isOptionalLeftJoin()
                                )
                        );
                    }
                }
            }
        }
        return result;
    }

    private static <E> Optional<E> loadElement(
            SqlgGraph sqlgGraph,
            Map<String, Integer> columnMap,
            ResultSet resultSet,
            SchemaTableTree leafSchemaTableTree) throws SQLException {

        SchemaTable schemaTable = leafSchemaTableTree.getSchemaTable();
//        String idProperty = schemaTable.getSchema() + SchemaTableTree.ALIAS_SEPARATOR + schemaTable.getTable() + SchemaTableTree.ALIAS_SEPARATOR + SchemaManager.ID;
        String idProperty = leafSchemaTableTree.idProperty();
//        Collection<Integer> propertyColumnsCounts = columnMap.get(idProperty);
        Integer propertyColumnsCounts = columnMap.get(idProperty);
//        Integer columnCount = propertyColumnsCounts.iterator().next();
        Integer columnCount = propertyColumnsCounts;
        Long id = resultSet.getLong(columnCount);
        if (!resultSet.wasNull()) {
            //Need to be removed so as not to load it again
//            propertyColumnsCounts.remove(columnCount);
            SqlgElement sqlgElement;
            if (schemaTable.isVertexTable()) {
                String rawLabel = schemaTable.getTable().substring(SchemaManager.VERTEX_PREFIX.length());
                sqlgElement = SqlgVertex.of(sqlgGraph, id, schemaTable.getSchema(), rawLabel);
            } else {
                String rawLabel = schemaTable.getTable().substring(SchemaManager.EDGE_PREFIX.length());
                sqlgElement = new SqlgEdge(sqlgGraph, id, schemaTable.getSchema(), rawLabel);
            }
            leafSchemaTableTree.loadProperty(resultSet, sqlgElement);
//            sqlgElement.loadResultSet(resultSet, columnNameCountMap, leafSchemaTableTree);
            return Optional.of((E) sqlgElement);
        } else {
            return Optional.empty();
        }
    }

//    /**
//     * Loads all labeled or emitted elements.
//     * For emitted elements the edge id to the element is also returned as that is needed in the traverser to calculate whether the element should be emitted or not.
//     *
//     * @param sqlgGraph
//     * @param columnNameCountMap
//     * @param resultSet
//     * @param schemaTableTreeStack
//     * @return
//     * @throws SQLException
//     */
//    private static <E extends SqlgElement> Multimap<String, Emit<E>> loadLabeledElements(
//            SqlgGraph sqlgGraph, Multimap<String, Integer> columnNameCountMap,
//            ResultSet resultSet, LinkedList<SchemaTableTree> schemaTableTreeStack) throws SQLException {
//
//        Multimap<String, Emit<E>> result = ArrayListMultimap.create();
//        for (SchemaTableTree schemaTableTree : schemaTableTreeStack) {
//            if (!schemaTableTree.getLabels().isEmpty()) {
//                String idProperty = schemaTableTree.labeledAliasId();
//                Collection<Integer> propertyColumnsCounts = columnNameCountMap.get(idProperty);
//                Integer columnCount = propertyColumnsCounts.iterator().next();
//                Long id = resultSet.getLong(columnCount);
//                if (!resultSet.wasNull()) {
//                    //Need to be removed so as not to load it again
//                    propertyColumnsCounts.remove(columnCount);
//                    SqlgElement sqlgElement;
//                    String rawLabel = schemaTableTree.getSchemaTable().getTable().substring(SchemaManager.VERTEX_PREFIX.length());
//                    if (schemaTableTree.getSchemaTable().isVertexTable()) {
//                        sqlgElement = SqlgVertex.of(sqlgGraph, id, schemaTableTree.getSchemaTable().getSchema(), rawLabel);
//                    } else {
//                        sqlgElement = new SqlgEdge(sqlgGraph, id, schemaTableTree.getSchemaTable().getSchema(), rawLabel);
//                    }
//                    sqlgElement.loadLabeledResultSet(resultSet, columnNameCountMap, schemaTableTree);
//                    schemaTableTree.getLabels().forEach(
//                            label -> result.put(
//                                    label,
//                                    new Emit<>(
//                                            (E) sqlgElement,
//                                            schemaTableTree.isOptionalLeftJoin()
//                                    )
//                            )
//                    );
//                }
//            }
//        }
//        return result;
//    }


    public static boolean isBulkWithinAndOut(SqlgGraph sqlgGraph, HasContainer hasContainer) {
        BiPredicate p = hasContainer.getPredicate().getBiPredicate();
        return (p == Contains.within || p == Contains.without) && ((Collection) hasContainer.getPredicate().getValue()).size() > sqlgGraph.configuration().getInt("bulk.within.count", BULK_WITHIN_COUNT);
    }

    public static boolean isBulkWithin(SqlgGraph sqlgGraph, HasContainer hasContainer) {
        BiPredicate p = hasContainer.getPredicate().getBiPredicate();
        return p == Contains.within && ((Collection) hasContainer.getPredicate().getValue()).size() > sqlgGraph.configuration().getInt("bulk.within.count", BULK_WITHIN_COUNT);
    }

    public static void setParametersOnStatement(SqlgGraph sqlgGraph, LinkedList<SchemaTableTree> schemaTableTreeStack, Connection conn, PreparedStatement preparedStatement, int parameterIndex) throws SQLException {
        Multimap<String, Object> keyValueMap = LinkedListMultimap.create();
        for (SchemaTableTree schemaTableTree : schemaTableTreeStack) {
            for (HasContainer hasContainer : schemaTableTree.getHasContainers()) {
                if (!sqlgGraph.getSqlDialect().supportsBulkWithinOut() || !isBulkWithinAndOut(sqlgGraph, hasContainer)) {
                    WhereClause whereClause = WhereClause.from(hasContainer.getPredicate());
                    whereClause.putKeyValueMap(hasContainer, keyValueMap);
                }
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
                        preparedStatement.setTimestamp(
                                parameterStartIndex++,
                                Timestamp.valueOf(((ZonedDateTime) pair.right).toLocalDateTime()),
                                Calendar.getInstance(TimeZone.getTimeZone(((ZonedDateTime) pair.right).getZone().getId()))
                        );
                    } else {
                        preparedStatement.setTimestamp(
                                parameterStartIndex++,
                                Timestamp.valueOf(((ZonedDateTime) pair.right).toLocalDateTime())
                        );
                    }
                    if (mod)
                        preparedStatement.setString(parameterStartIndex++, ((ZonedDateTime) pair.right).getZone().getId());
                    break;
                case LOCALTIME:
                    //looses nano seconds
                    preparedStatement.setTime(parameterStartIndex++, Time.valueOf((LocalTime) pair.right));
                    break;
                case PERIOD:
                    preparedStatement.setInt(parameterStartIndex++, ((Period) pair.right).getYears());
                    preparedStatement.setInt(parameterStartIndex++, ((Period) pair.right).getMonths());
                    preparedStatement.setInt(parameterStartIndex++, ((Period) pair.right).getDays());
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
                case POLYGON:
                    sqlgGraph.getSqlDialect().setPolygon(preparedStatement, parameterStartIndex, pair.getRight());
                    parameterStartIndex++;
                    break;
                case GEOGRAPHY_POINT:
                    sqlgGraph.getSqlDialect().setPoint(preparedStatement, parameterStartIndex, pair.getRight());
                    parameterStartIndex++;
                    break;
                case GEOGRAPHY_POLYGON:
                    sqlgGraph.getSqlDialect().setPolygon(preparedStatement, parameterStartIndex, pair.getRight());
                    parameterStartIndex++;
                    break;
                case BOOLEAN_ARRAY:
                    java.sql.Array booleanArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.BOOLEAN_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, booleanArray);
                    break;
                case boolean_ARRAY:
                    booleanArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.boolean_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, booleanArray);
                    break;
                case BYTE_ARRAY:
                    byte[] byteArray = SqlgUtil.convertObjectArrayToBytePrimitiveArray((Object[]) pair.getRight());
                    preparedStatement.setBytes(parameterStartIndex++, byteArray);
                    break;
                case byte_ARRAY:
                    preparedStatement.setBytes(parameterStartIndex++, (byte[]) pair.right);
                    break;
                case SHORT_ARRAY:
                    java.sql.Array shortArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.SHORT_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, shortArray);
                    break;
                case short_ARRAY:
                    shortArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.short_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, shortArray);
                    break;
                case INTEGER_ARRAY:
                    java.sql.Array intArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.INTEGER_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, intArray);
                    break;
                case int_ARRAY:
                    intArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.int_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, intArray);
                    break;
                case LONG_ARRAY:
                    java.sql.Array longArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.LONG_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, longArray);
                    break;
                case long_ARRAY:
                    longArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.long_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, longArray);
                    break;
                case FLOAT_ARRAY:
                    java.sql.Array floatArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.FLOAT_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, floatArray);
                    break;
                case float_ARRAY:
                    floatArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.float_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, floatArray);
                    break;
                case DOUBLE_ARRAY:
                    java.sql.Array doubleArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.DOUBLE_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, doubleArray);
                    break;
                case double_ARRAY:
                    doubleArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.double_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, doubleArray);
                    break;
                case STRING_ARRAY:
                    java.sql.Array stringArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.STRING_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, stringArray);
                    break;
                case LOCALDATETIME_ARRAY:
                    java.sql.Array localDateTimeArray = sqlgGraph.getSqlDialect().createArrayOf(conn, PropertyType.LOCALDATETIME_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, localDateTimeArray);
                    break;
                case LOCALDATE_ARRAY:
                    java.sql.Array localDateArray = sqlgGraph.getSqlDialect().createArrayOf(conn, PropertyType.LOCALDATE_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, localDateArray);
                    break;
                case LOCALTIME_ARRAY:
                    java.sql.Array localTimeArray = sqlgGraph.getSqlDialect().createArrayOf(conn, PropertyType.LOCALTIME_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, localTimeArray);
                    break;
                case ZONEDDATETIME_ARRAY:
                    java.sql.Array zonedDateTimeArray = sqlgGraph.getSqlDialect().createArrayOf(conn, PropertyType.ZONEDDATETIME_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, zonedDateTimeArray);
                    if (mod) {
                        List<String> zones = (Arrays.asList((ZonedDateTime[]) pair.right)).stream().map(z -> z.getZone().getId()).collect(Collectors.toList());
                        java.sql.Array zonedArray = sqlgGraph.getSqlDialect().createArrayOf(conn, PropertyType.STRING_ARRAY, SqlgUtil.transformArrayToInsertValue(PropertyType.STRING_ARRAY, zones.toArray()));
                        preparedStatement.setArray(parameterStartIndex++, zonedArray);
                    }
                    break;
                case DURATION_ARRAY:
                    Duration[] durations = (Duration[]) pair.getRight();
                    List<Long> seconds = Arrays.stream(durations).map(d -> d.getSeconds()).collect(Collectors.toList());
                    List<Integer> nanos = Arrays.stream(durations).map(d -> d.getNano()).collect(Collectors.toList());
                    java.sql.Array secondsArray = sqlgGraph.getSqlDialect().createArrayOf(conn, PropertyType.long_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, seconds.toArray()));
                    preparedStatement.setArray(parameterStartIndex++, secondsArray);
                    java.sql.Array nanoArray = sqlgGraph.getSqlDialect().createArrayOf(conn, PropertyType.int_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, nanos.toArray()));
                    preparedStatement.setArray(parameterStartIndex++, nanoArray);
                    break;
                case PERIOD_ARRAY:
                    Period[] periods = (Period[]) pair.getRight();
                    List<Integer> years = Arrays.stream(periods).map(d -> d.getYears()).collect(Collectors.toList());
                    List<Integer> months = Arrays.stream(periods).map(d -> d.getMonths()).collect(Collectors.toList());
                    List<Integer> days = Arrays.stream(periods).map(d -> d.getDays()).collect(Collectors.toList());
                    java.sql.Array yearsArray = sqlgGraph.getSqlDialect().createArrayOf(conn, PropertyType.int_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, years.toArray()));
                    preparedStatement.setArray(parameterStartIndex++, yearsArray);
                    java.sql.Array monthsArray = sqlgGraph.getSqlDialect().createArrayOf(conn, PropertyType.int_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, months.toArray()));
                    preparedStatement.setArray(parameterStartIndex++, monthsArray);
                    java.sql.Array daysArray = sqlgGraph.getSqlDialect().createArrayOf(conn, PropertyType.int_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, days.toArray()));
                    preparedStatement.setArray(parameterStartIndex++, daysArray);
                    break;
                case JSON_ARRAY:
                    JsonNode[] objectNodes = (JsonNode[]) pair.getRight();
                    java.sql.Array objectNodeArray = sqlgGraph.getSqlDialect().createArrayOf(conn, PropertyType.JSON_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, objectNodes));
                    preparedStatement.setArray(parameterStartIndex++, objectNodeArray);
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

    public static SchemaTable parseLabelMaybeNoSchema(SqlgGraph sqlgGraph, final String label) {
        Objects.requireNonNull(label, "label may not be null!");
        String[] schemaLabel = label.split("\\.");
        if (schemaLabel.length == 2) {
            return SchemaTable.of(schemaLabel[0], schemaLabel[1]);
        } else if (schemaLabel.length == 1) {
            return SchemaTable.of(sqlgGraph.getSqlDialect().getPublicSchema(), schemaLabel[0]);
        } else {
            throw new IllegalStateException("label must be if the format 'schema.table' or just 'table'");
        }
    }

    public static Object[] mapTokeyValues(Map<Object, Object> keyValues) {
        Object[] result = new Object[keyValues.size() * 2];
        int i = 0;
        for (Map.Entry<Object, Object> entry : keyValues.entrySet()) {
            result[i++] = entry.getKey();
            result[i++] = entry.getValue();
        }
        return result;
    }

    public static Object[] mapToStringKeyValues(Map<String, Object> keyValues) {
        Object[] result = new Object[keyValues.size() * 2];
        int i = 0;
        for (Map.Entry<String, Object> entry : keyValues.entrySet()) {
            result[i++] = entry.getKey();
            result[i++] = entry.getValue();
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
        Map<String, Object> result = new LinkedHashMap<>();
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
        for (Map.Entry<String, Object> entry : keyValues.entrySet()) {
            Object value = entry.getValue();
            //value
            //skip the label as that is not a property but the table
            if (entry.getKey().equals(T.label)) {
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
        return getArray(propertyType, value);
    }

    private static Object[] getArray(PropertyType propertyType, Object val) {
        int arrlength = Array.getLength(val);
        Object[] outputArray = new Object[arrlength];
        for (int i = 0; i < arrlength; ++i) {
            switch (propertyType) {
                case LOCALDATETIME_ARRAY:
                    outputArray[i] = Timestamp.valueOf((LocalDateTime) Array.get(val, i));
                    break;
                case LOCALDATE_ARRAY:
                    outputArray[i] = Timestamp.valueOf(((LocalDate) Array.get(val, i)).atStartOfDay());
                    break;
                case LOCALTIME_ARRAY:
                    outputArray[i] = Time.valueOf(((LocalTime) Array.get(val, i)));
                    break;
                case ZONEDDATETIME_ARRAY:
                    ZonedDateTime zonedDateTime = (ZonedDateTime) Array.get(val, i);
                    outputArray[i] = Timestamp.valueOf(zonedDateTime.toLocalDateTime());
                    break;
                case BYTE_ARRAY:
                    Byte aByte = (Byte) Array.get(val, i);
                    outputArray[i] = aByte.byteValue();
                    break;
                default:
                    outputArray[i] = Array.get(val, i);
            }
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

    public static Map<String, Map<String, PropertyType>> filterHasContainers(SchemaManager schemaManager, List<HasContainer> hasContainers) {
        HasContainer fromHasContainer = null;
        HasContainer withoutHasContainer = null;

        for (HasContainer hasContainer : hasContainers) {
            if (hasContainer.getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_FROM)) {
                fromHasContainer = hasContainer;
                break;
            } else if (hasContainer.getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_WITHOUT)) {
                withoutHasContainer = hasContainer;
                break;
            }
        }

        //from and without are mutually exclusive, only one will ever be set.
        Map<String, Map<String, PropertyType>> filteredAllTables;
        if (fromHasContainer != null) {
            filteredAllTables = schemaManager.getAllTablesFrom((List<String>) fromHasContainer.getPredicate().getValue());
        } else if (withoutHasContainer != null) {
            filteredAllTables = schemaManager.getAllTablesWithout((List<String>) withoutHasContainer.getPredicate().getValue());
        } else {
            filteredAllTables = schemaManager.getAllTables();
        }
        return filteredAllTables;
    }

    public static void removeTopologyStrategyHasContainer(List<HasContainer> hasContainers) {
        //remove the TopologyStrategy hasContainer
        Optional<HasContainer> fromHascontainer = hasContainers.stream().filter(h -> h.getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_FROM)).findAny();
        Optional<HasContainer> withoutHascontainer = hasContainers.stream().filter(h -> h.getKey().equals(TopologyStrategy.TOPOLOGY_SELECTION_WITHOUT)).findAny();
        if (fromHascontainer.isPresent()) {
            hasContainers.remove(fromHascontainer.get());
        }
        if (withoutHascontainer.isPresent()) {
            hasContainers.remove(withoutHascontainer.get());
        }
    }

    public static void calculateLeftJoins(List<LinkedList<SchemaTableTree>> distinctQueries) {
        Set<List<SchemaTable>> lastSchemaTableTrees = new HashSet<>();
        for (LinkedList<SchemaTableTree> distinctQuery : distinctQueries) {

            List<SchemaTable> list = new ArrayList<>();
            for (SchemaTableTree schemaTableTree : distinctQuery) {

                if (!list.isEmpty()) {
                    list = new ArrayList<>(list);
                }
                list.add(schemaTableTree.getSchemaTable());

                if (!lastSchemaTableTrees.contains(list)) {

                    lastSchemaTableTrees.add(list);

                } else {
                    if (schemaTableTree.isOptionalLeftJoin()) {
                        schemaTableTree.setOptionalLeftJoin(false);
                    }
                }

            }
        }

    }

    public static void dropDb(SqlgGraph sqlgGraph) {
        try {
            SqlDialect sqlDialect = sqlgGraph.getSqlDialect();
            Connection conn = sqlgGraph.tx().getConnection();
            DatabaseMetaData metadata = conn.getMetaData();
            if (sqlDialect.supportsCascade()) {
                String catalog = null;
                String schemaPattern = null;
                String tableNamePattern = "%";
                String[] types = {"TABLE"};
                ResultSet result = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
                while (result.next()) {
                    String schema = result.getString(2);
                    String table = result.getString(3);
                    if (sqlDialect.getGisSchemas().contains(schema) || sqlDialect.getSpacialRefTable().contains(table)) {
                        continue;
                    }
                    StringBuilder sql = new StringBuilder("DROP TABLE ");
                    sql.append(sqlDialect.maybeWrapInQoutes(schema));
                    sql.append(".");
                    sql.append(sqlDialect.maybeWrapInQoutes(table));
                    sql.append(" CASCADE");
                    if (sqlDialect.needsSemicolon()) {
                        sql.append(";");
                    }
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                        preparedStatement.executeUpdate();
                    }
                }
                catalog = null;
                schemaPattern = null;
                result = metadata.getSchemas(catalog, schemaPattern);
                while (result.next()) {
                    String schema = result.getString(1);
                    if (!sqlDialect.getDefaultSchemas().contains(schema)) {
                        StringBuilder sql = new StringBuilder("DROP SCHEMA ");
                        sql.append(sqlDialect.maybeWrapInQoutes(schema));
                        sql.append(" CASCADE");
                        if (sqlDialect.needsSemicolon()) {
                            sql.append(";");
                        }
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                            preparedStatement.executeUpdate();
                        }
                    }
                }
            } else if (!sqlDialect.supportSchemas()) {
                ResultSet result = metadata.getCatalogs();
                while (result.next()) {
                    StringBuilder sql = new StringBuilder("DROP DATABASE ");
                    String database = result.getString(1);
                    if (!sqlDialect.getDefaultSchemas().contains(database)) {
                        sql.append(sqlDialect.maybeWrapInQoutes(database));
                        if (sqlDialect.needsSemicolon()) {
                            sql.append(";");
                        }
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                            preparedStatement.executeUpdate();
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Byte[] convertPrimitiveByteArrayToByteArray(byte[] value) {
        Byte[] target = new Byte[value.length];
        for (int i = 0; i < value.length; i++) {
            Array.set(target, i, value[i]);
        }
        return target;
    }

    public static Object convertByteArrayToPrimitiveArray(Byte[] value) {
        byte[] target = new byte[value.length];
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException(PROPERTY_ARRAY_VALUE_ELEMENTS_MAY_NOT_BE_NULL);
            }
            Array.set(target, i, value[i].byteValue());
        }
        return target;
    }

    public static byte[] convertObjectArrayToBytePrimitiveArray(Object[] byteArray) {
        byte[] target = new byte[byteArray.length];
        for (int i = 0; i < byteArray.length; i++) {
            Array.set(target, i, byteArray[i]);
        }
        return target;
    }

    public static Boolean[] convertObjectArrayToBooleanArray(Object[] booleanArray) {
        Boolean[] target = new Boolean[booleanArray.length];
        for (int i = 0; i < booleanArray.length; i++) {
            Array.set(target, i, booleanArray[i]);
        }
        return target;
    }


    public static boolean[] convertObjectArrayToBooleanPrimitiveArray(Object[] booleanArray) {
        boolean[] target = new boolean[booleanArray.length];
        for (int i = 0; i < booleanArray.length; i++) {
            Array.set(target, i, booleanArray[i]);
        }
        return target;
    }

    public static Short[] convertObjectOfIntegersArrayToShortArray(Object[] shortArray) {
        Short[] target = new Short[shortArray.length];
        for (int i = 0; i < shortArray.length; i++) {
            Array.set(target, i, Short.valueOf(((Integer) shortArray[i]).shortValue()));
        }
        return target;
    }

    public static short[] convertObjectOfIntegersArrayToShortPrimitiveArray(Object[] shortArray) {
        short[] target = new short[shortArray.length];
        for (int i = 0; i < shortArray.length; i++) {
            Array.set(target, i, ((Integer) shortArray[i]).shortValue());
        }
        return target;
    }

    public static Integer[] convertObjectOfIntegersArrayToIntegerArray(Object[] integerArray) {
        Integer[] target = new Integer[integerArray.length];
        for (int i = 0; i < integerArray.length; i++) {
            Array.set(target, i, integerArray[i]);
        }
        return target;
    }

    public static int[] convertObjectOfIntegersArrayToIntegerPrimitiveArray(Object[] integerArray) {
        int[] target = new int[integerArray.length];
        for (int i = 0; i < integerArray.length; i++) {
            Array.set(target, i, integerArray[i]);
        }
        return target;
    }

    public static long[] convertObjectOfLongsArrayToLongPrimitiveArray(Object[] longArray) {
        long[] target = new long[longArray.length];
        for (int i = 0; i < longArray.length; i++) {
            Array.set(target, i, longArray[i]);
        }
        return target;
    }

    public static double[] convertObjectOfDoublesArrayToDoublePrimitiveArray(Object[] doubleArray) {
        double[] target = new double[doubleArray.length];
        for (int i = 0; i < doubleArray.length; i++) {
            Array.set(target, i, doubleArray[i]);
        }
        return target;
    }

    public static Long[] convertObjectOfLongsArrayToLongArray(Object[] longArray) {
        Long[] target = new Long[longArray.length];
        for (int i = 0; i < longArray.length; i++) {
            Array.set(target, i, longArray[i]);
        }
        return target;
    }

    public static Double[] convertObjectOfDoublesArrayToDoubleArray(Object[] doubleArray) {
        Double[] target = new Double[doubleArray.length];
        for (int i = 0; i < doubleArray.length; i++) {
            Array.set(target, i, doubleArray[i]);
        }
        return target;
    }

    public static String[] convertObjectOfStringsArrayToStringArray(Object[] stringArray) {
        String[] target = new String[stringArray.length];
        for (int i = 0; i < stringArray.length; i++) {
            Array.set(target, i, stringArray[i]);
        }
        return target;
    }

    public static float[] convertFloatArrayToPrimitiveFloat(Float[] floatArray) {
        float[] target = new float[floatArray.length];
        for (int i = 0; i < floatArray.length; i++) {
            Array.set(target, i, floatArray[i].floatValue());
        }
        return target;
    }

    public static <T> T copyToLocalDateTime(Timestamp[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException(PROPERTY_ARRAY_VALUE_ELEMENTS_MAY_NOT_BE_NULL);
            }
            Array.set(target, i, value[i].toLocalDateTime());
        }
        return target;
    }

    public static <T> T copyObjectArrayOfTimestampToLocalDateTime(Object[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException(PROPERTY_ARRAY_VALUE_ELEMENTS_MAY_NOT_BE_NULL);
            }
            Array.set(target, i, ((Timestamp) value[i]).toLocalDateTime());
        }
        return target;
    }

    public static <T> T copyToLocalDate(Date[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException(PROPERTY_ARRAY_VALUE_ELEMENTS_MAY_NOT_BE_NULL);
            }
            Array.set(target, i, value[i].toLocalDate());
        }
        return target;
    }

    public static <T> T copyObjectArrayOfDateToLocalDate(Object[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException(PROPERTY_ARRAY_VALUE_ELEMENTS_MAY_NOT_BE_NULL);
            }
            Array.set(target, i, ((Date) value[i]).toLocalDate());
        }
        return target;
    }

    private static <T> T copyToLocalDate(Object[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException(PROPERTY_ARRAY_VALUE_ELEMENTS_MAY_NOT_BE_NULL);
            }
            Array.set(target, i, ((Date) value[i]).toLocalDate());
        }
        return target;
    }

    public static <T> T copyToLocalTime(Time[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException(PROPERTY_ARRAY_VALUE_ELEMENTS_MAY_NOT_BE_NULL);
            }
            Array.set(target, i, (value[i]).toLocalTime());
        }
        return target;
    }

    public static <T> T copyObjectArrayOfTimeToLocalTime(Object[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException(PROPERTY_ARRAY_VALUE_ELEMENTS_MAY_NOT_BE_NULL);
            }
            Array.set(target, i, ((Time) value[i]).toLocalTime());
        }
        return target;
    }

}
