package org.umlg.sqlg.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.predicate.PropertyReference;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.sql.parse.AndOrHasContainer;
import org.umlg.sqlg.sql.parse.ColumnList;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.sql.parse.WhereClause;
import org.umlg.sqlg.strategy.BaseStrategy;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.structure.topology.Topology;

import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;

import static org.apache.tinkerpop.gremlin.structure.T.label;
import static org.umlg.sqlg.structure.PropertyType.*;
import static org.umlg.sqlg.structure.topology.Topology.EDGE_PREFIX;
import static org.umlg.sqlg.structure.topology.Topology.VERTEX_PREFIX;

/**
 * Date: 2014/07/12
 * Time: 3:13 PM
 */
@SuppressWarnings("Duplicates")
public class SqlgUtil {

    private static final Logger logger = LoggerFactory.getLogger(SqlgUtil.class);

    //This is the default count to indicate whether to use in statement or join onto a temp table.
    //As it happens postgres join to temp is always faster except for count = 1 when in is not used but '='
    private final static int BULK_WITHIN_COUNT = 1;
    private static final String PROPERTY_ARRAY_VALUE_ELEMENTS_MAY_NOT_BE_NULL = "Property array value elements may not be null.";

    private SqlgUtil() {
    }

    public static List<Emit<SqlgElement>> loadResultSetIntoResultIterator(
            SqlgGraph sqlgGraph,
            ResultSetMetaData resultSetMetaData,
            ResultSet resultSet,
            SchemaTableTree rootSchemaTableTree,
            List<LinkedList<SchemaTableTree>> subQueryStacks,
            boolean first,
            Map<String, Integer> lastElementIdCountMap

    ) throws SQLException {
        return loadResultSetIntoResultIterator(sqlgGraph, resultSetMetaData, resultSet, rootSchemaTableTree, subQueryStacks, first, lastElementIdCountMap, false);
    }

    /**
     * @param sqlgGraph
     * @param resultSetMetaData
     * @param resultSet
     * @param rootSchemaTableTree
     * @param subQueryStacks
     * @param first
     * @param idColumnCountMap
     * @param forParent           Indicates that the gremlin query is for SqlgVertexStep. It is in the context of an incoming traverser, the parent.
     * @return A list of @{@link Emit}s that represent a single @{@link org.apache.tinkerpop.gremlin.process.traversal.Path}
     * @throws SQLException
     */
    public static List<Emit<SqlgElement>> loadResultSetIntoResultIterator(
            SqlgGraph sqlgGraph,
            ResultSetMetaData resultSetMetaData,
            ResultSet resultSet,
            SchemaTableTree rootSchemaTableTree,
            List<LinkedList<SchemaTableTree>> subQueryStacks,
            boolean first,
            Map<String, Integer> idColumnCountMap,
            boolean forParent
    ) throws SQLException {

        List<Emit<SqlgElement>> result = new ArrayList<>();
        if (resultSet.next()) {
            if (first) {
                for (LinkedList<SchemaTableTree> subQueryStack : subQueryStacks) {
                    for (SchemaTableTree schemaTableTree : subQueryStack) {
                        schemaTableTree.clearColumnNamePropertyNameMap();
                    }
                }
                populateIdCountMap(resultSetMetaData, rootSchemaTableTree, idColumnCountMap);
            }
            int subQueryDepth = 1;
            for (LinkedList<SchemaTableTree> subQueryStack : subQueryStacks) {

                List<Emit<SqlgElement>> labeledElements = SqlgUtil.loadLabeledElements(
                        sqlgGraph,
                        resultSet,
                        subQueryStack,
                        subQueryDepth == subQueryStacks.size(),
                        idColumnCountMap,
                        forParent
//                        subQueryStacks.get(subQueryStacks.size() - 1).getLast().hasAggregateFunction()
                );
                result.addAll(labeledElements);
                if (subQueryDepth == subQueryStacks.size()) {
                    SchemaTableTree lastSchemaTableTree = subQueryStack.getLast();
                    Preconditions.checkState(!labeledElements.isEmpty());
                    if (lastSchemaTableTree.getReplacedStepDepth() == lastSchemaTableTree.getStepDepth() &&
                            lastSchemaTableTree.isEmit() &&
                            lastSchemaTableTree.isUntilFirst()) {

                        Emit<SqlgElement> repeatEmit = labeledElements.get(labeledElements.size() - 1);
                        repeatEmit.setRepeat(true);
                    }
                }
                subQueryDepth++;
            }
        }
        return result;
    }

    //TODO the identifier logic here is very unoptimal
    private static void populateIdCountMap(ResultSetMetaData resultSetMetaData, SchemaTableTree rootSchemaTableTree, Map<String, Integer> lastElementIdCountMap) throws SQLException {
        lastElementIdCountMap.clear();
        //First load all labeled entries from the resultSet
        //Translate the columns back from alias to meaningful column headings
        Set<String> identifiers = new HashSet<>();
        Set<String> allIdentifiers = rootSchemaTableTree.getAllIdentifiers();
        for (String allIdentifier : allIdentifiers) {
            identifiers.add(SchemaTableTree.ALIAS_SEPARATOR + allIdentifier);
        }
        for (int columnCount = 1; columnCount <= resultSetMetaData.getColumnCount(); columnCount++) {
            String columnLabel = resultSetMetaData.getColumnLabel(columnCount);
            String unAliased = rootSchemaTableTree.getAliasColumnNameMap().get(columnLabel);
            String mapKey = unAliased != null ? unAliased : columnLabel;
            if (mapKey.endsWith(SchemaTableTree.ALIAS_SEPARATOR + Topology.ID) || identifiers.stream().anyMatch(mapKey::endsWith)) {
                lastElementIdCountMap.put(mapKey, columnCount);
            }
        }
    }

    /**
     * Loads all labeled or emitted elements.
     *
     * @param sqlgGraph
     * @param resultSet
     * @param subQueryStack
     * @param lastQueryStack
     * @param idColumnCountMap
     * @return
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    private static <E extends SqlgElement> List<Emit<E>> loadLabeledElements(
            SqlgGraph sqlgGraph,
            final ResultSet resultSet,
            LinkedList<SchemaTableTree> subQueryStack,
            boolean lastQueryStack,
            Map<String, Integer> idColumnCountMap,
            boolean forParent
    ) throws SQLException {

        List<Emit<E>> result = new ArrayList<>();
        int count = 1;
        for (SchemaTableTree schemaTableTree : subQueryStack) {
            if (!schemaTableTree.getLabels().isEmpty()) {
                E sqlgElement = null;
                boolean resultSetWasNull = false;
                Long id = -1L;
                if (schemaTableTree.isHasIDPrimaryKey()) {
                    //aggregate queries have no ID
                    if (!schemaTableTree.hasAggregateFunction()) {
                        String idProperty = schemaTableTree.labeledAliasId();
                        Integer columnCount = idColumnCountMap.get(idProperty);
                        id = resultSet.getLong(columnCount);
                        resultSetWasNull = resultSet.wasNull();
                    }
                    if (!resultSetWasNull) {
                        if (schemaTableTree.getSchemaTable().isVertexTable()) {
                            String rawLabel = schemaTableTree.getSchemaTable().getTable().substring(VERTEX_PREFIX.length());
                            sqlgElement = (E) SqlgVertex.of(sqlgGraph, id, schemaTableTree.getSchemaTable().getSchema(), rawLabel);
                            schemaTableTree.loadProperty(resultSet, sqlgElement);
                        } else {
                            String rawLabel = schemaTableTree.getSchemaTable().getTable().substring(EDGE_PREFIX.length());
                            sqlgElement = (E) new SqlgEdge(sqlgGraph, id, schemaTableTree.getSchemaTable().getSchema(), rawLabel);
                            schemaTableTree.loadProperty(resultSet, sqlgElement);
                            schemaTableTree.loadEdgeInOutVertices(resultSet, (SqlgEdge) sqlgElement);
                        }
                    }
                } else {
                    List<Comparable> identifierObjects = List.of(-1);
                    //the idColumnCountMap can be empty on aggregation queries where the identifier fields are removed
                    // when building the outer select statement
                    if (!schemaTableTree.hasAggregateFunction() && !idColumnCountMap.isEmpty()) {
                        identifierObjects = schemaTableTree.loadIdentifierObjects(idColumnCountMap, resultSet);
                        resultSetWasNull = resultSet.wasNull();
                    }
                    if (!resultSetWasNull) {
                        if (schemaTableTree.getSchemaTable().isVertexTable()) {
                            String rawLabel = schemaTableTree.getSchemaTable().getTable().substring(VERTEX_PREFIX.length());
                            sqlgElement = (E) SqlgVertex.of(sqlgGraph, identifierObjects, schemaTableTree.getSchemaTable().getSchema(), rawLabel);
                            schemaTableTree.loadProperty(resultSet, sqlgElement);
                        } else {
                            String rawLabel = schemaTableTree.getSchemaTable().getTable().substring(EDGE_PREFIX.length());
                            sqlgElement = (E) new SqlgEdge(sqlgGraph, identifierObjects, schemaTableTree.getSchemaTable().getSchema(), rawLabel);
                            schemaTableTree.loadProperty(resultSet, sqlgElement);
                            schemaTableTree.loadEdgeInOutVertices(resultSet, (SqlgEdge) sqlgElement);
                        }
                    }
                }
                if (!resultSetWasNull) {
                    //The following if statement is for for "repeat(traversal()).emit().as('label')"
                    //i.e. for emit queries with labels
                    //Only the last node in the subQueryStacks' subQueryStack must get the labels as the label only apply to the exiting element that gets emitted.
                    //Elements that come before the last element in the path must not get the labels.
                    Emit<E> emit;
                    if (schemaTableTree.isEmit() && !lastQueryStack) {
                        if (forParent) {
                            //1 is the parentIndex. This is the id of the incoming parent.
                            emit = new Emit<>(resultSet.getLong(1), sqlgElement, Collections.emptySet(), schemaTableTree.getStepDepth(), schemaTableTree.getSqlgComparatorHolder());
                        } else {
                            emit = new Emit<>(sqlgElement, Collections.emptySet(), schemaTableTree.getStepDepth(), schemaTableTree.getSqlgComparatorHolder());
                        }
                    } else if (schemaTableTree.isEmit() && lastQueryStack && (count != subQueryStack.size())) {
                        if (forParent) {
                            emit = new Emit<>(resultSet.getLong(1), sqlgElement, Collections.emptySet(), schemaTableTree.getStepDepth(), schemaTableTree.getSqlgComparatorHolder());
                        } else {
                            emit = new Emit<>(sqlgElement, Collections.emptySet(), schemaTableTree.getStepDepth(), schemaTableTree.getSqlgComparatorHolder());
                        }
                    } else {
                        if (forParent) {
                            emit = new Emit<>(resultSet.getLong(1), sqlgElement, schemaTableTree.getRealLabels(), schemaTableTree.getStepDepth(), schemaTableTree.getSqlgComparatorHolder());
                        } else {
                            emit = new Emit<>(sqlgElement, schemaTableTree.getRealLabels(), schemaTableTree.getStepDepth(), schemaTableTree.getSqlgComparatorHolder());
                        }
                    }
                    SchemaTableTree lastSchemaTableTree = subQueryStack.getLast();
                    if (lastSchemaTableTree.isLocalStep() && lastSchemaTableTree.isOptionalLeftJoin()) {
                        emit.setIncomingOnlyLocalOptionalStep(true);
                    }
                    result.add(emit);
                }
            }
            count++;
        }
        return result;
    }

    public static boolean isBulkWithinAndOut(SqlgGraph sqlgGraph, HasContainer hasContainer) {
        BiPredicate<?, ?> p = hasContainer.getPredicate().getBiPredicate();
        return (p == Contains.within || p == Contains.without) && ((Collection<?>) hasContainer.getPredicate().getValue()).size() > sqlgGraph.configuration().getInt("bulk.within.count", BULK_WITHIN_COUNT);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean isBulkWithin(SqlgGraph sqlgGraph, HasContainer hasContainer) {
        BiPredicate<?, ?> p = hasContainer.getPredicate().getBiPredicate();
        return p == Contains.within && ((Collection<?>) hasContainer.getPredicate().getValue()).size() > sqlgGraph.configuration().getInt("bulk.within.count", BULK_WITHIN_COUNT);
    }

    public static void setParametersOnStatement(
            SqlgGraph sqlgGraph,
            LinkedList<SchemaTableTree> schemaTableTreeStack,
            PreparedStatement preparedStatement,
            int parameterIndex) throws SQLException {

        setParametersOnStatement(sqlgGraph, schemaTableTreeStack, preparedStatement, parameterIndex, false);
    }

    public static void setParametersOnStatement(
            SqlgGraph sqlgGraph,
            LinkedList<SchemaTableTree> schemaTableTreeStack,
            PreparedStatement preparedStatement,
            int parameterIndex,
            boolean includeAdditionalPartitionHasContainer) throws SQLException {

        Multimap<String, Object> keyValueMap = LinkedListMultimap.create();
        for (SchemaTableTree schemaTableTree : schemaTableTreeStack) {
            for (HasContainer hasContainer : schemaTableTree.getHasContainers()) {
                if (!sqlgGraph.getSqlDialect().supportsBulkWithinOut() || !isBulkWithinAndOut(sqlgGraph, hasContainer)) {
                    WhereClause whereClause = WhereClause.from(hasContainer.getPredicate());
                    whereClause.putKeyValueMap(hasContainer, keyValueMap, schemaTableTree);
                }
            }
            if (includeAdditionalPartitionHasContainer) {
                for (HasContainer hasContainer : schemaTableTree.getAdditionalPartitionHasContainers()) {
                    WhereClause whereClause = WhereClause.from(hasContainer.getPredicate());
                    whereClause.putKeyValueMap(hasContainer, keyValueMap, schemaTableTree);
                }
            }
            for (AndOrHasContainer andOrHasContainer : schemaTableTree.getAndOrHasContainers()) {
                andOrHasContainer.setParameterOnStatement(keyValueMap, schemaTableTree);
            }
        }
        List<ImmutablePair<PropertyType, Object>> typeAndValues = SqlgUtil.transformToTypeAndValue(keyValueMap);
        //This is for selects
        setKeyValuesAsParameter(sqlgGraph, false, parameterIndex, preparedStatement, typeAndValues);
    }

    //This is called for inserts
    public static int setKeyValuesAsParameterUsingPropertyColumn(SqlgGraph sqlgGraph, int i, PreparedStatement preparedStatement, Map<String, Pair<PropertyType, Object>> properties) throws SQLException {
        i = setKeyValuesAsParameterUsingPropertyColumn(sqlgGraph, true, i, preparedStatement, properties.values());
        return i;
    }

    public static int setKeyValuesAsParameterUsingPropertyColumn(SqlgGraph sqlgGraph, boolean mod, int parameterStartIndex, PreparedStatement preparedStatement, Collection<Pair<PropertyType, Object>> typeAndValues) throws SQLException {
        for (Pair<PropertyType, Object> pair : typeAndValues) {
            parameterStartIndex = setKeyValueAsParameter(sqlgGraph, mod, parameterStartIndex, preparedStatement, ImmutablePair.of(pair.getLeft(), pair.getRight()));
        }
        return parameterStartIndex;
    }

    public static int setKeyValuesAsParameter(SqlgGraph sqlgGraph, boolean mod, int parameterStartIndex, PreparedStatement preparedStatement, Collection<ImmutablePair<PropertyType, Object>> typeAndValues) throws SQLException {
        for (ImmutablePair<PropertyType, Object> pair : typeAndValues) {
            parameterStartIndex = setKeyValueAsParameter(sqlgGraph, mod, parameterStartIndex, preparedStatement, pair);
        }
        return parameterStartIndex;
    }

    public static int setKeyValueAsParameter(SqlgGraph sqlgGraph, boolean mod, int parameterStartIndex, PreparedStatement preparedStatement, ImmutablePair<PropertyType, Object> pair) throws SQLException {
        if (pair.right == null) {
            int[] sqlTypes = sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(pair.left);
            for (int sqlType : sqlTypes) {
                preparedStatement.setNull(parameterStartIndex++, sqlType);
            }
        } else {
            switch (pair.left.ordinal()) {
                case BOOLEAN_ORDINAL:
                    preparedStatement.setBoolean(parameterStartIndex++, (Boolean) pair.right);
                    break;
                case BYTE_ORDINAL:
                    preparedStatement.setByte(parameterStartIndex++, (Byte) pair.right);
                    break;
                case SHORT_ORDINAL:
                    preparedStatement.setShort(parameterStartIndex++, (Short) pair.right);
                    break;
                case INTEGER_ORDINAL:
                    preparedStatement.setInt(parameterStartIndex++, (Integer) pair.right);
                    break;
                case LONG_ORDINAL:
                    preparedStatement.setLong(parameterStartIndex++, (Long) pair.right);
                    break;
                case FLOAT_ORDINAL:
                    preparedStatement.setFloat(parameterStartIndex++, (Float) pair.right);
                    break;
                case DOUBLE_ORDINAL:
                    preparedStatement.setDouble(parameterStartIndex++, (Double) pair.right);
                    break;
                case STRING_ORDINAL:
                    preparedStatement.setString(parameterStartIndex++, (String) pair.right);
                    break;
                case VARCHAR_ORDINAL:
                    preparedStatement.setString(parameterStartIndex++, (String) pair.right);
                    break;
                case LOCALDATE_ORDINAL:
                    preparedStatement.setTimestamp(parameterStartIndex++, Timestamp.valueOf(((LocalDate) pair.right).atStartOfDay()));
                    break;
                case LOCALDATETIME_ORDINAL:
                    Timestamp timestamp = Timestamp.valueOf(((LocalDateTime) pair.right));
                    preparedStatement.setTimestamp(parameterStartIndex++, timestamp);
                    break;
                case ZONEDDATETIME_ORDINAL:
                    if (sqlgGraph.getSqlDialect().needsTimeZone()) {
                        //This is for postgresql that adjust the timestamp to the server's timezone
                        ZonedDateTime zonedDateTime = (ZonedDateTime) pair.right;
                        preparedStatement.setTimestamp(
                                parameterStartIndex++,
                                Timestamp.valueOf(zonedDateTime.toLocalDateTime())
                        );
                    } else {
                        preparedStatement.setTimestamp(
                                parameterStartIndex++,
                                Timestamp.valueOf(((ZonedDateTime) pair.right).toLocalDateTime())
                        );
                    }
                    if (mod) {
                        TimeZone tz = TimeZone.getTimeZone(((ZonedDateTime) pair.right).getZone());
                        preparedStatement.setString(parameterStartIndex++, tz.getID());
                    }
                    break;
                case LOCALTIME_ORDINAL:
                    //loses nano seconds
                    preparedStatement.setTime(parameterStartIndex++, Time.valueOf((LocalTime) pair.right));
                    break;
                case PERIOD_ORDINAL:
                    preparedStatement.setInt(parameterStartIndex++, ((Period) pair.right).getYears());
                    preparedStatement.setInt(parameterStartIndex++, ((Period) pair.right).getMonths());
                    preparedStatement.setInt(parameterStartIndex++, ((Period) pair.right).getDays());
                    break;
                case DURATION_ORDINAL:
                    preparedStatement.setLong(parameterStartIndex++, ((Duration) pair.right).getSeconds());
                    preparedStatement.setInt(parameterStartIndex++, ((Duration) pair.right).getNano());
                    break;
                case JSON_ORDINAL:
                    sqlgGraph.getSqlDialect().setJson(preparedStatement, parameterStartIndex, (JsonNode) pair.getRight());
                    parameterStartIndex++;
                    break;
                case POINT_ORDINAL:
                    sqlgGraph.getSqlDialect().setPoint(preparedStatement, parameterStartIndex, pair.getRight());
                    parameterStartIndex++;
                    break;
                case LINESTRING_ORDINAL:
                    sqlgGraph.getSqlDialect().setLineString(preparedStatement, parameterStartIndex, pair.getRight());
                    parameterStartIndex++;
                    break;
                case POLYGON_ORDINAL:
                    sqlgGraph.getSqlDialect().setPolygon(preparedStatement, parameterStartIndex, pair.getRight());
                    parameterStartIndex++;
                    break;
                case GEOGRAPHY_POINT_ORDINAL:
                    sqlgGraph.getSqlDialect().setPoint(preparedStatement, parameterStartIndex, pair.getRight());
                    parameterStartIndex++;
                    break;
                case GEOGRAPHY_POLYGON_ORDINAL:
                    sqlgGraph.getSqlDialect().setPolygon(preparedStatement, parameterStartIndex, pair.getRight());
                    parameterStartIndex++;
                    break;
                case BOOLEAN_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.BOOLEAN_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case boolean_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.boolean_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case BYTE_ARRAY_ORDINAL:
                    byte[] byteArray = SqlgUtil.convertObjectArrayToBytePrimitiveArray((Object[]) pair.getRight());
                    preparedStatement.setBytes(parameterStartIndex++, byteArray);
                    break;
                case byte_ARRAY_ORDINAL:
                    preparedStatement.setBytes(parameterStartIndex++, (byte[]) pair.right);
                    break;
                case SHORT_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.SHORT_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case short_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.short_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case INTEGER_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.INTEGER_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case int_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.int_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case LONG_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.LONG_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case long_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.long_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case FLOAT_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.FLOAT_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case float_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.float_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case DOUBLE_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.DOUBLE_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case double_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.double_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case STRING_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.STRING_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case LOCALDATETIME_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.LOCALDATETIME_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case LOCALDATE_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.LOCALDATE_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case LOCALTIME_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.LOCALTIME_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    break;
                case ZONEDDATETIME_ARRAY_ORDINAL:
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.ZONEDDATETIME_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    if (mod) {
                        sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.STRING_ARRAY, SqlgUtil.transformArrayToInsertValue(PropertyType.STRING_ARRAY, Arrays.stream((ZonedDateTime[]) pair.right).map(z -> z.getZone().getId()).toArray()));
                    }
                    break;
                case DURATION_ARRAY_ORDINAL:
                    Duration[] durations = (Duration[]) pair.getRight();
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.long_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, Arrays.stream(durations).map(Duration::getSeconds).toArray()));
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.int_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, Arrays.stream(durations).map(Duration::getNano).toArray()));
                    break;
                case PERIOD_ARRAY_ORDINAL:
                    Period[] periods = (Period[]) pair.getRight();
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.int_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, Arrays.stream(periods).map(Period::getYears).toArray()));
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.int_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, Arrays.stream(periods).map(Period::getMonths).toArray()));
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.int_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, Arrays.stream(periods).map(Period::getDays).toArray()));
                    break;
                case JSON_ARRAY_ORDINAL:
                    JsonNode[] objectNodes = (JsonNode[]) pair.getRight();
                    sqlgGraph.getSqlDialect().setArray(preparedStatement, parameterStartIndex++, PropertyType.JSON_ARRAY, SqlgUtil.transformArrayToInsertValue(pair.left, objectNodes));
                    break;
                default:
                    throw new IllegalStateException("Unhandled type " + pair.left.name());
            }
        }
        return parameterStartIndex;
    }

    public static SchemaTable parseLabel(final String label) {
        Objects.requireNonNull(label, "label may not be null!");
        int indexOfPeriod = label.indexOf(".");
        Preconditions.checkState(indexOfPeriod > -1, String.format("label must have a period to separate the schema from the table. label %s", label));
        String schema = label.substring(0, indexOfPeriod);
        String table = label.substring(indexOfPeriod + 1);
        return SchemaTable.of(schema, table);
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

    public static List<String> transformToKeyList(Object... keyValues) {
        List<String> keys = new ArrayList<>();
        int i = 1;
        for (Object keyValue : keyValues) {
            if (i++ % 2 != 0) {
                keys.add((String) keyValue);
            }
        }
        return keys;

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
                if (key == label || keys.contains(key)) {
                    continue;
                }
                keys.add((String) key);
                if (keyValue == null) {
                    //assume a String for null
                    result.put((String) key, PropertyType.STRING);
                } else {
                    result.put((String) key, PropertyType.from(keyValue));
                }
            }
        }
        return result;
    }

    /**
     * Validates the key values and converts it into a Triple with three maps.
     * The left  map is a map of keys together with their PropertyType.
     * The middle map is a map of keys together with their values.
     * The right map is a map of keys with values where the values are guaranteed not to be null.
     *
     * @param sqlDialect The dialect.
     * @param keyValues  The key value pairs.
     * @return A Triple with 3 maps.
     */
    public static Triple<Map<String, PropertyType>, Map<String, Object>, Map<String, Object>> validateVertexKeysValues(SqlDialect sqlDialect, Object[] keyValues) {
        Map<String, Object> resultAllValues = new LinkedHashMap<>();
        Map<String, Object> resultNotNullValues = new LinkedHashMap<>();
        Map<String, PropertyType> keyPropertyTypeMap = new LinkedHashMap<>();

        if (keyValues.length % 2 != 0)
            throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();

        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!(keyValues[i] instanceof String) && !(keyValues[i] instanceof T)) {
                throw Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices();
            }
            if (keyValues[i].equals(T.id)) {
                throw Vertex.Exceptions.userSuppliedIdsNotSupported();
            }
            if (!keyValues[i].equals(T.label)) {
                String key = (String) keyValues[i];
                sqlDialect.validateColumnName(key);
                Object value = keyValues[i + 1];
                ElementHelper.validateProperty(key, value);
                sqlDialect.validateProperty(key, value);
                if (value != null) {
                    resultNotNullValues.put(key, value);
                    keyPropertyTypeMap.put(key, PropertyType.from(value));
                } else {
                    keyPropertyTypeMap.put(key, PropertyType.STRING);
                }
                resultAllValues.put(key, value);
            }
        }
        return Triple.of(keyPropertyTypeMap, resultAllValues, resultNotNullValues);
    }

    public static Triple<Map<String, PropertyType>, Map<String, Object>, Map<String, Object>> validateVertexKeysValues(SqlDialect sqlDialect, Object[] keyValues, List<String> previousBatchModeKeys) {
        Map<String, Object> resultAllValues = new LinkedHashMap<>();
        Map<String, Object> resultNotNullValues = new LinkedHashMap<>();
        Map<String, PropertyType> keyPropertyTypeMap = new LinkedHashMap<>();

        if (keyValues.length % 2 != 0)
            throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();

        int keyCount = 0;
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!(keyValues[i] instanceof String) && !(keyValues[i] instanceof T)) {
                throw Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices();
            }
            if (keyValues[i].equals(T.id)) {
                throw Vertex.Exceptions.userSuppliedIdsNotSupported();
            }
            if (!keyValues[i].equals(T.label)) {
                String key = (String) keyValues[i];
                sqlDialect.validateColumnName(key);
                Object value = keyValues[i + 1];
                if (value != null) {
                    ElementHelper.validateProperty(key, value);
                    sqlDialect.validateProperty(key, value);
                }
                if (value != null) {
                    resultNotNullValues.put(key, value);
                    keyPropertyTypeMap.put(key, PropertyType.from(value));
                } else {
                    keyPropertyTypeMap.put(key, PropertyType.STRING);
                }
                resultAllValues.put(key, value);

                if (previousBatchModeKeys != null && !previousBatchModeKeys.isEmpty() && !key.equals(previousBatchModeKeys.get(keyCount++))) {
                    throw new IllegalStateException("Streaming batch mode must occur for the same keys in the same order. Expected " + previousBatchModeKeys.get(keyCount - 1) + " found " + key);
                }
            }
        }
        return Triple.of(keyPropertyTypeMap, resultAllValues, resultNotNullValues);
    }

    private static List<ImmutablePair<PropertyType, Object>> transformToTypeAndValue(Multimap<String, Object> keyValues) {
        List<ImmutablePair<PropertyType, Object>> result = new ArrayList<>();
        for (Map.Entry<String, Object> entry : keyValues.entries()) {
            Object value = entry.getValue();
            String key = entry.getKey();
            //value
            //skip the label as that is not a property but the table
            // skip column references that don't have values
            if (key.equals(label.getAccessor()) || value instanceof PropertyReference) {
                continue;
            }
            // we transform id in ID
            if (key.equals(T.id.getAccessor()) || "ID".equals(key)) {
                if (value instanceof Long) {
                    result.add(ImmutablePair.of(PropertyType.LONG, value));
                } else {
                    RecordId id;
                    if (!(value instanceof RecordId)) {
                        id = RecordId.from(value);
                    } else {
                        id = (RecordId) value;
                    }
                    result.add(ImmutablePair.of(PropertyType.LONG, id.sequenceId()));
                }
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
            if (entry.getKey().equals(label)) {
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
    private static Object[] transformArrayToInsertValue(PropertyType propertyType, Object value) {
        return getArray(propertyType, value);
    }

    private static Object[] getArray(PropertyType propertyType, Object val) {
        int arrlength = Array.getLength(val);
        Object[] outputArray = new Object[arrlength];
        for (int i = 0; i < arrlength; ++i) {
            switch (propertyType.ordinal()) {
                case LOCALDATETIME_ARRAY_ORDINAL:
                    outputArray[i] = Timestamp.valueOf((LocalDateTime) Array.get(val, i));
                    break;
                case LOCALDATE_ARRAY_ORDINAL:
                    outputArray[i] = Timestamp.valueOf(((LocalDate) Array.get(val, i)).atStartOfDay());
                    break;
                case LOCALTIME_ARRAY_ORDINAL:
                    outputArray[i] = Time.valueOf(((LocalTime) Array.get(val, i)));
                    break;
                case ZONEDDATETIME_ARRAY_ORDINAL:
                    ZonedDateTime zonedDateTime = (ZonedDateTime) Array.get(val, i);
                    outputArray[i] = Timestamp.valueOf(zonedDateTime.toLocalDateTime());
                    break;
                case BYTE_ARRAY_ORDINAL:
                    Byte aByte = (Byte) Array.get(val, i);
                    outputArray[i] = aByte;
                    break;
                case JSON_ARRAY_ORDINAL:
                    JsonNode jsonNode = (JsonNode) Array.get(val, i);
                    outputArray[i] = jsonNode.toString();
                    break;
                default:
                    outputArray[i] = Array.get(val, i);
            }
        }
        return outputArray;
    }

    public static String removeTrailingInId(String foreignKey) {
        if (foreignKey.endsWith(Topology.IN_VERTEX_COLUMN_END)) {
            return foreignKey.substring(0, foreignKey.length() - Topology.IN_VERTEX_COLUMN_END.length());
        } else {
            return foreignKey;
        }
    }

    public static String removeTrailingOutId(String foreignKey) {
        if (foreignKey.endsWith(Topology.OUT_VERTEX_COLUMN_END)) {
            return foreignKey.substring(0, foreignKey.length() - Topology.OUT_VERTEX_COLUMN_END.length());
        } else {
            return foreignKey;
        }
    }

    public static void dropDb(SqlDialect sqlDialect, Connection conn) {
        try {
            DatabaseMetaData metadata = conn.getMetaData();
            //Drop all the edges
            List<Triple<String, String, String>> edgeTables = sqlDialect.getEdgeTables(metadata);
            for (Triple<String, String, String> edgeTable : edgeTables) {
                String db = edgeTable.getLeft();
                String schema = edgeTable.getMiddle();
                String table = edgeTable.getRight();
                if (!sqlDialect.getInternalSchemas().contains(schema)) {
                    StringBuilder sql = new StringBuilder("DROP TABLE ");
                    sql.append(sqlDialect.maybeWrapInQoutes(schema));
                    sql.append(".");
                    sql.append(sqlDialect.maybeWrapInQoutes(table));
                    if (sqlDialect.supportsCascade()) {
                        sql.append(" CASCADE");
                    }
                    if (sqlDialect.needsSemicolon()) {
                        sql.append(";");
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql.toString());
                    }
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                        preparedStatement.executeUpdate();
                    }
                }
            }
            //Drop all the vertices
            List<Triple<String, String, String>> vertexTables = sqlDialect.getVertexTables(metadata);
            for (Triple<String, String, String> vertexTable : vertexTables) {
                String db = vertexTable.getLeft();
                String schema = vertexTable.getMiddle();
                String table = vertexTable.getRight();
                if (!sqlDialect.getInternalSchemas().contains(schema)) {
                    StringBuilder sql = new StringBuilder("DROP TABLE ");
                    sql.append(sqlDialect.maybeWrapInQoutes(schema));
                    sql.append(".");
                    sql.append(sqlDialect.maybeWrapInQoutes(table));
                    if (sqlDialect.supportsCascade()) {
                        sql.append(" CASCADE");
                    }
                    if (sqlDialect.needsSemicolon()) {
                        sql.append(";");
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql.toString());
                    }
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                        preparedStatement.executeUpdate();
                    }
                }
            }

            List<String> schemaNames = sqlDialect.getSchemaNames(metadata);
            for (String schema : schemaNames) {
                if (!sqlDialect.getInternalSchemas().contains(schema) && !sqlDialect.getPublicSchema().equals(schema)) {
                    String sql = sqlDialect.dropSchemaStatement(schema);
                    if (logger.isDebugEnabled()) {
                        logger.debug(sql);
                    }
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                        preparedStatement.executeUpdate();
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void dropDb(SqlgGraph sqlgGraph) {
        SqlDialect sqlDialect = sqlgGraph.getSqlDialect();
        Connection conn = sqlgGraph.tx().getConnection();
        dropDb(sqlDialect, conn);
        dropSqlgReadOnlyUser(sqlDialect, conn);
    }

    private static void dropSqlgReadOnlyUser(SqlDialect sqlDialect, Connection conn) {
        if (sqlDialect.isHsqldb()) {
            try (Statement statement = conn.createStatement()) {
                ResultSet rs = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.SYSTEM_USERS where USER_NAME = 'sqlgReadOnly'");
                if (rs.next()) {
                    try (Statement s = conn.createStatement()) {
                        s.execute("DROP USER \"sqlgReadOnly\"");
                        s.execute("DROP ROLE READ_ONLY");
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else if (sqlDialect.isPostgresql()) {
            try (Statement statement = conn.createStatement()) {
                ResultSet rs = statement.executeQuery("SELECT 1 FROM pg_roles WHERE rolname='sqlgReadOnly'");
                if (rs.next()) {
                    try (Statement s = conn.createStatement()) {
                        // Revokes all privileges against the public Postgres schema.
                        s.execute("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM \"sqlgReadOnly\"");
                        s.execute("REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM \"sqlgReadOnly\"");
                        s.execute("REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM \"sqlgReadOnly\"");
                        s.execute("REVOKE USAGE ON SCHEMA public FROM \"sqlgReadOnly\"");

                        s.execute("DROP ROLE \"sqlgReadOnly\"");
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else if (sqlDialect.isMariaDb() || sqlDialect.isMysql()) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP USER IF EXISTS 'sqlgReadOnly'@'localhost'");
                s.executeQuery("FLUSH PRIVILEGES");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else if (sqlDialect.isMssqlServer()) {
            try (Statement s = conn.createStatement()) {
                try (ResultSet rs = s.executeQuery("SELECT * FROM master.sys.sql_logins where name = 'sqlgReadOnly';")) {
                    if (rs.next()) {
                        s.execute("DROP USER sqlgReadOnly");
                        s.execute("DROP LOGIN sqlgReadOnly");
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
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

    public static Short[] convertObjectOfShortsArrayToShortArray(Object[] shortArray) {
        Short[] target = new Short[shortArray.length];
        for (int i = 0; i < shortArray.length; i++) {
            Array.set(target, i, shortArray[i]);
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

    public static short[] convertObjectOfShortsArrayToShortPrimitiveArray(Object[] shortArray) {
        short[] target = new short[shortArray.length];
        for (int i = 0; i < shortArray.length; i++) {
            Array.set(target, i, shortArray[i]);
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

    public static float[] convertObjectOfFloatsArrayToFloatPrimitiveArray(Object[] floatArray) {
        float[] target = new float[floatArray.length];
        for (int i = 0; i < floatArray.length; i++) {
            Array.set(target, i, floatArray[i]);
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

    public static Float[] convertObjectOfFloatsArrayToFloatArray(Object[] doubleArray) {
        Float[] target = new Float[doubleArray.length];
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
            Array.set(target, i, floatArray[i]);
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

    public static <T> T copyObjectArrayOfOffsetDateTimeToLocalDateTime(Object[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException(PROPERTY_ARRAY_VALUE_ELEMENTS_MAY_NOT_BE_NULL);
            }
            Array.set(target, i, ((OffsetDateTime) value[i]).toLocalDateTime());
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

    public static <T> T copyObjectArrayOfTimestampToLocalDate(Object[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException(PROPERTY_ARRAY_VALUE_ELEMENTS_MAY_NOT_BE_NULL);
            }
            Array.set(target, i, ((Timestamp) value[i]).toLocalDateTime().toLocalDate());
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

    public static <T> T copyObjectArrayOfOffsetTimeToLocalTime(Object[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException(PROPERTY_ARRAY_VALUE_ELEMENTS_MAY_NOT_BE_NULL);
            }
            Array.set(target, i, ((OffsetTime) value[i]).toLocalTime());
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

    public static String originalLabel(String label) {
        int indexOfLabel = label.indexOf(BaseStrategy.PATH_LABEL_SUFFIX);
        if (indexOfLabel != -1) {
            return label.substring(indexOfLabel + BaseStrategy.PATH_LABEL_SUFFIX.length());
        }
        indexOfLabel = label.indexOf(BaseStrategy.EMIT_LABEL_SUFFIX);
        if (indexOfLabel != -1) {
            return label.substring(indexOfLabel + BaseStrategy.EMIT_LABEL_SUFFIX.length());
        }
        throw new IllegalStateException("originalLabel must only be called on labels with Sqlg's path prepended to it");
    }

    public static List<Comparable> getValue(ResultSet resultSet, List<ColumnList.Column> columns) {
        List<Comparable> result = new ArrayList<>();
        try {
            for (ColumnList.Column column : columns) {
                PropertyType propertyType = column.getPropertyType();
                switch (propertyType.ordinal()) {
                    case STRING_ORDINAL:
                        String s = resultSet.getString(column.getColumnIndex());
                        if (!resultSet.wasNull()) {
                            result.add(s);
                        }
                        break;
                    case VARCHAR_ORDINAL:
                        s = resultSet.getString(column.getColumnIndex());
                        if (!resultSet.wasNull()) {
                            result.add(s);
                        }
                        break;
                    case INTEGER_ORDINAL:
                        Integer i = resultSet.getInt(column.getColumnIndex());
                        if (!resultSet.wasNull()) {
                            result.add(i);
                        }
                        break;
                    default:
                        throw new IllegalStateException(String.format("PropertyType %s is not implemented.", propertyType.name()));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object stringValueToType(PropertyType propertyType, String value) {
        switch (propertyType.ordinal()) {
            case STRING_ORDINAL:
            case VARCHAR_ORDINAL:
                return value;
            case LONG_ORDINAL:
                return Long.valueOf(value);
            case INTEGER_ORDINAL:
                return Integer.valueOf(value);
            default:
                throw new IllegalStateException(String.format("Unhandled propertyType %s", propertyType));
        }
    }

}
