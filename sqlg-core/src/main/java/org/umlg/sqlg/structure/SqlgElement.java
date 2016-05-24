package org.umlg.sqlg.structure;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.umlg.sqlg.structure.PropertyType.boolean_ARRAY;
import static org.umlg.sqlg.structure.PropertyType.int_ARRAY;

/**
 * Date: 2014/07/12
 * Time: 5:40 AM
 */
public abstract class SqlgElement implements Element {

    private Logger logger = LoggerFactory.getLogger(SqlgVertex.class.getName());

    protected String schema;
    protected String table;
    protected RecordId recordId;
    protected final SqlgGraph sqlgGraph;
    //Multiple threads can access the same element
    protected Map<String, Object> properties = new ConcurrentHashMap<>();
    private SqlgElementElementPropertyRollback elementPropertyRollback;
    protected boolean removed = false;

    /**
     * Constructor only used for the Dummy element.
     */
    SqlgElement() {
        this.sqlgGraph = null;
    }

    public SqlgElement(SqlgGraph sqlgGraph, String schema, String table) {
        this.sqlgGraph = sqlgGraph;
        this.schema = schema;
        this.table = table;
        this.elementPropertyRollback = new SqlgElementElementPropertyRollback();
        if (!this.sqlgGraph.tx().isInStreamingBatchMode() && !this.sqlgGraph.tx().isInStreamingWithLockBatchMode()) {
            sqlgGraph.tx().addElementPropertyRollback(this.elementPropertyRollback);
        }
    }

    public SqlgElement(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        if (table.startsWith(SchemaManager.VERTEX_PREFIX) || table.startsWith(SchemaManager.EDGE_PREFIX)) {
            throw new IllegalStateException("SqlgElement.table may not be prefixed with " + SchemaManager.VERTEX_PREFIX + " or " + SchemaManager.EDGE_PREFIX);
        }
        this.sqlgGraph = sqlgGraph;
        this.schema = schema;
        this.table = table;
        this.recordId = RecordId.from(SchemaTable.of(this.schema, this.table), id);
        this.elementPropertyRollback = new SqlgElementElementPropertyRollback();
        if (!this.sqlgGraph.tx().isInStreamingBatchMode() && !this.sqlgGraph.tx().isInStreamingWithLockBatchMode()) {
            sqlgGraph.tx().addElementPropertyRollback(this.elementPropertyRollback);
        }
    }

    @Override
    public Graph graph() {
        return this.sqlgGraph;
    }

    class SqlgElementElementPropertyRollback implements ElementPropertyRollback {
        @Override
        public void clearProperties() {
            SqlgElement.this.properties.clear();
        }
    }

    public void setInternalPrimaryKey(RecordId recordId) {
        this.recordId = recordId;
    }

    abstract SchemaTable getSchemaTablePrefixed();

    @Override
    public Object id() {
        return this.recordId;
    }

    @Override
    public String label() {
        return this.table;
    }

    @Override
    public void remove() {
        StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.schema));
        sql.append(".");
        sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes((this instanceof Vertex ? SchemaManager.VERTEX_PREFIX : SchemaManager.EDGE_PREFIX) + this.table));
        sql.append(" WHERE ");
        sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" = ?");
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(1, ((RecordId) this.id()).getId());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.removed = true;
    }

    @Override
    public Set<String> keys() {
        this.sqlgGraph.tx().readWrite();
        return this.internalGetProperties().keySet();
    }

    //TODO relook at hiddens, unnecessary looping and queries
    @Override
    public <V> Property<V> property(String key) {
        if (this.removed) {
            throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id());
        } else {
            Property property = internalGetProperties().get(key);
            if (property == null) {
                //try hiddens
                property = internalGetHiddens().get(key);
                if (property == null) {
                    return emptyProperty();
                } else {
                    return property;
                }
            } else {
                return property;
            }
        }
    }

    protected Property emptyProperty() {
        return Property.empty();
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        ElementHelper.validateProperty(key, value);
        this.sqlgGraph.getSqlDialect().validateProperty(key, value);
        if (!this.sqlgGraph.tx().isInStreamingBatchMode() && !this.sqlgGraph.tx().isInStreamingWithLockBatchMode()) {
            sqlgGraph.tx().addElementPropertyRollback(this.elementPropertyRollback);
        }
        //Validate the property
        PropertyType.from(value);
        //Check if column exist
        this.sqlgGraph.getSchemaManager().ensureColumnExist(
                this.schema,
                this instanceof Vertex ? SchemaManager.VERTEX_PREFIX + this.table : SchemaManager.EDGE_PREFIX + this.table,
                ImmutablePair.of(key, PropertyType.from(value)));
        load();
        updateRow(key, value);
        return instantiateProperty(key, value);
    }

    protected <V> SqlgProperty<V> instantiateProperty(String key, V value) {
        return new SqlgProperty<>(this.sqlgGraph, this, key, value);
    }

    /**
     * load the row from the db and caches the results in the element
     */
    protected abstract void load();

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    private void updateRow(String key, Object value) {

        boolean elementInInsertedCache = false;
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            elementInInsertedCache = this.sqlgGraph.tx().getBatchManager().updateProperty(this, key, value);
        }

        if (!elementInInsertedCache) {
            StringBuilder sql = new StringBuilder("UPDATE ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes((this instanceof Vertex ? SchemaManager.VERTEX_PREFIX : SchemaManager.EDGE_PREFIX) + this.table));
            sql.append(" SET ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(key));
            sql.append(" = ?");
            sql.append(" WHERE ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
            sql.append(" = ?");
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            Connection conn = this.sqlgGraph.tx().getConnection();
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                Map<String, Object> keyValue = new HashMap<>();
                keyValue.put(key, value);
                setKeyValuesAsParameter(this.sqlgGraph, 1, conn, preparedStatement, keyValue);
                preparedStatement.setLong(2, ((RecordId) this.id()).getId());
                preparedStatement.executeUpdate();
                preparedStatement.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        //Cache the properties
        this.properties.put(key, value);
    }

    /**
     * Called from SqlgVertexStepCompiler which compiled VertexStep and HasSteps.
     * This is only called when not in BatchMode
     *
     * @param replacedSteps The original VertexStep and HasSteps that were replaced.
     * @return The result of the query.
     * //
     */
    public <S, E extends SqlgElement> Iterator<Pair<E, Multimap<String, Emit<E>>>> elements(List<ReplacedStep<S, E>> replacedSteps) {
        this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.tx().getBatchManager().isStreaming()) {
            throw new IllegalStateException("streaming is in progress, first flush or commit before querying.");
        }
        return internalGetElements(replacedSteps);
    }

    /**
     * Generate a query for the replaced steps.
     * Each replaced step translates to a join statement and a section of the where clause.
     *
     * @param replacedSteps
     * @return The results of the query
     */
    private <S, E extends SqlgElement> Iterator<Pair<E, Multimap<String, Emit<E>>>> internalGetElements(List<ReplacedStep<S, E>> replacedSteps) {
        SchemaTable schemaTable = getSchemaTablePrefixed();
        SchemaTableTree rootSchemaTableTree = this.sqlgGraph.getGremlinParser().parse(schemaTable, replacedSteps);
        Set<SchemaTableTree> rootSchemaTableTrees = new HashSet<>();
        rootSchemaTableTrees.add(rootSchemaTableTree);
        SqlgCompiledResultIterator<Pair<E, Multimap<String, Emit<E>>>> resultIterator = new SqlgCompiledResultIterator<>(this.sqlgGraph, rootSchemaTableTrees, this.recordId);
        return resultIterator;
    }

    @Override
    public boolean equals(final Object object) {
        this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            if (this.id() != null && object != null && (object instanceof SqlgElement) && ((SqlgElement) object).id() != null) {
                return ElementHelper.areEqual(this, object);
            } else {
                return super.equals(object);
            }
        } else {
            return ElementHelper.areEqual(this, object);
        }
    }

    @Override
    public int hashCode() {
        this.sqlgGraph.tx().readWrite();
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            return super.hashCode();
        } else {
            return ElementHelper.hashCode(this);
        }
    }


    public static int setKeyValuesAsParameter(SqlgGraph sqlgGraph, int i, Connection conn, PreparedStatement preparedStatement, Map<String, Object> keyValues) throws SQLException {
        List<ImmutablePair<PropertyType, Object>> typeAndValues = SqlgUtil.transformToTypeAndValue(keyValues);
        i = setKeyValueAsParameter(sqlgGraph, i, conn, preparedStatement, typeAndValues);
        return i;
    }

    private static int setKeyValueAsParameter(SqlgGraph sqlgGraph, int parameterStartIndex, Connection conn, PreparedStatement preparedStatement, List<ImmutablePair<PropertyType, Object>> typeAndValues) throws SQLException {
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

                //TODO the array properties are hardcoded according to postgres's jdbc driver
                case boolean_ARRAY:
                    java.sql.Array booleanArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(boolean_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, booleanArray);
                    break;
                case byte_ARRAY:
                    preparedStatement.setBytes(parameterStartIndex++, (byte[]) pair.right);
                    break;
                case short_ARRAY:
                    java.sql.Array shortArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.short_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, shortArray);
                    break;
                case int_ARRAY:
                    java.sql.Array intArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(int_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, intArray);
                    break;
                case long_ARRAY:
                    java.sql.Array longArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.long_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, longArray);
                    break;
                case float_ARRAY:
                    java.sql.Array floatArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.float_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, floatArray);
                    break;
                case double_ARRAY:
                    java.sql.Array doubleArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.double_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, doubleArray);
                    break;
                case STRING_ARRAY:
                    java.sql.Array stringArray = conn.createArrayOf(sqlgGraph.getSqlDialect().getArrayDriverType(PropertyType.STRING_ARRAY), SqlgUtil.transformArrayToInsertValue(pair.left, pair.right));
                    preparedStatement.setArray(parameterStartIndex++, stringArray);
                    break;
                case JSON:
                    sqlgGraph.getSqlDialect().setJson(preparedStatement, parameterStartIndex++, (JsonNode) pair.getRight());
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

    protected <V> Map<String, ? extends Property<V>> internalGetAllProperties(final String... propertyKeys) {
        this.sqlgGraph.tx().readWrite();
        load();
        Map<String, SqlgProperty<V>> properties = new HashMap<>();
        this.properties.entrySet().stream()
                .filter(entry -> propertyKeys.length == 0 || Stream.of(propertyKeys).filter(k -> k.equals(entry.getKey())).findAny().isPresent())
                .filter(entry -> !entry.getKey().equals("ID"))
                .filter(entry -> entry.getValue() != null)
                .forEach(entry -> properties.put(entry.getKey(), instantiateProperty(entry.getKey(), (V) entry.getValue())));
        return properties;
    }

    protected <V> Map<String, ? extends Property<V>> internalGetProperties(final String... propertyKeys) {
        this.sqlgGraph.tx().readWrite();
        load();
        Map<String, SqlgProperty<V>> properties = new HashMap<>();
        this.properties.entrySet().stream()
                .filter(entry -> propertyKeys.length == 0 || Stream.of(propertyKeys).filter(k -> k.equals(entry.getKey())).findAny().isPresent())
                .filter(entry -> !entry.getKey().equals("ID"))
                .filter(entry -> entry.getValue() != null)
                .forEach(entry -> properties.put(entry.getKey(), instantiateProperty(entry.getKey(), (V) entry.getValue())));
        return properties;
    }

    protected <V> Map<String, ? extends Property<V>> internalGetHiddens(final String... propertyKeys) {
        this.sqlgGraph.tx().readWrite();
        load();
        Map<String, SqlgProperty<V>> properties = new HashMap<>();

        this.properties.entrySet().stream()
                .filter(entry -> propertyKeys.length == 0 || Stream.of(propertyKeys).filter(k -> k.equals(entry.getKey())).findAny().isPresent())
                .filter(entry -> !entry.getKey().equals("ID"))
                .filter(entry -> entry.getValue() != null)
                .forEach(entry -> properties.put(entry.getKey(), instantiateProperty(entry.getKey(), (V) entry.getValue())));

        return properties;
    }

    @Override
    public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
        SqlgElement.this.sqlgGraph.tx().readWrite();
        return SqlgElement.this.<V>internalGetAllProperties(propertyKeys).values().iterator();
    }

    protected void loadProperty(ResultSetMetaData resultSetMetaData, ResultSet resultSet, int i, String columnName, Object o, Multimap<String, String> threadLocalColumnNameAliasMap) throws SQLException {
        if (columnName.endsWith(SchemaManager.ZONEID) ||
                columnName.endsWith(SchemaManager.MONTHS) ||
                columnName.endsWith(SchemaManager.DAYS) ||
                columnName.endsWith(SchemaManager.DURATION_NANOS)
                ) {
            return;
        }
        PropertyType propertyType = this.sqlgGraph.getSchemaManager().getTableFor(getSchemaTablePrefixed()).get(columnName);
        switch (propertyType) {

            case BOOLEAN:
                this.properties.put(columnName, o);
                break;
            case BYTE:
                this.properties.put(columnName, ((Integer) o).byteValue());
                break;
            case SHORT:
                this.properties.put(columnName, ((Integer) o).shortValue());
                break;
            case INTEGER:
                this.properties.put(columnName, o);
                break;
            case LONG:
                this.properties.put(columnName, o);
                break;
            case FLOAT:
                this.properties.put(columnName, ((Number) o).floatValue());
                break;
            case DOUBLE:
                this.properties.put(columnName, ((Number) o).doubleValue());
                break;
            case STRING:
                this.properties.put(columnName, o);
                break;
            case LOCALDATE:
                this.properties.put(columnName, ((Date) o).toLocalDate());
                break;
            case LOCALDATETIME:
                this.properties.put(columnName, ((Timestamp) o).toLocalDateTime());
                break;
            case ZONEDDATETIME:
                String zoneIdColumn = getSchemaTablePrefixed().toString().replace(".", SchemaTableTree.ALIAS_SEPARATOR) + SchemaTableTree.ALIAS_SEPARATOR + columnName + propertyType.getPostFixes()[0];
                Collection<String> zonedId = threadLocalColumnNameAliasMap.get(zoneIdColumn);
                if (zonedId.isEmpty()) {
                    zonedId = Arrays.asList(columnName + propertyType.getPostFixes()[0]);
                }
                String zoneId = resultSet.getString(new ArrayList<>(zonedId).get(zonedId.size() - 1));
                ZoneId zoneId1 = ZoneId.of(zoneId);
                ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(((Timestamp) o).toLocalDateTime(), zoneId1);
                this.properties.put(columnName, zonedDateTimeAGT);
                break;
            case LOCALTIME:
                this.properties.put(columnName, ((Time) o).toLocalTime());
                break;
            case PERIOD:
                int years = (Integer) o;
                String monthColumn = getSchemaTablePrefixed().toString().replace(".", SchemaTableTree.ALIAS_SEPARATOR) + SchemaTableTree.ALIAS_SEPARATOR + columnName + propertyType.getPostFixes()[0];
                Collection<String> aliasedMonth = threadLocalColumnNameAliasMap.get(monthColumn);
                if (aliasedMonth.isEmpty()) {
                    aliasedMonth = Arrays.asList(columnName + propertyType.getPostFixes()[0]);
                }
                int months = resultSet.getInt(new ArrayList<>(aliasedMonth).get(aliasedMonth.size() - 1));
                String dayColumn = getSchemaTablePrefixed().toString().replace(".", SchemaTableTree.ALIAS_SEPARATOR) + SchemaTableTree.ALIAS_SEPARATOR + columnName + propertyType.getPostFixes()[1];
                Collection<String> aliasedDay = threadLocalColumnNameAliasMap.get(dayColumn);
                if (aliasedDay.isEmpty()) {
                    aliasedDay = Arrays.asList(columnName + propertyType.getPostFixes()[1]);
                }
                int days = resultSet.getInt(new ArrayList<>(aliasedDay).get(aliasedDay.size() - 1));
                this.properties.put(columnName, Period.of(years, months, days));
                break;
            case DURATION:
                long seconds = (Long) o;
                //load the months and days as its needed to construct the Period
                String nanosColumn = getSchemaTablePrefixed().toString().replace(".", SchemaTableTree.ALIAS_SEPARATOR) + SchemaTableTree.ALIAS_SEPARATOR + columnName + propertyType.getPostFixes()[0];
                Collection<String> aliasedNanos = threadLocalColumnNameAliasMap.get(nanosColumn);
                if (aliasedNanos.isEmpty()) {
                    aliasedNanos = Arrays.asList(columnName + propertyType.getPostFixes()[0]);
                }
                int nanos = resultSet.getInt(new ArrayList<>(aliasedNanos).get(aliasedNanos.size() - 1));
                this.properties.put(columnName, Duration.ofSeconds(seconds, nanos));
                break;
            case JSON:
                sqlgGraph.getSqlDialect().handleOther(this.properties, columnName, o, propertyType);
                break;
            case POINT:
                sqlgGraph.getSqlDialect().handleOther(this.properties, columnName, o, propertyType);
                break;
            case POLYGON:
                sqlgGraph.getSqlDialect().handleOther(this.properties, columnName, o, propertyType);
                break;
            case GEOGRAPHY_POINT:
                sqlgGraph.getSqlDialect().handleOther(this.properties, columnName, o, propertyType);
                break;
            case GEOGRAPHY_POLYGON:
                sqlgGraph.getSqlDialect().handleOther(this.properties, columnName, o, propertyType);
                break;
            case boolean_ARRAY:
                java.sql.Array array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            case BOOLEAN_ARRAY:
                array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            case byte_ARRAY:
                this.properties.put(columnName, o);
                break;
            case BYTE_ARRAY:
                this.properties.put(columnName, SqlgUtil.convertPrimitiveByteArrayToByteArray((byte[]) o));
                break;
            case short_ARRAY:
                array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            case SHORT_ARRAY:
                array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            case int_ARRAY:
                array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            case INTEGER_ARRAY:
                array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            case long_ARRAY:
                array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            case LONG_ARRAY:
                array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            case float_ARRAY:
                array = (java.sql.Array) o;
                Float[]floatArray = (Float[]) array.getArray();
                this.properties.put(columnName, SqlgUtil.convertFloatArrayToPrimitiveFloat(floatArray));
                break;
            case FLOAT_ARRAY:
                array = (java.sql.Array) o;
                floatArray = (Float[]) array.getArray();
                this.properties.put(columnName, floatArray);
                break;
            case double_ARRAY:
                array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            case DOUBLE_ARRAY:
                array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            case STRING_ARRAY:
                array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            case LOCALDATETIME_ARRAY:
                array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            case LOCALDATE_ARRAY:
                array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            case LOCALTIME_ARRAY:
                array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            case ZONEDDATETIME_ARRAY:
                array = (java.sql.Array) o;
                zoneIdColumn = getSchemaTablePrefixed().toString().replace(".", SchemaTableTree.ALIAS_SEPARATOR) + SchemaTableTree.ALIAS_SEPARATOR + columnName + propertyType.getPostFixes()[0];
                zonedId = threadLocalColumnNameAliasMap.get(zoneIdColumn);
                if (zonedId.isEmpty()) {
                    zonedId = Arrays.asList(columnName + propertyType.getPostFixes()[0]);
                }
                java.sql.Array zoneIdArray = resultSet.getArray(new ArrayList<>(zonedId).get(zonedId.size() - 1));
                String[] objectZoneIdArray = (String[]) this.sqlgGraph.getSqlDialect().convertArray(PropertyType.STRING_ARRAY, zoneIdArray);
                LocalDateTime[] localDateTimes = (LocalDateTime[]) this.sqlgGraph.getSqlDialect().convertArray(PropertyType.LOCALDATETIME_ARRAY, array);
                ZonedDateTime[] zonedDateTimes = new ZonedDateTime[localDateTimes.length];
                int count = 0;
                for (LocalDateTime localDateTime : localDateTimes) {
                    zoneId1 = ZoneId.of(objectZoneIdArray[count]);
                    ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, zoneId1);
                    zonedDateTimes[count++] = zonedDateTime;
                }
                this.properties.put(columnName, zonedDateTimes);
                break;
            case DURATION_ARRAY:
                array = (java.sql.Array) o;
                nanosColumn = getSchemaTablePrefixed().toString().replace(".", SchemaTableTree.ALIAS_SEPARATOR) + SchemaTableTree.ALIAS_SEPARATOR + columnName + propertyType.getPostFixes()[0];
                aliasedNanos = threadLocalColumnNameAliasMap.get(nanosColumn);
                if (aliasedNanos.isEmpty()) {
                    aliasedNanos = Arrays.asList(columnName + propertyType.getPostFixes()[0]);
                }
                long[] secondsArray = (long[]) this.sqlgGraph.getSqlDialect().convertArray(PropertyType.long_ARRAY, array);
                java.sql.Array nanosArray = resultSet.getArray(new ArrayList<>(aliasedNanos).get(aliasedNanos.size() - 1));
                int[] nanoArray = (int[]) this.sqlgGraph.getSqlDialect().convertArray(PropertyType.int_ARRAY, nanosArray);
                Duration[] durations = new Duration[secondsArray.length];
                count = 0;
                for (Long second : secondsArray) {
                    durations[count] = Duration.ofSeconds(second, nanoArray[count++]);
                }
                this.properties.put(columnName, durations);
                break;
            case PERIOD_ARRAY:
                array = (java.sql.Array) o;
                String monthsColumn = getSchemaTablePrefixed().toString().replace(".", SchemaTableTree.ALIAS_SEPARATOR) + SchemaTableTree.ALIAS_SEPARATOR + columnName + propertyType.getPostFixes()[0];
                Collection<String> aliasedMonths = threadLocalColumnNameAliasMap.get(monthsColumn);
                if (aliasedMonths.isEmpty()) {
                    aliasedMonths = Arrays.asList(columnName + propertyType.getPostFixes()[1]);
                }
                String daysColumn = getSchemaTablePrefixed().toString().replace(".", SchemaTableTree.ALIAS_SEPARATOR) + SchemaTableTree.ALIAS_SEPARATOR + columnName + propertyType.getPostFixes()[1];
                Collection<String> aliasedDays = threadLocalColumnNameAliasMap.get(daysColumn);
                if (aliasedDays.isEmpty()) {
                    aliasedDays = Arrays.asList(columnName + propertyType.getPostFixes()[2]);
                }
                Integer[] yearsIntegers = (Integer[]) this.sqlgGraph.getSqlDialect().convertArray(PropertyType.INTEGER_ARRAY, array);
                java.sql.Array monthsArray = resultSet.getArray(new ArrayList<>(aliasedMonths).get(aliasedMonths.size() - 1));
                Integer[] monthsIntegers = (Integer[]) this.sqlgGraph.getSqlDialect().convertArray(PropertyType.INTEGER_ARRAY, monthsArray);
                java.sql.Array daysArray = resultSet.getArray(new ArrayList<>(aliasedDays).get(aliasedDays.size() - 1));
                Integer[] daysIntegers = (Integer[]) this.sqlgGraph.getSqlDialect().convertArray(PropertyType.INTEGER_ARRAY, daysArray);
                Period[] periods = new Period[yearsIntegers.length];
                count = 0;
                for (Integer year : yearsIntegers) {
                    periods[count] = Period.of(year, monthsIntegers[count], daysIntegers[count++]);
                }
                this.properties.put(columnName, periods);
                break;
            case JSON_ARRAY:
                array = (java.sql.Array) o;
                this.properties.put(columnName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                break;
            default:
                throw SqlgExceptions.invalidPropertyType(propertyType);
        }
    }

    public abstract void loadResultSet(ResultSet resultSet, SchemaTableTree schemaTableTree) throws SQLException;

    public abstract void loadLabeledResultSet(ResultSet resultSet, Multimap<String, Integer> columnMap, SchemaTableTree schemaTableTree) throws SQLException;

    public abstract void loadResultSet(ResultSet resultSet) throws SQLException;

}
