package org.umlg.sqlg.structure;

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
import org.umlg.sqlg.sql.parse.AliasMapHolder;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.sql.parse.SchemaTableTree;
import org.umlg.sqlg.strategy.Emit;
import org.umlg.sqlg.util.SqlgUtil;

import java.lang.reflect.Array;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

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
        if (!this.sqlgGraph.tx().isInStreamingBatchMode() && !this.sqlgGraph.tx().isInStreamingFixedBatchMode()) {
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
        if (!this.sqlgGraph.tx().isInStreamingBatchMode()) {
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
        if (!this.sqlgGraph.tx().isInStreamingBatchMode()) {
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
        SqlgCompiledResultIterator<Pair<E, Multimap<String, Emit<E>>>> resultIterator = new SqlgCompiledResultIterator<>();
        SchemaTableTree rootSchemaTableTree = this.sqlgGraph.getGremlinParser().parse(schemaTable, replacedSteps);
        AliasMapHolder aliasMapHolder = rootSchemaTableTree.getAliasMapHolder();
        List<LinkedList<SchemaTableTree>> distinctQueries = rootSchemaTableTree.constructDistinctQueries();
        for (LinkedList<SchemaTableTree> distinctQueryStack : distinctQueries) {
            String sql = rootSchemaTableTree.constructSql(distinctQueryStack);
            try {
                Connection conn = this.sqlgGraph.tx().getConnection();
                if (logger.isDebugEnabled()) {
                    logger.debug(sql);
                }
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                    preparedStatement.setLong(1, this.recordId.getId());
                    SqlgUtil.setParametersOnStatement(this.sqlgGraph, distinctQueryStack, conn, preparedStatement, 2);
                    ResultSet resultSet = preparedStatement.executeQuery();
                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

                    SqlgUtil.loadResultSetIntoResultIterator(
                            this.sqlgGraph,
                            resultSetMetaData, resultSet,
                            rootSchemaTableTree, distinctQueryStack,
                            aliasMapHolder,
                            resultIterator);

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            } finally {
                if (rootSchemaTableTree != null)
                    rootSchemaTableTree.resetThreadVars();
            }
        }
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
            return this.id().hashCode();
        }
    }

    protected Object convertObjectArrayToPrimitiveArray(Object[] value, int baseType) {
        if (value instanceof String[]) {
            return value;
        }
        switch (baseType) {
            case Types.BIT:
                return copy(value, new boolean[value.length]);
            case Types.BOOLEAN:
                return copy(value, new boolean[value.length]);
            case Types.TINYINT:
                return copyToTinyInt((Integer[]) value, new byte[value.length]);
            case Types.SMALLINT:
                return copyToSmallInt(value, new short[value.length]);
            case Types.INTEGER:
                return copy(value, new int[value.length]);
            case Types.BIGINT:
                return copy(value, new long[value.length]);
            case Types.REAL:
                return copy(value, new float[value.length]);
            case Types.DOUBLE:
                return copy(value, new double[value.length]);
            case Types.VARCHAR:
                return copy(value, new String[value.length]);
        }
        if (value instanceof Integer[]) {
            switch (baseType) {
                case Types.TINYINT:
                    return copyToTinyInt((Integer[]) value, new byte[value.length]);
                case Types.SMALLINT:
                    return copyToSmallInt(value, new short[value.length]);
                default:
                    return copy(value, new int[value.length]);
            }
        }
        if (value instanceof Long[]) {
            return copy(value, new long[value.length]);
        }
        if (value instanceof Double[]) {
            return copy(value, new double[value.length]);
        }
        if (value instanceof Float[]) {
            return copy(value, new float[value.length]);
        }
        if (value instanceof Boolean[]) {
            return copy(value, new boolean[value.length]);
        }
        if (value instanceof Character[]) {
            return copy(value, new char[value.length]);
        }
        throw new IllegalArgumentException(
                String.format("%s[] is not a supported property value type",
                        value.getClass().getComponentType().getName()));
    }

    private <T> T copy(Object[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException("Property array value elements may not be null.");
            }
            Array.set(target, i, value[i]);
        }
        return target;
    }

    private <T> T copyToTinyInt(Integer[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException("Property array value elements may not be null.");
            }
            Array.set(target, i, value[i].byteValue());
        }
        return target;
    }

    private <T> T copyToSmallInt(Object[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException("Property array value elements may not be null.");
            }
            Array.set(target, i, ((Number) value[i]).shortValue());
        }
        return target;
    }

    public static int setKeyValuesAsParameter(SqlgGraph sqlgGraph, int i, Connection conn, PreparedStatement preparedStatement, Map<String, Object> keyValues) throws SQLException {
        List<ImmutablePair<PropertyType, Object>> typeAndValues = SqlgUtil.transformToTypeAndValue(keyValues);
        i = setKeyValueAsParameter(sqlgGraph, i, conn, preparedStatement, typeAndValues);
        return i;
    }

    public static int setKeyValuesAsParameter(SqlgGraph sqlgGraph, int parameterStartIndex, Connection conn, PreparedStatement preparedStatement, Multimap<String, Object> keyValues) throws SQLException {
        List<ImmutablePair<PropertyType, Object>> typeAndValues = SqlgUtil.transformToTypeAndValue(keyValues);
        return setKeyValueAsParameter(sqlgGraph, parameterStartIndex, conn, preparedStatement, typeAndValues);
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

    protected void loadProperty(ResultSetMetaData resultSetMetaData, ResultSet resultSet, int i, String columnName, Object o) throws SQLException {
        if (columnName.endsWith(SchemaManager.ZONEID) ||
                columnName.endsWith(SchemaManager.MONTHS) ||
                columnName.endsWith(SchemaManager.DAYS) ||
                columnName.endsWith(SchemaManager.DURATION_NANOS)
                ) {
            return;
        }
        int type = resultSetMetaData.getColumnType(i);
        switch (type) {
            case Types.SMALLINT:
                this.properties.put(columnName, ((Integer) o).shortValue());
                break;
            case Types.TINYINT:
                this.properties.put(columnName, ((Integer) o).byteValue());
                break;
            case Types.BIGINT:
                PropertyType propertyType = this.sqlgGraph.getSchemaManager().getTableFor(getSchemaTablePrefixed()).get(columnName);
                switch (propertyType) {
                    case DURATION:
//                        long seconds = (Long) o;
                        //load the months and days as its needed to construct the Period
//                        Collection<String> aliasedNanos = SchemaTableTree.threadLocalColumnNameAliasMap.get().get(getSchemaTablePrefixed() + "." + columnName + propertyType.getPostFixes()[0]);
//                        if (aliasedNanos.isEmpty()) {
//                            aliasedNanos = Arrays.asList(columnName + propertyType.getPostFixes()[0]);
//                        }
//                        int nanos = resultSet.getInt(aliasedNanos.iterator().next());
//                        this.properties.put(columnName, Duration.ofSeconds(seconds, nanos));
                        throw new IllegalStateException("Duration not yet supported!");
                    default:
                        this.properties.put(columnName, o);
                }
                break;
            case Types.INTEGER:
//                propertyType = this.sqlgGraph.getSchemaManager().getAllTables().get(getSchemaTablePrefixed().toString()).get(columnName);
                propertyType = this.sqlgGraph.getSchemaManager().getTableFor(getSchemaTablePrefixed()).get(columnName);
                switch (propertyType) {
                    case PERIOD:
//                        int years = (Integer) o;
//                        //load the months and days as its needed to construct the Period
//                        Collection<String> aliasedMonth = SchemaTableTree.threadLocalColumnNameAliasMap.get().get(getSchemaTablePrefixed() + "." + columnName + propertyType.getPostFixes()[0]);
//                        if (aliasedMonth.isEmpty()) {
//                            aliasedMonth = Arrays.asList(columnName + propertyType.getPostFixes()[0]);
//                        }
//                        int months = resultSet.getInt(aliasedMonth.iterator().next());
//                        Collection<String> aliasedDay = SchemaTableTree.threadLocalColumnNameAliasMap.get().get(getSchemaTablePrefixed() + "." + columnName + propertyType.getPostFixes()[1]);
//                        if (aliasedDay.isEmpty()) {
//                            aliasedDay = Arrays.asList(columnName + propertyType.getPostFixes()[1]);
//                        }
//                        int days = resultSet.getInt(aliasedDay.iterator().next());
//                        this.properties.put(columnName, Period.of(years, months, days));
                        throw new IllegalStateException("Period not yet supported!");
                    default:
                        this.properties.put(columnName, o);
                }
                break;
            case Types.REAL:
                this.properties.put(columnName, ((Number) o).floatValue());
                break;
            case Types.DOUBLE:
                this.properties.put(columnName, ((Number) o).doubleValue());
                break;
            case Types.DATE:
                this.properties.put(columnName, ((Date) o).toLocalDate());
                break;
            case Types.TIMESTAMP:
                propertyType = this.sqlgGraph.getSchemaManager().getTableFor(getSchemaTablePrefixed()).get(columnName);
                switch (propertyType) {
                    case LOCALDATETIME:
                        this.properties.put(columnName, ((Timestamp) o).toLocalDateTime());
                        break;
                    case ZONEDDATETIME:
//                        //load the months and days as its needed to construct the Period
//                        Collection<String> zonedId = SchemaTableTree.threadLocalColumnNameAliasMap.get().get(getSchemaTablePrefixed() + "." + columnName + propertyType.getPostFixes()[0]);
//                        if (zonedId.isEmpty()) {
//                            zonedId = Arrays.asList(columnName + propertyType.getPostFixes()[0]);
//                        }
//                        String zoneId = resultSet.getString(zonedId.iterator().next());
//                        ZoneId zoneId1 = ZoneId.of(zoneId);
//                        ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(((Timestamp) o).toLocalDateTime(), zoneId1);
//                        this.properties.put(columnName, zonedDateTimeAGT);
                        throw new IllegalStateException("ZonedDateTime not yet supported!");
                    default:
                        throw new IllegalStateException("Database timestamp column must be either for a LocalDateTime or ZonedDateTime, found " + propertyType.name());
                }
                break;
            case Types.TIME:
                this.properties.put(columnName, ((Time) o).toLocalTime());
                break;
            case Types.ARRAY:
                java.sql.Array array = (java.sql.Array) o;
                int baseType = array.getBaseType();
                Object[] objectArray = (Object[]) array.getArray();
                this.properties.put(columnName, convertObjectArrayToPrimitiveArray(objectArray, baseType));
                break;
            case Types.OTHER:
                sqlgGraph.getSqlDialect().handleOther(this.properties, columnName, o);
                break;
            default:
                this.properties.put(columnName, o);
        }
    }

    public abstract void loadResultSet(ResultSet resultSet, SchemaTableTree schemaTableTree) throws SQLException;

    public abstract void loadLabeledResultSet(ResultSet resultSet, Multimap<String, Integer> columnMap, SchemaTableTree schemaTableTree) throws SQLException;

    public abstract void loadResultSet(ResultSet resultSet) throws SQLException;

}
