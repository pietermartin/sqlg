package org.umlg.sqlg.structure;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.umlg.sqlg.sql.parse.SchemaTableTree.ALIAS_SEPARATOR;
import static org.umlg.sqlg.structure.Topology.EDGE_PREFIX;
import static org.umlg.sqlg.structure.Topology.VERTEX_PREFIX;

/**
 * Date: 2014/07/12
 * Time: 5:40 AM
 */
public abstract class SqlgElement implements Element {

    private Logger logger = LoggerFactory.getLogger(SqlgVertex.class.getName());

    protected String schema;
    protected String table;
    RecordId recordId;
    protected final SqlgGraph sqlgGraph;
    //Multiple threads can access the same element
    protected Map<String, Object> properties = new ConcurrentHashMap<>();
    private SqlgElementElementPropertyRollback elementPropertyRollback;
    boolean removed = false;
    //Used in the SqlgBranchStepBarrier to sort the results by the start elements.
    private long internalStartTraverserIndex;

    public SqlgElement(SqlgGraph sqlgGraph, String schema, String table) {
        this.sqlgGraph = sqlgGraph;
        this.schema = schema;
        this.table = table;
        this.elementPropertyRollback = new SqlgElementElementPropertyRollback();
//        if (!this.graph.tx().isInStreamingBatchMode() && !this.graph.tx().isInStreamingWithLockBatchMode()) {
//            graph.tx().addElementPropertyRollback(this.elementPropertyRollback);
//        }
    }

    public SqlgElement(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        if (table.startsWith(VERTEX_PREFIX) || table.startsWith(EDGE_PREFIX)) {
            throw new IllegalStateException("SqlgElement.table may not be prefixed with " + VERTEX_PREFIX + " or " + EDGE_PREFIX);
        }
        this.sqlgGraph = sqlgGraph;
        this.schema = schema;
        this.table = table;
        this.recordId = RecordId.from(SchemaTable.of(this.schema, this.table), id);
        this.elementPropertyRollback = new SqlgElementElementPropertyRollback();
//        if (!this.graph.tx().isInStreamingBatchMode() && !this.graph.tx().isInStreamingWithLockBatchMode()) {
//            graph.tx().addElementPropertyRollback(this.elementPropertyRollback);
//        }
    }

    @Override
    public Graph graph() {
        return this.sqlgGraph;
    }

    private class SqlgElementElementPropertyRollback implements ElementPropertyRollback {
        @Override
        public void clearProperties() {
            SqlgElement.this.properties.clear();
        }

//        @Override
//        public boolean equals(Object object) {
//            return SqlgElement.this.equals(object);
//        }
//
//        @Override
//        public int hashCode() {
//            return SqlgElement.this.hashCode();
//        }
    }

    public void setInternalPrimaryKey(RecordId recordId) {
        this.recordId = recordId;
    }

    public abstract SchemaTable getSchemaTablePrefixed();

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
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema));
        sql.append(".");
        sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes((this instanceof Vertex ? VERTEX_PREFIX : EDGE_PREFIX) + this.table));
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
            preparedStatement.setLong(1, ((RecordId) this.id()).getId());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        this.removed = true;
        removeGlobalUniqueIndex();
    }

    private void removeGlobalUniqueIndex() {
        Map<String, PropertyColumn> properties = this.sqlgGraph.getTopology().getPropertiesWithGlobalUniqueIndexFor(this.getSchemaTablePrefixed());
        for (PropertyColumn propertyColumn : properties.values()) {
            for (GlobalUniqueIndex globalUniqueIndex : propertyColumn.getGlobalUniqueIndices()) {

                StringBuilder sql = new StringBuilder("DELETE FROM ");
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA));
                sql.append(".");
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(VERTEX_PREFIX + globalUniqueIndex.getName()));
                sql.append(" WHERE ");
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("recordId"));
                sql.append(" = ? AND ");
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("property"));
                sql.append(" = ?");
                if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                    sql.append(";");
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(sql.toString());
                }
                Connection conn = this.sqlgGraph.tx().getConnection();
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                    preparedStatement.setString(1, this.id().toString());
                    preparedStatement.setString(2, propertyColumn.getName());
                    preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            }
        }
    }

    @Override
    public Set<String> keys() {
        this.sqlgGraph.tx().readWrite();
        return this.internalGetProperties().keySet();
    }

    @Override
    public <V> Property<V> property(String key) {
        if (this.removed) {
            throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id());
        } else {
            load();
            V propertyValue = (V) this.properties.get(key);
            if (propertyValue != null) {
                return instantiateProperty(key, propertyValue);
            } else {
                return emptyProperty();
            }
        }
    }

    protected <V> Property<V> emptyProperty() {
        return Property.empty();
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        ElementHelper.validateProperty(key, value);
        this.sqlgGraph.tx().readWrite();
        this.sqlgGraph.getSqlDialect().validateProperty(key, value);
        if (!this.sqlgGraph.tx().isInStreamingBatchMode() && !this.sqlgGraph.tx().isInStreamingWithLockBatchMode()) {
            sqlgGraph.tx().addElementPropertyRollback(this.elementPropertyRollback);
        }
        //Validate the property
        PropertyType.from(value);
        //Check if column exist
        if (this instanceof Vertex) {
            Map<String, PropertyType> columns = new HashMap<>();
            columns.put(key, PropertyType.from(value));
            this.sqlgGraph.getTopology().ensureVertexLabelPropertiesExist(
                    this.schema,
                    this.table,
                    columns
            );
        } else {
            Map<String, PropertyType> columns = new HashedMap<>();
            columns.put(key, PropertyType.from(value));
            this.sqlgGraph.getTopology().ensureEdgePropertiesExist(
                    this.schema,
                    this.table,
                    columns);
        }
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
        if (this.sqlgGraph.getSqlDialect().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            elementInInsertedCache = this.sqlgGraph.tx().getBatchManager().updateProperty(this, key, value);
        }

        if (!elementInInsertedCache) {

            Object oldValue = this.property(key).orElse(null);
            if (oldValue != null && oldValue.equals(value)) {
                return;
            }

            //TODO needs optimizing, firing this all the time when there probably are no GlobalUniqueIndexes is a tad dum.
            //GlobalUniqueIndex
            Map<String, PropertyColumn> properties;
            if (this instanceof Vertex) {
                properties = this.sqlgGraph.getTopology()
                        .getSchema(this.schema).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", this.schema)))
                        .getVertexLabel(this.table).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel %s not found", this.table)))
                        .getProperties();
            } else {
                properties = this.sqlgGraph.getTopology()
                        .getSchema(this.schema).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", this.schema)))
                        .getEdgeLabel(this.table).orElseThrow(() -> new IllegalStateException(String.format("EdgeLabel %s not found", this.table)))
                        .getProperties();
            }
            //sync up the keyValueMap with its PropertyColumn
            PropertyColumn propertyColumn = properties.get(key);
            Pair<PropertyColumn, Object> propertyColumnObjectPair = Pair.of(propertyColumn, value);
            for (GlobalUniqueIndex globalUniqueIndex : propertyColumn.getGlobalUniqueIndices()) {
                SqlgElement.updateGlobalUniqueIndex(this.sqlgGraph, globalUniqueIndex, this.recordId, propertyColumnObjectPair);
            }

            String tableName = (this instanceof Vertex ? VERTEX_PREFIX : EDGE_PREFIX) + this.table;
            StringBuilder sql = new StringBuilder("UPDATE ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(tableName));
            sql.append(" SET ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(key));
            sql.append(" = ?");
            // some data types require several columns in the db, make sure to update them all
            PropertyType pt = PropertyType.from(value);
            String[] postfixes = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(pt);
            if (postfixes != null && postfixes.length > 1) {
                for (int i = 1; i < postfixes.length; i++) {
                    sql.append(",");
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(key + pt.getPostFixes()[i - 1]));
                    sql.append(" = ?");
                }
            }

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
                // the index of the id column in the statement depend on how many columns we had to use to store that data type
                int idx = setKeyValuesAsParameter(this.sqlgGraph, 1, preparedStatement, keyValue);
                preparedStatement.setLong(idx, ((RecordId) this.id()).getId());
                preparedStatement.executeUpdate();
                preparedStatement.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        //Cache the properties
        this.properties.put(key, value);
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
        if (this.id() != null) {
            return id().hashCode();
        }
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            return super.hashCode();
        } else {
            return id().hashCode();
        }
    }


    private static int setKeyValuesAsParameter(SqlgGraph sqlgGraph, int i, PreparedStatement preparedStatement, Map<String, Object> keyValues) throws SQLException {
        List<ImmutablePair<PropertyType, Object>> typeAndValues = SqlgUtil.transformToTypeAndValue(keyValues);
        i = setKeyValueAsParameter(sqlgGraph, i, preparedStatement, typeAndValues);
        return i;
    }

    private static int setKeyValueAsParameter(SqlgGraph sqlgGraph, int parameterStartIndex, PreparedStatement preparedStatement, List<ImmutablePair<PropertyType, Object>> typeAndValues) throws SQLException {
        return SqlgUtil.setKeyValuesAsParameter(sqlgGraph, true, parameterStartIndex++, preparedStatement, typeAndValues);
    }

    protected <V> Map<String, ? extends Property<V>> internalGetProperties(final String... propertyKeys) {
        load();
        Map<String, SqlgProperty<V>> properties = new HashMap<>();

        //Check the propertyKeys parameter
        if (propertyKeys.length > 0) {
            for (String propertyKey : propertyKeys) {
                if (!propertyKey.equals(Topology.ID)) {
                    V propertyValue = (V) this.properties.get(propertyKey);
                    if (propertyValue != null) {
                        properties.put(propertyKey, instantiateProperty(propertyKey, propertyValue));
                    }
                }
            }
        } else {
            for (Map.Entry<String, Object> propertyEntry : this.properties.entrySet()) {
                String key = propertyEntry.getKey();
                V propertyValue = (V) propertyEntry.getValue();
                if (key.equals(Topology.ID) || propertyValue == null) {
                    continue;
                }
                properties.put(key, instantiateProperty(key, propertyValue));
            }
        }
        return properties;
    }

    protected void writeColumnNames(Map<String, Pair<PropertyType, Object>> keyValueMap, StringBuilder sql) {
        int i = 1;
        for (String column : keyValueMap.keySet()) {
            Pair<PropertyType, Object> propertyColumnValue = keyValueMap.get(column);
            PropertyType propertyType = propertyColumnValue.getLeft();
            String[] sqlDefinitions = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
            int count = 1;
            for (@SuppressWarnings("unused") String sqlDefinition : sqlDefinitions) {
                if (count > 1) {
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(column + propertyType.getPostFixes()[count - 2]));
                } else {
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(column));
                }
                if (count++ < sqlDefinitions.length) {
                    sql.append(",");
                }
            }
            if (i++ < keyValueMap.size()) {
                sql.append(", ");
            }
        }
    }

    protected void writeColumnParameters(Map<String, Pair<PropertyType, Object>> keyValueMap, StringBuilder sql) {
        int i = 1;
        for (String column : keyValueMap.keySet()) {
            PropertyType propertyType = keyValueMap.get(column).getLeft();
            String[] sqlDefinitions = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
            int count = 1;
            for (@SuppressWarnings("unused") String sqlDefinition : sqlDefinitions) {
                if (count > 1) {
                    sql.append("?");
                } else {
                    sql.append("?");
                }
                if (count++ < sqlDefinitions.length) {
                    sql.append(",");
                }
            }
            if (i++ < keyValueMap.size()) {
                sql.append(", ");
            }
        }
    }

    protected void insertGlobalUniqueIndex(Map<String, Object> keyValueMap, Map<String, PropertyColumn> propertyColumns) {
        for (PropertyColumn propertyColumn : propertyColumns.values()) {
            for (GlobalUniqueIndex globalUniqueIndex : propertyColumn.getGlobalUniqueIndices()) {
                Object value = keyValueMap.get(propertyColumn.getName());
                Pair<PropertyColumn, Object> propertyColumnObjectPair = Pair.of(propertyColumn, value);
                this.insertGlobalUniqueIndex(this.sqlgGraph, globalUniqueIndex, propertyColumnObjectPair);
            }
        }
    }

    private void insertGlobalUniqueIndex(SqlgGraph sqlgGraph, GlobalUniqueIndex globalUniqueIndex, Pair<PropertyColumn, Object> propertyColumnObjectPair) {
        if (propertyColumnObjectPair.getRight() != null) {
            sqlgGraph.addVertex(
                    T.label, Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "." + globalUniqueIndex.getName(),
                    GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE, propertyColumnObjectPair.getValue(),
                    GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID, this.recordId.toString(),
                    GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_PROPERTY_NAME, propertyColumnObjectPair.getKey().getName()
            );
        } else {
            //Need to insert null values else the update the code will not work.
            if (sqlgGraph.getSqlDialect().uniqueIndexConsidersNullValuesEqual()) {
                sqlgGraph.addVertex(
                        T.label, Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "." + globalUniqueIndex.getName(),
                        GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE, "dummy_" + UUID.randomUUID().toString(),
                        GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID, this.recordId.toString(),
                        GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_PROPERTY_NAME, propertyColumnObjectPair.getKey().getName()
                );
            } else {
                sqlgGraph.addVertex(
                        T.label, Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "." + globalUniqueIndex.getName(),
                        GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID, this.recordId.toString(),
                        GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_PROPERTY_NAME, propertyColumnObjectPair.getKey().getName()
                );
            }
        }
    }

    private static void updateGlobalUniqueIndex(SqlgGraph sqlgGraph, GlobalUniqueIndex globalUniqueIndex, RecordId recordId, Pair<PropertyColumn, Object> propertyColumnObjectPair) {
        List<Vertex> globalUniqueIndexVertexes = sqlgGraph.globalUniqueIndexes()
                .V().hasLabel(Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "." + globalUniqueIndex.getName())
                .has(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID, recordId.toString())
                .has(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_PROPERTY_NAME, propertyColumnObjectPair.getKey().getName())
                .toList();

        Preconditions.checkState(globalUniqueIndexVertexes.size() <= 1, "More than one GlobalUniqueIndex for %s and recordId %s found", Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "." + globalUniqueIndex.getName(), recordId.toString());
        if (!globalUniqueIndexVertexes.isEmpty()) {
            Vertex globalUniqueIndexVertex = globalUniqueIndexVertexes.get(0);
            globalUniqueIndexVertex.property(GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE, propertyColumnObjectPair.getValue());
        } else {
            //This happens if the property is not present at all in which case the entry in the GlobalUniqueIndex table has not been created yet.
            sqlgGraph.addVertex(
                    T.label, Schema.GLOBAL_UNIQUE_INDEX_SCHEMA + "." + globalUniqueIndex.getName(),
                    GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_VALUE, propertyColumnObjectPair.getValue(),
                    GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_RECORD_ID, recordId.toString(),
                    GlobalUniqueIndex.GLOBAL_UNIQUE_INDEX_PROPERTY_NAME, propertyColumnObjectPair.getKey().getName()
            );
        }
    }

    @Override
    public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
//        SqlgElement.this.sqlgGraph.tx().readWrite();
        return SqlgElement.this.<V>internalGetProperties(propertyKeys).values().iterator();
    }

    public void loadProperty(ResultSet resultSet, String propertyName, int columnIndex, Map<String, String> columnNameAliasMap, int stepDepth, PropertyType propertyType) throws SQLException {
        if (propertyName.endsWith(Topology.ZONEID) ||
                propertyName.endsWith(Topology.MONTHS) ||
                propertyName.endsWith(Topology.DAYS) ||
                propertyName.endsWith(Topology.DURATION_NANOS)
                ) {
            return;
        }
        switch (propertyType) {

            case BOOLEAN:
                boolean aBoolean = resultSet.getBoolean(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, aBoolean);
                }
                break;
            case BYTE:
                byte aByte = resultSet.getByte(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, aByte);
                }
                break;
            case SHORT:
                short s = resultSet.getShort(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, s);
                }
                break;
            case INTEGER:
                int anInt = resultSet.getInt(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, anInt);
                }
                break;
            case LONG:
                long aLong = resultSet.getLong(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, aLong);
                }
                break;
            case FLOAT:
                float aFloat = resultSet.getFloat(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, aFloat);
                }
                break;
            case DOUBLE:
                double aDouble = resultSet.getDouble(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, aDouble);
                }
                break;
            case STRING:
                String string = resultSet.getString(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, string);
                }
                break;
            case LOCALDATE:
                java.sql.Date date = resultSet.getDate(columnIndex);
                if (date != null) {
                    this.properties.put(propertyName, date.toLocalDate());
                }
                break;
            case LOCALDATETIME:
                Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                if (timestamp != null) {
                    this.properties.put(propertyName, timestamp.toLocalDateTime());
                }
                break;
            case ZONEDDATETIME:
                Timestamp timestamp1 = resultSet.getTimestamp(columnIndex);
                if (timestamp1 != null) {
                    String zoneIdColumn = stepDepth + ALIAS_SEPARATOR + getSchemaTablePrefixed().toString().replace(".", ALIAS_SEPARATOR) + ALIAS_SEPARATOR + propertyName + propertyType.getPostFixes()[0];
                    String zonedId = columnNameAliasMap.get(zoneIdColumn);
                    if (zonedId == null) {
                        zonedId = propertyName + propertyType.getPostFixes()[0];
                    }
                    String zoneId = resultSet.getString(zonedId);
                    ZoneId zoneId1 = ZoneId.of(zoneId);
                    ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(timestamp1.toLocalDateTime(), zoneId1);
                    this.properties.put(propertyName, zonedDateTimeAGT);
                }
                break;
            case LOCALTIME:
                Time time = resultSet.getTime(columnIndex);
                if (time != null) {
                    this.properties.put(propertyName, time.toLocalTime());
                }
                break;
            case PERIOD:
                int years = resultSet.getInt(columnIndex);
                if (!resultSet.wasNull()) {
                    String monthColumn = stepDepth + ALIAS_SEPARATOR + getSchemaTablePrefixed().toString().replace(".", ALIAS_SEPARATOR) + ALIAS_SEPARATOR + propertyName + propertyType.getPostFixes()[0];
                    String aliasedMonth = columnNameAliasMap.get(monthColumn);
                    if (aliasedMonth == null) {
                        aliasedMonth = propertyName + propertyType.getPostFixes()[0];
                    }
                    int months = resultSet.getInt(aliasedMonth);
                    String dayColumn = stepDepth + ALIAS_SEPARATOR + getSchemaTablePrefixed().toString().replace(".", ALIAS_SEPARATOR) + ALIAS_SEPARATOR + propertyName + propertyType.getPostFixes()[1];
                    String aliasedDay = columnNameAliasMap.get(dayColumn);
                    if (aliasedDay == null) {
                        aliasedDay = propertyName + propertyType.getPostFixes()[1];
                    }
                    int days = resultSet.getInt(aliasedDay);
                    this.properties.put(propertyName, Period.of(years, months, days));
                }
                break;
            case DURATION:
                long seconds = resultSet.getLong(columnIndex);
                if (!resultSet.wasNull()) {
                    //load the months and days as its needed to construct the Period
                    String nanosColumn = stepDepth + ALIAS_SEPARATOR + getSchemaTablePrefixed().toString().replace(".", ALIAS_SEPARATOR) + ALIAS_SEPARATOR + propertyName + propertyType.getPostFixes()[0];
                    String aliasedNanos = columnNameAliasMap.get(nanosColumn);
                    if (aliasedNanos == null) {
                        aliasedNanos = propertyName + propertyType.getPostFixes()[0];
                    }
                    int nanos = resultSet.getInt(aliasedNanos);
                    this.properties.put(propertyName, Duration.ofSeconds(seconds, nanos));
                }
                break;
            case JSON:
                Object object = resultSet.getObject(columnIndex);
                if (object != null) {
                    sqlgGraph.getSqlDialect().handleOther(this.properties, propertyName, object, propertyType);
                }
                break;
            case POINT:
                Object object1 = resultSet.getObject(columnIndex);
                if (object1 != null) {
                    sqlgGraph.getSqlDialect().handleOther(this.properties, propertyName, object1, propertyType);
                }
                break;
            case LINESTRING:
                Object object2 = resultSet.getObject(columnIndex);
                if (object2 != null) {
                    sqlgGraph.getSqlDialect().handleOther(this.properties, propertyName, object2, propertyType);
                }
                break;
            case POLYGON:
                Object object3 = resultSet.getObject(columnIndex);
                if (object3 != null) {
                    sqlgGraph.getSqlDialect().handleOther(this.properties, propertyName, object3, propertyType);
                }
                break;
            case GEOGRAPHY_POINT:
                Object object4 = resultSet.getObject(columnIndex);
                if (object4 != null) {
                    sqlgGraph.getSqlDialect().handleOther(this.properties, propertyName, object4, propertyType);
                }
                break;
            case GEOGRAPHY_POLYGON:
                Object object5 = resultSet.getObject(columnIndex);
                if (object5 != null) {
                    sqlgGraph.getSqlDialect().handleOther(this.properties, propertyName, object5, propertyType);
                }
                break;
            case boolean_ARRAY:
                java.sql.Array array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case BOOLEAN_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case byte_ARRAY:
                Object object6 = resultSet.getObject(columnIndex);
                if (object6 != null) {
                    this.properties.put(propertyName, object6);
                }
                break;
            case BYTE_ARRAY:
                Object object7 = resultSet.getObject(columnIndex);
                if (object7 != null) {
                    this.properties.put(propertyName, SqlgUtil.convertPrimitiveByteArrayToByteArray((byte[]) object7));
                }
                break;
            case short_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case SHORT_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case int_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case INTEGER_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case long_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case LONG_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case float_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case FLOAT_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case double_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case DOUBLE_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case STRING_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case LOCALDATETIME_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case LOCALDATE_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case LOCALTIME_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            case ZONEDDATETIME_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    String zoneIdColumn = stepDepth + ALIAS_SEPARATOR + getSchemaTablePrefixed().toString().replace(".", ALIAS_SEPARATOR) + ALIAS_SEPARATOR + propertyName + propertyType.getPostFixes()[0];
                    String zonedId = columnNameAliasMap.get(zoneIdColumn);
                    if (zonedId == null) {
                        zonedId = propertyName + propertyType.getPostFixes()[0];
                    }
                    java.sql.Array zoneIdArray = resultSet.getArray(zonedId);
                    String[] objectZoneIdArray = (String[]) this.sqlgGraph.getSqlDialect().convertArray(PropertyType.STRING_ARRAY, zoneIdArray);
                    LocalDateTime[] localDateTimes = (LocalDateTime[]) this.sqlgGraph.getSqlDialect().convertArray(PropertyType.LOCALDATETIME_ARRAY, array);
                    ZonedDateTime[] zonedDateTimes = new ZonedDateTime[localDateTimes.length];
                    int count = 0;
                    for (LocalDateTime localDateTime : localDateTimes) {
                        ZoneId zoneId1 = ZoneId.of(objectZoneIdArray[count]);
                        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, zoneId1);
                        zonedDateTimes[count++] = zonedDateTime;
                    }
                    this.properties.put(propertyName, zonedDateTimes);
                }
                break;
            case DURATION_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    String nanosColumn = stepDepth + ALIAS_SEPARATOR + getSchemaTablePrefixed().toString().replace(".", ALIAS_SEPARATOR) + ALIAS_SEPARATOR + propertyName + propertyType.getPostFixes()[0];
                    String aliasedNanos = columnNameAliasMap.get(nanosColumn);
                    if (aliasedNanos == null) {
                        aliasedNanos = propertyName + propertyType.getPostFixes()[0];
                    }
                    long[] secondsArray = (long[]) this.sqlgGraph.getSqlDialect().convertArray(PropertyType.long_ARRAY, array);
                    java.sql.Array nanosArray = resultSet.getArray(aliasedNanos);
                    int[] nanoArray = (int[]) this.sqlgGraph.getSqlDialect().convertArray(PropertyType.int_ARRAY, nanosArray);
                    Duration[] durations = new Duration[secondsArray.length];
                    int count = 0;
                    for (Long second : secondsArray) {
                        durations[count] = Duration.ofSeconds(second, nanoArray[count++]);
                    }
                    this.properties.put(propertyName, durations);
                }
                break;
            case PERIOD_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    String monthsColumn = stepDepth + ALIAS_SEPARATOR + getSchemaTablePrefixed().toString().replace(".", ALIAS_SEPARATOR) + ALIAS_SEPARATOR + propertyName + propertyType.getPostFixes()[0];
                    String aliasedMonths = columnNameAliasMap.get(monthsColumn);
                    if (aliasedMonths == null) {
                        aliasedMonths = propertyName + propertyType.getPostFixes()[0];
                    }
                    String daysColumn = stepDepth + ALIAS_SEPARATOR + getSchemaTablePrefixed().toString().replace(".", ALIAS_SEPARATOR) + ALIAS_SEPARATOR + propertyName + propertyType.getPostFixes()[1];
                    String aliasedDays = columnNameAliasMap.get(daysColumn);
                    if (aliasedDays == null) {
                        aliasedDays = propertyName + propertyType.getPostFixes()[1];
                    }
                    Integer[] yearsIntegers = (Integer[]) this.sqlgGraph.getSqlDialect().convertArray(PropertyType.INTEGER_ARRAY, array);
                    java.sql.Array monthsArray = resultSet.getArray(aliasedMonths);
                    Integer[] monthsIntegers = (Integer[]) this.sqlgGraph.getSqlDialect().convertArray(PropertyType.INTEGER_ARRAY, monthsArray);
                    java.sql.Array daysArray = resultSet.getArray(aliasedDays);
                    Integer[] daysIntegers = (Integer[]) this.sqlgGraph.getSqlDialect().convertArray(PropertyType.INTEGER_ARRAY, daysArray);
                    Period[] periods = new Period[yearsIntegers.length];
                    int count = 0;
                    for (Integer year : yearsIntegers) {
                        periods[count] = Period.of(year, monthsIntegers[count], daysIntegers[count++]);
                    }
                    this.properties.put(propertyName, periods);
                }
                break;
            case JSON_ARRAY:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                }
                break;
            default:
                throw SqlgExceptions.invalidPropertyType(propertyType);
        }

    }

    void loadProperty(ResultSet resultSet, String propertyName, int columnIndex) throws SQLException {
        if (propertyName.endsWith(Topology.ZONEID) ||
                propertyName.endsWith(Topology.MONTHS) ||
                propertyName.endsWith(Topology.DAYS) ||
                propertyName.endsWith(Topology.DURATION_NANOS)
                ) {
            return;
        }
        PropertyType propertyType = this.sqlgGraph.getTopology().getTableFor(getSchemaTablePrefixed()).get(propertyName);
        loadProperty(resultSet, propertyName, columnIndex, Collections.emptyMap(), -1, propertyType);
    }

    public abstract void loadResultSet(ResultSet resultSet) throws SQLException;

    public long getInternalStartTraverserIndex() {
        return this.internalStartTraverserIndex;
    }

    public void setInternalStartTraverserIndex(long internalStartTraverserIndex) {
        this.internalStartTraverserIndex = internalStartTraverserIndex;
    }
}
