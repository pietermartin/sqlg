package org.umlg.sqlg.structure;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.topology.AbstractLabel;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.util.SqlgUtil;

import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.umlg.sqlg.sql.parse.SchemaTableTree.ALIAS_SEPARATOR;
import static org.umlg.sqlg.structure.PropertyType.*;
import static org.umlg.sqlg.structure.topology.Topology.EDGE_PREFIX;
import static org.umlg.sqlg.structure.topology.Topology.VERTEX_PREFIX;

/**
 * Date: 2014/07/12
 * Time: 5:40 AM
 */
public abstract class SqlgElement implements Element {

    private static final Logger logger = LoggerFactory.getLogger(SqlgVertex.class);

    String schema;
    String table;
    RecordId recordId;
    final SqlgGraph sqlgGraph;
    //Multiple threads can access the same element
    Map<String, Object> properties = new ConcurrentHashMap<>();
    private final SqlgElementElementPropertyRollback elementPropertyRollback;
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

    public SqlgElement(SqlgGraph sqlgGraph, List<Comparable> identifiers, String schema, String table) {
        if (table.startsWith(VERTEX_PREFIX) || table.startsWith(EDGE_PREFIX)) {
            throw new IllegalStateException("SqlgElement.table may not be prefixed with " + VERTEX_PREFIX + " or " + EDGE_PREFIX);
        }
        this.sqlgGraph = sqlgGraph;
        this.schema = schema;
        this.table = table;
        this.recordId = RecordId.from(SchemaTable.of(this.schema, this.table), identifiers);
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
        if (this.recordId.hasSequenceId()) {
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
            sql.append(" = ?");
        } else {
            int count = 1;
            Schema schema = this.sqlgGraph.getTopology().getSchema(this.schema).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found.", this.schema)));
            AbstractLabel abstractLabel = getAbstractLabel(schema);
            for (String identifier : abstractLabel.getIdentifiers()) {
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                sql.append(" = ?");
                if (count++ < this.recordId.getIdentifiers().size()) {
                    sql.append(" AND ");
                }
            }
        }
        if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sql.toString());
        }
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            if (this.recordId.hasSequenceId()) {
                preparedStatement.setLong(1, this.recordId.sequenceId());
            } else {
                int count = 1;
                for (Comparable identifier : this.recordId.getIdentifiers()) {
                    preparedStatement.setObject(count++, identifier);
                }
            }
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

    public <V> V value(final String key) throws NoSuchElementException {
        Object value = this.properties.get(key);
        if (value != null) {
            //noinspection unchecked
            return (V) value;
        } else {
            return this.<V>property(key).orElseThrow(() -> Property.Exceptions.propertyDoesNotExist(this,key));
        }
    }

    @Override
    public <V> Property<V> property(String key) {
        if (this.removed) {
            throw new IllegalStateException(String.format("%s with id %s was removed.", getClass().getSimpleName(), id().toString()));
        } else {
            load();
            @SuppressWarnings("unchecked")
            V propertyValue = (V) this.properties.get(key);
            if (propertyValue != null) {
                return instantiateProperty(key, propertyValue);
            } else {
                return emptyProperty();
            }
        }
    }

    <V> Property<V> emptyProperty() {
        return Property.empty();
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        ElementHelper.validateProperty(key, value);
        this.sqlgGraph.tx().readWrite();
        this.sqlgGraph.getSqlDialect().validateProperty(key, value);
        this.sqlgGraph.getTopology().threadWriteLock();
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
            Map<String, PropertyType> columns = new HashMap<>();
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

    <V> SqlgProperty<V> instantiateProperty(String key, V value) {
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
            AbstractLabel abstractLabel;
            Map<String, PropertyColumn> properties;
            if (this instanceof Vertex) {
                abstractLabel = this.sqlgGraph.getTopology()
                        .getSchema(this.schema).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", this.schema)))
                        .getVertexLabel(this.table).orElseThrow(() -> new IllegalStateException(String.format("VertexLabel %s not found", this.table)));
                properties = abstractLabel.getProperties();
            } else {
                abstractLabel = this.sqlgGraph.getTopology()
                        .getSchema(this.schema).orElseThrow(() -> new IllegalStateException(String.format("Schema %s not found", this.schema)))
                        .getEdgeLabel(this.table).orElseThrow(() -> new IllegalStateException(String.format("EdgeLabel %s not found", this.table)));
                properties = abstractLabel.getProperties();
            }
            //sync up the keyValueMap with its PropertyColumn

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
            if (abstractLabel.hasIDPrimaryKey()) {
                sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes("ID"));
                sql.append(" = ?");
            } else {
                int count = 1;
                for (String identifier : abstractLabel.getIdentifiers()) {
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(identifier));
                    sql.append(" = ?");
                    if (count++ < abstractLabel.getIdentifiers().size()) {
                        sql.append(" AND ");
                    }
                }
            }
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if (logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
            Connection conn = this.sqlgGraph.tx().getConnection();
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                Map<String, Object> keyValue = new HashMap<>(1);
                keyValue.put(key, value);
                // the index of the id column in the statement depend on how many columns we had to use to store that data type
                int idx = setKeyValuesAsParameter(this.sqlgGraph, 1, preparedStatement, keyValue);
                if (abstractLabel.hasIDPrimaryKey()) {
                    preparedStatement.setLong(idx, ((RecordId) this.id()).sequenceId());
                } else {
                    for (String identifier : abstractLabel.getIdentifiers()) {
                        keyValue = new HashMap<>(abstractLabel.getIdentifiers().size());
                        keyValue.put(identifier, value(identifier));
                        idx = setKeyValuesAsParameter(this.sqlgGraph, idx, preparedStatement, keyValue);
                    }
                }
                preparedStatement.executeUpdate();
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
            if (this.id() != null && (object instanceof SqlgElement) && ((SqlgElement) object).id() != null) {
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
        return SqlgUtil.setKeyValuesAsParameter(sqlgGraph, true, parameterStartIndex, preparedStatement, typeAndValues);
    }

    <V> Map<String, ? extends Property<V>> internalGetProperties(final String... propertyKeys) {
        load();
        Map<String, SqlgProperty<V>> properties = new HashMap<>();

        //Check the propertyKeys parameter
        if (propertyKeys.length > 0) {
            for (String propertyKey : propertyKeys) {
                if (!propertyKey.equals(Topology.ID)) {
                    @SuppressWarnings("unchecked")
                    V propertyValue = (V) this.properties.get(propertyKey);
                    if (propertyValue != null) {
                        properties.put(propertyKey, instantiateProperty(propertyKey, propertyValue));
                    }
                }
            }
        } else {
            for (Map.Entry<String, Object> propertyEntry : this.properties.entrySet()) {
                String key = propertyEntry.getKey();
                @SuppressWarnings("unchecked")
                V propertyValue = (V) propertyEntry.getValue();
                if (key.equals(Topology.ID) || propertyValue == null) {
                    continue;
                }
                properties.put(key, instantiateProperty(key, propertyValue));
            }
        }
        return properties;
    }

    void writeColumnNames(Map<String, Pair<PropertyType, Object>> keyValueMap, StringBuilder sql) {
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

    @SuppressWarnings("Duplicates")
    void writeColumnParameters(Map<String, Pair<PropertyType, Object>> keyValueMap, StringBuilder sql) {
        int i = 1;
        for (String column : keyValueMap.keySet()) {
            PropertyType propertyType = keyValueMap.get(column).getLeft();
            String[] sqlDefinitions = this.sqlgGraph.getSqlDialect().propertyTypeToSqlDefinition(propertyType);
            int count = 1;
            for (String ignore : sqlDefinitions) {
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

    @Override
    public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
        return SqlgElement.this.<V>internalGetProperties(propertyKeys).values().iterator();
    }

    public boolean loadProperty(
            ResultSet resultSet,
            String propertyName,
            int columnIndex,
            Map<String, String> columnNameAliasMap,
            int stepDepth,
            PropertyType propertyType
    ) throws SQLException {
        return loadProperty(resultSet, propertyName, columnIndex, columnNameAliasMap, stepDepth, propertyType, false);
    }

    /**
     * @return true if the property was setted, else false.
     * @throws SQLException
     */
    public boolean loadProperty(
            ResultSet resultSet,
            String propertyName,
            int columnIndex,
            Map<String, String> columnNameAliasMap,
            int stepDepth,
            PropertyType propertyType,
            boolean isAverage) throws SQLException {

        if (propertyName.endsWith(Topology.ZONEID) ||
                propertyName.endsWith(Topology.MONTHS) ||
                propertyName.endsWith(Topology.DAYS) ||
                propertyName.endsWith(Topology.DURATION_NANOS)
        ) {
            return false;
        }
        switch (propertyType.ordinal()) {
            case BOOLEAN_ORDINAL:
                boolean aBoolean = resultSet.getBoolean(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, aBoolean);
                    return true;
                } else {
                    return false;
                }
            case BYTE_ORDINAL:
                byte aByte = resultSet.getByte(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, aByte);
                    return true;
                } else {
                    return false;
                }
            case SHORT_ORDINAL:
                short s = resultSet.getShort(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, s);
                    return true;
                } else {
                    return false;
                }
            case INTEGER_ORDINAL:
                int anInt = resultSet.getInt(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, anInt);
                    return true;
                } else {
                    return false;
                }
            case LONG_ORDINAL:
                long aLong = resultSet.getLong(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, aLong);
                    return true;
                } else {
                    return false;
                }
            case FLOAT_ORDINAL:
                float aFloat = resultSet.getFloat(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, aFloat);
                    return true;
                } else {
                    return false;
                }
            case DOUBLE_ORDINAL:
                double aDouble = resultSet.getDouble(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, aDouble);
                    if (isAverage) {
                        long weight = resultSet.getLong(columnIndex + 1);
                        this.properties.put(propertyName, Pair.of(aDouble, weight));
                    }
                    return true;
                } else {
                    return false;
                }
            case STRING_ORDINAL:
                String string = resultSet.getString(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, string);
                    return true;
                } else {
                    return false;
                }
            case VARCHAR_ORDINAL:
                string = resultSet.getString(columnIndex);
                if (!resultSet.wasNull()) {
                    this.properties.put(propertyName, string);
                    return true;
                } else {
                    return false;
                }
            case LOCALDATE_ORDINAL:
                java.sql.Date date = resultSet.getDate(columnIndex);
                if (date != null) {
                    this.properties.put(propertyName, date.toLocalDate());
                    return true;
                } else {
                    return false;
                }
            case LOCALDATETIME_ORDINAL:
                Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                if (timestamp != null) {
                    this.properties.put(propertyName, timestamp.toLocalDateTime());
                    return true;
                } else {
                    return false;
                }
            case ZONEDDATETIME_ORDINAL:
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
                    return true;
                } else {
                    return false;
                }
            case LOCALTIME_ORDINAL:
                Time time = resultSet.getTime(columnIndex);
                if (time != null) {
                    this.properties.put(propertyName, time.toLocalTime());
                    return true;
                } else {
                    return false;
                }
            case PERIOD_ORDINAL:
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
                    return true;
                } else {
                    return false;
                }
            case DURATION_ORDINAL:
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
                    return true;
                } else {
                    return false;
                }
            case JSON_ORDINAL:
                Object object = resultSet.getObject(columnIndex);
                if (object != null) {
                    sqlgGraph.getSqlDialect().handleOther(this.properties, propertyName, object, propertyType);
                    return true;
                } else {
                    return false;
                }
            case POINT_ORDINAL:
                Object object1 = resultSet.getObject(columnIndex);
                if (object1 != null) {
                    sqlgGraph.getSqlDialect().handleOther(this.properties, propertyName, object1, propertyType);
                    return true;
                } else {
                    return false;
                }
            case LINESTRING_ORDINAL:
                Object object2 = resultSet.getObject(columnIndex);
                if (object2 != null) {
                    sqlgGraph.getSqlDialect().handleOther(this.properties, propertyName, object2, propertyType);
                    return true;
                } else {
                    return false;
                }
            case POLYGON_ORDINAL:
                Object object3 = resultSet.getObject(columnIndex);
                if (object3 != null) {
                    sqlgGraph.getSqlDialect().handleOther(this.properties, propertyName, object3, propertyType);
                    return true;
                } else {
                    return false;
                }
            case GEOGRAPHY_POINT_ORDINAL:
                Object object4 = resultSet.getObject(columnIndex);
                if (object4 != null) {
                    sqlgGraph.getSqlDialect().handleOther(this.properties, propertyName, object4, propertyType);
                    return true;
                } else {
                    return false;
                }
            case GEOGRAPHY_POLYGON_ORDINAL:
                Object object5 = resultSet.getObject(columnIndex);
                if (object5 != null) {
                    sqlgGraph.getSqlDialect().handleOther(this.properties, propertyName, object5, propertyType);
                    return true;
                } else {
                    return false;
                }
            case boolean_ARRAY_ORDINAL:
                java.sql.Array array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case BOOLEAN_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case byte_ARRAY_ORDINAL:
                Object object6 = resultSet.getObject(columnIndex);
                if (object6 != null) {
                    this.properties.put(propertyName, object6);
                    return true;
                } else {
                    return false;
                }
            case BYTE_ARRAY_ORDINAL:
                Object object7 = resultSet.getObject(columnIndex);
                if (object7 != null) {
                    this.properties.put(propertyName, SqlgUtil.convertPrimitiveByteArrayToByteArray((byte[]) object7));
                    return true;
                } else {
                    return false;
                }
            case short_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case SHORT_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case int_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case INTEGER_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case long_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case LONG_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case float_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case FLOAT_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case double_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case DOUBLE_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case STRING_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case LOCALDATETIME_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case LOCALDATE_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case LOCALTIME_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            case ZONEDDATETIME_ARRAY_ORDINAL:
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
                    return true;
                } else {
                    return false;
                }
            case DURATION_ARRAY_ORDINAL:
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
                    return true;
                } else {
                    return false;
                }
            case PERIOD_ARRAY_ORDINAL:
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
                    return true;
                } else {
                    return false;
                }
            case JSON_ARRAY_ORDINAL:
                array = resultSet.getArray(columnIndex);
                if (array != null) {
                    this.properties.put(propertyName, this.sqlgGraph.getSqlDialect().convertArray(propertyType, array));
                    return true;
                } else {
                    return false;
                }
            default:
                throw SqlgExceptions.invalidPropertyType(propertyType);
        }

    }

    public void internalSetProperty(String property, Object value) {
        this.properties.put(property, value);
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

    public long getInternalStartTraverserIndex() {
        return this.internalStartTraverserIndex;
    }

    public void setInternalStartTraverserIndex(long internalStartTraverserIndex) {
        this.internalStartTraverserIndex = internalStartTraverserIndex;
    }

    void appendProperties(AbstractLabel edgeLabel, StringBuilder sql) {
        for (PropertyColumn propertyColumn : edgeLabel.getProperties().values()) {
            sql.append(", ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(propertyColumn.getName()));
            // additional columns for time zone, etc.
            String[] ps = propertyColumn.getPropertyType().getPostFixes();
            if (ps != null) {
                for (String p : propertyColumn.getPropertyType().getPostFixes()) {
                    sql.append(", ");
                    sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(propertyColumn.getName() + p));
                }
            }
        }
    }

    abstract AbstractLabel getAbstractLabel(Schema schema);
}
