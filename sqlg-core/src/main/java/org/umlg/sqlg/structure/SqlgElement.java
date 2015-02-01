package org.umlg.sqlg.structure;

import com.google.common.collect.Multimap;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

/**
 * Date: 2014/07/12
 * Time: 5:40 AM
 */
public abstract class SqlgElement implements Element, Element.Iterators {

    private Logger logger = LoggerFactory.getLogger(SqlgVertex.class.getName());

    protected String schema;
    protected String table;
    protected final SqlgGraph sqlgGraph;
    protected long primaryKey;
    protected Map<String, Object> properties = new HashMap<>();
    private SqlgElementElementPropertyRollback elementPropertyRollback;
    protected boolean removed = false;

    public SqlgElement(SqlgGraph sqlgGraph, String schema, String table) {
        this.sqlgGraph = sqlgGraph;
        this.schema = schema;
        this.table = table;
        this.elementPropertyRollback = new SqlgElementElementPropertyRollback();
        sqlgGraph.tx().addElementPropertyRollback(this.elementPropertyRollback);
    }

    public SqlgElement(SqlgGraph sqlgGraph, Long id, String label) {
        this.sqlgGraph = sqlgGraph;
        this.primaryKey = id;
        SchemaTable schemaTable = SqlgUtil.parseLabel(label, this.sqlgGraph.getSqlDialect().getPublicSchema());
        this.schema = schemaTable.getSchema();
        this.table = schemaTable.getTable();
        this.elementPropertyRollback = new SqlgElementElementPropertyRollback();
        sqlgGraph.tx().addElementPropertyRollback(this.elementPropertyRollback);
    }

    public SqlgElement(SqlgGraph sqlgGraph, Long id, String schema, String table) {
        if (table.startsWith(SchemaManager.VERTEX_PREFIX) || table.startsWith(SchemaManager.EDGE_PREFIX)) {
            throw new IllegalStateException("SqlgElement.table may not be prefixed with " + SchemaManager.VERTEX_PREFIX + " or " + SchemaManager.EDGE_PREFIX);
        }
        this.sqlgGraph = sqlgGraph;
        this.primaryKey = id;
        this.schema = schema;
        this.table = table;
        this.elementPropertyRollback = new SqlgElementElementPropertyRollback();
        sqlgGraph.tx().addElementPropertyRollback(this.elementPropertyRollback);
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

    SchemaTable getSchemaTable() {
        return SchemaTable.of(this.getSchema(), this.getTable());
    }

    public void setInternalPrimaryKey(Long id) {
        this.primaryKey = id;
    }

    @Override
    public Object id() {
        return primaryKey;
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
            preparedStatement.setLong(1, (Long) this.id());
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

//    @Override
//    public Set<String> hiddenKeys() {
//        this.sqlgGraph.tx().readWrite();
//        return this.internalGetHiddens().keySet();
//    }

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
        sqlgGraph.tx().addElementPropertyRollback(this.elementPropertyRollback);
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
                preparedStatement.setLong(2, (Long) this.id());
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
            return super.equals(object);
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
                    return copyToSmallInt((Integer[]) value, new short[value.length]);
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
//                .filter(entry -> !Graph.Key.isHidden(entry.getKey()))
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
    public <V> Iterator<? extends Property<V>> propertyIterator(final String... propertyKeys) {
        SqlgElement.this.sqlgGraph.tx().readWrite();
        return SqlgElement.this.<V>internalGetAllProperties(propertyKeys).values().iterator();
    }

    protected void loadProperty(ResultSetMetaData resultSetMetaData, int i, String columnName, Object o) throws SQLException {
        int type = resultSetMetaData.getColumnType(i);
        switch (type) {
            case Types.SMALLINT:
                this.properties.put(columnName, ((Integer) o).shortValue());
                break;
            case Types.TINYINT:
                this.properties.put(columnName, ((Integer) o).byteValue());
                break;
            case Types.REAL:
                this.properties.put(columnName, ((Number) o).floatValue());
                break;
            case Types.DOUBLE:
                this.properties.put(columnName, ((Number) o).doubleValue());
                break;
            case Types.ARRAY:
                java.sql.Array array = (java.sql.Array) o;
                int baseType = array.getBaseType();
                Object[] objectArray = (Object[]) array.getArray();
                this.properties.put(columnName, convertObjectArrayToPrimitiveArray(objectArray, baseType));
                break;
            default:
                this.properties.put(columnName, o);
        }
    }
}
