package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2014/07/12
 * Time: 5:40 AM
 */
public abstract class SqlElement implements Element {

    public static final String IN_VERTEX_COLUMN_END = "_INID";
    public static final String OUT_VERTEX_COLUMN_END = "_OUTID";

    protected String label;
    protected SqlGraph sqlGraph;
    protected long primaryKey;

    public SqlElement(SqlGraph sqlGraph, String label, Object... keyValues) {
        this.label = label;
        this.sqlGraph = sqlGraph;
    }

    public SqlElement(SqlGraph sqlGraph, Long id, String label) {
        this.sqlGraph = sqlGraph;
        this.primaryKey = id;
        this.label = label;
    }

    @Override
    public Object id() {
        return primaryKey;
    }

    @Override
    public String label() {
        return this.label;
    }

    @Override
    public void remove() {
        StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.label()));
        sql.append(" WHERE ");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" = ?");
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            preparedStatement.setLong(1, (Long) this.id());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //TODO caching???
    @Override
    public Map<String, Property> properties() {
        this.sqlGraph.tx().readWrite();
        Object[] keyValues = load();
        Map<String, Property> properties = new HashMap<>();
        int i = 1;
        String key = "";
        Object value;
        for (Object o : keyValues) {
            if (i++ % 2 != 0) {
                key = (String) o;
            } else {
                value = o;
                if (!key.equals("ID") && value != null) {
                    properties.put(key, new SqlProperty<>(this.sqlGraph, this, key, value));
                }
            }
        }
        return properties;
    }

    protected abstract Object[] load();

    @Override
    public Map<String, Property> hiddens() {
        this.sqlGraph.tx().readWrite();
        Map<String, Property> properties = this.properties();
        Map<String, Property> hiddens = new HashMap<>();
        for (String key : properties.keySet()) {
            if (Graph.Key.isHidden(key))
                hiddens.put(Graph.Key.unHide(key), properties.get(key));
        }
        return hiddens;

    }

    @Override
    public <V> Property<V> property(String key) {
        Property property = properties().get(key);
        if (property == null) {
            return Property.empty();
        } else {
            return property;
        }
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        ElementHelper.validateProperty(key, value);
        //Validate the property
        PropertyType.from(value);
        //Check if column exist
        this.sqlGraph.getSchemaManager().ensureColumnExist(this.label, ImmutablePair.of(key, PropertyType.from(value)));
        updateRow(key, value);
        return new SqlProperty<>(this.sqlGraph, this, key, value);
    }

    private void updateRow(String key, Object value) {
        StringBuilder sql = new StringBuilder("UPDATE ");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.label));
        sql.append(" SET ");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(key));
        sql.append(" = ?");
        sql.append(" WHERE ");
        sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
        sql.append(" = ?");
        if (this.sqlGraph.getSqlDialect().needsSemicolon()) {
            sql.append(";");
        }
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            PropertyType propertyType = PropertyType.from(value);
            switch (propertyType) {
                case BOOLEAN:
                    preparedStatement.setBoolean(1, (Boolean) value);
                    break;
                case BYTE:
                    preparedStatement.setByte(1, (Byte) value);
                    break;
                case SHORT:
                    preparedStatement.setShort(1, (Short) value);
                    break;
                case INTEGER:
                    preparedStatement.setInt(1, (Integer) value);
                    break;
                case LONG:
                    preparedStatement.setLong(1, (Long) value);
                    break;
                case FLOAT:
                    preparedStatement.setFloat(1, (Float) value);
                    break;
                case DOUBLE:
                    preparedStatement.setDouble(1, (Double) value);
                    break;
                case STRING:
                    preparedStatement.setString(1, (String) value);
                    break;
                default:
                    throw new IllegalStateException("Unhandled type " + propertyType.name());
            }
            preparedStatement.setLong(2, (Long) this.id());
            preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return this.id().hashCode();
    }

}
