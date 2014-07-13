package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import org.javatuples.Pair;
import org.umlg.sqlgraph.sql.impl.SchemaManager;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Date: 2014/07/12
 * Time: 5:40 AM
 */
public abstract class SqlElement implements Element {

    public static final String IN_VERTEX_COLUMN_END = "_INID";
    public static final String OUT_VERTEX_COLUMN_END = "_OUTID";

    protected String label;
    protected SqlGraph sqlGraph;
    protected Object[] keyValues;
    protected long primaryKey;
    protected boolean loaded = false;

    public SqlElement(SqlGraph sqlGraph, String label, Object... keyValues) {
        this.label = label;
        this.sqlGraph = sqlGraph;
        this.keyValues = keyValues;
        this.loaded = true;
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
        StringBuilder sql = new StringBuilder("DELETE FROM \"");
        sql.append(this.label());
        sql.append("\" WHERE ID = ?;");
        Connection conn;
        PreparedStatement preparedStatement = null;
        try {
            conn = this.sqlGraph.tx().getConnection();
            preparedStatement = conn.prepareStatement(sql.toString());
            preparedStatement.setLong(1, (Long) this.id());
            int numberOfRowsUpdated = preparedStatement.executeUpdate();
            if (numberOfRowsUpdated != 1) {
                throw new IllegalStateException("Remove element failed!");
            }
            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (preparedStatement != null)
                    preparedStatement.close();
            } catch (SQLException se2) {
            }
        }
    }

    //TODO caching???
    @Override
    public Map<String, Property> properties() {
        this.sqlGraph.tx().readWrite();
        if (!loaded) {
            load();
            this.loaded = true;
        }
        Map<String, Property> properties = new HashMap<>();
        int i = 1;
        String key = "";
        Object value;
        for (Object o : keyValues) {
            if (i++ % 2 != 0) {
                key = (String) o;
            } else {
                value = o;
                if (!value.equals(SqlProperty.REMOVED)) {
                    properties.put(key, new SqlProperty<>(this.sqlGraph, this, key, value));
                }
            }
        }
        return properties;
    }

    protected abstract void load();

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
        SchemaManager.PropertyType.from(value);
        //Check if column exist
        if (!columnExist(key)) {
            addColumn(key, value);
        }
        updateRow(key, value);
        this.loaded = false;
        return new SqlProperty<>(this.sqlGraph, this, key, value);
    }

    private boolean columnExist(String key) {
        Connection conn;
        try {
            conn = this.sqlGraph.tx().getConnection();
            DatabaseMetaData metadata = conn.getMetaData();
            String catalog = null;
            String schemaPattern = null;
            String tableNamePattern = this.label;
            String columnNamePattern = null;
            ResultSet result = metadata.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
            while (result.next()) {
                String columnName = result.getString(4);
                if (key.equals(columnName)) {
                    return true;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
        }
        return false;
    }

    private void addColumn(String key, Object value) {
        StringBuilder sql = new StringBuilder("ALTER TABLE \"");
        sql.append(this.label);
        sql.append("\" ADD ");
        sql.append("\"");
        sql.append(key);
        sql.append("\" ");
        sql.append(SchemaManager.PropertyType.from(value).getDbName());
        sql.append(";");
        Connection conn;
        PreparedStatement preparedStatement = null;
        try {
            conn = this.sqlGraph.tx().getConnection();
            preparedStatement = conn.prepareStatement(sql.toString());
            preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (preparedStatement != null)
                    preparedStatement.close();
            } catch (SQLException se2) {
            }
        }
    }

    private void updateRow(String key, Object value) {
        StringBuilder sql = new StringBuilder("UPDATE \"");
        sql.append(this.label);
        sql.append("\" SET ");
        sql.append("\"");
        sql.append(key);
        sql.append("\" = ?");
        sql.append(" WHERE ID = ?;");
        Connection conn;
        PreparedStatement preparedStatement = null;
        try {
            conn = this.sqlGraph.tx().getConnection();
            preparedStatement = conn.prepareStatement(sql.toString());
            SchemaManager.PropertyType propertyType = SchemaManager.PropertyType.from(value);
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
            preparedStatement.setLong(2, (Long)this.id());
            preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (preparedStatement != null)
                    preparedStatement.close();
            } catch (SQLException se2) {
            }
        }
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

}
