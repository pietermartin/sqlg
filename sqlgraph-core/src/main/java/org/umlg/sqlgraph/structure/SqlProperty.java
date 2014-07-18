package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Date: 2014/07/12
 * Time: 5:43 AM
 */
public class SqlProperty<V> implements Property<V>, Serializable {

    private final String key;
    private V value;
    private SqlElement element;
    protected SqlGraph sqlGraph;
    public static final String REMOVED = "___REMOVED___";

    public SqlProperty(SqlGraph sqlGraph, SqlElement element, String key, V value) {
        this.sqlGraph = sqlGraph;
        this.element = element;
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return Graph.Key.unHide(this.key);
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return this.value != null;
    }

    @Override
    public boolean isHidden() {
        return Graph.Key.isHidden(this.key);
    }

    @Override
    public <E extends Element> E getElement() {
        return (E) this.element;
    }

    @Override
    public void remove() {
        StringBuilder sql = new StringBuilder("UPDATE \"");
        sql.append(this.element.label());
        sql.append("\" SET \"");
        sql.append(this.key);
        sql.append("\" = ? WHERE \"ID\" = ?;");
        Connection conn = this.sqlGraph.tx().getConnection();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
            PropertyType propertyType = PropertyType.from(value);
            preparedStatement.setNull(1, this.sqlGraph.getSchemaManager().getSqlDialect().propertyTypeToJavaSqlType(propertyType));
            preparedStatement.setLong(2, (Long) this.element.id());
            int numberOfRowsUpdated = preparedStatement.executeUpdate();
            if (numberOfRowsUpdated != 1) {
                throw new IllegalStateException("Remove property failed!");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String toString() {
        return StringFactory.propertyString(this);
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public int hashCode() {
        return this.key.hashCode() + this.value.hashCode() + this.element.hashCode();
    }
}
