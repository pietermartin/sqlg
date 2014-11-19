package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.NoSuchElementException;

/**
 * Date: 2014/07/12
 * Time: 5:43 AM
 */
public class SqlgProperty<V> implements Property<V>, Serializable {

    private final String key;
    private V value;
    private SqlgElement element;
    protected SqlgGraph sqlgGraph;

    public SqlgProperty(SqlgGraph sqlgGraph, SqlgElement element, String key, V value) {
        this.sqlgGraph = sqlgGraph;
        this.element = element;
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return this.key;
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
    public Element element() {
        return this.element;
    }

    @Override
    public void remove() {
        this.element.properties.remove(this.key);
        boolean elementInInsertedCache = false;
        if (this.sqlgGraph.features().supportsBatchMode() && this.sqlgGraph.tx().isInBatchMode()) {
            elementInInsertedCache = this.sqlgGraph.tx().getBatchManager().removeProperty(this, key);
        }

        if (!elementInInsertedCache) {
            StringBuilder sql = new StringBuilder("UPDATE ");
            sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.element.schema));
            sql.append(".");
            sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes((this.element instanceof Vertex ? SchemaManager.VERTEX_PREFIX : SchemaManager.EDGE_PREFIX) + this.element.table));
            sql.append(" SET ");
            sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(this.key));
            sql.append(" = ? WHERE ");
            sql.append(this.sqlgGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("ID"));
            sql.append(" = ?");
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            Connection conn = this.sqlgGraph.tx().getConnection();
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                PropertyType propertyType = PropertyType.from(value);
                preparedStatement.setNull(1, this.sqlgGraph.getSqlDialect().propertyTypeToJavaSqlType(propertyType));
                preparedStatement.setLong(2, (Long) this.element.id());
                int numberOfRowsUpdated = preparedStatement.executeUpdate();
                if (numberOfRowsUpdated != 1) {
                    throw new IllegalStateException("Remove property failed!");
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
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
