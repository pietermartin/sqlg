package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;

/**
 * Date: 2014/09/10
 * Time: 8:39 PM
 */
public class SqlgVertexProperty<V> extends SqlgProperty<V> implements VertexProperty<V> {

    public SqlgVertexProperty(SqlgGraph sqlgGraph, SqlgElement element, String key, V value) {
        super(sqlgGraph, element, key, value);
    }

    @Override
    public Object id() {
        return (long) (this.key().hashCode() + this.value().hashCode() + this.element().id().hashCode());
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public Vertex element() {
        return super.element();
    }

    @Override
    public Iterators iterators() {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

}
