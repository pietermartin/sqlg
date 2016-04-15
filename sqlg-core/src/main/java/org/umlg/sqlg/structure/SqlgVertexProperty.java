package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Iterator;

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
        return (Vertex)super.element();
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

}
