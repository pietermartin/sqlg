package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * Date: 2014/09/10
 * Time: 8:39 PM
 */
public class SqlgMetaProperty<V> extends SqlgProperty<V> implements MetaProperty<V> {

    public SqlgMetaProperty(SqlG sqlG, SqlgElement element, String key, V value) {
        super(sqlG, element, key, value);
    }

    @Override
    public Object id() {
        return (long) (this.key().hashCode() + this.value().hashCode() + this.getElement().id().hashCode());
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw new UnsupportedOperationException("Graph does not support MetaProperties!");
    }

    @Override
    public Vertex getElement() {
        return super.getElement();
    }

    @Override
    public Iterators iterators() {
        throw new UnsupportedOperationException("Graph does not support MetaProperties!");
    }

}
