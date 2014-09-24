package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

/**
 * Date: 2014/09/10
 * Time: 8:39 PM
 */
public class SqlgVertexProperty<V> extends SqlgProperty<V> implements VertexProperty<V> {

    public SqlgVertexProperty(SqlG sqlG, SqlgElement element, String key, V value) {
        super(sqlG, element, key, value);
    }

    @Override
    public Object id() {
        return (long) (this.key().hashCode() + this.value().hashCode() + this.getElement().id().hashCode());
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public Vertex getElement() {
        return super.getElement();
    }

    @Override
    public Iterators iterators() {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
//        return new Iterators() {
//            @Override
//            public <U> Iterator<Property<U>> properties(String... propertyKeys) {
//                return Collections.emptyIterator();
//            }
//
//            @Override
//            public <U> Iterator<Property<U>> hiddens(String... propertyKeys) {
//                return Collections.emptyIterator();
//            }
//        };
    }

}
