package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * Date: 2014/07/12
 * Time: 5:43 AM
 */
public class SqlProperty<V> implements Property<V>, Serializable {

    private final Element element;
    private final String key;
    private final SqlGraph graph;
    private V value;

    public SqlProperty(Element element, String key, SqlGraph graph, V value) {
        this.element = element;
        this.key = key;
        this.graph = graph;
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
        return null != this.value;
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
        //Set to ___removed___
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
