package org.umlg.sqlgraph.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;

import java.util.Map;

/**
 * Date: 2014/07/12
 * Time: 5:40 AM
 */
public abstract class SqlElement implements Element {

    protected String label;
    protected SqlGraph sqlGraph;
    protected Object[] keyValues;
    protected long primaryKey;

    public SqlElement(SqlGraph sqlGraph, String label, Object ... keyValues) {
        this.label = label;
        this.sqlGraph = sqlGraph;
        this.keyValues = keyValues;
    }

    public SqlElement(Long id, SqlGraph sqlGraph) {
        this.primaryKey = id;
        this.sqlGraph = sqlGraph;
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

    }

    @Override
    public Map<String, Property> properties() {
        return null;
    }

    @Override
    public Map<String, Property> hiddens() {
        return null;
    }

    @Override
    public <V> Property<V> property(String key) {
        return null;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        //Check if column exist
        return null;
    }
}
