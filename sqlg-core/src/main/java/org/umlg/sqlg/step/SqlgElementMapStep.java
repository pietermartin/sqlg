package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

public class SqlgElementMapStep<K, E> extends SqlgMapStep<Element, Map<K, E>> implements TraversalParent {

    protected final String[] propertyKeys;

    public SqlgElementMapStep(Traversal.Admin traversal, final String... propertyKeys) {
        super(traversal);
        this.propertyKeys = propertyKeys;
    }

    @Override
    protected Map<K, E> map(Traverser.Admin<Element> traverser) {
        final Map<Object, Object> map = new LinkedHashMap<>();
        final Element element = traverser.get();
        map.put(T.id, element.id());
        if (element instanceof VertexProperty) {
            map.put(T.key, ((VertexProperty<?>) element).key());
            map.put(T.value, ((VertexProperty<?>) element).value());
        } else {
            map.put(T.label, element.label());
        }

        if (element instanceof Edge) {
            final Edge e = (Edge) element;
            map.put(Direction.IN, getVertexStructure(e.inVertex()));
            map.put(Direction.OUT, getVertexStructure(e.outVertex()));
        }

        final Iterator<? extends Property> properties = element.properties(this.propertyKeys);
        while (properties.hasNext()) {
            final Property<?> property = properties.next();
            map.put(property.key(), property.value());
        }

        return (Map) map;
    }

    protected Map<Object, Object> getVertexStructure(final Vertex v) {
        final Map<Object, Object> m = new LinkedHashMap<>();
        m.put(T.id, v.id());
        m.put(T.label, v.label());
        return m;
    }

    public String[] getPropertyKeys() {
        return propertyKeys;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, Arrays.asList(this.propertyKeys));
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for (final String propertyKey : this.propertyKeys) {
            result ^= Objects.hashCode(propertyKey);
        }
        return result;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }
}
