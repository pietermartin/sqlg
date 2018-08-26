package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/08/25
 */
public class SqlgPropertyMapStep<K, E> extends SqlgMapStep<Element, Map<K, E>> implements TraversalParent {

    protected final String[] propertyKeys;
    protected final PropertyType returnType;
    protected final boolean includeTokens;
    protected Traversal.Admin<Element, ? extends Property> propertyTraversal;
    private Set<String> appliesToLabels;

    public SqlgPropertyMapStep(final Traversal.Admin traversal, final boolean includeTokens, final PropertyType propertyType, final String... propertyKeys) {
        super(traversal);
        this.includeTokens = includeTokens;
        this.propertyKeys = propertyKeys;
        this.returnType = propertyType;
        this.propertyTraversal = null;
    }

    public void setAppliesToLabels(Set<String> appliesToLabels) {
        this.appliesToLabels = appliesToLabels;
    }

    @Override
    protected Map<K, E> map(final Traverser.Admin<Element> traverser) {
        final Map<Object, Object> map = new HashMap<>();
        final Element element = traverser.get();
        final boolean isVertex = traverser.get() instanceof Vertex;
        final Iterator<? extends Property> properties = null == this.propertyTraversal ?
                element.properties(this.propertyKeys) :
                TraversalUtil.applyAll(traverser, this.propertyTraversal);
        while (properties.hasNext()) {
            final Property<?> property = properties.next();
            if (isVertex) {
                List<Object> values = (List<Object>) map.get(property.key());
                if (null == values) {
                    values = new ArrayList<>();
                    map.put(property.key(), values);
                }
                values.add(this.returnType == PropertyType.VALUE ? property.value() : property);
            } else
                map.put(property.key(), this.returnType == PropertyType.VALUE ? property.value() : property);
        }
        if (this.returnType == PropertyType.VALUE && this.includeTokens) {
            // add tokens, as string keys
            if (element instanceof VertexProperty) {
                map.put(T.id, element.id());
                map.put(T.key, ((VertexProperty<?>) element).key());
                map.put(T.value, ((VertexProperty<?>) element).value());
            } else {
                map.put(T.id, element.id());
                map.put(T.label, element.label());
            }
        }
        return (Map) map;
    }

    @Override
    public List<Traversal.Admin<Element, ? extends Property>> getLocalChildren() {
        return null == this.propertyTraversal ? Collections.emptyList() : Collections.singletonList(this.propertyTraversal);
    }

    public void setPropertyTraversal(final Traversal.Admin<Element, ? extends Property> propertyTraversal) {
        this.propertyTraversal = this.integrateChild(propertyTraversal);
    }

    public PropertyType getReturnType() {
        return this.returnType;
    }

    public String[] getPropertyKeys() {
        return propertyKeys;
    }

    public boolean isIncludeTokens() {
        return includeTokens;
    }

    public String toString() {
        return null != this.propertyTraversal ?
                StringFactory.stepString(this, this.propertyTraversal, this.returnType.name().toLowerCase()) :
                StringFactory.stepString(this, Arrays.asList(this.propertyKeys), this.returnType.name().toLowerCase());
    }

    @Override
    public SqlgPropertyMapStep<K,E> clone() {
        final SqlgPropertyMapStep<K,E> clone = (SqlgPropertyMapStep<K,E>) super.clone();
        if (null != this.propertyTraversal)
            clone.propertyTraversal = this.propertyTraversal.clone();
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.returnType.hashCode() ^ Boolean.hashCode(this.includeTokens);
        if (null == this.propertyTraversal) {
            for (final String propertyKey : this.propertyKeys) {
                result ^= propertyKey.hashCode();
            }
        } else {
            result ^= this.propertyTraversal.hashCode();
        }
        return result;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        if (null != this.propertyTraversal)
            this.integrateChild(this.propertyTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }
}
