package org.umlg.sqlg.step;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.umlg.sqlg.structure.SqlgElement;
import org.umlg.sqlg.util.SqlgUtil;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/10/25
 */
public class SqlgPropertiesStep<E> extends SqlgFlatMapStep<Element, E> implements AutoCloseable {

    protected final String[] propertyKeys;
    protected final PropertyType returnType;
    private Set<String> appliesToLabels;

    public SqlgPropertiesStep(final Traversal.Admin traversal, final PropertyType propertyType, final String... propertyKeys) {
        super(traversal);
        this.returnType = propertyType;
        this.propertyKeys = propertyKeys;
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Element> traverser) {
        for (String appliesToLabel : appliesToLabels) {
            String label = SqlgUtil.originalLabel(appliesToLabel);
            Object o = traverser.path().get(label);
            if (o instanceof List) {
                List<SqlgElement> objects = (List) o;
                SqlgElement last = objects.get(objects.size() - 1);
                if (this.returnType.equals(PropertyType.VALUE)) {
                    return last.values(this.propertyKeys);
                } else {
                    return (Iterator)last.properties(this.propertyKeys);
                }
            } else {
                SqlgElement sqlgElement = traverser.path().get(label);
                if (this.returnType.equals(PropertyType.VALUE)) {
                    return sqlgElement.values(this.propertyKeys);
                } else {
                    return (Iterator) sqlgElement.properties(this.propertyKeys);
                }
            }
        }
        return EmptyIterator.INSTANCE;
    }

    public void setAppliesToLabels(Set<String> appliesToLabels) {
        this.appliesToLabels = appliesToLabels;
    }

    public PropertyType getReturnType() {
        return this.returnType;
    }

    public String[] getPropertyKeys() {
        return this.propertyKeys;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, Arrays.asList(this.propertyKeys), this.returnType.name().toLowerCase());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.returnType.hashCode();
        for (final String propertyKey : this.propertyKeys) {
            result ^= propertyKey.hashCode();
        }
        return result;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public void close() throws Exception {
        closeIterator();
    }
}
