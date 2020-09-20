package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/10/13
 */
public class SqlgHasStep<S extends Element> extends SqlgFilterStep<S> implements HasContainerHolder {

    private List<HasContainer> hasContainers;

    public SqlgHasStep(final Traversal.Admin traversal, final HasContainer... hasContainers) {
        super(traversal);
        this.hasContainers = new ArrayList<>();
        Collections.addAll(this.hasContainers, hasContainers);
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        // the generic S is defined as Element but Property can also be used with HasStep so this seems to cause
        // problems with some jdk versions.
        //noinspection ConstantConditions
        if (traverser.get() instanceof Element)
            return HasContainer.testAll(traverser.get(), this.hasContainers);
        else if (traverser.get() instanceof Property)
            //noinspection unchecked
            return HasContainer.testAll((Property) traverser.get(), this.hasContainers);
        else
            throw new IllegalStateException(String.format(
                    "Traverser to has() must be of type Property or Element, not %s",
                    traverser.get().getClass().getName()));
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void removeHasContainer(final HasContainer hasContainer) {
        this.hasContainers.remove(hasContainer);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.OBJECT);
    }

    @Override
    public SqlgHasStep<S> clone() {
        final SqlgHasStep<S> clone = (SqlgHasStep<S>) super.clone();
        clone.hasContainers = new ArrayList<>();
        for (final HasContainer hasContainer : this.hasContainers) {
            clone.addHasContainer(hasContainer.clone());
        }
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for (final HasContainer hasContainer : this.hasContainers) {
            result ^= hasContainer.hashCode();
        }
        return result;
    }
}
