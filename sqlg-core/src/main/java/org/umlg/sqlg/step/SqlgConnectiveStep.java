package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/10/26
 */
public abstract class SqlgConnectiveStep<S>  extends SqlgFilterStep<S> implements TraversalParent {

    public enum Connective {AND, OR}

    protected List<Traversal.Admin<S, ?>> traversals;

    public SqlgConnectiveStep(final Traversal.Admin traversal, final Traversal<S, ?>... traversals) {
        super(traversal);
        this.traversals = Stream.of(traversals).map(Traversal::asAdmin).collect(Collectors.toList());
        this.traversals.forEach(this::integrateChild);
    }

    @Override
    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        return this.traversals;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    public void addLocalChild(final Traversal.Admin<?, ?> localChildTraversal) {
        this.traversals.add(this.integrateChild((Traversal.Admin) localChildTraversal));
    }

    @Override
    public SqlgConnectiveStep<S> clone() {
        final SqlgConnectiveStep<S> clone = (SqlgConnectiveStep<S>) super.clone();
        clone.traversals = new ArrayList<>();
        for (final Traversal.Admin<S, ?> traversal : this.traversals) {
            clone.traversals.add(traversal.clone());
        }
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        for (final Traversal.Admin<S, ?> traversal : this.traversals) {
            integrateChild(traversal);
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.traversals);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.traversals.hashCode();
    }
}
