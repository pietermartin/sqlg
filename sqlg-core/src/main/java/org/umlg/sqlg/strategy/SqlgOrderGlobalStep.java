package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.TraversalComparator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by pieter on 2015/09/05.
 */
public class SqlgOrderGlobalStep<S> extends CollectingBarrierStep<S> implements ComparatorHolder<S>, TraversalParent {

    private OrderGlobalStep<S> orderGlobalStep;
    private List<Comparator<S>> comparators = new ArrayList<>();
    //ignore indicates whether the order by clause has been executed by the rdbms.
    //if so then there is no need to do so in java, else do so.
    private boolean ignore = false;

    public SqlgOrderGlobalStep(OrderGlobalStep orderGlobalStep) {
        super(orderGlobalStep.getTraversal());
        this.orderGlobalStep = orderGlobalStep;
    }

    public void setIgnore(boolean ignore) {
        this.ignore = ignore;
    }

    @Override
    public void barrierConsumer(TraverserSet<S> traverserSet) {
        if (!ignore)
            this.orderGlobalStep.barrierConsumer(traverserSet);
    }

    @Override
    public void addComparator(Comparator<S> comparator) {
        this.orderGlobalStep.addComparator(comparator);
    }

    @Override
    public List<Comparator<S>> getComparators() {
        return this.orderGlobalStep.getComparators();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, getComparators());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for (final Comparator<S> comparator : getComparators()) {
            result ^= comparator.hashCode();
        }
        return result;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return Collections.unmodifiableList(getComparators().stream()
                .filter(comparator -> comparator instanceof TraversalComparator)
                .map(traversalComparator -> ((TraversalComparator<S, E>) traversalComparator).getTraversal())
                .collect(Collectors.toList()));
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> localChildTraversal) {
        throw new UnsupportedOperationException("Use OrderGlobalStep.addComparator(" + TraversalComparator.class.getSimpleName() + ") to add a local child traversal:" + this);
    }

    @Override
    public SqlgOrderGlobalStep<S> clone() {
        final SqlgOrderGlobalStep<S> clone = (SqlgOrderGlobalStep<S>) super.clone();
        clone.comparators = new ArrayList<>();
        for (final Comparator<S> comparator : getComparators()) {
            if (comparator instanceof TraversalComparator) {
                final TraversalComparator<S, ?> clonedTraversalComparator = ((TraversalComparator<S, ?>) comparator).clone();
                clone.integrateChild(clonedTraversalComparator.getTraversal());
                clone.comparators.add(clonedTraversalComparator);
            } else
                clone.comparators.add(comparator);
        }
        return clone;
    }


    /////

    private static class ComparatorTraverser<S> implements Comparator<Traverser<S>>, Serializable {

        private final Comparator<S> comparator;

        public ComparatorTraverser(final Comparator<S> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(final Traverser<S> traverserA, final Traverser<S> traverserB) {
            return this.comparator.compare(traverserA.get(), traverserB.get());
        }

        public static <S> List<ComparatorTraverser<S>> convertComparator(final List<Comparator<S>> comparators) {
            return comparators.stream().map(comparator -> new ComparatorTraverser<>(comparator)).collect(Collectors.toList());
        }
    }
}
