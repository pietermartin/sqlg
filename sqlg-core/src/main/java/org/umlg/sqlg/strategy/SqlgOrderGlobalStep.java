package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.ChainedComparator;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by pieter on 2015/09/05.
 */
public class SqlgOrderGlobalStep<S, C extends Comparable> extends CollectingBarrierStep<S> implements ComparatorHolder<S, C>, TraversalParent {

    private ChainedComparator<S, C> chainedComparator = null;
    private OrderGlobalStep<S, C> orderGlobalStep;
    private List<Pair<Traversal.Admin<S, C>, Comparator<C>>> comparators = new ArrayList<>();
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
    public void addComparator(final Traversal.Admin<S, C> traversal, final Comparator<C> comparator) {
        this.comparators.add(new Pair<>(this.integrateChild(traversal), comparator));
    }

    @Override
    public List<Pair<Traversal.Admin<S, C>, Comparator<C>>> getComparators() {
        return this.comparators.isEmpty() ? Collections.singletonList(new Pair<>(new IdentityTraversal(), (Comparator) Order.incr)) : Collections.unmodifiableList(this.comparators);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, getComparators());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        SqlgOrderGlobalStep<?, ?> that = (SqlgOrderGlobalStep<?, ?>) o;

        if (ignore != that.ignore) return false;
        if (chainedComparator != null ? !chainedComparator.equals(that.chainedComparator) : that.chainedComparator != null)
            return false;
        if (orderGlobalStep != null ? !orderGlobalStep.equals(that.orderGlobalStep) : that.orderGlobalStep != null)
            return false;
        return comparators != null ? comparators.equals(that.comparators) : that.comparators == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for (int i = 0; i < this.comparators.size(); i++) {
            result ^= this.comparators.get(i).hashCode() * (i+1);
        }
        return result;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public List<Traversal.Admin<S, C>> getLocalChildren() {
        return (List) this.comparators.stream().map(Pair::getValue0).collect(Collectors.toList());
    }

    @Override
    public SqlgOrderGlobalStep<S, C> clone() {
        final SqlgOrderGlobalStep<S, C> clone = (SqlgOrderGlobalStep<S, C>) super.clone();
        clone.comparators = new ArrayList<>();
        for (final Pair<Traversal.Admin<S, C>, Comparator<C>> comparator : this.comparators) {
            clone.comparators.add(new Pair<>(comparator.getValue0().clone(), comparator.getValue1()));
        }
        clone.chainedComparator = null;
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
