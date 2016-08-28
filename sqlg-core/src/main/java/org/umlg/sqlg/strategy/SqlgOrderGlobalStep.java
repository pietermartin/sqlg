package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by pieter on 2015/09/05.
 */
class SqlgOrderGlobalStep<S, C extends Comparable> extends CollectingBarrierStep<S> implements ComparatorHolder<S, C>, TraversalParent {

    private OrderGlobalStep<S, C> orderGlobalStep;
    private List<Pair<Traversal.Admin<S, C>, Comparator<C>>> comparators = new ArrayList<>();
    //ignore indicates whether the order by clause has been executed by the rdbms.
    //if so then there is no need to do so in java, else do so.
    private boolean ignore = false;

    SqlgOrderGlobalStep(OrderGlobalStep<S, C> orderGlobalStep) {
        super(orderGlobalStep.getTraversal());
        this.orderGlobalStep = orderGlobalStep;
    }

    void setIgnore(boolean ignore) {
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
        return clone;
    }

}
