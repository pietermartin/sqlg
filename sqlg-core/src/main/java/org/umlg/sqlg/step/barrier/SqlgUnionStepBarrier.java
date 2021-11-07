package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.umlg.sqlg.step.SqlgAbstractStep;
import org.umlg.sqlg.structure.traverser.SqlgTraverser;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2019/08/12
 */
public class SqlgUnionStepBarrier<S, E> extends SqlgAbstractStep<S, E> implements TraversalParent, Step<S, E> {

    private boolean first = true;
    private final List<Traversal.Admin<S, E>> globalTraversals;
    private final List<Traversal.Admin<S, TraversalOptionParent.Pick>> localTraversals;
    private final List<Traverser.Admin<E>> results = new ArrayList<>();
    private Iterator<Traverser.Admin<E>> resultIterator;
    private boolean hasStarts = false;

    public SqlgUnionStepBarrier(final Traversal.Admin traversal, UnionStep<S, E> unionStep) {
        super(traversal);
        this.globalTraversals = unionStep.getGlobalChildren();
        this.localTraversals = unionStep.getLocalChildren();
    }

    @Override
    public boolean hasStarts() {
        if (this.first) {
            this.hasStarts = this.starts.hasNext();
        }
        return this.hasStarts;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Traversal.Admin<S, TraversalOptionParent.Pick>> getLocalChildren() {
        return this.localTraversals;
    }

    @Override
    public List<Traversal.Admin<S, E>> getGlobalChildren() {
        return this.globalTraversals;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        Set<TraverserRequirement> requirements = new HashSet<>();
        for (Traversal.Admin<S, TraversalOptionParent.Pick> localTraversal : this.localTraversals) {
            requirements.addAll(localTraversal.getTraverserRequirements());
        }
        for (Traversal.Admin<S, E> globalTraversal : this.globalTraversals) {
           requirements.addAll(globalTraversal.getTraverserRequirements());
        }
        return requirements;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        if (this.first) {
            this.first = false;
            while (this.starts.hasNext()) {
                Traverser.Admin<S> start = this.starts.next();
                for (Traversal.Admin<S, E> globalTraversal : this.globalTraversals) {
                    globalTraversal.addStart(start);
                }
            }
            for (Traversal.Admin<S, E> globalTraversal : this.globalTraversals) {
                while (globalTraversal.hasNext()) {
                    this.results.add(globalTraversal.nextTraverser());
                }
            }
            this.results.sort((o1, o2) -> {
                SqlgTraverser x = (SqlgTraverser) o1;
                SqlgTraverser y = (SqlgTraverser) o2;
                return Long.compare(x.getStartElementIndex(), y.getStartElementIndex());
            });
            this.resultIterator = this.results.iterator();
        }
        if (this.resultIterator.hasNext()) {
            return this.resultIterator.next();
        } else {
            reset();
            throw FastNoSuchElementException.instance();
        }
    }

    @Override
    public void reset() {
//        super.reset();
        this.first = true;
        this.results.clear();
    }

    @Override
    public SqlgUnionStepBarrier<S, E> clone() {
        final SqlgUnionStepBarrier<S, E> clone = (SqlgUnionStepBarrier<S, E>) super.clone();
        for (Traversal.Admin<S, TraversalOptionParent.Pick> localTraversal : localTraversals) {
            clone.localTraversals.add(localTraversal.clone());
        }
        for (Traversal.Admin<S, E> globalTraversal : globalTraversals) {
            clone.globalTraversals.add(globalTraversal.clone());
        }
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        for (Traversal.Admin<S, E> globalTraversal : globalTraversals) {
           integrateChild(globalTraversal);
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.globalTraversals);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
            result ^= this.globalTraversals.hashCode();
        return result;
    }
}
