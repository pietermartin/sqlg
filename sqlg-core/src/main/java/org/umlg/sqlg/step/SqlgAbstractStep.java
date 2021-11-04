package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/06/11
 */
public abstract class SqlgAbstractStep<S, E> implements Step<S, E> {

    Set<String> labels = new LinkedHashSet<>();
    private String id = Traverser.Admin.HALT;
    private Traversal.Admin<?, ?> traversal;
    protected SqlgExpandableStepIterator<S> starts;
    private Traverser.Admin<E> nextEnd = null;
    boolean traverserStepIdAndLabelsSetByChild = false;

    private Step<?, S> previousStep = EmptyStep.instance();
    private Step<E, ?> nextStep = EmptyStep.instance();

    protected SqlgAbstractStep(final Traversal.Admin traversal) {
        this.traversal = traversal;
        this.starts = new SqlgExpandableStepIterator<>(this);
    }

    @Override
    public boolean hasStarts() {
        return this.starts.hasNext();
    }

    @Override
    public void setId(final String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public void addLabel(final String label) {
        this.labels.add(label);
    }

    @Override
    public void removeLabel(final String label) {
        this.labels.remove(label);
    }

    @Override
    public Set<String> getLabels() {
        return Collections.unmodifiableSet(this.labels);
    }

    @Override
    public void reset() {
        this.starts.clear();
        this.nextEnd = null;
    }

    @Override
    public void addStarts(final Iterator<Traverser.Admin<S>> starts) {
        this.starts.add(starts);
    }

    @Override
    public void addStart(final Traverser.Admin<S> start) {
        this.starts.add(start);
    }

    @Override
    public void setPreviousStep(final Step<?, S> step) {
        this.previousStep = step;
    }

    @Override
    public Step<?, S> getPreviousStep() {
        return this.previousStep;
    }

    @Override
    public void setNextStep(final Step<E, ?> step) {
        this.nextStep = step;
    }

    @Override
    public Step<E, ?> getNextStep() {
        return this.nextStep;
    }

    @Override
    public Traverser.Admin<E> next() {
        if (null != this.nextEnd) {
            try {
                return this.prepareTraversalForNextStep(this.nextEnd);
            } finally {
                this.nextEnd = null;
            }
        } else {
            while (true) {
                if (Thread.interrupted()) throw new TraversalInterruptedException();
                final Traverser.Admin<E> traverser = this.processNextStart();
                if (null != traverser.get() && 0 != traverser.bulk())
                    return this.prepareTraversalForNextStep(traverser);
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (null != this.nextEnd)
            return true;
        else {
            try {
                while (true) {
                    if (Thread.interrupted()) throw new TraversalInterruptedException();
                    this.nextEnd = this.processNextStart();
                    if (null != this.nextEnd.get() && 0 != this.nextEnd.bulk())
                        return true;
                    else
                        this.nextEnd = null;
                }
            } catch (final NoSuchElementException e) {
                return false;
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <A, B> Traversal.Admin<A, B> getTraversal() {
        return (Traversal.Admin<A, B>) this.traversal;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> traversal) {
        this.traversal = traversal;
    }

    protected abstract Traverser.Admin<E> processNextStart() throws NoSuchElementException;

    @Override
    public String toString() {
        return StringFactory.stepString(this);
    }

    @Override
    @SuppressWarnings({"CloneDoesntDeclareCloneNotSupportedException", "unchecked"})
    public SqlgAbstractStep<S, E> clone() {
        try {
            final SqlgAbstractStep<S, E> clone = (SqlgAbstractStep<S, E>) super.clone();
            clone.starts = new SqlgExpandableStepIterator<>(clone);
            clone.previousStep = EmptyStep.instance();
            clone.nextStep = EmptyStep.instance();
            clone.nextEnd = null;
            clone.traversal = EmptyTraversal.instance();
            clone.labels = new LinkedHashSet<>(this.labels);
            clone.reset();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other != null && other.getClass().equals(this.getClass()) && this.hashCode() == other.hashCode();
    }

    @Override
    public int hashCode() {
        int result = this.getClass().hashCode();
        for (final String label : this.getLabels()) {
            result ^= label.hashCode();
        }
        return result;
    }

    private Traverser.Admin<E> prepareTraversalForNextStep(final Traverser.Admin<E> traverser) {
        if (!this.traverserStepIdAndLabelsSetByChild) {
            traverser.setStepId(this.nextStep.getId());
            traverser.addLabels(this.labels);
        }
        return traverser;
    }

}
