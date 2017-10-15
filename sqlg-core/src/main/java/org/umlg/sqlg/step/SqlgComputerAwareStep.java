package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/10/14
 */
public abstract class SqlgComputerAwareStep<S, E> extends SqlgAbstractStep<S, E> implements GraphComputing {

    private Iterator<Traverser.Admin<E>> previousIterator = EmptyIterator.instance();

    public SqlgComputerAwareStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        while (true) {
            if (this.previousIterator.hasNext())
                return this.previousIterator.next();
            this.previousIterator = this.traverserStepIdAndLabelsSetByChild ? this.computerAlgorithm() : this.standardAlgorithm();
        }
    }

    @Override
    public void onGraphComputer() {
        this.traverserStepIdAndLabelsSetByChild = true;
    }

    @Override
    public SqlgComputerAwareStep<S, E> clone() {
        final SqlgComputerAwareStep<S, E> clone = (SqlgComputerAwareStep<S, E>) super.clone();
        clone.previousIterator = EmptyIterator.instance();
        return clone;
    }

    protected abstract Iterator<Traverser.Admin<E>> standardAlgorithm() throws NoSuchElementException;

    protected abstract Iterator<Traverser.Admin<E>> computerAlgorithm() throws NoSuchElementException;

    //////

    public static class EndStep<S> extends AbstractStep<S, S> implements GraphComputing {

        public EndStep(final Traversal.Admin traversal) {
            super(traversal);
        }

        @Override
        protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
            final Traverser.Admin<S> start = this.starts.next();
            if (this.traverserStepIdAndLabelsSetByChild) {
                final ComputerAwareStep<?, ?> step = (ComputerAwareStep<?, ?>) this.getTraversal().getParent();
                start.setStepId(step.getNextStep().getId());
                start.addLabels(step.getLabels());
            }
            return start;
        }

        @Override
        public String toString() {
            return StringFactory.stepString(this);
        }

        @Override
        public void onGraphComputer() {
            this.traverserStepIdAndLabelsSetByChild = true;
        }
    }
}
