package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/10/10
 */
public class SqlgExpandableStepIterator<S> implements Iterator<Traverser.Admin<S>>, Serializable {

    private final Step<S, ?> hostStep;
    private final Queue<Traverser.Admin<S>> traversers = new LinkedList<>();

    public SqlgExpandableStepIterator(Step<S, ?> hostStep) {
        this.hostStep = hostStep;
    }

    @Override
    public boolean hasNext() {
        return !this.traversers.isEmpty() || this.hostStep.getPreviousStep().hasNext();
    }

    @Override
    public Traverser.Admin<S> next() {
        if (!this.traversers.isEmpty())
            return this.traversers.poll();
        /////////////
        if (this.hostStep.getPreviousStep().hasNext())
            return this.hostStep.getPreviousStep().next();
        /////////////
        throw FastNoSuchElementException.instance();
    }

    public void add(final Iterator<Traverser.Admin<S>> iterator) {
        iterator.forEachRemaining(this.traversers::add);
    }

    public void add(final Traverser.Admin<S> traverser) {
        this.traversers.add(traverser);
    }

    @Override
    public String toString() {
        return this.traversers.toString();
    }

    public void clear() {
        this.traversers.clear();
    }
}
