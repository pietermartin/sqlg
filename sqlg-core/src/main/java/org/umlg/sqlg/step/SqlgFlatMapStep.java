package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.Iterator;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/10/25
 */
public abstract class SqlgFlatMapStep<S, E> extends SqlgAbstractStep<S, E> {

    private Traverser.Admin<S> head = null;
    private Iterator<E> iterator = EmptyIterator.instance();

    public SqlgFlatMapStep(final Traversal.Admin<?, ?> traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        while (true) {
            if (this.iterator.hasNext()) {
                return this.head.split(this.iterator.next(), this);
            } else {
                closeIterator();
                this.head = this.starts.next();
                this.iterator = this.flatMap(this.head);
            }
        }
    }

    protected abstract Iterator<E> flatMap(final Traverser.Admin<S> traverser);

    @Override
    public void reset() {
        super.reset();
        closeIterator();
        this.iterator = EmptyIterator.instance();
    }

    protected void closeIterator() {
        CloseableIterator.closeIterator(iterator);
    }
}
