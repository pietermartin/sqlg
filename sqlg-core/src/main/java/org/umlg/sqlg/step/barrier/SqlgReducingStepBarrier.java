package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.umlg.sqlg.step.SqlgAbstractStep;
import org.umlg.sqlg.structure.traverser.SqlgTraverserGenerator;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2019/07/04
 */
@SuppressWarnings("unchecked")
public abstract class SqlgReducingStepBarrier<S, E> extends SqlgAbstractStep<S, E> {

    private boolean first = true;
    private E result = null;
    private Iterator<Traverser.Admin<E>> resultIterator = null;

    SqlgReducingStepBarrier(Traversal.Admin<?, ?> traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        if (this.first) {
            this.first = false;
            while (this.starts.hasNext()) {
                Traverser.Admin<S> s = this.starts.next();
                this.result = reduce(result, s.get());
            }
            if (this.result == null) {
                throw FastNoSuchElementException.instance();
            } else {
                Traverser.Admin<E> traverser = produceFinalResult(result);
                this.resultIterator = IteratorUtils.asIterator(traverser);
            }
        }
        if (this.resultIterator != null && this.resultIterator.hasNext()) {
            return this.resultIterator.next();
        } else {
            reset();
            throw FastNoSuchElementException.instance();
        }
    }

    protected Traverser.Admin<E> produceFinalResult(E result) {
        return SqlgTraverserGenerator.instance().generate(result, this, 1L, false, false);
    }

    public E reduce(E a, S b) {
        throw new IllegalStateException("noop");
    }

}
