package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.umlg.sqlg.step.SqlgAbstractStep;

import java.util.NoSuchElementException;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/10/26
 */
public class SqlgEmptyStepBarrier<S> extends SqlgAbstractStep<S, S> {

    public SqlgEmptyStepBarrier(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        if (this.starts.hasNext()) {
            final Traverser.Admin<S> traverser = this.starts.next();
            return traverser;
        } else {
            reset();
            throw FastNoSuchElementException.instance();
        }
    }

}
