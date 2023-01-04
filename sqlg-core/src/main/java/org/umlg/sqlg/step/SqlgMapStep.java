package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/10/25
 */
public abstract class SqlgMapStep<S, E> extends SqlgAbstractStep<S, E> {


    protected SqlgMapStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        final Traverser.Admin<S> traverser = this.starts.next();
        return traverser.split(this.map(traverser), this);
    }

    protected abstract E map(final Traverser.Admin<S> traverser);
}
