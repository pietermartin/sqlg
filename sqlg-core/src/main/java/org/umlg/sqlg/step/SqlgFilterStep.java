package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/10/13
 */
public abstract class SqlgFilterStep<S> extends SqlgAbstractStep<S, S> {

    public SqlgFilterStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<S> processNextStart() {
        while (true) {
            final Traverser.Admin<S> traverser = this.starts.next();
            if (this.filter(traverser))
                return traverser;
        }
    }

    protected abstract boolean filter(final Traverser.Admin<S> traverser);
}

