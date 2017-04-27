package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.umlg.sqlg.strategy.SqlgNoSuchElementException;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/24
 */
public abstract class SqlgFilterStep<S> extends AbstractStep<S, S> {

    private boolean isForLocalTraversal;

    public SqlgFilterStep(final Traversal.Admin traversal) {
        super(traversal);
        this.isForLocalTraversal = !(traversal.getParent().asStep() instanceof EmptyStep);
    }

    @Override
    protected Traverser.Admin<S> processNextStart() {
        while (true) {
            final Traverser.Admin<S> traverser = this.starts.next();
            if (this.filter(traverser)) {
                return traverser;
            } else if (this.isForLocalTraversal) {
                throw new SqlgNoSuchElementException();
            }
        }
    }

    protected abstract boolean filter(final Traverser.Admin<S> traverser);
}
