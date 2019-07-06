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
public abstract class SqlgReducingStepBarrier<S extends Number> extends SqlgAbstractStep<S, S> {

    private boolean first = true;
    private Number result = Double.NaN;
    private Iterator<Traverser.Admin<S>> resultIterator = null;

    SqlgReducingStepBarrier(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        if (this.first) {
            this.first = false;
            while (this.starts.hasNext()) {
                Traverser.Admin<S> s = this.starts.next();
                this.result = reduce(result, s.get());
//                if (result == null) {
//                    result = s.get();
//                } else {
//                    if (result.equals(Double.NaN)) {
//                        result = s.get();
//                    } else {
//                        this.result = reduce(result, s.get());
//                    }
//                }
            }
            Traverser.Admin<S> traverser = SqlgTraverserGenerator.instance().generate((S) result, this, 1L, false, false);
            this.resultIterator = IteratorUtils.asIterator(traverser);
        }
        if (this.resultIterator.hasNext()) {
            return this.resultIterator.next();
        } else {
            reset();
            throw FastNoSuchElementException.instance();
        }
    }

    protected abstract Number reduce(Number a, Number b);
}
