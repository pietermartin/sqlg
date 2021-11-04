package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.umlg.sqlg.step.SqlgAbstractStep;
import org.umlg.sqlg.structure.traverser.SqlgTraverserGenerator;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2019/07/04
 */
@SuppressWarnings("unchecked")
public abstract class SqlgReducingStepBarrier<S, E> extends SqlgAbstractStep<S, E> {

    private boolean first = true;
    private E result = null;
    private Supplier<E> seedSupplier;
    private Iterator<Traverser.Admin<E>> resultIterator = null;
    private boolean resetted = false;
    private boolean inSideStartLoop = false;
    private boolean hasStarts = false;

    public SqlgReducingStepBarrier(Traversal.Admin<?, ?> traversal) {
        super(traversal);
    }

    public void setSeedSupplier(final Supplier<E> seedSupplier) {
        this.seedSupplier = seedSupplier;
    }

    @Override
    public boolean hasStarts() {
        if (this.first) {
            this.hasStarts = this.starts.hasNext();
        }
        return this.hasStarts;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        if (this.first) {
            this.first = false;
//            boolean seedSupplierIsCollection = false;
//            boolean seedSupplierIsMap = false;
            if (this.seedSupplier != null) {
                this.result = this.seedSupplier.get();
//                seedSupplierIsCollection = this.result instanceof Collection;
//                seedSupplierIsMap = !seedSupplierIsCollection && this.result instanceof Map;
            }
            while (this.starts.hasNext()) {
                this.inSideStartLoop = true;
                if (this.resetted) {
                    this.resetted = false;
                    this.first = true;
                    break;
                }
                Traverser.Admin<S> s = this.starts.next();
                this.result = reduce(this.result, s.get());
            }
            this.inSideStartLoop = false;

//            if (this.result == null || (seedSupplierIsCollection && ((Collection<?>) result).isEmpty()) || (seedSupplierIsMap && ((Map<?, ?>) this.result).isEmpty())) {
            if (this.result == null) {
                throw FastNoSuchElementException.instance();
            } else {
                Traverser.Admin<E> traverser = produceFinalResult(this.result);
                this.resultIterator = IteratorUtils.asIterator(traverser);
            }
        }
        if (this.resultIterator != null && this.resultIterator.hasNext()) {
            return this.resultIterator.next();
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    protected Traverser.Admin<E> produceFinalResult(E result) {
        return SqlgTraverserGenerator.instance().generate(result, this, 1L, false, false);
    }

    public E reduce(E a, S b) {
        throw new IllegalStateException("noop");
    }

    @Override
    public void reset() {
        super.reset();
        this.first = true;
        if (!this.inSideStartLoop) {
            this.result = null;
        } else {
            this.resetted = true;
        }
    }
}
