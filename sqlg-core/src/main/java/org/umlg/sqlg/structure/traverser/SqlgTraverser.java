package org.umlg.sqlg.structure.traverser;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_P_S_SE_SL_Traverser;

import java.util.HashSet;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/05/01
 */
public class SqlgTraverser<T> extends B_LP_NL_O_P_S_SE_SL_Traverser<T> implements ISqlgTraverser {

    private long startElementIndex;
    private final boolean requiresSack;
    private boolean requiresOneBulk;

    SqlgTraverser(T t, Step<T, ?> step, long initialBulk, boolean requiresSack, boolean requiresOneBulk) {
        super(t, step, initialBulk);
        this.requiresSack = requiresSack;
        this.requiresOneBulk = requiresOneBulk;
    }

    public void setStartElementIndex(long startElementIndex) {
        this.startElementIndex = startElementIndex;
    }

    public long getStartElementIndex() {
        return this.startElementIndex;
    }

    @Override
    public void resetLoops() {
        if (!this.nestedLoops.isEmpty()) {
            this.nestedLoops.pop();
        }
    }

//    @Override
//    public <R> Traverser.Admin<R> split(final R r, final Step<T, R> step) {
//        final SqlgTraverser<R> clone = (SqlgTraverser<R>) super.split(r, step);
//        clone.path = clone.path.clone().extend(r, step.getLabels());
//        return clone;
//    }
//
//    @Override
//    public Traverser.Admin<T> split() {
//        final SqlgTraverser<T> clone = (SqlgTraverser<T>) super.split();
//        clone.path = clone.path.clone();
//        return clone;
//    }

    @Override
    public void merge(final Traverser.Admin<?> other) {
        if (this.requiresOneBulk) {
            //O_Traverser
            if (!other.getTags().isEmpty()) {
                if (this.tags == null) this.tags = new HashSet<>();
                this.tags.addAll(other.getTags());
            }

            //skip the B_O_Traverser
            //B_O_Traverser
            //this.bulk = this.bulk + other.bulk();

            //B_O_S_SE_SL_Traverser
            if (null != this.sack && null != this.sideEffects.getSackMerger())
                this.sack = this.sideEffects.getSackMerger().apply(this.sack, other.sack());
        } else {
            super.merge(other);
        }

    }

    @Override
    public int hashCode() {
        if (this.requiresSack) {
            return this.t.hashCode() + this.future.hashCode() + this.loops;
        } else {
            return super.hashCode();
        }
    }

    @Override
    public boolean equals(final Object object) {
        if (this.requiresSack) {
            return (object instanceof SqlgTraverser)
                    && ((SqlgTraverser) object).t.equals(this.t)
                    && ((SqlgTraverser) object).future.equals(this.future)
                    && ((SqlgTraverser) object).loops == this.loops
                    && (null == this.sack || null != this.sideEffects.getSackMerger());
        } else {
            return super.equals(object);
        }
    }

    public void setRequiresOneBulk(boolean requiresOneBulk) {
        this.requiresOneBulk = requiresOneBulk;
    }
}
