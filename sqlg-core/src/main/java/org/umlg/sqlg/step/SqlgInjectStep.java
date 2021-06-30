package org.umlg.sqlg.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.util.iterator.ArrayIterator;
import org.umlg.sqlg.step.barrier.SqlgStartStepBarrier;

public class SqlgInjectStep<S> extends SqlgStartStepBarrier<S> {

    private final S[] injections;

    public SqlgInjectStep(Traversal.Admin traversal, final S... injections) {
        super(traversal);
        this.injections = injections;
        this.start = new ArrayIterator<>(this.injections);
    }

    @Override
    public void reset() {
        super.reset();
        this.start = new ArrayIterator<>(this.injections);
    }
}
