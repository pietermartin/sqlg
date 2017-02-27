package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.umlg.sqlg.structure.SqlgElement;

import java.util.NoSuchElementException;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/02/24
 */
public class SqlgBatchBarrierStep<S extends SqlgElement> extends AbstractStep<S, S> implements Step<S, S> {


    public SqlgBatchBarrierStep(Step step, Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin processNextStart() throws NoSuchElementException {
        return this.starts.next();
    }

}
