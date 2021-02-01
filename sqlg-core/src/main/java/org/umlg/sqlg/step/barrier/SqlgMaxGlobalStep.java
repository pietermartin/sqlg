package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.util.NumberHelper;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2019/07/04
 */
@SuppressWarnings("rawtypes")
public class SqlgMaxGlobalStep extends SqlgReducingStepBarrier<Comparable, Comparable> {

    public SqlgMaxGlobalStep(Traversal.Admin<?, ?> traversal) {
        super(traversal);
    }

    @Override
    public Comparable reduce(Comparable a, Comparable b) {
        if (a == null) {
            return b;
        } else if (b.equals(Double.NaN)) {
            return a;
        } else {
            return NumberHelper.max(a, b);
        }
    }

}
