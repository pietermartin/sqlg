package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.util.NumberHelper;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2019/07/04
 */
public class SqlgSumGlobalStep extends SqlgReducingStepBarrier<Number, Number> {

    public SqlgSumGlobalStep(Traversal.Admin<?, ?> traversal) {
        super(traversal);
    }

    @Override
    public Number reduce(Number a, Number b) {
        if (a == null) {
            return b;
        } else if (b.equals(Double.NaN)) {
            return b;
        } else {
            return NumberHelper.add(a, b);
        }
    }

}
