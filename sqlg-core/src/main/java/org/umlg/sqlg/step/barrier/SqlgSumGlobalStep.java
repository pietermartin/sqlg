package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.util.NumberHelper;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2019/07/04
 */
public class SqlgSumGlobalStep extends SqlgReducingStepBarrier {

    public SqlgSumGlobalStep(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Number reduce(Number a, Number b) {
        if (a.equals(Double.NaN)) {
            return b;
        } else if (b.equals(Double.NaN)) {
            return b;
        } else {
            return NumberHelper.add(a, b);
        }
    }

}
