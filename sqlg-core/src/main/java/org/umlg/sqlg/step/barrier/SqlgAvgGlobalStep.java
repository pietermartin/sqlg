package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.util.NumberHelper;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2019/07/04
 */
public class SqlgAvgGlobalStep extends SqlgReducingStepBarrier {

    private Number total = 0D;
    private long count = 0;

    public SqlgAvgGlobalStep(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Number reduce(Number a, Number b) {
        Number result;
        if (a.equals(Double.NaN)) {
            this.count++;
            this.total = b;
            result = this.total;
        } else if (b.equals(Double.NaN)) {
            this.total = a;
            result = this.total;
        } else {
            this.count++;
            this.total = NumberHelper.add(a, b);
            result =  this.total.doubleValue() / this.count;
        }
        return result;
    }

}
