package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.util.NumberHelper;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2019/07/04
 */
@SuppressWarnings("rawtypes")
public class SqlgMinGlobalStep extends SqlgReducingStepBarrier<Comparable, Comparable> {

    public SqlgMinGlobalStep(Traversal.Admin<?, ?> traversal) {
        super(traversal);
    }

    @Override
    public Comparable reduce(Comparable a, Comparable b) {
        if (a == null) {
            return b;
//        } else if (b.equals(Double.NaN)) {
//            return b;
        } else {
            return NumberHelper.min(a, b);
        }
//        if (a instanceof Number && b instanceof Number && !a.equals(Double.NaN) && !b.equals(Double.NaN)) {
//            final Number an = (Number) a, bn = (Number) b;
//            final Class<? extends Number> clazz = getHighestCommonNumberClass(an, bn);
//            return (Comparable) getHelper(clazz).min.apply(an, bn);
//        }
//        return isNonValue(a) ? b :
//                isNonValue(b) ? a :
//                        a.compareTo(b) < 0 ? a : b;
    }

}
