package org.umlg.sqlg.step.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.util.NumberHelper;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;

/**
 * @author Pieter Martin
 * Date: 2021/01/04
 */
public class SqlgCountGlobalStep extends SqlgReducingStepBarrier<Number, Number> {

    public SqlgCountGlobalStep(Traversal.Admin<?, ?> traversal) {
        super(traversal);
        this.setSeedSupplier(new ConstantSupplier<>(0L));
    }

    @Override
    public Number reduce(Number a, Number b) {
        if (a == null) {
            return b;
        } else {
            return NumberHelper.add(a, b);
        }
    }
}
