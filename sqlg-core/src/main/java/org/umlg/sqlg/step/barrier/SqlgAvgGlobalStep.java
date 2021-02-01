package org.umlg.sqlg.step.barrier;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.umlg.sqlg.structure.traverser.SqlgTraverserGenerator;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2019/07/04
 */
public class SqlgAvgGlobalStep extends SqlgReducingStepBarrier<Pair<Number, Long>, Number> {

    public SqlgAvgGlobalStep(Traversal.Admin<?, ?> traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<Number> produceFinalResult(Number result) {
        return SqlgTraverserGenerator.instance().generate(((MeanGlobalStep.MeanNumber) result).getFinal(), this, 1L, false, false);
    }

    @Override
    public Number reduce(Number meanNumber, Pair<Number, Long> bPair) {
        if (meanNumber == null) {
            meanNumber = new MeanGlobalStep.MeanNumber(0, 0);
        }
        return ((MeanGlobalStep.MeanNumber) meanNumber).add(new MeanGlobalStep.MeanNumber(bPair.getLeft(), bPair.getRight()));
    }

}
