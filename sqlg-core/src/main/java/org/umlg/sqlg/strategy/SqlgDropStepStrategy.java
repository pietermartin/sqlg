package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.step.SqlgVertexStep;
import org.umlg.sqlg.strategy.barrier.SqlgVertexStepStrategy;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/11/11
 */
public class SqlgDropStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy  {

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (!(traversal.getGraph().get() instanceof SqlgGraph)) {
            return;
        }
        Optional<DropStep> dropStepOptional = TraversalHelper.getLastStepOfAssignableClass(DropStep.class, traversal);
        if (dropStepOptional.isPresent()) {
            DropStep dropStep = dropStepOptional.get();
            Step previousStep = dropStep.getPreviousStep();
            if (previousStep instanceof SqlgGraphStep || previousStep instanceof SqlgVertexStep) {

                if (previousStep instanceof SqlgGraphStep) {
                    optimize((SqlgGraphStep)previousStep);
                } else {
                    optimize((SqlgVertexStep)previousStep);
                }

            }
        }
    }

    private void optimize(SqlgGraphStep previousStep) {
        System.out.println(previousStep);
    }

    private void optimize(SqlgVertexStep previousStep) {

    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Stream.of(
                SqlgGraphStepStrategy.class,
                SqlgVertexStepStrategy.class
        ).collect(Collectors.toSet());
    }

//    @Override
//    public Set<Class<? extends OptimizationStrategy>> applyPost() {
//        return null;
//    }
}
