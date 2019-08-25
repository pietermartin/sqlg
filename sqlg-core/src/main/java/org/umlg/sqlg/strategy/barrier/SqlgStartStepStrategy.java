package org.umlg.sqlg.strategy.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.barrier.SqlgStartStepBarrier;
import org.umlg.sqlg.strategy.SqlgGraphStepStrategy;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2019/08/25
 */
public class SqlgStartStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {


    public SqlgStartStepStrategy() {
        super();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        //Only optimize SqlgGraph. StarGraph also passes through here.
        if (!(traversal.getGraph().orElseThrow(IllegalStateException::new) instanceof SqlgGraph)) {
            return;
        }
        if (!SqlgTraversalUtil.mayOptimize(traversal)) {
            return;
        }
        while (true) {
            Optional<StartStep> startStepOptional = TraversalHelper.getLastStepOfAssignableClass(StartStep.class, traversal);
            if (startStepOptional.isPresent()) {
                StartStep<?> startStep = startStepOptional.get();


                SqlgStartStepBarrier<?> sqlgStartStepBarrier = new SqlgStartStepBarrier<>(traversal, startStep);
                for (String label : startStep.getLabels()) {
                    sqlgStartStepBarrier.addLabel(label);
                }

                //noinspection unchecked
                TraversalHelper.replaceStep((Step) startStep, sqlgStartStepBarrier, traversal);
            } else {
                break;
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return Stream.of(
                SqlgVertexStepStrategy.class
        ).collect(Collectors.toSet());
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Stream.of(
                SqlgGraphStepStrategy.class
        ).collect(Collectors.toSet());
    }

}
