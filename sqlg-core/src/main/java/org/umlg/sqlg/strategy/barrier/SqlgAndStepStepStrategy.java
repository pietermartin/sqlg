package org.umlg.sqlg.strategy.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.barrier.SqlgAndStepBarrier;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2014/08/15
 */
public class SqlgAndStepStepStrategy<S> extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    public SqlgAndStepStepStrategy() {
        super();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        //Only optimize SqlgGraph. StarGraph also passes through here.
        //noinspection OptionalGetWithoutIsPresent
        if (traversal.getGraph().isEmpty() || !(traversal.getGraph().orElseThrow(IllegalStateException::new) instanceof SqlgGraph)) {
            return;
        }
        if (!SqlgTraversalUtil.mayOptimize(traversal)) {
            return;
        }
        List<AndStep> andSteps = TraversalHelper.getStepsOfAssignableClass(AndStep.class, traversal);
        for (AndStep<S> andStep : andSteps) {

            Collection<Traversal.Admin<S, ?>> andTraversals = andStep.getLocalChildren();

            //reducing barrier steps like count does not work with Sqlg's barrier optimizations
            List<ReducingBarrierStep> reducingBarrierSteps = TraversalHelper.getStepsOfAssignableClassRecursively(ReducingBarrierStep.class, traversal);
            if (!reducingBarrierSteps.isEmpty()) {
                return;
            }

            SqlgAndStepBarrier<S> sqlgAndStepBarrier = new SqlgAndStepBarrier(
                    traversal,
                    andTraversals
            );
            for (String label : andStep.getLabels()) {
                sqlgAndStepBarrier.addLabel(label);
            }
            TraversalHelper.replaceStep(
                    andStep,
                    sqlgAndStepBarrier,
                    andStep.getTraversal()
            );
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Stream.of(
                SqlgWhereTraversalStepStrategy.class
        ).collect(Collectors.toSet());
    }

}
