package org.umlg.sqlg.strategy.barrier;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.barrier.SqlgTraversalFilterStepBarrier;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2014/08/15
 */
public class SqlgTraversalFilterStepStrategy<S> extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    public SqlgTraversalFilterStepStrategy() {
        super();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        //Only optimize SqlgGraph. StarGraph also passes through here.
        if (traversal.getGraph().isEmpty() || !(traversal.getGraph().orElseThrow(IllegalStateException::new) instanceof SqlgGraph)) {
            return;
        }
//        if (!SqlgTraversalUtil.mayOptimize(traversal)) {
//            return;
//        }
        List<TraversalFilterStep> traversalFilterSteps = TraversalHelper.getStepsOfAssignableClass(TraversalFilterStep.class, traversal);
        for (@SuppressWarnings("unchecked") TraversalFilterStep<S> traversalFilterStep : traversalFilterSteps) {

            List<Traversal.Admin<S, ?>> filterTraversals = traversalFilterStep.getLocalChildren();
            Preconditions.checkState(filterTraversals.size() == 1);
            Traversal.Admin<S, ?> filterTraversal = filterTraversals.get(0);

            //reducing barrier steps like count does not work with Sqlg's barrier optimizations
            List<ReducingBarrierStep> reducingBarrierSteps = TraversalHelper.getStepsOfAssignableClassRecursively(ReducingBarrierStep.class, filterTraversal);
            if (!reducingBarrierSteps.isEmpty()) {
                continue;
            }

            SqlgTraversalFilterStepBarrier sqlgTraversalFilterStepBarrier = new SqlgTraversalFilterStepBarrier<>(
                    traversal,
                    filterTraversal
            );
            for (String label : traversalFilterStep.getLabels()) {
                sqlgTraversalFilterStepBarrier.addLabel(label);
            }
            //noinspection unchecked
            TraversalHelper.replaceStep(
                    traversalFilterStep,
                    sqlgTraversalFilterStepBarrier,
                    traversalFilterStep.getTraversal()
            );
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Stream.of(
                //Inline must happen first as it sometimes removes the need for a TraversalFilterStep
                InlineFilterStrategy.class
        ).collect(Collectors.toSet());
    }

}
