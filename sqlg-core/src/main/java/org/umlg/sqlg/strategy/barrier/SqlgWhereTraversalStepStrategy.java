package org.umlg.sqlg.strategy.barrier;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.barrier.SqlgWhereTraversalStepBarrier;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2014/08/15
 */
public class SqlgWhereTraversalStepStrategy<S> extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    public SqlgWhereTraversalStepStrategy() {
        super();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        //Only optimize SqlgGraph. StarGraph also passes through here.
        if (traversal.getGraph().isEmpty() || !(traversal.getGraph().orElseThrow(IllegalStateException::new) instanceof SqlgGraph)) {
            return;
        }
        if (!SqlgTraversalUtil.mayOptimize(traversal)) {
            return;
        }
        List<WhereTraversalStep> whereTraversalSteps = TraversalHelper.getStepsOfAssignableClass(WhereTraversalStep.class, traversal);
        for (@SuppressWarnings("unchecked") WhereTraversalStep<S> whereTraversalStep : whereTraversalSteps) {

            List<Traversal.Admin<?, ?>> whereTraversals = whereTraversalStep.getLocalChildren();
            Preconditions.checkState(whereTraversals.size() == 1);
            Traversal.Admin<?, ?> whereTraversal = whereTraversals.get(0);

            //reducing barrier steps like count does not work with Sqlg's barrier optimizations
            List<ReducingBarrierStep> reducingBarrierSteps = TraversalHelper.getStepsOfAssignableClassRecursively(ReducingBarrierStep.class, whereTraversal);
            if (!reducingBarrierSteps.isEmpty()) {
                continue;
            }

            SqlgWhereTraversalStepBarrier sqlgTraversalFilterStepBarrier = new SqlgWhereTraversalStepBarrier<>(
                    traversal,
                    whereTraversalStep
            );
            for (String label : whereTraversalStep.getLabels()) {
                sqlgTraversalFilterStepBarrier.addLabel(label);
            }
            //noinspection unchecked
            TraversalHelper.replaceStep(
                    whereTraversalStep,
                    sqlgTraversalFilterStepBarrier,
                    whereTraversalStep.getTraversal()
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
