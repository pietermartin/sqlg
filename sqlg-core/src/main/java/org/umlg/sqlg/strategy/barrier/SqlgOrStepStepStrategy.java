package org.umlg.sqlg.strategy.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.barrier.SqlgOrStepBarrier;
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
public class SqlgOrStepStepStrategy<S> extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    public SqlgOrStepStepStrategy() {
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
        List<OrStep> orSteps = TraversalHelper.getStepsOfAssignableClass(OrStep.class, traversal);
        for (@SuppressWarnings("unchecked") OrStep<S> orStep : orSteps) {

            Collection<Traversal.Admin<S, ?>> orTraversals = orStep.getLocalChildren();

            //reducing barrier steps like count does not work with Sqlg's barrier optimizations
            List<ReducingBarrierStep> reducingBarrierSteps = TraversalHelper.getStepsOfAssignableClassRecursively(ReducingBarrierStep.class, traversal);
            if (!reducingBarrierSteps.isEmpty()) {
                return;
            }

            @SuppressWarnings("unchecked") SqlgOrStepBarrier<S> sqlgOrStepBarrier = new SqlgOrStepBarrier(
                    traversal,
                    orTraversals
            );
            for (String label : orStep.getLabels()) {
                sqlgOrStepBarrier.addLabel(label);
            }
            TraversalHelper.replaceStep(
                    orStep,
                    sqlgOrStepBarrier,
                    orStep.getTraversal()
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
