package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Rewrites union followed by path. The path step is moved to each traversal inside the union.
 */
public class SqlgUnionPathStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    public SqlgUnionPathStrategy() {
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
        Optional<UnionStep> unionStepOptional = TraversalHelper.getLastStepOfAssignableClass(UnionStep.class, traversal);
        if (unionStepOptional.isPresent()) {
            UnionStep<?, ?> unionStep = unionStepOptional.get();

            //Any traversal with a reducing barrier step can not be optimized. As of yet...
            List<? extends Traversal.Admin<?, ?>> globalChildren = unionStep.getGlobalChildren();

            int unionStepIndex = TraversalHelper.stepIndex(unionStep, traversal);
            Optional<PathStep> pathStepOptional = TraversalHelper.getLastStepOfAssignableClass(PathStep.class, traversal);
            if (pathStepOptional.isPresent()) {
                PathStep toMove = pathStepOptional.get();
                int pathStepIndex = TraversalHelper.stepIndex(toMove, traversal);
                if (pathStepIndex > unionStepIndex) {
                    for (Traversal.Admin<?, ?> globalChild : unionStep.getGlobalChildren()) {
                        globalChild.addStep(globalChild.getSteps().size() - 1, toMove.clone());
                    }
                    traversal.removeStep(toMove);
                }
            }
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return Stream.of(
                SqlgGraphStepStrategy.class
        ).collect(Collectors.toSet());
    }

}
