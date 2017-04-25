package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.SqlgChooseStep;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2014/08/15
 */
public class SqlgChooseStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy  {


    public SqlgChooseStepStrategy() {
        super();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        //Only optimize SqlgGraph. StarGraph also passes through here.
        //noinspection OptionalGetWithoutIsPresent
        if (!(traversal.getGraph().get() instanceof SqlgGraph)) {
            return;
        }
        List<ChooseStep> chooseSteps = TraversalHelper.getStepsOfAssignableClassRecursively(ChooseStep.class, traversal);
        for (ChooseStep chooseStep : chooseSteps) {
            TraversalHelper.replaceStep(
                    chooseStep,
                    new SqlgChooseStep(
                            chooseStep.getTraversal(),
                            (Traversal.Admin)chooseStep.getLocalChildren().get(0),
                            (Traversal.Admin)chooseStep.getGlobalChildren().get(0),
                            (Traversal.Admin)chooseStep.getGlobalChildren().get(1)),
                    chooseStep.getTraversal()
            );
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Stream.of(
                SqlgVertexStepStrategy.class
        ).collect(Collectors.toSet());
    }

}
