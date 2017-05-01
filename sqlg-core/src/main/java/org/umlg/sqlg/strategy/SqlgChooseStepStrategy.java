package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.HasNextStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.step.SqlgChooseStepBarrier;
import org.umlg.sqlg.structure.SqlgGraph;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2014/08/15
 */
public class SqlgChooseStepStrategy<M, S, E> extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {


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
        List<ChooseStep> chooseSteps = TraversalHelper.getStepsOfAssignableClass(ChooseStep.class, traversal);
        for (ChooseStep<S, E, M> chooseStep : chooseSteps) {
            Traversal.Admin<S, M> predicateTraversal = chooseStep.getLocalChildren().get(0);

            //remove the HasNextStep
            HasNextStep hasNextStep = null;
            for (Step step : predicateTraversal.getSteps()) {
                if (step instanceof HasNextStep) {
                    hasNextStep = (HasNextStep) step;
                }
            }
            if (hasNextStep != null) {
                predicateTraversal.removeStep(hasNextStep);
            }

            SqlgChooseStepBarrier<S, E, M> sqlgChooseStepBarrier = new SqlgChooseStepBarrier<>(traversal, predicateTraversal);
            try {
                Field traversalOptionsField = chooseStep.getClass().getSuperclass().getDeclaredField("traversalOptions");
                traversalOptionsField.setAccessible(true);
                Map<M, List<Traversal.Admin<S, E>>> traversalOptions = (Map<M, List<Traversal.Admin<S, E>>>) traversalOptionsField.get(chooseStep);
                for (Map.Entry<M, List<Traversal.Admin<S, E>>> entry : traversalOptions.entrySet()) {
                    for (Traversal.Admin<S, E> admin : entry.getValue()) {
                        sqlgChooseStepBarrier.addGlobalChildOption(entry.getKey(), admin);
                    }
                }
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            TraversalHelper.replaceStep(
                    chooseStep,
                    sqlgChooseStepBarrier,
                    chooseStep.getTraversal()
            );
//            Pair<Traversal.Admin<?, ?>, Traversal.Admin<?, ?>> trueFalseTraversalPair = SqlgTraversalUtil.trueFalseTraversals(chooseStep);
//            TraversalHelper.replaceStep(
//                    chooseStep,
//                    new SqlgChooseStepBarrier(
//                            chooseStep.getTraversal(),
//                            (Traversal.Admin) chooseStep.getLocalChildren().get(0),
//                            trueFalseTraversalPair.getLeft(),
//                            trueFalseTraversalPair.getRight()
//                    ),
//                    chooseStep.getTraversal()
//            );
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Stream.of(
                SqlgVertexStepStrategy.class
        ).collect(Collectors.toSet());
    }

}
