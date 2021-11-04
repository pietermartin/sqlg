package org.umlg.sqlg.strategy.barrier;

import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.MessagePassingReductionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.LambdaFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.HasNextStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.MatchPredicateStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.javatuples.Pair;
import org.umlg.sqlg.step.SqlgLambdaFilterStep;
import org.umlg.sqlg.step.barrier.SqlgBranchStepBarrier;
import org.umlg.sqlg.step.barrier.SqlgChooseStepBarrier;
import org.umlg.sqlg.step.SqlgHasNextStep;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgTraversalUtil;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2014/08/15
 */
@SuppressWarnings("rawtypes")
public class SqlgChooseStepStrategy<M, S, E> extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    public SqlgChooseStepStrategy() {
        super();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        //Only optimize SqlgGraph. StarGraph also passes through here.
        if (traversal.getGraph().isEmpty() || !(traversal.getGraph().orElseThrow(IllegalStateException::new) instanceof SqlgGraph)) {
            return;
        }
        if (!SqlgTraversalUtil.mayOptimize(traversal)) {
            return;
        }
        List<ChooseStep> chooseSteps = TraversalHelper.getStepsOfAssignableClass(ChooseStep.class, traversal);
        for (ChooseStep<S, E, M> chooseStep : chooseSteps) {
            Traversal.Admin<S, M> predicateTraversal = chooseStep.getLocalChildren().get(0);

            //The predicate branch step is a local traversal.
            //As such if it contains a ReducingBarrierStep the SqlgBranchStepBarrier will not work.
            List<ReducingBarrierStep> reducingBarrierSteps = TraversalHelper.getStepsOfAssignableClass(ReducingBarrierStep.class, predicateTraversal);
            if (!reducingBarrierSteps.isEmpty()) {
                continue;
            }

            List<LambdaFilterStep> lambdaFilterSteps = TraversalHelper.getStepsOfAssignableClass(LambdaFilterStep.class, predicateTraversal);
            for (LambdaFilterStep lambdaFilterStep : lambdaFilterSteps) {
                SqlgLambdaFilterStep<?> sqlgLambdaFilterStep = new SqlgLambdaFilterStep<>(predicateTraversal, lambdaFilterStep.getPredicate());
                TraversalHelper.replaceStep(
                        lambdaFilterStep,
                        sqlgLambdaFilterStep,
                        predicateTraversal
                );
                Step hasNextStep = predicateTraversal.getSteps().get(predicateTraversal.getSteps().size() - 1);
                if (hasNextStep instanceof HasNextStep) {
                    SqlgHasNextStep<?> sqlgHasNextStep = new SqlgHasNextStep<>(predicateTraversal);
                    TraversalHelper.replaceStep(
                            hasNextStep,
                            sqlgHasNextStep,
                            predicateTraversal
                    );
                }
            }

            SqlgBranchStepBarrier sqlgBranchStepBarrier = new SqlgChooseStepBarrier<>(
                    traversal,
                    predicateTraversal
            );
            for (String label : chooseStep.getLabels()) {
                sqlgBranchStepBarrier.addLabel(label);
            }
            try {
                //protected List<Pair<Traversal.Admin, Traversal.Admin<S, E>>> traversalOptions = new ArrayList<>();
                Field traversalOptionsField = chooseStep.getClass().getSuperclass().getDeclaredField("traversalOptions");
                traversalOptionsField.setAccessible(true);
                List<Pair<Traversal.Admin, Traversal.Admin<S, E>>> traversalOptions = (List<Pair<Traversal.Admin, Traversal.Admin<S, E>>>)traversalOptionsField.get(chooseStep);
                for (Pair<Traversal.Admin, Traversal.Admin<S, E>> traversalOptionPair : traversalOptions) {
                    sqlgBranchStepBarrier.addGlobalChildOption(traversalOptionPair.getValue0(), traversalOptionPair.getValue1());
                }
                //protected Map<Pick, List<Traversal.Admin<S, E>>> traversalPickOptions = new HashMap<>();
                Field traversalPickOptionsField = chooseStep.getClass().getSuperclass().getDeclaredField("traversalPickOptions");
                traversalPickOptionsField.setAccessible(true);
                Map<TraversalOptionParent.Pick, List<Traversal.Admin<S, E>>> traversalPickOptions = (Map<TraversalOptionParent.Pick, List<Traversal.Admin<S, E>>>)traversalPickOptionsField.get(chooseStep);
                for (TraversalOptionParent.Pick pick : traversalPickOptions.keySet()) {
                    for (Traversal.Admin<S, E> admin : traversalPickOptions.get(pick)) {
                        sqlgBranchStepBarrier.addGlobalChildOption(pick, admin);
                    }
                }
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            TraversalHelper.replaceStep(
                    chooseStep,
                    sqlgBranchStepBarrier,
                    chooseStep.getTraversal()
            );
        }
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPost() {
        return Stream.of(
                MatchPredicateStrategy.class,
                RepeatUnrollStrategy.class,
                PathRetractionStrategy.class,
                MessagePassingReductionStrategy.class,
                IncidentToAdjacentStrategy.class
        ).collect(Collectors.toSet());
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Stream.of(
                SqlgVertexStepStrategy.class
        ).collect(Collectors.toSet());
    }

}
