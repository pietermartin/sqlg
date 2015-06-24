package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.umlg.sqlg.structure.SqlgGraphStep;

import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Date: 2014/07/12
 * Time: 5:45 AM
 */
public class SqlgGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final SqlgGraphStepStrategy INSTANCE = new SqlgGraphStepStrategy();
    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(VertexStep.class);
    private static final List<BiPredicate> SUPPORTED_BI_PREDICATE = Arrays.asList(Compare.eq);

    private SqlgGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final Step<?, ?> startStep = traversal.getStartStep();
        if (startStep instanceof GraphStep) {
            final GraphStep<?> originalGraphStep = (GraphStep) startStep;
            final SqlgGraphStep<?> sqlgGraphStep = new SqlgGraphStep<>(originalGraphStep);
            traversal.addStep(traversal.getSteps().indexOf(originalGraphStep), sqlgGraphStep);
            traversal.removeStep(originalGraphStep);

            Step<?, ?> currentStep = sqlgGraphStep.getNextStep();
            while (true) {
                if (currentStep instanceof HasContainerHolder) {
                    sqlgGraphStep.hasContainers.addAll(((HasContainerHolder) currentStep).getHasContainers());
                    if (!currentStep.getLabels().isEmpty()) {
                        final IdentityStep identityStep = new IdentityStep<>(traversal);
                        currentStep.getLabels().forEach(identityStep::addLabel);
                        TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                    }
                    traversal.removeStep(currentStep);
                } else if (currentStep instanceof IdentityStep) {
                    // do nothing
                } else {
                    break;
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }

    private void collectImmediateHasSteps(SqlgGraphStepCompiled sqlgGraphStep, ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal) {
        //Collect the hasSteps
        while (iterator.hasNext()) {
            Step<?, ?> currentStep = iterator.next();
            if (currentStep instanceof HasContainerHolder && ((HasContainerHolder) currentStep).getHasContainers().size() != 1) {
                throw new IllegalStateException("Only handle HasContainerHolder with one HasContainer: BUG");
            }
            if (currentStep instanceof HasContainerHolder && SUPPORTED_BI_PREDICATE.contains(((HasContainerHolder) currentStep).getHasContainers().get(0).getBiPredicate())) {
                if (!currentStep.getLabels().isEmpty()) {
                    final IdentityStep identityStep = new IdentityStep<>(traversal);
                    currentStep.getLabels().forEach(identityStep::addLabel);
                    TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                }
                iterator.remove();
                traversal.removeStep(currentStep);
                sqlgGraphStep.addHasContainer(((HasContainerHolder) currentStep).getHasContainers().get(0));
            } else if (currentStep instanceof IdentityStep) {
                // do nothing
            } else {
                iterator.previous();
                break;
            }
        }
    }
    private void collectHasSteps(ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, Pair<Step<?, ?>, List<HasContainer>> stepPair) {
        //Collect the hasSteps
        while (iterator.hasNext()) {
            Step<?, ?> currentStep = iterator.next();
            if (currentStep instanceof HasContainerHolder && ((HasContainerHolder) currentStep).getHasContainers().size() != 1) {
                throw new IllegalStateException("Only handle HasContainerHolder with one HasContainer: BUG");
            }
            if (currentStep instanceof HasContainerHolder && SUPPORTED_BI_PREDICATE.contains(((HasContainerHolder) currentStep).getHasContainers().get(0).getBiPredicate())) {
                if (!currentStep.getLabels().isEmpty()) {
                    final IdentityStep identityStep = new IdentityStep<>(traversal);
                    currentStep.getLabels().forEach(identityStep::addLabel);
                    TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                }
                iterator.remove();
                traversal.removeStep(currentStep);
                stepPair.getValue().addAll(((HasContainerHolder) currentStep).getHasContainers());
            } else if (currentStep instanceof IdentityStep) {
                // do nothing
            } else {
                iterator.previous();
                break;
            }
        }
    }

    public static SqlgGraphStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return Stream.of(SqlgVertexStepStrategy.class).collect(Collectors.toSet());
    }
}
