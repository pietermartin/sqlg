package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.graph.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Compare;
import org.umlg.sqlg.structure.SqlgGraphStep;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiPredicate;

/**
 * Date: 2014/07/12
 * Time: 5:45 AM
 */
public class SqlgGraphStepStrategy extends AbstractTraversalStrategy {

    private static final SqlgGraphStepStrategy INSTANCE = new SqlgGraphStepStrategy();
    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(VertexStep.class);
    private static final List<BiPredicate> SUPPORTED_BI_PREDICATE = Arrays.asList(Compare.eq);

    private SqlgGraphStepStrategy() {
    }

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
                    if (currentStep.getLabel().isPresent()) {
                        final IdentityStep identityStep = new IdentityStep<>(traversal);
                        identityStep.setLabel(currentStep.getLabel().get());
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

//            final GraphStep<?> originalGraphStep = (GraphStep) startStep;
//            final SqlgGraphStepCompiled sqlgGraphStep = new SqlgGraphStepCompiled<>(originalGraphStep);
//            traversal.addStep(traversal.getSteps().indexOf(originalGraphStep), sqlgGraphStep);
//            traversal.removeStep(originalGraphStep);
//
//            //Replace all consecutive HasStep and VertexStep with one step
//            List<Step> steps = new ArrayList<>(traversal.asAdmin().getSteps());
//            ListIterator<Step> stepIterator = steps.listIterator();
//
//            //this first step will always be the GraphStep, collect all immediately following hasSteps
//            collectImmediateHasSteps(sqlgGraphStep, stepIterator, traversal);
//
//            while (stepIterator.hasNext()) {
//                Step<?, ?> step = stepIterator.next();
//                if (CONSECUTIVE_STEPS_TO_REPLACE.contains(step.getClass())) {
//                    Pair<Step<?, ?>, List<HasContainer>> stepPair = Pair.of(step, new ArrayList<>());
//                    traversal.removeStep(step);
//                    collectHasSteps(stepIterator, traversal, stepPair);
//                    sqlgGraphStep.addReplacedStep(stepPair);
//                }
//            }
        }
    }

    private void collectImmediateHasSteps(SqlgGraphStepCompiled sqlgGraphStep, ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal) {
        //Collect the hasSteps
        while (iterator.hasNext()) {
            Step<?, ?> currentStep = iterator.next();
            if (currentStep instanceof HasContainerHolder && ((HasContainerHolder) currentStep).getHasContainers().size() != 1) {
                throw new IllegalStateException("Only handle HasContainerHolder with one HasContainer: BUG");
            }
            if (currentStep instanceof HasContainerHolder && SUPPORTED_BI_PREDICATE.contains(((HasContainerHolder) currentStep).getHasContainers().get(0).predicate)) {
                if (currentStep.getLabel().isPresent()) {
                    final IdentityStep identityStep = new IdentityStep<>(traversal);
                    identityStep.setLabel(currentStep.getLabel().get());
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
            if (currentStep instanceof HasContainerHolder && SUPPORTED_BI_PREDICATE.contains(((HasContainerHolder) currentStep).getHasContainers().get(0).predicate)) {
                if (currentStep.getLabel().isPresent()) {
                    final IdentityStep identityStep = new IdentityStep<>(traversal);
                    identityStep.setLabel(currentStep.getLabel().get());
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

}
