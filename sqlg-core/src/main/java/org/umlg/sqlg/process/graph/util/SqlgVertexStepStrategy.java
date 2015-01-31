package org.umlg.sqlg.process.graph.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.traversal.step.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.traversal.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.traversal.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiPredicate;

/**
 * Date: 2014/08/15
 * Time: 7:34 PM
 */
public class SqlgVertexStepStrategy extends AbstractTraversalStrategy {

    private static final SqlgVertexStepStrategy INSTANCE = new SqlgVertexStepStrategy();
    private static final List<Class> CONSECUTIVE_STEPS_TO_REPLACE = Arrays.asList(VertexStep.class);
    private static final List<BiPredicate> SUPPORTED_BI_PREDICATE = Arrays.asList(Compare.eq);

    private SqlgVertexStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine traversalEngine) {
        //Replace all consecutive VertexStep and HasStep with one step
        List<Step> steps = new ArrayList<>(traversal.asAdmin().getSteps());
        Step previous = null;
        SqlgVertexStepCompiler sqlgVertexStepCompiler = null;
        ListIterator<Step> stepIterator = steps.listIterator();
        while (stepIterator.hasNext()) {
            Step step = stepIterator.next();
            if (CONSECUTIVE_STEPS_TO_REPLACE.contains(step.getClass())) {
                Pair<Step<?, ?>, List<HasContainer>> stepPair = Pair.of(step, new ArrayList<>());
                if (previous == null) {
                    sqlgVertexStepCompiler = new SqlgVertexStepCompiler(traversal);
                    TraversalHelper.replaceStep(step, sqlgVertexStepCompiler, traversal);
                    collectHasSteps(stepIterator, traversal, stepPair);
                } else {
                    traversal.removeStep(step);
                    collectHasSteps(stepIterator, traversal, stepPair);
                }
                previous = step;
                sqlgVertexStepCompiler.addReplacedStep(stepPair);
            } else {
                previous = null;
            }
        }
    }

    private void collectHasSteps(ListIterator<Step> iterator, Traversal.Admin<?, ?> traversal, Pair<Step<?, ?>, List<HasContainer>> stepPair) {
        //Collect the hasSteps
        while (iterator.hasNext()) {
            Step<?, ?> currentStep = iterator.next();
            if (currentStep instanceof HasContainerHolder && ((HasContainerHolder)currentStep).getHasContainers().size() != 1) {
                throw new IllegalStateException("Only handle HasContainderHolder with one HasContainer: BUG");
            }
            if (currentStep instanceof HasContainerHolder && SUPPORTED_BI_PREDICATE.contains(((HasContainerHolder)currentStep).getHasContainers().get(0).predicate)) {
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

    public static SqlgVertexStepStrategy instance() {
        return INSTANCE;
    }

}
