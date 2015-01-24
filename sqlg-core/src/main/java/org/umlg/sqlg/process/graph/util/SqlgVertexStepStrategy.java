package org.umlg.sqlg.process.graph.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Date: 2014/08/15
 * Time: 7:34 PM
 */
public class SqlgVertexStepStrategy extends AbstractTraversalStrategy {

    private static final SqlgVertexStepStrategy INSTANCE = new SqlgVertexStepStrategy();
    private static final List<Class> consecutiveStepsToReplace = Arrays.asList(VertexStep.class);

    private SqlgVertexStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine traversalEngine) {
        //Replace all consecutive out out out with one step
        List<Step> steps = new ArrayList<>(traversal.asAdmin().getSteps());
        Step previous = null;
        SqlgVertexStepCompiler sqlgVertexStepCompiler = null;
        for (Step step : steps) {
            if (consecutiveStepsToReplace.contains(step.getClass())) {
                Pair<Step<?, ?>, List<HasContainer>> stepPair = Pair.of(step, new ArrayList<>());
                if (previous == null) {
                    sqlgVertexStepCompiler = new SqlgVertexStepCompiler(traversal);
                    TraversalHelper.replaceStep(step, sqlgVertexStepCompiler, traversal);
                    collectHasSteps(traversal, step, stepPair);
                } else {
                    traversal.removeStep(step);
                    collectHasSteps(traversal, step, stepPair);
                }
                previous = step;
                sqlgVertexStepCompiler.addReplacedStep(stepPair);
            } else {
                previous = null;
            }
        }
    }

    private void collectHasSteps(Traversal.Admin<?, ?> traversal, Step step, Pair<Step<?, ?>, List<HasContainer>> stepPair) {
        //Collect the hasSteps
        Step<?, ?> currentStep = step.getNextStep();
        while (true) {
            if (currentStep instanceof HasContainerHolder) {
                if (currentStep.getLabel().isPresent()) {
                    final IdentityStep identityStep = new IdentityStep<>(traversal);
                    identityStep.setLabel(currentStep.getLabel().get());
                    TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                }
                traversal.removeStep(currentStep);
                stepPair.getValue().addAll(((HasContainerHolder) currentStep).getHasContainers());
            } else if (currentStep instanceof IdentityStep) {
                // do nothing
            } else {
                break;
            }
            currentStep = currentStep.getNextStep();
        }
    }

    public static SqlgVertexStepStrategy instance() {
        return INSTANCE;
    }

}
