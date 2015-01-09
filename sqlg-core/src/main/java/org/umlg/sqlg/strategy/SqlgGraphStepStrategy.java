package org.umlg.sqlg.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.process.graph.util.SqlgHasStep;
import org.umlg.sqlg.process.graph.util.SqlgVertexStep;
import org.umlg.sqlg.structure.SqlgGraphStep;

import java.util.HashSet;
import java.util.Set;

/**
 * Date: 2014/07/12
 * Time: 5:45 AM
 */
public class SqlgGraphStepStrategy extends AbstractTraversalStrategy {

    private static final SqlgGraphStepStrategy INSTANCE = new SqlgGraphStepStrategy();

    private SqlgGraphStepStrategy() {
    }

    public void apply(final Traversal traversal, final TraversalEngine traversalEngine) {

        final Step<?, ?> startStep = TraversalHelper.getStart(traversal);
        if (startStep instanceof GraphStep) {
            final GraphStep<?> originalGraphStep = (GraphStep) startStep;
            final SqlgGraphStep<?> sqlgGraphStep = new SqlgGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(startStep, sqlgGraphStep, traversal);

            Step<?, ?> currentStep = sqlgGraphStep.getNextStep();
            while (true) {
                if (currentStep instanceof HasContainerHolder) {
                    sqlgGraphStep.hasContainers.addAll(((HasContainerHolder) currentStep).getHasContainers());
                    if (TraversalHelper.isLabeled(currentStep)) {
                        final IdentityStep identityStep = new IdentityStep<>(traversal);
                        identityStep.setLabel(currentStep.getLabel());
                        TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                    }
                    TraversalHelper.removeStep(currentStep, traversal);
                } else if (currentStep instanceof IdentityStep) {
                    // do nothing
                } else {
                    break;
                }
                currentStep = currentStep.getNextStep();
            }
        }

        //TODO do has on edges
        Set<Step> toRemove = new HashSet<>();
        for (Object step : traversal.asAdmin().getSteps()) {
            if (step instanceof SqlgVertexStep && Vertex.class.isAssignableFrom(((SqlgVertexStep) step).returnClass)) {
                SqlgVertexStep sqlgVertexStep = (SqlgVertexStep) step;
                Step currentStep = sqlgVertexStep.getNextStep();
                while (true) {
                    if (currentStep == EmptyStep.instance() || TraversalHelper.isLabeled(currentStep)) break;
                    if (currentStep instanceof SqlgHasStep) {
                        sqlgVertexStep.hasContainers.add(((SqlgHasStep) currentStep).getHasContainer());
                        toRemove.add(currentStep);
                    } else if (currentStep instanceof IdentityStep) {
                        // do nothing
                    } else {
                        break;
                    }
                    currentStep = currentStep.getNextStep();
                }
            }
        }
        for (Step stepToRemove : toRemove) {
            TraversalHelper.removeStep(stepToRemove, traversal);
        }
    }

    public static SqlgGraphStepStrategy instance() {
        return INSTANCE;
    }

}
