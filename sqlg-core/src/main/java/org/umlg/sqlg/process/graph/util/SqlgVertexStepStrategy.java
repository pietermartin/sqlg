package org.umlg.sqlg.process.graph.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Date: 2014/08/15
 * Time: 7:34 PM
 */
public class SqlgVertexStepStrategy extends AbstractTraversalStrategy {

    private static final SqlgVertexStepStrategy INSTANCE = new SqlgVertexStepStrategy();

    private SqlgVertexStepStrategy() {
    }

    @Override
    public void apply(final Traversal traversal, final TraversalEngine traversalEngine) {
        List<VertexStep> vertexSteps = TraversalHelper.getStepsOfClass(VertexStep.class, traversal);
        vertexSteps.forEach(
                (s) -> TraversalHelper.replaceStep(s, new SqlgVertexStep(s.getTraversal(), s.getReturnClass(), s.getDirection(), s.getLabel(), s.getEdgeLabels()), traversal)
        );
        //The HasSteps following directly after a VertexStep is merged into the SqlgVertexStep
        Set<Step> toRemove = new HashSet<>();
        for (Object step : traversal.asAdmin().getSteps()) {
            if (step instanceof SqlgVertexStep && Vertex.class.isAssignableFrom(((SqlgVertexStep) step).returnClass)) {
                SqlgVertexStep sqlgVertexStep = (SqlgVertexStep) step;
                Step currentStep = sqlgVertexStep.getNextStep();
                while (true) {
                    if (currentStep instanceof HasContainerHolder) {
                        sqlgVertexStep.hasContainers.addAll(((HasContainerHolder) currentStep).getHasContainers());
                        if (TraversalHelper.isLabeled(currentStep)) {
                            final IdentityStep identityStep = new IdentityStep<>(traversal);
                            identityStep.setLabel(currentStep.getLabel());
                            TraversalHelper.insertAfterStep(identityStep, currentStep, traversal);
                        }
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

    public static SqlgVertexStepStrategy instance() {
        return INSTANCE;
    }

}
