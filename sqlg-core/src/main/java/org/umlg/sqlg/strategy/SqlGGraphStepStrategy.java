package org.umlg.sqlg.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.IntervalStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;
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
public class SqlGGraphStepStrategy extends AbstractTraversalStrategy {

    private static final SqlGGraphStepStrategy INSTANCE = new SqlGGraphStepStrategy();
    private static final Set<Class<? extends TraversalStrategy>> POSTS = new HashSet<>();

    static {
        POSTS.add(TraverserSourceStrategy.class);
    }

    private SqlGGraphStepStrategy() {
    }

    public void apply(final Traversal traversal, final TraversalEngine traversalEngine) {

        if (traversal.getSteps().get(0) instanceof SqlgGraphStep) {
            final SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
            Step currentStep = sqlgGraphStep.getNextStep();
            while (true) {
                if (currentStep == EmptyStep.instance() || TraversalHelper.isLabeled(currentStep)) break;

                if (currentStep instanceof SqlgHasStep) {
                    sqlgGraphStep.hasContainers.add(((SqlgHasStep) currentStep).getHasContainer());
                    TraversalHelper.removeStep(currentStep, traversal);
                } else if (currentStep instanceof IntervalStep) {
                    sqlgGraphStep.hasContainers.add(((IntervalStep) currentStep).getHasContainers());
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
        for (Object step : traversal.getSteps()) {
            if (step instanceof SqlgVertexStep && Vertex.class.isAssignableFrom(((SqlgVertexStep)step).returnClass)) {
                SqlgVertexStep sqlgVertexStep = (SqlgVertexStep) step;
                Step currentStep = sqlgVertexStep.getNextStep();
                while (true) {
                    if (currentStep == EmptyStep.instance() || TraversalHelper.isLabeled(currentStep)) break;
                    if (currentStep instanceof SqlgHasStep) {
                        sqlgVertexStep.hasContainers.add(((SqlgHasStep) currentStep).getHasContainer());
                        toRemove.add(currentStep);
                    } else if (currentStep instanceof IntervalStep) {
                        sqlgVertexStep.hasContainers.add(((IntervalStep) currentStep).getHasContainers());
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

    public static SqlGGraphStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPost() {
        return POSTS;
    }

}
