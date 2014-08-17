package org.umlg.sqlg.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.filter.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.filter.IntervalStep;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import org.umlg.sqlg.process.step.map.SqlGGraphStep;

/**
 * Date: 2014/07/12
 * Time: 5:45 AM
 */
public class SqlGGraphStepStrategy implements TraversalStrategy.NoDependencies {

    private static final SqlGGraphStepStrategy INSTANCE = new SqlGGraphStepStrategy();

    private SqlGGraphStepStrategy() {
    }

    public void apply(final Traversal traversal) {

        if (traversal.getSteps().get(0) instanceof SqlGGraphStep) {
            final SqlGGraphStep sqlGGraphStep = (SqlGGraphStep) traversal.getSteps().get(0);
            Step currentStep = sqlGGraphStep.getNextStep();
            while (true) {
                if (currentStep == EmptyStep.instance() || TraversalHelper.isLabeled(currentStep)) break;

                if (currentStep instanceof HasStep) {
                    sqlGGraphStep.hasContainers.add(((HasStep) currentStep).hasContainer);
                    TraversalHelper.removeStep(currentStep, traversal);
                } else if (currentStep instanceof IntervalStep) {
                    sqlGGraphStep.hasContainers.add(((IntervalStep) currentStep).startContainer);
                    sqlGGraphStep.hasContainers.add(((IntervalStep) currentStep).endContainer);
                    TraversalHelper.removeStep(currentStep, traversal);
                } else if (currentStep instanceof IdentityStep) {
                    // do nothing
                } else {
                    break;
                }

                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static SqlGGraphStepStrategy instance() {
        return INSTANCE;
    }
}
