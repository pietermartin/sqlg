package org.umlg.sqlgraph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import org.umlg.sqlgraph.process.step.map.SqlGraphStep;

/**
 * Date: 2014/07/12
 * Time: 5:45 AM
 */
public class SqlGraphStepStrategy implements TraversalStrategy.NoDependencies {

    private static final SqlGraphStepStrategy INSTANCE = new SqlGraphStepStrategy();

    private SqlGraphStepStrategy() {
    }

    public void apply(final Traversal traversal) {

        if (traversal.getSteps().get(0) instanceof SqlGraphStep) {
            final SqlGraphStep sqlGraphStep = (SqlGraphStep) traversal.getSteps().get(0);
            Step currentStep = sqlGraphStep.getNextStep();
            while (true) {
                if (currentStep == EmptyStep.instance() || TraversalHelper.isLabeled(currentStep)) break;

                if (currentStep instanceof HasStep) {
                    sqlGraphStep.hasContainers.add(((HasStep) currentStep).hasContainer);
                    TraversalHelper.removeStep(currentStep, traversal);
                } else {
                    break;
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static SqlGraphStepStrategy instance() {
        return INSTANCE;
    }
}
