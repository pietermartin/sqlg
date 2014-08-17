package org.umlg.sqlg.process.graph.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.List;

/**
 * Date: 2014/08/15
 * Time: 7:34 PM
 */
public class SqlgHasStepStrategy implements TraversalStrategy.NoDependencies {

    private static final SqlgHasStepStrategy INSTANCE = new SqlgHasStepStrategy();

    private SqlgHasStepStrategy() {
    }

    @Override
    public void apply(Traversal traversal) {
        List<HasStep> hasSteps = TraversalHelper.getStepsOfClass(HasStep.class, traversal);
        hasSteps.forEach(
                (s) -> TraversalHelper.replaceStep(s, new SqlgHasStep(traversal, s), traversal)
        );
    }

    public static SqlgHasStepStrategy instance() {
        return INSTANCE;
    }

}
