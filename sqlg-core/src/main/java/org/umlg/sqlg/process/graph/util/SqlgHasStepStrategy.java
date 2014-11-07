package org.umlg.sqlg.process.graph.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Date: 2014/08/15
 * Time: 7:34 PM
 */
public class SqlgHasStepStrategy extends AbstractTraversalStrategy {

    private static final SqlgHasStepStrategy INSTANCE = new SqlgHasStepStrategy();
    private static final Set<Class<? extends TraversalStrategy>> POSTS = new HashSet<>();

    static {
        POSTS.add(TraverserSourceStrategy.class);
    }

    private SqlgHasStepStrategy() {
    }

    @Override
    public void apply(Traversal traversal, final TraversalEngine traversalEngine) {
        List<HasStep> hasSteps = TraversalHelper.getStepsOfClass(HasStep.class, traversal);
        hasSteps.forEach(
                (s) -> TraversalHelper.replaceStep(s, new SqlgHasStep(traversal, s), traversal)
        );
    }

    public static SqlgHasStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPost() {
        return POSTS;
    }

}
