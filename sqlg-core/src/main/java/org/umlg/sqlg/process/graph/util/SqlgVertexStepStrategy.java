package org.umlg.sqlg.process.graph.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.filter.HasStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.List;

/**
 * Date: 2014/08/15
 * Time: 7:34 PM
 */
public class SqlgVertexStepStrategy implements TraversalStrategy.NoDependencies {

    private static final SqlgVertexStepStrategy INSTANCE = new SqlgVertexStepStrategy();

    private SqlgVertexStepStrategy() {
    }

    @Override
    public void apply(Traversal traversal) {
        List<VertexStep> vertexSteps = TraversalHelper.getStepsOfClass(VertexStep.class, traversal);
        vertexSteps.forEach(
                (s) -> TraversalHelper.replaceStep(s, new SqlgVertexStep(s.getTraversal(), s.getReturnClass(), s.getDirection(), s.getBranchFactor(), s.getLabel(), s.getEdgeLabels()), traversal)
        );
    }

    public static SqlgVertexStepStrategy instance() {
        return INSTANCE;
    }

}
