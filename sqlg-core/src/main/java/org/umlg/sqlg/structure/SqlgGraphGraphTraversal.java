package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import org.umlg.sqlg.process.graph.util.SqlgHasStepStrategy;
import org.umlg.sqlg.process.graph.util.SqlgVertexStepStrategy;
import org.umlg.sqlg.strategy.SqlGGraphStepStrategy;

/**
 * Date: 2014/11/07
 * Time: 3:06 PM
 */
public class SqlgGraphGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        traversalStrategies.addStrategy(SqlgHasStepStrategy.instance());
        traversalStrategies.addStrategy(SqlgVertexStepStrategy.instance());
        traversalStrategies.addStrategy(SqlGGraphStepStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(SqlgGraphGraphTraversal.class, traversalStrategies);
    }

    public SqlgGraphGraphTraversal() {
        super();
    }

    public SqlgGraphGraphTraversal(final SqlgGraph sqlgGraph) {
        super(sqlgGraph);
    }

    @Override
    public <E2> SqlgGraphGraphTraversal<S, E2> addStep(final Step<?, E2> step) {
        if (this.locked) throw Exceptions.traversalIsLocked();
        TraversalHelper.insertStep(step, this);
        return (SqlgGraphGraphTraversal) this;
    }
}
