package org.umlg.sqlg.structure;

import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.graph.strategy.GraphTraversalStrategyRegistry;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversalStrategies;
import org.umlg.sqlg.process.graph.util.SqlgHasStepStrategy;
import org.umlg.sqlg.process.graph.util.SqlgVertexStepStrategy;
import org.umlg.sqlg.strategy.SqlGGraphStepStrategy;

/**
 * Date: 2014/11/07
 * Time: 3:06 PM
 */
public class SqlgGraphTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    static {
        final DefaultTraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
        GraphTraversalStrategyRegistry.instance().getTraversalStrategies().forEach(traversalStrategies::addStrategy);
        traversalStrategies.addStrategy(SqlgHasStepStrategy.instance());
        traversalStrategies.addStrategy(SqlgVertexStepStrategy.instance());
        traversalStrategies.addStrategy(SqlGGraphStepStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(SqlgGraphTraversal.class, traversalStrategies);
    }

    public SqlgGraphTraversal() {
        super();
    }

}
