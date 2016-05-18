package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.umlg.sqlg.strategy.TopologyStrategy;

/**
 * Date: 2016/05/16
 * Time: 8:37 PM
 */
@SuppressWarnings("ALL")
public class SqlgGraphTraversalSource extends GraphTraversalSource {

    public SqlgGraphTraversalSource(Graph graph) {
        super(graph, TraversalStrategies.GlobalCache.getStrategies(graph.getClass()).clone().addStrategies(TopologyStrategy.build().create()));
    }

    public SqlgGraphTraversalSource(Graph graph, TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
    }
}
