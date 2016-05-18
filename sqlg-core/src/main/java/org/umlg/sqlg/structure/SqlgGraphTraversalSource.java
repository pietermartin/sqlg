package org.umlg.sqlg.structure;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Date: 2016/05/16
 * Time: 8:37 PM
 */
@SuppressWarnings("ALL")
public class SqlgGraphTraversalSource extends GraphTraversalSource {

    public SqlgGraphTraversalSource(Graph graph) {
        super(graph, TraversalStrategies.GlobalCache.getStrategies(graph.getClass()));
    }

    public SqlgGraphTraversalSource(Graph graph, TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
    }
}
