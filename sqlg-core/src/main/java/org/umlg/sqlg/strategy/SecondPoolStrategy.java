package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.umlg.sqlg.structure.SqlgGraph;

public class SecondPoolStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy{

    public static final String READ_ONLY = "readOnly";
    private final String dataSourceName;

    public SecondPoolStrategy(String dataSourceName) {
        this.dataSourceName =  dataSourceName;
    }

    @SuppressWarnings("resource")
    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        SqlgGraph sqlgGraph = (SqlgGraph) traversal.getGraph().orElseThrow();
        sqlgGraph.dataSource(this.dataSourceName);
    }

}
