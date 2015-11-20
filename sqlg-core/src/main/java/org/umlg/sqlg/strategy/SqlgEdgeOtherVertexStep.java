package org.umlg.sqlg.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * Date: 2015/11/20
 * Time: 7:55 AM
 */
public class SqlgEdgeOtherVertexStep extends MapStep<Edge, Vertex> {

    public SqlgEdgeOtherVertexStep(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Vertex map(Traverser.Admin<Edge> traverser) {
        return null;
    }
}
