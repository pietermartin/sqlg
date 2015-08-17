package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Created by pieter on 2015/08/16.
 */
public class TestColumnNameTranslation extends BaseTest {

    @Test
    public void testLongName() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Persoooooooooooon");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Doooooooooooog");
        Edge e1= v1.addEdge("sequenceRoot_sequenceTestOrderedSet_1", v2);
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Edge> gt = this.sqlgGraph.traversal().V(v1).outE("sequenceRoot_sequenceTestOrderedSet_1");
        Assert.assertTrue(gt.hasNext());
        Assert.assertEquals(e1, gt.next());
    }
}
