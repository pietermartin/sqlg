package org.umlg.sqlg.test.edgehas;

import org.apache.tinkerpop.gremlin.process.graph.traversal.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/09/11
 * Time: 7:41 AM
 */
public class TestEdgeHas extends BaseTest {

    @Test
    public void testEdgeHas() {
        Vertex stephen = this.sqlgGraph.addVertex("name", "stephen");
        Vertex marko = this.sqlgGraph.addVertex("name", "marko");
        stephen.addEdge("knows", marko, "weight", 1.0d);
        stephen.addEdge("knows", marko, "weight", 2.0d);
        this.sqlgGraph.tx().commit();
        GraphTraversal knows = stephen.outE("knows");
        knows.has("weight", 1.0d);
        Assert.assertEquals(1L, knows.count().next());
        Assert.assertEquals(1, stephen.outE("knows").has("weight", 1.0d).count().next(), 0);
    }
}
