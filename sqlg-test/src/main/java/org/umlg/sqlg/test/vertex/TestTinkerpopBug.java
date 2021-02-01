package org.umlg.sqlg.test.vertex;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/10/19
 * Time: 8:55 AM
 */
public class TestTinkerpopBug extends BaseTest {

    @Test(expected = IllegalStateException.class)
    public void hasNextCountBug() {
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> gt = this.sqlgGraph.traversal().V().has(T.label, "Person");
        Assert.assertTrue(gt.hasNext());
        Assert.assertEquals(3, gt.count().next().intValue());
    }


}
