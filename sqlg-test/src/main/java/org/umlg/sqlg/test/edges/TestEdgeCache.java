package org.umlg.sqlg.test.edges;

import org.apache.tinkerpop.gremlin.process.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/10/04
 * Time: 12:33 PM
 */
public class TestEdgeCache extends BaseTest {

    @Test
    public void testEdgeCreateEndsUpInVertexEdgeCache() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);
        Assert.assertEquals(1, v1.out("friend").count().next().intValue());
        this.sqlgGraph.tx().commit();
    }
}
