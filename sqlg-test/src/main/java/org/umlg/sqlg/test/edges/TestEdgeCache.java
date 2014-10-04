package org.umlg.sqlg.test.edges;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
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
        Vertex v1 = this.sqlG.addVertex(T.label, "Person");
        Vertex v2 = this.sqlG.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);
        Assert.assertEquals(1, v1.out("friend").count().next().intValue());
        this.sqlG.tx().commit();
    }
}
