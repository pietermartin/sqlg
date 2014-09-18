package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 4:48 PM
 */
public class TestAllEdges extends BaseTest {

    @Test
    public void testAllEdges() {
        Vertex marko = this.sqlG.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlG.addVertex(T.label, "Person", "name", "john");
        marko.addEdge("friend", john);
        marko.addEdge("family", john);
        this.sqlG.tx().commit();
        Assert.assertEquals(2L, this.sqlG.E().count().next(), 0);
    }
}
