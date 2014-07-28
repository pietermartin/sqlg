package org.umlg.sqlgraph.test;

import com.tinkerpop.gremlin.structure.Element;
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
        Vertex marko = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex john = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "john");
        marko.addEdge("friend", john);
        marko.addEdge("family", john);
        this.sqlGraph.tx().commit();
        Assert.assertEquals(2L, this.sqlGraph.E().count().next(), 0);
    }
}
