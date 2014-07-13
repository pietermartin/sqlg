package org.umlg.sqlgraph.h2database.test;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 5:23 PM
 */
public class TestLoadElementProperties extends BaseTest {

    @Test
    public void testLoadVertexProperties() {
        Vertex marko = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "marko");
        this.sqlGraph.tx().commit();
        marko = this.sqlGraph.v(marko.id());
        Assert.assertEquals("marko", marko.property("name").value());
    }

    @Test
    public void testLoadEdgeProperties() {
        Vertex marko = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex john = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "john");
        Edge friend = marko.addEdge("friend", john, "weight", 1);
        this.sqlGraph.tx().commit();
        Assert.assertEquals(1, this.sqlGraph.e(friend.id()).property("weight").value());
    }

}
