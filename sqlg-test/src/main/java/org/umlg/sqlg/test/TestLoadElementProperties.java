package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 5:23 PM
 */
public class TestLoadElementProperties extends BaseTest {

    @Test
    public void testLoadVertexProperties() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        this.sqlgGraph.tx().commit();
        marko = this.sqlgGraph.v(marko.id());
        Assert.assertEquals("marko", marko.property("name").value());
    }

    @Test
    public void testLoadEdgeProperties() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Edge friend = marko.addEdge("friend", john, "weight", 1);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.e(friend.id()).property("weight").value());
    }

}
