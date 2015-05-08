package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 5:08 PM
 */
public class TestGetById extends BaseTest {

    @Test
    public void testGetVertexById() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
        this.sqlgGraph.tx().commit();
        Vertex v = this.sqlgGraph.v(marko.id());
        Assert.assertEquals(marko, v);
        v = this.sqlgGraph.v(john.id());
        Assert.assertEquals(john, v);
        v = this.sqlgGraph.v(peter.id());
        Assert.assertEquals(peter, v);
    }

    @Test
    public void testGetEdgeById() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Edge friendEdge = marko.addEdge("friend", john);
        Edge familyEdge = marko.addEdge("family", john);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(friendEdge, this.sqlgGraph.e(friendEdge.id()));
        Assert.assertEquals(familyEdge, this.sqlgGraph.e(familyEdge.id()));
    }
}
