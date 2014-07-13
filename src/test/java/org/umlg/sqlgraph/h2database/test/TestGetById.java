package org.umlg.sqlgraph.h2database.test;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 5:08 PM
 */
public class TestGetById extends BaseTest {

    @Test
    public void testGetVertexById() {
        Vertex marko = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex john = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "john");
        Vertex peter = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "peter");
        this.sqlGraph.tx().commit();
        Vertex v = this.sqlGraph.v(marko.id());
        Assert.assertEquals(marko, v);
        v = this.sqlGraph.v(john.id());
        Assert.assertEquals(john, v);
        v = this.sqlGraph.v(peter.id());
        Assert.assertEquals(peter, v);
    }

    @Test
    public void testGetEdgeById() {
        Vertex marko = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex john = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "john");
        Edge friendEdge = marko.addEdge("friend", john);
        Edge famliyEdge = marko.addEdge("family", john);
        this.sqlGraph.tx().commit();
        Assert.assertEquals(friendEdge, this.sqlGraph.e(friendEdge.id()));
        Assert.assertEquals(famliyEdge, this.sqlGraph.e(famliyEdge.id()));
    }
}
