package org.umlg.sqlgraph.h2database.test;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 7:22 PM
 */
public class TestRemoveElement extends BaseTest {

    @Test
    public void testRemoveVertex() {
        Vertex marko = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex john = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "john");
        Vertex peter = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "peter");
        this.sqlGraph.tx().commit();
        Assert.assertEquals(3L, this.sqlGraph.V().count().next(), 0);
        marko.remove();
        this.sqlGraph.tx().commit();
        Assert.assertEquals(2L, this.sqlGraph.V().count().next(), 0);
    }

    @Test
    public void testRemoveEdge() {
        Vertex marko = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex john = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "john");
        Vertex peter = this.sqlGraph.addVertex(Element.LABEL, "Person", "name", "peter");
        Edge edge1 = marko.addEdge("friend", john);
        Edge edge2 =marko.addEdge("friend", peter);
        this.sqlGraph.tx().commit();
        Assert.assertEquals(3L, this.sqlGraph.V().count().next(), 0);
        Assert.assertEquals(2L, marko.out("friend").count().next(), 0);
        edge1.remove();
        this.sqlGraph.tx().commit();
        Assert.assertEquals(3L, this.sqlGraph.V().count().next(), 0);
        Assert.assertEquals(1L, marko.out("friend").count().next(), 0);
    }
}
