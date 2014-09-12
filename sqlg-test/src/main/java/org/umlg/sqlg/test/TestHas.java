package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 6:36 PM
 */
public class TestHas extends BaseTest {

    @Test
    public void testHas() {
        Vertex v1 = this.sqlG.addVertex("name", "marko");
        Vertex v2 = this.sqlG.addVertex("name", "peter");
        this.sqlG.tx().commit();
        Assert.assertEquals(1, this.sqlG.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testQueryTableNotYetExists() {
        this.sqlG.addVertex(Element.LABEL, "Animal");
        this.sqlG.tx().commit();
        Assert.assertEquals(0, this.sqlG.V().has(Element.LABEL, "Person").count().next(), 0);
    }

    @Test
    public void testQueryPropertyNotYetExists() {
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.tx().commit();
        Assert.assertEquals(0, this.sqlG.V().has(Element.LABEL, "Person").has("name", "john").count().next(), 0);
    }

    @Test
    public void testHasOnEdge() {
        Vertex v1 = this.sqlG.addVertex("name", "marko");
        Vertex v2 = this.sqlG.addVertex("name", "peter");
        v1.addEdge("friend", v2, "weight", "5");
        this.sqlG.tx().commit();
        Assert.assertEquals(1, this.sqlG.E().has("weight", "5").count().next(), 0);
    }

    @Test
    public void testEdgeQueryTableNotYetExists() {
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Animal");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Animal");
        v1.addEdge("friend", v2);
        this.sqlG.tx().commit();
        Assert.assertEquals(0, this.sqlG.E().has(Element.LABEL, "friendXXX").count().next(), 0);
    }

    @Test
    public void testEdgeQueryPropertyNotYetExists() {
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person");
        v1.addEdge("friend", v2);
        this.sqlG.tx().commit();
        Assert.assertEquals(0, this.sqlG.V().has(Element.LABEL, "friend").has("weight", "5").count().next(), 0);
    }

}
