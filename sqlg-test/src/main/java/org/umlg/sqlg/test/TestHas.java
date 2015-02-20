package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.process.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 6:36 PM
 */
public class TestHas extends BaseTest {

    @Test
    public void testHas() {
        Vertex v1 = this.sqlgGraph.addVertex("name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex("name", "peter");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testQueryTableNotYetExists() {
        this.sqlgGraph.addVertex(T.label, "Animal");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.V().has(T.label, "Person").count().next(), 0);
    }

    @Test
    public void testQueryPropertyNotYetExists() {
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.V().has(T.label, "Person").has("name", "john").count().next(), 0);
    }

    @Test
    public void testHasOnEdge() {
        Vertex v1 = this.sqlgGraph.addVertex("name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex("name", "peter");
        v1.addEdge("friend", v2, "weight", "5");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.E().has("weight", "5").count().next(), 0);
    }

    @Test
    public void testEdgeQueryTableNotYetExists() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Animal");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Animal");
        v1.addEdge("friend", v2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.E().has(T.label, "friendXXX").count().next(), 0);
    }

    @Test
    public void testEdgeQueryPropertyNotYetExists() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.V().has(T.label, "friend").has("weight", "5").count().next(), 0);
    }

    @Test
    public void testHasOnTableThatDoesNotExist() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.V().has(T.label, "friend").has("weight", "5").count().next(), 0);
        Assert.assertFalse(this.sqlgGraph.V().has(T.label, "xxx").hasNext());
        Assert.assertFalse(this.sqlgGraph.V().has(T.label, "public.xxx").hasNext());
    }

}
