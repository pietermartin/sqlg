package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.process.T;
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
        this.sqlG.addVertex(T.label, "Animal");
        this.sqlG.tx().commit();
        Assert.assertEquals(0, this.sqlG.V().has(T.label, "Person").count().next(), 0);
    }

    @Test
    public void testQueryPropertyNotYetExists() {
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.tx().commit();
        Assert.assertEquals(0, this.sqlG.V().has(T.label, "Person").has("name", "john").count().next(), 0);
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
        Vertex v1 = this.sqlG.addVertex(T.label, "Animal");
        Vertex v2 = this.sqlG.addVertex(T.label, "Animal");
        v1.addEdge("friend", v2);
        this.sqlG.tx().commit();
        Assert.assertEquals(0, this.sqlG.E().has(T.label, "friendXXX").count().next(), 0);
    }

    @Test
    public void testEdgeQueryPropertyNotYetExists() {
        Vertex v1 = this.sqlG.addVertex(T.label, "Person");
        Vertex v2 = this.sqlG.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);
        this.sqlG.tx().commit();
        Assert.assertEquals(0, this.sqlG.V().has(T.label, "friend").has("weight", "5").count().next(), 0);
    }

    @Test
    public void testHasOnTableThatDoesNotExist() {
        Vertex v1 = this.sqlG.addVertex(T.label, "Person");
        Vertex v2 = this.sqlG.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);
        this.sqlG.tx().commit();
        Assert.assertEquals(0, this.sqlG.V().has(T.label, "friend").has("weight", "5").count().next(), 0);
        Assert.assertFalse(this.sqlG.V().has(T.label, "xxx").hasNext());
        Assert.assertFalse(this.sqlG.V().has(T.label, "public.xxx").hasNext());
    }

}
