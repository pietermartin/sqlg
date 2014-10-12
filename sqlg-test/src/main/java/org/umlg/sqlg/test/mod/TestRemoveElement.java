package org.umlg.sqlg.test.mod;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/07/13
 * Time: 7:22 PM
 */
public class TestRemoveElement extends BaseTest {

    @Test
    public void testRemoveVertex() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(3L, this.sqlgGraph.V().count().next(), 0);
        marko.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2L, this.sqlgGraph.V().count().next(), 0);
    }

    @Test
    public void testRemoveEdge() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Vertex peter = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
        Edge edge1 = marko.addEdge("friend", john);
        Edge edge2 =marko.addEdge("friend", peter);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(3L, this.sqlgGraph.V().count().next(), 0);
        Assert.assertEquals(2L, marko.out("friend").count().next(), 0);
        edge1.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(3L, this.sqlgGraph.V().count().next(), 0);
        Assert.assertEquals(1L, marko.out("friend").count().next(), 0);
    }
}
