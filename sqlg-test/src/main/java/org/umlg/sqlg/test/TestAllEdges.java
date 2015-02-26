package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.process.T;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/13
 * Time: 4:48 PM
 */
public class TestAllEdges extends BaseTest {

    @Test
    public void testAllEdges() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        marko.addEdge("friend", john);
        marko.addEdge("family", john);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2L, this.sqlgGraph.E().count().next(), 0);
    }

    @Test
    public void shouldNotGetConcurrentModificationException() {
        for (int i = 0; i < 25; i++) {
            final Vertex v = this.sqlgGraph.addVertex();
            v.addEdge("friend", v);
        }

        Assert.assertEquals(25, this.sqlgGraph.E().count().next().intValue());
        Assert.assertEquals(25, this.sqlgGraph.V().count().next().intValue());
        this.sqlgGraph.tx().commit();

        for (Edge e : this.sqlgGraph.E().toList()) {
            e.remove();
            this.sqlgGraph.tx().commit();
        }

        Assert.assertEquals(0, this.sqlgGraph.E().count().next().intValue());
        Assert.assertEquals(25, this.sqlgGraph.V().count().next().intValue());
        this.sqlgGraph.tx().commit();
    }
}
