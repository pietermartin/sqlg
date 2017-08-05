package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2014/07/21
 * Time: 7:25 PM
 */
public class TestCountVerticesAndEdges extends BaseTest {

    @Test
    public void testCountVertices()  {
        this.sqlgGraph.addVertex(T.label, "A1", "name", "a1");
        this.sqlgGraph.addVertex(T.label, "v2", "name", "v2");
        this.sqlgGraph.addVertex(T.label, "v3", "name", "v3");
        this.sqlgGraph.addVertex(T.label, "v1", "name", "v4");
        this.sqlgGraph.addVertex(T.label, "v2", "name", "v5");
        this.sqlgGraph.addVertex(T.label, "v3", "name", "v6");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(6L, this.sqlgGraph.countVertices(), 0);
    }

    @Test
    public void testCountEdges() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "v1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "v2");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "v3");
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "v4");
        v1.addEdge("e1", v2);
        v1.addEdge("e2", v3);
        v1.addEdge("e3", v4);
        v2.addEdge("e4", v1);
        v2.addEdge("e5", v3);
        v2.addEdge("e6", v4);
        v3.addEdge("e7", v1);
        v3.addEdge("e8", v2);
        v3.addEdge("e9", v4);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(9L, this.sqlgGraph.countEdges(), 0);
    }
}
