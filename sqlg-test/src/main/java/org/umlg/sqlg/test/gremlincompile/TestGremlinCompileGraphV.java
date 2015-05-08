package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2015/02/20
 * Time: 8:05 PM
 */
public class TestGremlinCompileGraphV extends BaseTest {

    @Test
    public void testGraphVHas() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");

        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");

        a1.addEdge("b", b1);
        a1.addEdge("b", b2);
        a1.addEdge("b", b3);
        a1.addEdge("b", b4);

        a2.addEdge("b", b1);
        a2.addEdge("b", b2);
        a2.addEdge("b", b3);
        a2.addEdge("b", b4);

        this.sqlgGraph.tx().commit();

        List<Vertex> bs = this.sqlgGraph.traversal().V().has(T.label, "A").out("b").toList();
        Assert.assertEquals(8, bs.size());
    }
}
