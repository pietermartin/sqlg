package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2015/01/19
 * Time: 6:22 AM
 */
public class TestGremlinCompileWithInOutV extends BaseTest {

    @Test
    public void testHasLabelOutWithInV() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("outB", b1, "seqID", 0);
        this.sqlgGraph.tx().commit();
        List<Vertex> result = this.sqlgGraph.traversal().V(a1).outE("outB").inV().toList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(b1, result.get(0));
    }

    @Test
    public void testToFromEdge() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Edge e1 = a.addEdge("outB", b);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V(a).out().in().toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(a, vertices.get(0));

        vertices = this.sqlgGraph.traversal().V(a).outE().outV().toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(a, vertices.get(0));
    }

    @Test
    public void testInVOutV() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        Edge e1 = a.addEdge("outB", b);
        Edge e2 = b.addEdge("outC", c);
        Edge e3 = c.addEdge("outD", d);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V(a).outE().outV().out().outE().outV().toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(b, vertices.get(0));
    }

    @Test
    public void testNavFromEdge() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        Edge e1 = a.addEdge("outB", b);
        Edge e2 = a.addEdge("outC", c);
        this.sqlgGraph.tx().commit();
        List<Edge> edges = this.sqlgGraph.traversal().E(e1).outV().outE().has(T.id, e2.id().toString()).toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(e2, edges.get(0));
    }

}
