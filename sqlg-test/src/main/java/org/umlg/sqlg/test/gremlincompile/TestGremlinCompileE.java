package org.umlg.sqlg.test.gremlincompile;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2015/01/18
 * Time: 5:30 PM
 */
public class TestGremlinCompileE extends BaseTest {

    @Test
    public void testOutE() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Edge e = a1.addEdge("outB",b1);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, a1.outE().count().next().intValue());
        Assert.assertEquals(e, a1.outE().next());
    }

    @Test
    public void testEdgeOut() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Edge e = a1.addEdge("outB",b1);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(a1, e.outV().next());
        Assert.assertEquals(1, e.outV().outE().count().next().intValue());
        Assert.assertEquals(e, e.outV().outE().next());
    }

    @Test
    public void testOutEOut() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c5 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c6 = this.sqlgGraph.addVertex(T.label, "C");
        Edge e1 = a1.addEdge("outB", b1);
        Edge e2 = a1.addEdge("outB", b2);
        Edge e3 = b1.addEdge("outC", c1);
        Edge e4 = b1.addEdge("outC", c2);
        Edge e5 = b1.addEdge("outC", c3);
        Edge e6 = b1.addEdge("outC", c4);
        Edge e7 = b1.addEdge("outC", c5);
        Edge e8 = b1.addEdge("outC", c6);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(6, a1.out().outE().count().next().intValue());
        List<Edge> edges = a1.out().outE().toList();
        Assert.assertTrue(edges.contains(e3));
        Assert.assertTrue(edges.contains(e4));
        Assert.assertTrue(edges.contains(e5));
        Assert.assertTrue(edges.contains(e6));
        Assert.assertTrue(edges.contains(e7));
        Assert.assertTrue(edges.contains(e8));
    }
}
