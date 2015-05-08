package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2015/01/18
 * Time: 5:30 PM
 */
public class TestGremlinCompileE extends BaseTest {

    @Test
    public void testOutE() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Edge e = a1.addEdge("outB", b1);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, vertexTraversal(a1).outE().count().next().intValue());
        Assert.assertEquals(e, vertexTraversal(a1).outE().next());
    }

    @Test
    public void testInE() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Edge e = a1.addEdge("outB", b1);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, vertexTraversal(b1).inE().count().next().intValue());
        Assert.assertEquals(e, vertexTraversal(b1).inE().next());
    }

    @Test
    public void testEdgeOut() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Edge e = a1.addEdge("outB", b1);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(a1, edgeTraversal(e).outV().next());
        Assert.assertEquals(1, edgeTraversal(e).outV().outE().count().next().intValue());
        Assert.assertEquals(e, edgeTraversal(e).outV().outE().next());
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

        Assert.assertEquals(6, vertexTraversal(a1).out().outE().count().next().intValue());
        List<Edge> edges = vertexTraversal(a1).out().outE().toList();
        Assert.assertTrue(edges.contains(e3));
        Assert.assertTrue(edges.contains(e4));
        Assert.assertTrue(edges.contains(e5));
        Assert.assertTrue(edges.contains(e6));
        Assert.assertTrue(edges.contains(e7));
        Assert.assertTrue(edges.contains(e8));
    }

    @Test
    public void testBothEOnEdgeToSelf() {
        final Vertex v1 = this.sqlgGraph.addVertex("name", "marko");
        final Vertex v2 = this.sqlgGraph.addVertex("name", "puppy");
        v1.addEdge("knows", v2, "since", 2010);
        v1.addEdge("pets", v2);
        v1.addEdge("walks", v2, "location", "arroyo");
        v2.addEdge("knows", v1, "since", 2010);
        assertEquals(4, vertexTraversal(v1).bothE().count().next().intValue());
        assertEquals(4, vertexTraversal(v2).bothE().count().next().intValue());
        v1.edges(Direction.BOTH).forEachRemaining(edge -> {
            v1.addEdge("livesWith", v2);
            v1.addEdge("walks", v2, "location", "river");
            edge.remove();
        });
        assertEquals(8, vertexTraversal(v1).outE().count().next().intValue());
        assertEquals(0, vertexTraversal(v2).outE().count().next().intValue());
        v1.edges(Direction.BOTH).forEachRemaining(Edge::remove);
        assertEquals(0, vertexTraversal(v1).bothE().count().next().intValue());
        assertEquals(0, vertexTraversal(v2).bothE().count().next().intValue());
    }
}
