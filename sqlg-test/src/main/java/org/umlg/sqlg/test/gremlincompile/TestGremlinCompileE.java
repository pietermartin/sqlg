package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2015/01/18
 * Time: 5:30 PM
 */
public class TestGremlinCompileE extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        BaseTest.beforeClass();
        if (configuration.getString("jdbc.url").contains("postgresql")) {
            configuration.addProperty("distributed", true);
        }
    }

    @Test
    public void testOutE() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Edge e = a1.addEdge("outB", b1);
        this.sqlgGraph.tx().commit();
        testOutE_assert(this.sqlgGraph, a1, e);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testOutE_assert(this.sqlgGraph, a1, e);
        }
    }

    private void testOutE_assert(SqlgGraph sqlgGraph, Vertex a1, Edge e) {
        Assert.assertEquals(1, vertexTraversal(sqlgGraph, a1).outE().count().next().intValue());
        Assert.assertEquals(e, vertexTraversal(sqlgGraph, a1).outE().next());
    }

    @Test
    public void testInE() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Edge e = a1.addEdge("outB", b1);
        this.sqlgGraph.tx().commit();
        testInE_assert(this.sqlgGraph, b1, e);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testInE_assert(this.sqlgGraph1, b1, e);
        }
    }

    private void testInE_assert(SqlgGraph sqlgGraph, Vertex b1, Edge e) {
        Assert.assertEquals(1, vertexTraversal(sqlgGraph, b1).inE().count().next().intValue());
        Assert.assertEquals(e, vertexTraversal(sqlgGraph, b1).inE().next());
    }

    @Test
    public void testEdgeOut() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Edge e = a1.addEdge("outB", b1);
        this.sqlgGraph.tx().commit();
        testEdgeOut_assert(this.sqlgGraph, a1, e);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testEdgeOut_assert(this.sqlgGraph1, a1, e);
        }
    }

    private void testEdgeOut_assert(SqlgGraph sqlgGraph, Vertex a1, Edge e) {
        Assert.assertEquals(a1, edgeTraversal(sqlgGraph, e).outV().next());
        Assert.assertEquals(1, edgeTraversal(sqlgGraph, e).outV().outE().count().next().intValue());
        Assert.assertEquals(e, edgeTraversal(sqlgGraph, e).outV().outE().next());
    }

    @Test
    public void testOutEOut() throws InterruptedException {
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

        testOutEOut_assert(this.sqlgGraph, a1, e3, e4, e5, e6, e7, e8);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testOutEOut_assert(this.sqlgGraph1, a1, e3, e4, e5, e6, e7, e8);
        }
    }

    private void testOutEOut_assert(SqlgGraph sqlgGraph, Vertex a1, Edge e3, Edge e4, Edge e5, Edge e6, Edge e7, Edge e8) {
        Assert.assertEquals(2, vertexTraversal(sqlgGraph, a1).outE().count().next().intValue());
        Assert.assertEquals(6, vertexTraversal(sqlgGraph, a1).out().outE().count().next().intValue());
        List<Edge> edges = vertexTraversal(sqlgGraph, a1).out().outE().toList();
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
        this.sqlgGraph.tx().setLazyQueries(false);
        assertEquals(4, vertexTraversal(this.sqlgGraph, v1).bothE().count().next().intValue());
        assertEquals(4, vertexTraversal(this.sqlgGraph, v2).bothE().count().next().intValue());
        v1.edges(Direction.BOTH).forEachRemaining(edge -> {
            v1.addEdge("livesWith", v2);
            v1.addEdge("walks", v2, "location", "river");
            edge.remove();
        });

        this.sqlgGraph.tx().commit();

        assertEquals(8, vertexTraversal(this.sqlgGraph, v1).outE().count().next().intValue());
        assertEquals(0, vertexTraversal(this.sqlgGraph, v2).outE().count().next().intValue());
        v1.edges(Direction.BOTH).forEachRemaining(Edge::remove);
        assertEquals(0, vertexTraversal(this.sqlgGraph, v1).bothE().count().next().intValue());
        assertEquals(0, vertexTraversal(this.sqlgGraph, v2).bothE().count().next().intValue());
    }
}
