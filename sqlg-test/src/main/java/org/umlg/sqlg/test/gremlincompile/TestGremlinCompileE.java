package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2015/01/18
 * Time: 5:30 PM
 */
public class TestGremlinCompileE extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
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
        DefaultGraphTraversal<Vertex, Long> traversal = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(sqlgGraph, a1).outE().count();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertEquals(1, traversal.next().intValue());
        Assert.assertEquals(3, traversal.getSteps().size());

        DefaultGraphTraversal<Vertex, Edge> traversal1 = (DefaultGraphTraversal<Vertex, Edge>) vertexTraversal(sqlgGraph, a1).outE();
        Assert.assertEquals(2, traversal1.getSteps().size());
        Assert.assertEquals(e, traversal1.next());
        Assert.assertEquals(1, traversal1.getSteps().size());
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
        DefaultGraphTraversal<Vertex, Long> traversal = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(sqlgGraph, b1).inE().count();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertEquals(1, traversal.next().intValue());
        Assert.assertEquals(3, traversal.getSteps().size());

        DefaultGraphTraversal<Vertex, Edge> traversal1 = (DefaultGraphTraversal<Vertex, Edge>) vertexTraversal(sqlgGraph, b1).inE();
        Assert.assertEquals(2, traversal1.getSteps().size());
        Assert.assertEquals(e, traversal1.next());
        Assert.assertEquals(1, traversal1.getSteps().size());
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
        DefaultGraphTraversal<Edge, Vertex> traversal = (DefaultGraphTraversal<Edge, Vertex>) edgeTraversal(sqlgGraph, e).outV();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(a1, traversal.next());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, edgeTraversal(sqlgGraph, e).outV().outE().count().next().intValue());

        DefaultGraphTraversal<Edge, Edge> traversal1 = (DefaultGraphTraversal<Edge, Edge>) edgeTraversal(sqlgGraph, e).outV().outE();
        Assert.assertEquals(3, traversal1.getSteps().size());
        Assert.assertEquals(e, traversal1.next());
        Assert.assertEquals(1, traversal1.getSteps().size());
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
        DefaultGraphTraversal<Vertex, Long> traversal = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(sqlgGraph, a1).outE().count();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertEquals(2, traversal.next().intValue());
        Assert.assertEquals(3, traversal.getSteps().size());

        DefaultGraphTraversal<Vertex, Long> traversal1 = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(sqlgGraph, a1).out().outE().count();
        Assert.assertEquals(4, traversal1.getSteps().size());
        Assert.assertEquals(6, traversal1.next().intValue());
        Assert.assertEquals(3, traversal1.getSteps().size());

        DefaultGraphTraversal<Vertex, Edge> traversal2 = (DefaultGraphTraversal<Vertex, Edge>) vertexTraversal(sqlgGraph, a1).out().outE();
        Assert.assertEquals(3, traversal2.getSteps().size());
        List<Edge> edges = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
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
        Assert.assertEquals(4, vertexTraversal(this.sqlgGraph, v1).bothE().count().next().intValue());
        Assert.assertEquals(4, vertexTraversal(this.sqlgGraph, v2).bothE().count().next().intValue());
        v1.edges(Direction.BOTH).forEachRemaining(edge -> {
            v1.addEdge("livesWith", v2);
            v1.addEdge("walks", v2, "location", "river");
            edge.remove();
        });

        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Long> traversal = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(this.sqlgGraph, v1).outE().count();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertEquals(8, traversal.next().intValue());
        Assert.assertEquals(3, traversal.getSteps().size());

        DefaultGraphTraversal<Vertex, Long> traversal1 = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(this.sqlgGraph, v2).outE().count();
        Assert.assertEquals(3, traversal1.getSteps().size());
        Assert.assertEquals(0, traversal1.next().intValue());
        Assert.assertEquals(3, traversal1.getSteps().size());

        v1.edges(Direction.BOTH).forEachRemaining(Edge::remove);

        DefaultGraphTraversal<Vertex, Long> traversal2 = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(this.sqlgGraph, v1).bothE().count();
        Assert.assertEquals(3, traversal2.getSteps().size());
        Assert.assertEquals(0, traversal2.next().intValue());
        Assert.assertEquals(3, traversal2.getSteps().size());

        DefaultGraphTraversal<Vertex, Long> traversal3 = (DefaultGraphTraversal<Vertex, Long>) vertexTraversal(this.sqlgGraph, v2).bothE().count();
        Assert.assertEquals(3, traversal3.getSteps().size());
        Assert.assertEquals(0, traversal3.next().intValue());
        Assert.assertEquals(3, traversal3.getSteps().size());
    }
}
