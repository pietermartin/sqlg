package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2015/01/19
 * Time: 6:22 AM
 */
public class TestGremlinCompileWithInOutV extends BaseTest {

    @Test
    public void testHasWithInMultipleHasContainers() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "3");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "4");
        Vertex a5 = this.sqlgGraph.addVertex(T.label, "A", "name", "5");
        Vertex a6 = this.sqlgGraph.addVertex(T.label, "A", "name", "6");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(3, this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within("1", "2", "3")).count().next().intValue());
        Assert.assertEquals(3, this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within("4", "5", "6")).count().next().intValue());
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within("2", "3", "4")).has("name", P.within("4", "5", "6")).count().next().intValue());
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within("2", "3", "4")).has("name", P.within("4", "5", "6"));
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(a4, traversal.next());
        Assert.assertEquals(1, traversal.getSteps().size());
    }

    @Test
    public void testHasLabelOutWithInV() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("outB", b1, "seqID", 0);
        this.sqlgGraph.tx().commit();

        testHasLabelOutWithInV_assert(this.sqlgGraph, a1, b1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testHasLabelOutWithInV_assert(this.sqlgGraph1, a1, b1);
        }
    }

    private void testHasLabelOutWithInV_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex b1) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V(a1.id()).outE("outB").inV();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> result = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(b1, result.get(0));
    }

    @Test
    public void testToFromEdge() throws InterruptedException {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Edge e1 = a.addEdge("outB", b);
        this.sqlgGraph.tx().commit();
        testToFromEdge_assert(this.sqlgGraph, a);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testToFromEdge_assert(this.sqlgGraph1, a);
        }
    }

    private void testToFromEdge_assert(SqlgGraph sqlgGraph, Vertex a) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(a.id()).out().in();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(a, vertices.get(0));

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(a.id()).outE().outV();
        Assert.assertEquals(3, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(a, vertices.get(0));
    }

    @Test
    public void testInVOutV() throws InterruptedException {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        Edge e1 = a.addEdge("outB", b);
        Edge e2 = b.addEdge("outC", c);
        Edge e3 = c.addEdge("outD", d);
        this.sqlgGraph.tx().commit();
        testInVOutV_assert(this.sqlgGraph, a, b);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testInVOutV_assert(this.sqlgGraph1, a, b);
        }
    }

    private void testInVOutV_assert(SqlgGraph sqlgGraph, Vertex a, Vertex b) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(a.id()).outE().outV().out().outE().outV();
        Assert.assertEquals(6, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(b, vertices.get(0));
    }

    @Test
    public void testNavFromEdge() throws InterruptedException {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        Edge e1 = a.addEdge("outB", b);
        Edge e2 = a.addEdge("outC", c);
        this.sqlgGraph.tx().commit();
        testNavFromEdge_assert(this.sqlgGraph, e1, e2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testNavFromEdge_assert(this.sqlgGraph1, e1, e2);
        }
    }

    private void testNavFromEdge_assert(SqlgGraph sqlgGraph, Edge e1, Edge e2) {
        DefaultGraphTraversal<Edge, Edge> traversal = (DefaultGraphTraversal<Edge, Edge>) sqlgGraph.traversal().E(e1.id()).outV().outE().has(T.id, e2.id().toString());
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Edge> edges = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(e2, edges.get(0));
    }

}
