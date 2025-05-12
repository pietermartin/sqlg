package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.DefaultSqlgTraversal;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

/**
 * Date: 2015/01/19
 * Time: 6:22 AM
 */
public class TestGremlinCompileWithAs extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Test
    public void testSchemaTableTreeNextSchemaTableTreeIsEdgeVertex() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("outA", b1);
        b1.addEdge("outB", c1);
        this.sqlgGraph.tx().commit();
        testSchemaTableTreeNextSchemaTableTreeIsEdgeVertex_assert(this.sqlgGraph, a1, b1, c1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testSchemaTableTreeNextSchemaTableTreeIsEdgeVertex_assert(this.sqlgGraph1, a1, b1, c1);
        }

    }

    @SuppressWarnings("unchecked")
    private void testSchemaTableTreeNextSchemaTableTreeIsEdgeVertex_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex b1, Vertex c1) {
        DefaultSqlgTraversal<Vertex, Map<String, Vertex>> gt = (DefaultSqlgTraversal)sqlgGraph.traversal().V(a1).out().as("b").out().as("c").select("b", "c");
        Assert.assertEquals(4, gt.getSteps().size());
        List<Map<String, Vertex>> list = gt.toList();
        Assert.assertEquals(2, gt.getSteps().size());
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(b1, list.get(0).get("b"));
        Assert.assertEquals(c1, list.get(0).get("c"));
    }

    @Test
    public void testHasLabelOutWithAs() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
        Edge e1 = a1.addEdge("outB", b1, "edgeName", "edge1");
        Edge e2 = a1.addEdge("outB", b2, "edgeName", "edge2");
        Edge e3 = a1.addEdge("outB", b3, "edgeName", "edge3");
        Edge e4 = a1.addEdge("outB", b4, "edgeName", "edge4");
        this.sqlgGraph.tx().commit();
        testHasLabelOutWithAs_assert(this.sqlgGraph, a1, b1, b2, b3, b4, e1, e2, e3, e4);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testHasLabelOutWithAs_assert(this.sqlgGraph1, a1, b1, b2, b3, b4, e1, e2, e3, e4);
        }

    }

    private void testHasLabelOutWithAs_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex b1, Vertex b2, Vertex b3, Vertex b4, Edge e1, Edge e2, Edge e3, Edge e4) {
        DefaultSqlgTraversal<Vertex, Map<String, Element>> traversal = (DefaultSqlgTraversal<Vertex, Map<String, Element>>)sqlgGraph.traversal().V(a1)
                .outE("outB")
                .as("e")
                .inV()
                .as("B")
                .<Element>select("e", "B");
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Map<String, Element>> result = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        result.sort(Comparator.comparing(o -> o.get("e").<String>value("edgeName")));
        Assert.assertEquals(4, result.size());

        Edge queryE0 = (Edge) result.get(0).get("e");
        Vertex queryB0 = (Vertex) result.get(0).get("B");
        Assert.assertEquals(e1, queryE0);
        Assert.assertEquals("b1", queryE0.inVertex().value("name"));
        Assert.assertEquals("a1", queryE0.outVertex().value("name"));
        Assert.assertEquals("edge1", queryE0.value("edgeName"));
        Assert.assertEquals(b1, queryB0);
        Assert.assertEquals("b1", queryB0.value("name"));

        Element queryE2 = result.get(1).get("e");
        Element queryB2 = result.get(1).get("B");
        Assert.assertEquals(e2, queryE2);
        Assert.assertEquals("edge2", queryE2.value("edgeName"));
        Assert.assertEquals(b2, queryB2);
        Assert.assertEquals("b2", queryB2.value("name"));

        Element queryE3 = result.get(2).get("e");
        Element queryB3 = result.get(2).get("B");
        Assert.assertEquals(e3, queryE3);
        Assert.assertEquals("edge3", queryE3.value("edgeName"));
        Assert.assertEquals(b3, queryB3);
        Assert.assertEquals("b3", queryB3.value("name"));

        Element queryE4 = result.get(3).get("e");
        Element queryB4 = result.get(3).get("B");
        Assert.assertEquals(e4, queryE4);
        Assert.assertEquals("edge4", queryE4.value("edgeName"));
        Assert.assertEquals(b4, queryB4);
        Assert.assertEquals("b4", queryB4.value("name"));

        DefaultSqlgTraversal<Vertex, Edge> traversal1 = (DefaultSqlgTraversal<Vertex, Edge>) sqlgGraph.traversal().V(a1.id()).bothE();
        Assert.assertEquals(2, traversal1.getSteps().size());
        final List<Edge> a1Edges = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(4, a1Edges.size());
        List<String> names = new ArrayList<>(Arrays.asList("b1", "b2", "b3", "b4"));
        for (Edge a1Edge : a1Edges) {
            names.remove(a1Edge.inVertex().<String>value("name"));
            Assert.assertEquals("a1", a1Edge.outVertex().<String>value("name"));
        }
        Assert.assertTrue(names.isEmpty());
    }

    @Test
    public void testHasLabelOutWithAsNotFromStart() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        a1.addEdge("outB", b1, "edgeName", "edge1");
        a1.addEdge("outB", b2, "edgeName", "edge2");
        a1.addEdge("outB", b3, "edgeName", "edge3");
        a1.addEdge("outB", b4, "edgeName", "edge4");
        b1.addEdge("outC", c1, "edgeName", "edge5");
        b2.addEdge("outC", c2, "edgeName", "edge6");

        this.sqlgGraph.tx().commit();
        testHasLabelOutWithAsNotFromStart_assert(this.sqlgGraph, a1, c1, c2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testHasLabelOutWithAsNotFromStart_assert(this.sqlgGraph1, a1, c1, c2);
        }

    }

    private void testHasLabelOutWithAsNotFromStart_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex c1, Vertex c2) {
        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) sqlgGraph.traversal().V(a1)
                .out("outB")
                .out("outC")
                .as("x")
                .<Vertex>select("x");
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Vertex> result = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        result.sort(Comparator.comparing(o -> o.<String>value("name")));
        Assert.assertEquals(2, result.size());

        Assert.assertEquals(c1, result.get(0));
        Assert.assertEquals(c2, result.get(1));
    }

    @Test
    public void testAsWithDuplicatePaths() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a2");
        Edge e1 = a1.addEdge("friend", a2, "weight", 5);
        this.sqlgGraph.tx().commit();

        testAsWithDuplicatePaths_assert(this.sqlgGraph, a1, e1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testAsWithDuplicatePaths_assert(this.sqlgGraph1, a1, e1);
        }
    }

    private void testAsWithDuplicatePaths_assert(SqlgGraph sqlgGraph, Vertex a1, Edge e1) {
        DefaultSqlgTraversal<Vertex, Map<String, Element>> gt = (DefaultSqlgTraversal<Vertex, Map<String, Element>>) sqlgGraph.traversal()
                .V(a1)
                .outE().as("e")
                .inV()
                .in().as("v")
                .<Element>select("e", "v");
        Assert.assertEquals(5, gt.getSteps().size());

        List<Map<String, Element>> result = gt.toList();
        Assert.assertEquals(2, gt.getSteps().size());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(e1, result.get(0).get("e"));
        Assert.assertEquals(a1, result.get(0).get("v"));
    }

    @Test
    public void testChainSelect() throws Exception {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a2");
        a1.addEdge("friend", a2, "weight", 5);
        this.sqlgGraph.tx().commit();

        testChainSelect_assert(this.sqlgGraph, a2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testChainSelect_assert(this.sqlgGraph1, a2);
        }

    }

    private void testChainSelect_assert(SqlgGraph sqlgGraph, Vertex a2) throws Exception {
        try (DefaultSqlgTraversal<Vertex, Vertex> gt = (DefaultSqlgTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", "a1").as("v1")
                .values("name").as("name1")
                .select("v1")
                .out("friend")) {
            Assert.assertEquals(5, gt.getSteps().size());
            Assert.assertTrue(gt.hasNext());
            Assert.assertEquals(5, gt.getSteps().size());
            Assert.assertEquals(a2, gt.next());
            Assert.assertFalse(gt.hasNext());
        }
    }
}
