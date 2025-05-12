package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.DefaultSqlgTraversal;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by pieter on 2015/07/19.
 */
@SuppressWarnings("unchecked")
public class TestGremlinCompileGraphStep extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Test
    public void gg_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_selectXa_bX_addInEXa_codeveloper_b_year_2009X() throws InterruptedException {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        DefaultSqlgTraversal<Vertex, Map<String, Vertex>> traversal1 = (DefaultSqlgTraversal<Vertex, Map<String, Vertex>>) this.sqlgGraph.traversal()
                .V().as("a")
                .out("created")
                .in("created")
                .where(P.neq("a")).as("b")
                .<Vertex>select("a", "b");
        Assert.assertEquals(5, traversal1.getSteps().size());
        List<Map<String, Vertex>> vertices = traversal1.toList();
        Assert.assertEquals(3, traversal1.getSteps().size());
        Assert.assertEquals(6, vertices.size());

        DefaultSqlgTraversal<Vertex, Edge> traversal = (DefaultSqlgTraversal<Vertex, Edge>) this.sqlgGraph.traversal()
                .V().as("a")
                .out("created")
                .in("created")
                .where(P.neq("a")).as("b")
                .<Vertex>select("a", "b")
                .addE("co-developer").from("a").to("b").property("year", 2009);
        int count = 0;
        Assert.assertEquals(6, traversal.getSteps().size());
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            Assert.assertEquals("co-developer", edge.label());
            Assert.assertEquals(2009, (int) edge.value("year"));
            Assert.assertEquals(1, IteratorUtils.count(edge.properties()));
            Assert.assertEquals("person", edge.inVertex().label());
            Assert.assertEquals("person", edge.outVertex().label());
            Assert.assertFalse(edge.inVertex().value("name").equals("vadas"));
            Assert.assertFalse(edge.outVertex().value("name").equals("vadas"));
            Assert.assertFalse(edge.inVertex().equals(edge.outVertex()));
            count++;
        }
        Assert.assertEquals(4, traversal.getSteps().size());
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(6, count);
        Assert.assertEquals(12, IteratorUtils.count(this.sqlgGraph.edges()));
        Assert.assertEquals(6, IteratorUtils.count(this.sqlgGraph.vertices()));
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            Assert.assertEquals(12, IteratorUtils.count(this.sqlgGraph1.edges()));
            Assert.assertEquals(6, IteratorUtils.count(this.sqlgGraph1.vertices()));
        }
    }

    @Test
    public void g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_selectXa_bX_addInEXa_codeveloper_b_year_2009X() throws InterruptedException {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        DefaultSqlgTraversal<Vertex, Edge> traversal = (DefaultSqlgTraversal<Vertex, Edge>) this.sqlgGraph.traversal()
                .V().as("a")
                .out("created")
                .in("created")
                .where(P.neq("a")).as("b")
                .select("a", "b")
                .addE("co-developer").property("year", 2009).from("a").to("b");
        Assert.assertEquals(6, traversal.getSteps().size());
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Edge edge = traversal.next();
            Assert.assertEquals("co-developer", edge.label());
            Assert.assertEquals(2009, (int) edge.value("year"));
            Assert.assertEquals(1, IteratorUtils.count(edge.properties()));
            Assert.assertEquals("person", edge.inVertex().label());
            Assert.assertEquals("person", edge.outVertex().label());
            Assert.assertNotEquals("vadas", edge.inVertex().value("name"));
            Assert.assertNotEquals("vadas", edge.outVertex().value("name"));
            Assert.assertNotEquals(edge.inVertex(), edge.outVertex());
            count++;
        }
        Assert.assertEquals(4, traversal.getSteps().size());
        Assert.assertEquals(6, count);
        Assert.assertEquals(12, IteratorUtils.count(this.sqlgGraph.edges()));
        Assert.assertEquals(6, IteratorUtils.count(this.sqlgGraph.vertices()));
        this.sqlgGraph.tx().commit();
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            Assert.assertEquals(12, IteratorUtils.count(this.sqlgGraph1.edges()));
            Assert.assertEquals(6, IteratorUtils.count(this.sqlgGraph1.vertices()));
        }
    }

    @Test
    public void g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX() throws InterruptedException {
        Graph g = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(g, true, false);
        Object v1Id = convertToVertexId("marko");
        List<DefaultSqlgTraversal<Vertex, Edge>> traversals = Arrays.asList(
                (DefaultSqlgTraversal)g.traversal().V(v1Id).outE("knows").as("here").has("weight", 1.0d).as("fake").inV().has("name", "josh").<Edge>select("here")
        );
        traversals.forEach(traversal -> {
            Assert.assertEquals(6, traversal.getSteps().size());
            Assert.assertTrue(traversal.hasNext());
            Assert.assertTrue(traversal.hasNext());
            final Edge edge = traversal.next();
            Assert.assertEquals("knows", edge.label());
            Assert.assertEquals(1.0d, edge.<Double>value("weight"), 0.00001d);
            Assert.assertFalse(traversal.hasNext());
            Assert.assertFalse(traversal.hasNext());
            Assert.assertEquals(3, traversal.getSteps().size());
        });
        this.sqlgGraph.tx().commit();
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            traversals = Arrays.asList(
                    (DefaultSqlgTraversal)this.sqlgGraph1.traversal().V(v1Id)
                            .outE("knows").as("here").has("weight", 1.0d).as("fake").inV().has("name", "josh").<Edge>select("here")
            );
            traversals.forEach(traversal -> {
                Assert.assertEquals(6, traversal.getSteps().size());
                Assert.assertTrue(traversal.hasNext());
                Assert.assertTrue(traversal.hasNext());
                final Edge edge = traversal.next();
                Assert.assertEquals("knows", edge.label());
                Assert.assertEquals(1.0d, edge.<Double>value("weight"), 0.00001d);
                Assert.assertFalse(traversal.hasNext());
                Assert.assertFalse(traversal.hasNext());
                Assert.assertEquals(3, traversal.getSteps().size());
            });

        }
    }

    @Test
    public void g_V_localXinEXknowsX_limitX2XX_outV_name() {
        loadModern();
        assertModernGraph(this.sqlgGraph, true, false);
        DefaultSqlgTraversal<Vertex, String> traversal = (DefaultSqlgTraversal<Vertex, String>)this.sqlgGraph.traversal()
                .V()
                .local(
                        __.inE("knows").limit(2)
                )
                .outV()
                .<String>values("name");
        Assert.assertEquals(4, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(4, traversal.getSteps().size());
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            Assert.assertEquals(traversal.next(), "marko");
        }
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(2, counter);
    }

    @Test
    public void testCompileGraphStep() throws InterruptedException {

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");

        Vertex b11 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b12 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b13 = this.sqlgGraph.addVertex(T.label, "B");

        Vertex b21 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b22 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b23 = this.sqlgGraph.addVertex(T.label, "B");

        Vertex b31 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b32 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b33 = this.sqlgGraph.addVertex(T.label, "B");

        a1.addEdge("ab", b11);
        a1.addEdge("ab", b12);
        a1.addEdge("ab", b13);

        a2.addEdge("ab", b21);
        a2.addEdge("ab", b22);
        a2.addEdge("ab", b23);

        a3.addEdge("ab", b31);
        a3.addEdge("ab", b32);
        a3.addEdge("ab", b33);

        this.sqlgGraph.tx().commit();
        testCompileGraphStep_assert(this.sqlgGraph, a1, a2, a3, b11, b12, b13, b21, b22, b23, b31, b32, b33);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testCompileGraphStep_assert(this.sqlgGraph1, a1, a2, a3, b11, b12, b13, b21, b22, b23, b31, b32, b33);
        }
    }

    private void testCompileGraphStep_assert(SqlgGraph sqlgGraph, Vertex a1, Vertex a2, Vertex a3, Vertex b11, Vertex b12, Vertex b13, Vertex b21, Vertex b22, Vertex b23, Vertex b31, Vertex b32, Vertex b33) {
        GraphTraversalSource g = sqlgGraph.traversal();
        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) g.V().hasLabel("A").out("ab");
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> vertexes = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(9, vertexes.size());
        Assert.assertEquals(9, vertexes.size());
        Assert.assertTrue(vertexes.containsAll(Arrays.asList(b11, b12, b13, b21, b22, b23, b31, b32, b33)));

        vertexes = g.V().has(T.label, "A").out("ab").toList();
        Assert.assertEquals(9, vertexes.size());
        Assert.assertEquals(9, vertexes.size());
        Assert.assertTrue(vertexes.containsAll(Arrays.asList(b11, b12, b13, b21, b22, b23, b31, b32, b33)));

        vertexes = g.V().has(T.label, "A").toList();
        Assert.assertEquals(3, vertexes.size());

        DefaultSqlgTraversal<Vertex, Map<String, Vertex>> traversal1 = (DefaultSqlgTraversal)g.V().hasLabel("A").as("a").out("ab").as("b").select("a", "b");
        Assert.assertEquals(4, traversal1.getSteps().size());
        List<Map<String, Vertex>> list = traversal1.toList();
        Assert.assertEquals(3, traversal1.getSteps().size());
        Assert.assertEquals(9, list.size());
        for (int i = 0; i < 9; i++) {
            Vertex a = list.get(i).get("a");
            Vertex b = list.get(i).get("b");
            Assert.assertTrue(a.equals(a1) || a.equals(a2) || a.equals(a3));
            if (a.equals(a1)) {
                Assert.assertTrue(b.equals(b11) || b.equals(b12) || b.equals(b13));
            } else if (a.equals(a2)) {
                Assert.assertTrue(b.equals(b21) || b.equals(b22) || b.equals(b23));
            } else if (a.equals(a3)) {
                Assert.assertTrue(b.equals(b31) || b.equals(b32) || b.equals(b33));
            } else {
                Assert.fail();
            }
        }
    }

    @Test
    public void testVWithHas() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "b");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "c");
        this.sqlgGraph.tx().commit();
        testVWithHas_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testVWithHas_assert(this.sqlgGraph1);
        }
    }

    private void testVWithHas_assert(SqlgGraph sqlgGraph) {
        GraphTraversalSource g = sqlgGraph.traversal();
        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) g.V().has(T.label, "A").has("name", "a");
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertexes = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertexes.size());
    }

    @Test
    public void testE() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Edge e1 = a1.addEdge("ab", b1);
        Edge e2 = a1.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();
        testE_assert(this.sqlgGraph, e1, e2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testE_assert(this.sqlgGraph1, e1, e2);
        }
    }

    private void testE_assert(SqlgGraph sqlgGraph, Edge e1, Edge e2) {
        GraphTraversalSource gts = sqlgGraph.traversal();
        DefaultSqlgTraversal<Edge, Edge> traversal = (DefaultSqlgTraversal<Edge, Edge>) gts.E().hasLabel("ab");
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Edge> edges = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(2, edges.size());
        Assert.assertTrue(edges.containsAll(Arrays.asList(e1, e2)));
    }

    @Test
    public void TestEWithLabels() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c11 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c12 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c21 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c22 = this.sqlgGraph.addVertex(T.label, "C");
        Edge e1 = a1.addEdge("ab", b1);
        Edge e2 = a1.addEdge("ab", b2);
        Edge bc1 = b1.addEdge("bc", c11);
        Edge bc2 = b1.addEdge("bc", c12);
        Edge bc3 = b2.addEdge("bc", c21);
        Edge bc4 = b2.addEdge("bc", c22);
        this.sqlgGraph.tx().commit();
        testEWithLabels_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testEWithLabels_assert(this.sqlgGraph1);
        }
    }

    private void testEWithLabels_assert(SqlgGraph sqlgGraph) {
        GraphTraversalSource gts = sqlgGraph.traversal();
        DefaultSqlgTraversal<Edge, Map<String, Object>> traversal = (DefaultSqlgTraversal)gts
                .E().hasLabel("ab").as("ab").inV().as("b").out("bc").as("c").select("ab", "b", "c");
        Assert.assertEquals(5, traversal.getSteps().size());
        List<Map<String, Object>> result = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertEquals(4, result.size());
        Map<String, Object> c1 = result.get(0);
        Assert.assertTrue(c1.containsKey("ab") && c1.containsKey("b") && c1.containsKey("c"));
        Map<String, Object> c2 = result.get(1);
        Assert.assertTrue(c2.containsKey("ab") && c2.containsKey("b") && c2.containsKey("c"));
        Map<String, Object> c3 = result.get(2);
        Assert.assertTrue(c3.containsKey("ab") && c1.containsKey("b") && c3.containsKey("c"));
        Map<String, Object> c4 = result.get(3);
        Assert.assertTrue(c4.containsKey("ab") && c1.containsKey("b") && c4.containsKey("c"));

        Assert.assertTrue(c1.get("c") instanceof Vertex);
        Vertex c = (Vertex) c1.get("c");
        Assert.assertEquals("C", c.label());
        Vertex b = (Vertex) c1.get("b");
        Assert.assertEquals("B", b.label());
        Edge e = (Edge) c1.get("ab");
        Assert.assertEquals("ab", e.label());
    }
}
