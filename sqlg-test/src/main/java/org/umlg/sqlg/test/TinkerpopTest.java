package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.GraphProvider;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.algorithm.generator.DistributionGenerator;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import com.tinkerpop.gremlin.structure.strategy.GraphStrategy;
import com.tinkerpop.gremlin.structure.strategy.StrategyWrappedGraph;
import com.tinkerpop.gremlin.structure.strategy.SubgraphStrategy;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2014/07/13
 * Time: 6:32 PM
 */
public class TinkerpopTest extends BaseTest {

    @Test
    public void shouldTraverseInOutFromVertexWithMultipleEdgeLabelFilter() {
        final Graph graph = this.sqlG;
        final Vertex a = graph.addVertex();
        final Vertex b = graph.addVertex();
        final Vertex c = graph.addVertex();

        final String labelFriend = "friend";
        final String labelHate = "hate";

        final Edge aFriendB = a.addEdge(labelFriend, b);
        final Edge aFriendC = a.addEdge(labelFriend, c);
        final Edge aHateC = a.addEdge(labelHate, c);
        final Edge cHateA = c.addEdge(labelHate, a);
        final Edge cHateB = c.addEdge(labelHate, b);

        List<Edge> results = a.outE(labelFriend, labelHate).toList();
        assertEquals(3, results.size());
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(aFriendC));
        assertTrue(results.contains(aHateC));

        results = a.inE(labelFriend, labelHate).toList();
        assertEquals(1, results.size());
        assertTrue(results.contains(cHateA));

        results = b.inE(labelFriend, labelHate).toList();
        assertEquals(2, results.size());
        assertTrue(results.contains(aFriendB));
        assertTrue(results.contains(cHateB));

        results = b.inE("blah1", "blah2").toList();
        assertEquals(0, results.size());
    }

//    @Test
    @LoadGraphWith(CLASSIC)
    public void g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_2X_capXaX() throws IOException {
        readGraphMLIntoGraph(this.sqlG);

        Traversal<Vertex, Map<String, Integer>> t = get_g_V_asXxX_out_groupByXname();
        printTraversalForm(t);
        final Map<String, Integer> map = t.next();
        t.forEach(
                (a) -> System.out.println(a)
        );

//        Traversal<Vertex, Map<String, Integer>> traversal = get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_2X_capXaX();
//        printTraversalForm(traversal);
//        final Map<String, Integer> map = traversal.next();
//        assertFalse(traversal.hasNext());
//        assertEquals(4, map.size());
//        assertTrue(map.containsKey("vadas"));
//        assertEquals(Integer.valueOf(1), map.get("vadas"));
//        assertTrue(map.containsKey("josh"));
//        assertEquals(Integer.valueOf(1), map.get("josh"));
//        assertTrue(map.containsKey("lop"));
//        assertEquals(Integer.valueOf(4), map.get("lop"));
//        assertTrue(map.containsKey("ripple"));
//        assertEquals(Integer.valueOf(2), map.get("ripple"));

    }

    public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXname() {
        return this.sqlG.V().as("x").out().groupBy(v -> v.value("name"), v -> v, vv -> vv.size()).as("a").jump("x", 1).cap("a");
    }

    public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_2X_capXaX() {
        return this.sqlG.V().as("x").out().groupBy(v -> v.value("name"), v -> v, vv -> vv.size()).as("a").jump("x", 2).cap("a");
    }

    public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXname_sizeX_asXaX_jumpXx_loops_lt_2X_capXaX() {
        return this.sqlG.V().as("x").out().groupBy(v -> v.value("name"), v -> v, vv -> vv.size()).as("a").jump("x", t -> t.getLoops() < 2).cap("a");
    }

    //    @Test
    @LoadGraphWith(CLASSIC)
    public void g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX() throws IOException {
        readGraphMLIntoGraph(this.sqlG);

        final Traversal<Vertex, Edge> traversal = get_g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertTrue(traversal.hasNext());
        final Edge edge = traversal.next();
        assertEquals("knows", edge.label());
        assertEquals(Float.valueOf(1.0f), edge.<Float>value("weight"));
        assertFalse(traversal.hasNext());
        assertFalse(traversal.hasNext());
    }

    public Traversal<Vertex, Edge> get_g_v1_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_backXhereX(final Object v1Id) {
        return this.sqlG.v(v1Id).outE("knows").has("weight", 1.0f).as("here").inV().has("name", "josh").back("here");
    }

    protected void printTraversalForm(final Traversal traversal) {
        System.out.println("Testing: " + traversal);
        traversal.strategies().apply();
        System.out.println("         " + traversal);
    }

//    @Test
//    @LoadGraphWith(CLASSIC)
    public void testMixedCriteria() throws Exception {

        readGraphMLIntoGraph(this.sqlG);

        Predicate<Vertex> vertexCriterion = vertex -> vertex.value("name").equals("josh") || vertex.value("name").equals("lop") || vertex.value("name").equals("ripple");
        Predicate<Edge> edgeCriterion = edge -> {
            // 9 && 11
            if (edge.<Double>value("weight") == 0.4D && edge.label().equals("created"))
                return true;
                // 10
            else if (edge.<Double>value("weight") == 1.0D && edge.label().equals("created"))
                return true;
            else return false;
        };
        Graph g = this.sqlG;
        GraphStrategy strategyToTest = new SubgraphStrategy(vertexCriterion, edgeCriterion);
        StrategyWrappedGraph sg = new StrategyWrappedGraph(g);
        sg.strategy().setGraphStrategy(strategyToTest);

        // three vertices are included in the subgraph
        assertEquals(6, g.V().count().next().longValue());
        assertEquals(3, sg.V().count().next().longValue());

        // three edges are explicitly included, but one is missing its out-vertex due to the vertex criterion
        assertEquals(6, g.E().count().next().longValue());
        assertEquals(2, sg.E().count().next().longValue());

        // from vertex

        assertEquals(2, g.v(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).out().count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).out().count().next().longValue());

        assertEquals(1, g.v(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).in().count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).in().count().next().longValue());

        assertEquals(3, g.v(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(3, g.v(convertToVertexId("josh")).both().count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).both().count().next().longValue());

        // with label

        assertEquals(2, g.v(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(2, g.v(convertToVertexId("josh")).both("created").count().next().longValue());
        assertEquals(2, sg.v(convertToVertexId("josh")).both("created").count().next().longValue());

        assertEquals(1, g.v(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).both("knows").count().next().longValue());
        assertEquals(0, sg.v(convertToVertexId("josh")).both("knows").count().next().longValue());

        // with branch factor

        assertEquals(1, g.v(convertToVertexId("josh")).bothE(1).count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).bothE(1).count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).both(1).count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).both(1).count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).bothE(1, "knows", "created").count().next().longValue());
// TODO: Why Neo4j not happy?
//        assertEquals(1, sg.v(convertToVertexId("josh")).bothE(1, "knows", "created").count().next().longValue());
        assertEquals(1, g.v(convertToVertexId("josh")).both(1, "knows", "created").count().next().longValue());
        assertEquals(1, sg.v(convertToVertexId("josh")).both(1, "knows", "created").count().next().longValue());

        // from edge

        assertEquals(2, g.e(convertToEdgeId("marko", "created", "lop")).bothV().count().next().longValue());
        assertEquals(1, sg.e(convertToEdgeId("marko", "created", "lop")).bothV().count().next().longValue());
    }

    protected Object convertToVertexId(final String vertexName) {
        return convertToVertexId(this.sqlG, vertexName);
    }

    protected Object convertToVertexId(final Graph g, final String vertexName) {
        return convertToVertex(g, vertexName).id();
    }

    protected Vertex convertToVertex(final Graph g, final String vertexName) {
        // all test graphs have "name" as a unique id which makes it easy to hardcode this...works for now
        return ((Vertex) g.V().has("name", vertexName).next());
    }

    protected Object convertToEdgeId(final String outVertexName, String edgeLabel, final String inVertexName) {
        return convertToEdgeId(this.sqlG, outVertexName, edgeLabel, inVertexName);
    }

    protected Object convertToEdgeId(final Graph g, final String outVertexName, String edgeLabel, final String inVertexName) {
        return ((Edge) g.V().has("name", outVertexName).outE(edgeLabel).as("e").inV().has("name", inVertexName).back("e").next()).id();
    }



//    @Test
//    @LoadGraphWith(CLASSIC)
//    public void g_V_bothX1_createdX_name() throws IOException {
//        readGraphMLIntoGraph(this.sqlG);
//        this.sqlG.tx().commit();
//        final Iterator<String> step = get_g_V_bothX1_createdX_name();
//        System.out.println("Testing: " + step);
//        int counter = 0;
//        while (step.hasNext()) {
//            counter++;
//            final String name = step.next();
//            assertTrue(name.equals("marko") || name.equals("lop") || name.equals("josh") || name.equals("ripple") || name.equals("peter"));
//        }
//        assertEquals(5, counter);
//    }
//
//    public Traversal<Vertex, String> get_g_V_bothX1_createdX_name() {
//        return this.sqlG.V().both(1, "created").value("name");
//    }
//
////    @Test
//    public void g_V_asXxX_out_groupByXa_name_sizeX_jumpXx_2X_capXaX() throws IOException {
//        readGraphMLIntoGraph(this.sqlG);
//        assertEquals(6, this.sqlG.V().count().next(), 0);
//        assertEquals(6, this.sqlG.V().out().count().next(), 0);
//
//        List<Traversal<Vertex, Map<String, Integer>>> traversals = new ArrayList<>();
//        traversals.add(get_g_V_asXxX_out_groupByXa_name_sizeX_jumpXx_2X_capXaX());
////        traversals.add(get_g_V_asXxX_out_groupByXa_name_sizeX_jumpXx_loops_lt_2X_capXaX());
//        traversals.forEach(traversal -> {
//            System.out.println("Testing: " + traversal);
//            final Map<String, Integer> map = traversal.next();
//            assertFalse(traversal.hasNext());
//            assertEquals(4, map.size());
//            assertTrue(map.containsKey("vadas"));
//            assertEquals(Integer.valueOf(1), map.get("vadas"));
//            assertTrue(map.containsKey("josh"));
//            assertEquals(Integer.valueOf(1), map.get("josh"));
//            assertTrue(map.containsKey("lop"));
//            assertEquals(Integer.valueOf(4), map.get("lop"));
//            assertTrue(map.containsKey("ripple"));
//            assertEquals(Integer.valueOf(2), map.get("ripple"));
//        });
//    }
//
//    public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXa_name_sizeX_jumpXx_2X_capXaX() {
//        return this.sqlG.V().as("x").out().groupBy(
//                "a",
//                v -> v.value("name"),
//                v -> v,
//                vv -> vv.size()
//        ).jump("x", 2).cap("a");
//    }
//
//    public Traversal<Vertex, Map<String, Integer>> get_g_V_asXxX_out_groupByXa_name_sizeX_jumpXx_loops_lt_2X_capXaX() {
//        return this.sqlG.V().as("x").out().groupBy(
//                "a",
//                v -> v.value("name"),
//                v -> v,
//                vv -> vv.size()
//        ).jump("x", t -> t.getLoops() < 2).cap("a");
//    }
//
//    public Traversal<Vertex, String> get_g_E_subgraphXcreatedX(final Graph subgraph) {
//        return this.sqlG.V().inE().subgraph(subgraph, e -> e.label().equals("created")).value("name");
//    }
//
//    public Traversal<Vertex, Path> get_g_V_asXxX_both_simplePath_jumpXx_loops_lt_3X_path() {
//        return this.sqlG.V().as("x").both().simplePath().jump("x", t -> t.getLoops() < 3).path();
//    }
//
////    @Test
////    @LoadGraphWith(CLASSIC)
////    public void g_e7_hasXlabelXknowsX() throws IOException {
////        readGraphMLIntoGraph(this.sqlGraph);
////        //System.out.println(convertToEdgeId("marko", "knows", "vadas"));
////        Iterator<Edge> traversal = get_g_e7_hasXlabelXknowsX(convertToEdgeId("marko", "knows", "vadas"));
////        System.out.println("Testing: " + traversal);
////        int counter = 0;
////        while (traversal.hasNext()) {
////            counter++;
////            assertEquals("knows", traversal.next().label());
////        }
////        assertEquals(1, counter);
////    }
//
//    public Traversal<Edge, Edge> get_g_e7_hasXlabelXknowsX(final Object e7Id) {
//        return this.sqlG.e(e7Id).has("label", "knows");
//    }
//
//    protected Object convertToEdgeId(final String identifier1, String edgeLabel, final String identifier2) {
//        return convertToEdgeId(this.sqlG, identifier1, edgeLabel, identifier2);
//    }
//
//    protected Object convertToEdgeId(final Graph g, final String identifier1, String edgeLabel, final String identifier2) {
//        return ((Edge) g.V().has("name", identifier1).outE(edgeLabel).as("e").inV().has("name", identifier2).back("e").next()).id();
//    }
//
//
////        @Test
//    public void g_v1_outXcreatedX_inXcreatedX_rangeX1_2X() throws IOException {
//        readGraphMLIntoGraph(this.sqlG);
//
//        Object id = convertToVertexId("josh");
////        Traversal<Vertex, Vertex> t = this.sqlGraph.v(id).out("created").in("created");
//        Traversal<Vertex, Vertex> t = this.sqlG.v(id).both("knows");
//        while (t.hasNext()) {
//            System.out.println(t.next().property("name").value());
//        }
//
////        final Iterator<Vertex> traversal = get_g_v1_outXcreatedX_inXcreatedX_rangeX1_2X(convertToVertexId("marko"));
////        System.out.println("Testing: " + traversal);
////        int counter = 0;
////        while (traversal.hasNext()) {
////            counter++;
////            final String name = traversal.next().value("name");
////            assertTrue(name.equals("marko") || name.equals("josh") || name.equals("peter"));
////        }
////        assertEquals(2, counter);
//    }
//
//    public Traversal<Vertex, Vertex> get_g_v1_outXcreatedX_inXcreatedX_rangeX1_2X(final Object v1Id) {
//        return this.sqlG.v(v1Id).out("created").in("created").range(1, 2);
//    }
//
//    //    @Test
//    public void g_v1_outXcreatedX_inXcreatedX_cyclicPath_path() throws IOException {
//        readGraphMLIntoGraph(this.sqlG);
//        final Traversal<Vertex, Path> traversal = get_g_v1_outXcreatedX_inXcreatedX_cyclicPath_path(convertToVertexId("marko"));
//        System.out.println("Testing: " + traversal);
//        int counter = 0;
//        while (traversal.hasNext()) {
//            counter++;
//            Path path = traversal.next();
//            assertFalse(path.isSimple());
//        }
//        assertEquals(1, counter);
//        assertFalse(traversal.hasNext());
//    }
//
//    public Traversal<Vertex, Path> get_g_v1_outXcreatedX_inXcreatedX_cyclicPath_path(final Object v1Id) {
//        return this.sqlG.v(v1Id).out("created").in("created").cyclicPath().path();
//    }
//
//    /**
//     * Looks up the identifier as generated by the current source graph being tested.
//     *
//     * @param identifier a unique string that will identify a graph element within a graph
//     * @return the id as generated by the graph
//     */
//    protected Object convertToVertexId(final String identifier) {
//        return convertToVertexId(this.sqlG, identifier);
//    }
//
//    /**
//     * Looks up the identifier as generated by the current source graph being tested.
//     *
//     * @param g          the graph to get the element id from
//     * @param identifier a unique string that will identify a graph element within a graph
//     * @return the id as generated by the graph
//     */
//    protected Object convertToVertexId(final Graph g, final String identifier) {
//        // all test graphs have "name" as a unique id which makes it easy to hardcode this...works for now
//        return ((Vertex) g.V().has("name", identifier).next()).id();
//    }
//
//    //    @Test
//    public void shouldSupportTransactionIsolationWithSeparateThreads() throws Exception {
//        // one thread modifies the graph and a separate thread reads before the transaction is committed.
//        // the expectation is that the changes in the transaction are isolated to the thread that made the change
//        // and the second thread should not see the change until commit() in the first thread.
//        final Graph graph = this.sqlG;
//
//        final CountDownLatch latchCommit = new CountDownLatch(1);
//        final CountDownLatch latchFirstRead = new CountDownLatch(1);
//        final CountDownLatch latchSecondRead = new CountDownLatch(1);
//
//        final Thread threadMod = new Thread() {
//            public void run() {
//                graph.addVertex();
//
//                latchFirstRead.countDown();
//
//                try {
//                    latchCommit.await();
//                } catch (InterruptedException ie) {
//                    throw new RuntimeException(ie);
//                }
//
//                graph.tx().commit();
//
//                latchSecondRead.countDown();
//            }
//        };
//
//        threadMod.start();
//
//        final AtomicLong beforeCommitInOtherThread = new AtomicLong(0);
//        final AtomicLong afterCommitInOtherThread = new AtomicLong(0);
//        final Thread threadRead = new Thread() {
//            public void run() {
//                try {
//                    latchFirstRead.await();
//                } catch (InterruptedException ie) {
//                    throw new RuntimeException(ie);
//                }
//
//                // reading vertex before tx from other thread is committed...should have zero vertices
//                beforeCommitInOtherThread.set(graph.V().count().next());
//
//                latchCommit.countDown();
//
//                try {
//                    latchSecondRead.await();
//                } catch (InterruptedException ie) {
//                    throw new RuntimeException(ie);
//                }
//
//                // tx in other thread is committed...should have one vertex
//                afterCommitInOtherThread.set(graph.V().count().next());
//            }
//        };
//
//        threadRead.start();
//
//        threadMod.join();
//        threadRead.join();
//
//        assertEquals(0l, beforeCommitInOtherThread.get());
//        assertEquals(1l, afterCommitInOtherThread.get());
//    }
//
//    //    @Test
//    public void shouldReadGraphML() throws IOException {
//        readGraphMLIntoGraph(this.sqlG);
//        assertClassicGraph(this.sqlG, true, true);
//    }
//
    private static void readGraphMLIntoGraph(final Graph g) throws IOException {
        final GraphReader reader = GraphMLReader.build().create();
        try (final InputStream stream = new FileInputStream(new File("sqlg-test/src/main/resources/tinkerpop-classic.xml"))) {
            reader.readGraph(stream, g);
        }
//        try (final InputStream stream = TinkerpopTest.class.getResourceAsStream("tinkerpop-classic.xml")) {
//            reader.readGraph(stream, g);
//        }
    }

//    public static void assertClassicGraph(final Graph g1, final boolean lossyForFloat, final boolean lossyForId) {
//        assertEquals(new Long(6), g1.V().count().next());
//        assertEquals(new Long(6), g1.E().count().next());
//
//        final Vertex v1 = (Vertex) g1.V().has("name", "marko").next();
//        assertEquals(29, v1.<Integer>value("age").intValue());
//        assertEquals(2, v1.keys().size());
//        assertClassicId(g1, lossyForId, v1, 1);
//
//        final List<Edge> v1Edges = v1.bothE().toList();
//        assertEquals(3, v1Edges.size());
//        v1Edges.forEach(e -> {
//            if (e.inV().value("name").next().equals("vadas")) {
//                assertEquals("knows", e.label());
//                if (lossyForFloat)
//                    assertEquals(0.5d, e.value("weight"), 0.0001d);
//                else
//                    assertEquals(0.5f, e.value("weight"), 0.0001f);
//                assertEquals(1, e.keys().size());
//                assertClassicId(g1, lossyForId, e, 7);
//            } else if (e.inV().value("name").next().equals("josh")) {
//                assertEquals("knows", e.label());
//                if (lossyForFloat)
//                    assertEquals(1.0, e.value("weight"), 0.0001d);
//                else
//                    assertEquals(1.0f, e.value("weight"), 0.0001f);
//                assertEquals(1, e.keys().size());
//                assertClassicId(g1, lossyForId, e, 8);
//            } else if (e.inV().value("name").next().equals("lop")) {
//                assertEquals("created", e.label());
//                if (lossyForFloat)
//                    assertEquals(0.4d, e.value("weight"), 0.0001d);
//                else
//                    assertEquals(0.4f, e.value("weight"), 0.0001f);
//                assertEquals(1, e.keys().size());
//                assertClassicId(g1, lossyForId, e, 9);
//            } else {
//                fail("Edge not expected");
//            }
//        });
//
//        final Vertex v2 = (Vertex) g1.V().has("name", "vadas").next();
//        assertEquals(27, v2.<Integer>value("age").intValue());
//        assertEquals(2, v2.keys().size());
//        assertClassicId(g1, lossyForId, v2, 2);
//
//        final List<Edge> v2Edges = v2.bothE().toList();
//        assertEquals(1, v2Edges.size());
//        v2Edges.forEach(e -> {
//            if (e.outV().value("name").next().equals("marko")) {
//                assertEquals("knows", e.label());
//                if (lossyForFloat)
//                    assertEquals(0.5d, e.value("weight"), 0.0001d);
//                else
//                    assertEquals(0.5f, e.value("weight"), 0.0001f);
//                assertEquals(1, e.keys().size());
//                assertClassicId(g1, lossyForId, e, 7);
//            } else {
//                fail("Edge not expected");
//            }
//        });
//
//        final Vertex v3 = (Vertex) g1.V().has("name", "lop").next();
//        assertEquals("java", v3.<String>value("lang"));
//        assertEquals(2, v2.keys().size());
//        assertClassicId(g1, lossyForId, v3, 3);
//
//        final List<Edge> v3Edges = v3.bothE().toList();
//        assertEquals(3, v3Edges.size());
//        v3Edges.forEach(e -> {
//            if (e.outV().value("name").next().equals("peter")) {
//                assertEquals("created", e.label());
//                if (lossyForFloat)
//                    assertEquals(0.2d, e.value("weight"), 0.0001d);
//                else
//                    assertEquals(0.2f, e.value("weight"), 0.0001f);
//                assertEquals(1, e.keys().size());
//                assertClassicId(g1, lossyForId, e, 12);
//            } else if (e.outV().next().value("name").equals("josh")) {
//                assertEquals("created", e.label());
//                if (lossyForFloat)
//                    assertEquals(0.4d, e.value("weight"), 0.0001d);
//                else
//                    assertEquals(0.4f, e.value("weight"), 0.0001f);
//                assertEquals(1, e.keys().size());
//                assertClassicId(g1, lossyForId, e, 11);
//            } else if (e.outV().value("name").next().equals("marko")) {
//                assertEquals("created", e.label());
//                if (lossyForFloat)
//                    assertEquals(0.4d, e.value("weight"), 0.0001d);
//                else
//                    assertEquals(0.4f, e.value("weight"), 0.0001f);
//                assertEquals(1, e.keys().size());
//                assertClassicId(g1, lossyForId, e, 9);
//            } else {
//                fail("Edge not expected");
//            }
//        });
//
//        final Vertex v4 = (Vertex) g1.V().has("name", "josh").next();
//        assertEquals(32, v4.<Integer>value("age").intValue());
//        assertEquals(2, v4.keys().size());
//        assertClassicId(g1, lossyForId, v4, 4);
//
//        final List<Edge> v4Edges = v4.bothE().toList();
//        assertEquals(3, v4Edges.size());
//        v4Edges.forEach(e -> {
//            if (e.inV().value("name").next().equals("ripple")) {
//                assertEquals("created", e.label());
//                if (lossyForFloat)
//                    assertEquals(1.0d, e.value("weight"), 0.0001d);
//                else
//                    assertEquals(1.0f, e.value("weight"), 0.0001f);
//                assertEquals(1, e.keys().size());
//                assertClassicId(g1, lossyForId, e, 10);
//            } else if (e.inV().value("name").next().equals("lop")) {
//                assertEquals("created", e.label());
//                if (lossyForFloat)
//                    assertEquals(0.4d, e.value("weight"), 0.0001d);
//                else
//                    assertEquals(0.4f, e.value("weight"), 0.0001f);
//                assertEquals(1, e.keys().size());
//                assertClassicId(g1, lossyForId, e, 11);
//            } else if (e.outV().value("name").next().equals("marko")) {
//                assertEquals("knows", e.label());
//                if (lossyForFloat)
//                    assertEquals(1.0d, e.value("weight"), 0.0001d);
//                else
//                    assertEquals(1.0f, e.value("weight"), 0.0001f);
//                assertEquals(1, e.keys().size());
//                assertClassicId(g1, lossyForId, e, 8);
//            } else {
//                fail("Edge not expected");
//            }
//        });
//
//        final Vertex v5 = (Vertex) g1.V().has("name", "ripple").next();
//        assertEquals("java", v5.<String>value("lang"));
//        assertEquals(2, v5.keys().size());
//        assertClassicId(g1, lossyForId, v5, 5);
//
//        final List<Edge> v5Edges = v5.bothE().toList();
//        assertEquals(1, v5Edges.size());
//        v5Edges.forEach(e -> {
//            if (e.outV().value("name").next().equals("josh")) {
//                assertEquals("created", e.label());
//                if (lossyForFloat)
//                    assertEquals(1.0d, e.value("weight"), 0.0001d);
//                else
//                    assertEquals(1.0f, e.value("weight"), 0.0001f);
//                assertEquals(1, e.keys().size());
//                assertClassicId(g1, lossyForId, e, 10);
//            } else {
//                fail("Edge not expected");
//            }
//        });
//
//        final Vertex v6 = (Vertex) g1.V().has("name", "peter").next();
//        assertEquals(35, v6.<Integer>value("age").intValue());
//        assertEquals(2, v6.keys().size());
//        assertClassicId(g1, lossyForId, v6, 6);
//
//        final List<Edge> v6Edges = v6.bothE().toList();
//        assertEquals(1, v6Edges.size());
//        v6Edges.forEach(e -> {
//            if (e.inV().value("name").next().equals("lop")) {
//                assertEquals("created", e.label());
//                if (lossyForFloat)
//                    assertEquals(0.2d, e.value("weight"), 0.0001d);
//                else
//                    assertEquals(0.2f, e.value("weight"), 0.0001f);
//                assertEquals(1, e.keys().size());
//                assertClassicId(g1, lossyForId, e, 12);
//            } else {
//                fail("Edge not expected");
//            }
//        });
//    }
//
//    private static void assertClassicId(final Graph g, final boolean lossyForId, final Element e, final Object expected) {
//        if (g.getFeatures().edge().supportsUserSuppliedIds()) {
//            if (lossyForId)
//                assertEquals(expected.toString(), e.id().toString());
//            else
//                assertEquals(expected, e.id());
//        }
//    }
//
//    protected void tryCommit(final SqlG g, final Consumer<Graph> assertFunction) {
//        assertFunction.accept(g);
//        if (g.getFeatures().graph().supportsTransactions()) {
//            g.tx().commit();
//            assertFunction.accept(g);
//        }
//    }
//
//    protected void tryCommit(final Graph g) {
//        if (g.getFeatures().graph().supportsTransactions())
//            g.tx().commit();
//    }
//
//    protected void tryRollback(final Graph g) {
//        if (g.getFeatures().graph().supportsTransactions())
//            g.tx().rollback();
//    }

}
