package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.*;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.match.Bindings;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.structure.util.batch.BatchGraph;
import com.tinkerpop.gremlin.structure.util.batch.Exists;
import com.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import com.tinkerpop.gremlin.util.function.TriFunction;
import org.javatuples.Pair;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Function;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.structure.Graph.Features.DataTypeFeatures.FEATURE_INTEGER_VALUES;
import static com.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Date: 2014/07/13
 * Time: 6:32 PM
 */
public class TinkerpopTest extends BaseTest {


    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotReHideAnAlreadyHiddenKeyWhenGettingHiddenValue() {
        final Vertex v = this.sqlG.addVertex("name", "marko", Graph.Key.hide("acl"), "rw", Graph.Key.hide("other"), "rw");
        this.sqlG.tx().commit();
        final Vertex v1 = this.sqlG.v(v.id());
        v1.hiddenKeys().stream().forEach(hiddenKey -> assertTrue(v1.hiddenValue(hiddenKey).hasNext()));
        assertFalse(v1.hiddenValue(Graph.Key.hide("other")).hasNext());
        assertTrue(v1.hiddenValue("other").hasNext());

        final Vertex u = this.sqlG.addVertex();
        Edge e = v1.addEdge("knows", u, Graph.Key.hide("acl"), "private", "acl", "public");
        this.sqlG.tx().commit();
        final Edge e1 = this.sqlG.e(e.id());
        e1.hiddenKeys().stream().forEach(hiddenKey -> assertTrue(e1.hiddenValue(hiddenKey).hasNext()));
        assertFalse(e1.hiddenValue(Graph.Key.hide("acl")).hasNext());
        assertTrue(e1.hiddenValue("acl").hasNext());
        assertEquals("private", e1.iterators().hiddens("acl").next().value());
        assertEquals("public", e1.iterators().properties("acl").next().value());
    }

//    @Test
    public void shouldHandleHiddenVertexProperties() {

        final Vertex v = this.sqlG.addVertex(Graph.Key.hide("age"), 29, "age", 16, "name", "marko", "food", "taco", Graph.Key.hide("color"), "purple");
        this.sqlG.tx().commit();

        boolean multi = false;

        assertTrue(v.property("age").isPresent());
        assertTrue(v.value("age").equals(16));
        assertTrue(v.properties("age").count().next().intValue() == 1);
        assertTrue(v.properties("age").value().next().equals(16));
        assertTrue(v.hiddens("age").count().next().intValue() == (multi ? 2 : 1));
        assertTrue(v.hiddens(Graph.Key.hide("age")).count().next().intValue() == 0);
        assertTrue(v.properties(Graph.Key.hide("age")).count().next().intValue() == 0);
        assertTrue(multi ? v.hiddens("age").value().toList().contains(34) : v.hiddens("age").value().toList().contains(29));
        assertTrue(v.hiddens("age").value().toList().contains(29));
        assertTrue(v.hiddenKeys().size() == 2);
        assertTrue(v.keys().size() == 3);
        assertTrue(v.keys().contains("age"));
        assertTrue(v.keys().contains("name"));
        assertTrue(v.hiddenKeys().contains("age"));
        assertTrue(v.property(Graph.Key.hide("color")).key().equals("color"));

    }

//    @Test
//    @LoadGraphWith(MODERN)
//    public void g_V_matchXa_created_b__b_0created_cX_whereXa_neq_cX_selectXa_c_nameX() throws Exception {
//        Graph g = this.sqlG;
//        final GraphReader reader = KryoReader.build().setWorkingDirectory(File.separator + "tmp").create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.gio")) {
//            reader.readGraph(stream, g);
//        }
//        Traversal<Vertex, Map<String, String>> traversal = a();
//        printTraversalForm(traversal);
//        List<Map<String, String>> vertices = traversal.toList();
//        for (Map<String, String> stringStringMap : vertices) {
//            System.out.println(stringStringMap);
//        }
//        assertResults(Function.identity(), traversal,
//                new Bindings<String>().put("a", "marko").put("c", "josh"),
//                new Bindings<String>().put("a", "marko").put("c", "peter"),
//                new Bindings<String>().put("a", "josh").put("c", "marko"),
//                new Bindings<String>().put("a", "josh").put("c", "peter"),
//                new Bindings<String>().put("a", "peter").put("c", "marko"),
//                new Bindings<String>().put("a", "peter").put("c", "josh"),
//                new Bindings<String>().put("a", "josh").put("c", "marko")); // TODO: THIS IS REPEATED
//    }

    public Traversal<Vertex, Map<String, String>> a() {
        return this.sqlG.V().match("a",
                this.sqlG.of().as("a").out("created").as("b"),
                this.sqlG.of().as("b").in("created").as("c"))
                .where("a", Compare.neq, "c")
                .select(Arrays.asList("a", "c"), v -> ((Vertex) v).value("name"));
    }

    public Traversal<Vertex, Map<String, String>> b() {
        return this.sqlG.V().match("a",
                this.sqlG.of().as("a").out("created").as("b"))
//                .where("a", T.neq, "c")
                .select(Arrays.asList("a", "b"), v -> ((Vertex) v).value("name"));
    }

    private <S, E> void assertResults(final Function<E, String> toStringFunction,
                                      final Traversal<S, Map<String, E>> actual,
                                      final Bindings<E>... expected) {
        Comparator<Bindings<E>> comp = new Bindings.BindingsComparator<>(toStringFunction);

        List<Bindings<E>> actualList = toBindings(actual);
        List<Bindings<E>> expectedList = new LinkedList<>();
        Collections.addAll(expectedList, expected);

        if (expectedList.size() > actualList.size()) {
            fail("" + (expectedList.size() - actualList.size()) + " expected results not found, including " + expectedList.get(actualList.size()));
        } else if (actualList.size() > expectedList.size()) {
            fail("" + (actualList.size() - expectedList.size()) + " unexpected results, including " + actualList.get(expectedList.size()));
        }

        Collections.sort(actualList, comp);
        Collections.sort(expectedList, comp);

        for (int j = 0; j < actualList.size(); j++) {
            Bindings<E> a = actualList.get(j);
            Bindings<E> e = expectedList.get(j);

            if (0 != comp.compare(a, e)) {
                fail("unexpected result(s), including " + a);
            }
        }
        assertFalse(actual.hasNext());
    }

    private <S, E> List<Bindings<E>> toBindings(final Traversal<S, Map<String, E>> traversal) {
        List<Bindings<E>> result = new LinkedList<>();
        traversal.forEach(o -> {
            result.add(new Bindings<>(o));
        });
        return result;
    }

    //    @Test
    @LoadGraphWith(MODERN)
    public void g_v4_out_asXhereX_hasXlang_javaX_backXhereX() throws IOException {
        Graph g = this.sqlG;
        final GraphReader reader = KryoReader.build().setWorkingDirectory(File.separator + "tmp").create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.gio")) {
            reader.readGraph(stream, g);
        }

        final Traversal<Vertex, Vertex> traversal = get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Vertex vertex = traversal.next();
            assertEquals("java", vertex.<String>value("lang"));
            assertTrue(vertex.value("name").equals("ripple") || vertex.value("name").equals("lop"));
        }
        assertEquals(2, counter);
    }

    public Traversal<Vertex, Vertex> get_g_v4_out_asXhereX_hasXlang_javaX_backXhereX(final Object v4Id) {
        return this.sqlG.v(v4Id).out().as("here").has("lang", "java").back("here");
    }

    /**
     * Looks up the identifier as generated by the current source graph being tested.
     *
     * @param vertexName a unique string that will identify a graph element within a graph
     * @return the id as generated by the graph
     */
    public Object convertToVertexId(final String vertexName) {
        return convertToVertexId(this.sqlG, vertexName);
    }

    /**
     * Looks up the identifier as generated by the current source graph being tested.
     *
     * @param g          the graph to get the element id from
     * @param vertexName a unique string that will identify a graph element within a graph
     * @return the id as generated by the graph
     */
    public Object convertToVertexId(final Graph g, final String vertexName) {
        return convertToVertex(g, vertexName).id();
    }

    public Vertex convertToVertex(final Graph g, final String vertexName) {
        // all test graphs have "name" as a unique id which makes it easy to hardcode this...works for now
        return ((Vertex) g.V().has("name", vertexName).next());
    }


    public Object convertToEdgeId(final String outVertexName, String edgeLabel, final String inVertexName) {
        return convertToEdgeId(this.sqlG, outVertexName, edgeLabel, inVertexName);
    }

    public Object convertToEdgeId(final Graph g, final String outVertexName, String edgeLabel, final String inVertexName) {
        return ((Edge) g.V().has("name", outVertexName).outE(edgeLabel).as("e").inV().has("name", inVertexName).back("e").next()).id();
    }

    protected void printTraversalForm(final Traversal traversal) {
        System.out.println("Testing: " + traversal);
        traversal.strategies().apply();
        System.out.println("         " + traversal);
    }

    private static void readGraphMLIntoGraph(final Graph g) throws IOException {
        final GraphReader reader = GraphMLReader.build().create();
        try (final InputStream stream = new FileInputStream(new File("sqlg-test/src/main/resources/tinkerpop-classic.xml"))) {
            reader.readGraph(stream, g);
        }
//        try (final InputStream stream = TinkerpopTest.class.getResourceAsStream("tinkerpop-classic.xml")) {
//            reader.readGraph(stream, g);
//        }
    }

}
