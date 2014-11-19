package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.*;
import com.tinkerpop.gremlin.algorithm.generator.CommunityGenerator;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.match.Bindings;
import com.tinkerpop.gremlin.process.marker.CapTraversal;
import com.tinkerpop.gremlin.process.marker.CountTraversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.io.GraphReader;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import com.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import com.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

/**
 * Date: 2014/07/13
 * Time: 6:32 PM
 */
public class TinkerpopTest extends BaseTest {

    @Test
    @LoadGraphWith(CLASSIC)
    public void shouldHaveSameTraversalReturnTypesForAllMethods() throws IOException {
        Graph g = this.sqlgGraph;
        final GraphReader reader = GraphMLReader.build().create();
        try (final InputStream stream = new FileInputStream(new File("sqlg-test/src/main/resources/tinkerpop-classic.xml"))) {
            reader.readGraph(stream, g);
        }

        final Set<String> allReturnTypes = new HashSet<>();
        final List<Pair<String, Object>> vendorsClasses = Arrays.asList(
                Pair.<String, Object>with("g.V Traversal", g.V()),
                Pair.<String, Object>with("g.E Traversal", g.E()),
                Pair.<String, Object>with("v.identity Traversal", g.V().next().identity()),
                Pair.<String, Object>with("e.identity Traversal", g.E().next().identity()),
                Pair.<String, Object>with("v", g.V().next()),
                Pair.<String, Object>with("e", g.E().next()),
                Pair.<String, Object>with("g.of()", g.of())
        );
        vendorsClasses.forEach(triplet -> {
            final String situation = triplet.getValue0();
            final Class vendorClass = triplet.getValue1().getClass();
            final Set<String> returnTypes = new HashSet<>();
            Arrays.asList(vendorClass.getMethods())
                    .stream()
                    .filter(m -> GraphTraversal.class.isAssignableFrom(m.getReturnType()))
                    .filter(m -> !Modifier.isStatic(m.getModifiers()))
                    .map(m -> getDeclaredVersion(m, vendorClass))
                    .forEach(m -> {
                        returnTypes.add(m.getReturnType().getCanonicalName());
                        allReturnTypes.add(m.getReturnType().getCanonicalName());
                    });
            if (returnTypes.size() > 1) {
                final boolean muted = Boolean.parseBoolean(System.getProperty("muteTestLogs", "false"));
                if (!muted) System.out.println("FAILURE: " + vendorClass.getCanonicalName() + " methods do not return in full fluency for [" + situation + "]");
                fail("The return types of all traversal methods should be the same to ensure proper fluency: " + returnTypes);
            } else {
                final boolean muted = Boolean.parseBoolean(System.getProperty("muteTestLogs", "false"));
                if (!muted) System.out.println("SUCCESS: " + vendorClass.getCanonicalName() + " methods return in full fluency for [" + situation + "]");
            }
        });
        if (allReturnTypes.size() > 1)
            fail("All traversals possible do not maintain the same return types and thus, not fully fluent for entire graph system: " + allReturnTypes);
    }

    private static Method getDeclaredVersion(final Method method, final Class vendorClass) {
        return Arrays.asList(vendorClass.getMethods())
                .stream()
                .filter(m -> !GraphTraversal.class.equals(m.getReturnType()))
                .filter(m -> !Traversal.class.equals(m.getReturnType()))
                .filter(m -> !CountTraversal.class.equals(m.getReturnType()))
                .filter(m -> !CapTraversal.class.equals(m.getReturnType()))
                        //.filter(m -> !Traversal.class.isAssignableFrom(m.getReturnType()))
                .filter(m -> !Modifier.isStatic(m.getModifiers()))
                .filter(m -> m.getName().equals(method.getName()))
                .filter(m -> Arrays.asList(m.getParameterTypes()).toString().equals(Arrays.asList(method.getParameterTypes()).toString()))
                .findAny().orElseGet(() -> {
                    final boolean muted = Boolean.parseBoolean(System.getProperty("muteTestLogs", "false"));
                    if (!muted) System.out.println("IGNORE IF TEST PASSES: Can not find native implementation of: " + method);
                    return method;
                });
    }

    //    @Test
    @LoadGraphWith(MODERN)
    public void g_v4_bothE_localLimitX2X_otherV_name() throws IOException {
        Graph g = this.sqlgGraph;
        final GraphReader reader = KryoReader.build().workingDirectory(File.separator + "tmp").create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.gio")) {
            reader.readGraph(stream, g);
        }

        final Traversal<Vertex, String> traversal = get_g_v4_bothE_localLimitX2X_otherV_name(convertToVertexId("josh"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Object name = traversal.next();
            System.out.println(name);
//            assertTrue(name.equals("marko") || name.equals("ripple") || name.equals("lop"));
        }
        assertEquals(2, counter);
        assertFalse(traversal.hasNext());
    }

    public Traversal<Vertex, String> get_g_v4_bothE_localLimitX2X_otherV_name(final Object v4Id) {
        return this.sqlgGraph.v(v4Id).bothE().localLimit(2).inV().values("name");
//        return this.sqlgGraph.v(v4Id).bothE().inV().values("name");
//        return this.sqlgGraph.v(v4Id).bothE().inV();
    }

    //    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXageX_propertiesXname_ageX_value() throws IOException {
        Graph g = this.sqlgGraph;
        final GraphReader reader = KryoReader.build().workingDirectory(File.separator + "tmp").create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.gio")) {
            reader.readGraph(stream, g);
        }

//        Traversal<Vertex, Object> gt = get_g_V_hasXageX_propertiesXage_nameX_value();
//        gt.toList().forEach(a-> System.out.println(a));
        System.out.println("------------");
        Traversal<Vertex, Object>gt = get_g_V_hasXageX_propertiesXname_ageX_value();
        gt.toList().forEach(a-> System.out.println(a));

//        Arrays.asList(/*get_g_V_hasXageX_propertiesXage_nameX_value(),*/ get_g_V_hasXageX_propertiesXname_ageX_value()).forEach(traversal -> {
//            printTraversalForm(traversal);
//            checkResults(Arrays.asList("marko", 29, "vadas", 27, "josh", 32, "peter", 35), traversal);
//        });
    }

    public <T> void checkResults(final List<T> expectedResults, final Traversal<?, T> traversal) {
        final List<T> results = traversal.toList();
        assertEquals("Checking result size", expectedResults.size(), results.size());
        for (T t : results) {
            if (t instanceof Map) {
                assertTrue("Checking map result existence: " + t, expectedResults.stream().filter(e -> e instanceof Map).filter(e -> checkMap((Map) e, (Map) t)).findAny().isPresent());
            } else {
                assertTrue("Checking result existence: " + t, expectedResults.contains(t));
            }
        }
        final Map<T, Long> expectedResultsCount = new HashMap<>();
        final Map<T, Long> resultsCount = new HashMap<>();
        assertEquals("Checking indexing is equivalent", expectedResultsCount.size(), resultsCount.size());
        expectedResults.forEach(t -> MapHelper.incr(expectedResultsCount, t, 1l));
        results.forEach(t -> MapHelper.incr(resultsCount, t, 1l));
        expectedResultsCount.forEach((k, v) -> assertEquals("Checking result group counts", v, resultsCount.get(k)));
        assertFalse(traversal.hasNext());
    }

    private <A, B> boolean checkMap(final Map<A, B> expectedMap, final Map<A, B> actualMap) {
        final List<Map.Entry<A, B>> actualList = actualMap.entrySet().stream().sorted((a, b) -> a.getKey().toString().compareTo(b.getKey().toString())).collect(Collectors.toList());
        final List<Map.Entry<A, B>> expectedList = expectedMap.entrySet().stream().sorted((a, b) -> a.getKey().toString().compareTo(b.getKey().toString())).collect(Collectors.toList());

        if (expectedList.size() > actualList.size()) {
            return false;
        } else if (actualList.size() > expectedList.size()) {
            return false;
        }

        for (int i = 0; i < actualList.size(); i++) {
            if (!actualList.get(i).getKey().equals(expectedList.get(i).getKey())) {
                return false;
            }
            if (!actualList.get(i).getValue().equals(expectedList.get(i).getValue())) {
                return false;
            }
        }
        return true;
    }

    public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXname_ageX_value() {
        return this.sqlgGraph.V().has("age").properties("name", "age").value();
    }

    public Traversal<Vertex, Object> get_g_V_hasXageX_propertiesXage_nameX_value() {
        return this.sqlgGraph.V().has("age").properties("age", "name").value();
    }

    //    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotBeEqualsPropertiesAsThereIsDifferentValue() throws IOException {
        Graph g = this.sqlgGraph;
        final GraphReader reader = KryoReader.build().workingDirectory(File.separator + "tmp").create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.gio")) {
            reader.readGraph(stream, g);
        }
        final Edge e = g.v(4).addEdge("created", g.v(convertToVertexId("josh")), "weight", 123.0001d);
        assertFalse(DetachedProperty.detach(e.property("weight")).equals(DetachedProperty.detach(g.e(convertToEdgeId("josh", "created", "lop")).property("weight"))));
    }

//    @Test
    public void shouldNotEvaluateToEqualDifferentId() throws IOException {
        Graph g = this.sqlgGraph;
        final GraphReader reader = KryoReader.build().workingDirectory(File.separator + "tmp").create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.gio")) {
            reader.readGraph(stream, g);
        }
        Object joshCreatedLopEdgeId = convertToEdgeId("josh", "created", "lop");
        final Vertex vOut = g.v(convertToVertexId("josh"));
        final Vertex vIn = g.v(convertToVertexId("lop"));
        final Edge e = vOut.addEdge("created", vIn, "weight", 0.4d);
        assertFalse(DetachedEdge.detach(g.e(joshCreatedLopEdgeId)).equals(DetachedEdge.detach(e)));
    }

    //    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldNotReHideAnAlreadyHiddenKeyWhenGettingHiddenValue() {
        final Vertex v = this.sqlgGraph.addVertex("name", "marko", Graph.Key.hide("acl"), "rw", Graph.Key.hide("other"), "rw");
        this.sqlgGraph.tx().commit();
        final Vertex v1 = this.sqlgGraph.v(v.id());
        v1.hiddenKeys().stream().forEach(hiddenKey -> assertTrue(v1.hiddenValues(hiddenKey).hasNext()));
        assertFalse(v1.hiddenValues(Graph.Key.hide("other")).hasNext());
        assertTrue(v1.hiddenValues("other").hasNext());

        final Vertex u = this.sqlgGraph.addVertex();
        Edge e = v1.addEdge("knows", u, Graph.Key.hide("acl"), "private", "acl", "public");
        this.sqlgGraph.tx().commit();
        final Edge e1 = this.sqlgGraph.e(e.id());
        e1.hiddenKeys().stream().forEach(hiddenKey -> assertTrue(e1.hiddenValues(hiddenKey).hasNext()));
        assertFalse(e1.hiddenValues(Graph.Key.hide("acl")).hasNext());
        assertTrue(e1.hiddenValues("acl").hasNext());
        assertEquals("private", e1.iterators().hiddenPropertyIterator("acl").next().value());
        assertEquals("public", e1.iterators().propertyIterator("acl").next().value());
    }

    //    @Test
    public void shouldHandleHiddenVertexProperties() {

        final Vertex v = this.sqlgGraph.addVertex(Graph.Key.hide("age"), 29, "age", 16, "name", "marko", "food", "taco", Graph.Key.hide("color"), "purple");
        this.sqlgGraph.tx().commit();

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
        return this.sqlgGraph.V().match("a",
                this.sqlgGraph.of().as("a").out("created").as("b"),
                this.sqlgGraph.of().as("b").in("created").as("c"))
                .where("a", Compare.neq, "c")
                .select(Arrays.asList("a", "c"), v -> ((Vertex) v).value("name"));
    }

    public Traversal<Vertex, Map<String, String>> b() {
        return this.sqlgGraph.V().match("a",
                this.sqlgGraph.of().as("a").out("created").as("b"))
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
        traversal.forEachRemaining(o -> {
            result.add(new Bindings<>(o));
        });
        return result;
    }

    //    @Test
    @LoadGraphWith(MODERN)
    public void g_v4_out_asXhereX_hasXlang_javaX_backXhereX() throws IOException {
        Graph g = this.sqlgGraph;
        final GraphReader reader = KryoReader.build().workingDirectory(File.separator + "tmp").create();
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
        return this.sqlgGraph.v(v4Id).out().as("here").has("lang", "java").back("here");
    }

    /**
     * Looks up the identifier as generated by the current source graph being tested.
     *
     * @param vertexName a unique string that will identify a graph element within a graph
     * @return the id as generated by the graph
     */
    public Object convertToVertexId(final String vertexName) {
        return convertToVertexId(this.sqlgGraph, vertexName);
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
        return convertToEdgeId(this.sqlgGraph, outVertexName, edgeLabel, inVertexName);
    }

    //josh created lop
    public Object convertToEdgeId(final Graph g, final String outVertexName, String edgeLabel, final String inVertexName) {
        return ((Edge) g.V().has("name", outVertexName).outE(edgeLabel).as("e").inV().has("name", inVertexName).back("e").next()).id();
    }

    public void printTraversalForm(final Traversal traversal) {
        final boolean muted = Boolean.parseBoolean(System.getProperty("muteTestLogs", "false"));

        if (!muted) System.out.println("Testing: " + traversal);
        traversal.applyStrategies(TraversalEngine.STANDARD); // TODO!!!!
        if (!muted) System.out.println("         " + traversal);
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
