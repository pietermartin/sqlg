package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2015/01/19
 * Time: 6:22 AM
 */
public class TestGremlinCompileWithHas extends BaseTest {

    @Test
    public void testHasId() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        a1.addEdge("ab", b4);
        this.sqlgGraph.tx().commit();

        RecordId recordIda1 = RecordId.from(SchemaTable.of("public", "A"), 1l);
        RecordId recordIda2 = RecordId.from(SchemaTable.of("public", "A"), 2l);
        RecordId recordIda3 = RecordId.from(SchemaTable.of("public", "A"), 3l);
        RecordId recordIda4 = RecordId.from(SchemaTable.of("public", "A"), 4l);
        RecordId recordIdb1 = RecordId.from(SchemaTable.of("public", "B"), 1l);
        RecordId recordIdb2 = RecordId.from(SchemaTable.of("public", "B"), 2l);
        RecordId recordIdb3 = RecordId.from(SchemaTable.of("public", "B"), 3l);
        RecordId recordIdb4 = RecordId.from(SchemaTable.of("public", "B"), 4l);
        RecordId recordIdc1 = RecordId.from(SchemaTable.of("public", "B"), 1l);

//        List<Vertex> vertices = this.sqlgGraph.traversal().V(recordIda1).hasLabel("A").toList();
//        Assert.assertEquals(1, vertices.size());
//
//        vertices = this.sqlgGraph.traversal().V(recordIda1).has(T.id, P.within(recordIda2, recordIdb1)).toList();
//        Assert.assertEquals(3, vertices.size());
//
//        vertices = this.sqlgGraph.traversal().V().has(T.id, P.within(recordIda1, recordIda2, recordIdb1)).toList();
//        Assert.assertEquals(3, vertices.size());
//        vertices = this.sqlgGraph.traversal().V(recordIda1, recordIda2, recordIda3, recordIdb1).toList();
//        Assert.assertEquals(4, vertices.size());
//        vertices = this.sqlgGraph.traversal().V().has(T.id, P.within(recordIda1)).toList();
//        Assert.assertEquals(1, vertices.size());

//        List<Vertex> vertices = this.sqlgGraph.traversal().V(recordIda1).out().hasId(recordIdb1).toList();
//        Assert.assertEquals(1, vertices.size());
        List<Vertex> vertices = this.sqlgGraph.traversal().V(recordIda1, recordIda2, recordIda3).out().hasId(recordIdb1, recordIdb2, recordIdb3).toList();
        Assert.assertEquals(3, vertices.size());

    }

//    @Test
//    public void g_V_asXaX_both_asXbX_dedupXa_bX_byXlabelX_selectXa_bX() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        Traversal<Vertex, Map<String, Vertex>> t = g.traversal().V().as("a").both().as("b").dedup("a", "b").by(T.label).select("a", "b");
//        printTraversalForm(t);
//        List<Map<String,Vertex>> result = t.toList();
//        System.out.println(result);
//    }
//
//    @Test
//    public void g_V_asXaX_out_asXbX_selectXa_bX_byXnameX() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        GraphTraversal traversal = g.traversal().V().as("a").out().aggregate("x").as("b").<String>select("a", "b").by("name");
//        printTraversalForm(traversal);
//        final List<Map<String, String>> expected = makeMapList(2,
//                "a", "marko", "b", "lop",
//                "a", "marko", "b", "vadas",
//                "a", "marko", "b", "josh",
//                "a", "josh", "b", "ripple",
//                "a", "josh", "b", "lop",
//                "a", "peter", "b", "lop");
//        checkResults(expected, traversal);
//    }
//
//    @Test
//    public void g_VX1AsStringX_out_hasXid_2AsStringX() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        final Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V(convertToVertexId("marko")).out().hasId(convertToVertexId("vadas"));
//        printTraversalForm(traversal);
//        Assert.assertThat(traversal.hasNext(), CoreMatchers.is(true));
//        Assert.assertEquals(convertToVertexId("vadas"), traversal.next().id());
//        Assert.assertThat(traversal.hasNext(), CoreMatchers.is(false));
//    }
//
//    @Test
//    public void g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX_name() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        Object vertexId = convertToVertexId("josh");
//        final Traversal<Vertex, String> traversal = g.traversal().V(vertexId)
//                .out().as("here")
//                .has("lang", "java")
//                .select("here").values("name");
//        printTraversalForm(traversal);
//        int counter = 0;
//        final Set<String> names = new HashSet<>();
//        while (traversal.hasNext()) {
//            counter++;
//            names.add(traversal.next());
//        }
//        Assert.assertEquals(2, counter);
//        Assert.assertEquals(2, names.size());
//        Assert.assertTrue(names.contains("ripple"));
//        Assert.assertTrue(names.contains("lop"));
//    }
//
//    @Test
//    public void g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_bothXknowsX_bothXknowsX_asXdX_whereXc__notXeqXaX_orXeqXdXXXX_selectXa_b_c_dX() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        Traversal<Vertex, Map<String, Object>> traversal = this.sqlgGraph.traversal()
//                .V().as("a")
//                .out("created").as("b")
//                .in("created").as("c")
//                .both("knows")
//                .both("knows").as("d")
//                .where("c", P.not(P.eq("a").or(P.eq("d")))).select("a", "b", "c", "d");
//        printTraversalForm(traversal);
//        checkResults(makeMapList(4,
//                "a", convertToVertex(this.sqlgGraph, "marko"), "b", convertToVertex(this.sqlgGraph, "lop"), "c", convertToVertex(this.sqlgGraph, "josh"), "d", convertToVertex(this.sqlgGraph, "vadas"),
//                "a", convertToVertex(this.sqlgGraph, "peter"), "b", convertToVertex(this.sqlgGraph, "lop"), "c", convertToVertex(this.sqlgGraph, "josh"), "d", convertToVertex(this.sqlgGraph, "vadas")), traversal);
//    }
//
//    public static <T> void checkResults(final List<T> expectedResults, final Traversal<?, T> traversal) {
//        final List<T> results = traversal.toList();
//        Assert.assertFalse(traversal.hasNext());
//        if (expectedResults.size() != results.size()) {
//            System.err.println("Expected results: " + expectedResults);
//            System.err.println("Actual results:   " + results);
//            Assert.assertEquals("Checking result size", expectedResults.size(), results.size());
//        }
//
//        for (T t : results) {
//            if (t instanceof Map) {
//                Assert.assertTrue("Checking map result existence: " + t, expectedResults.stream().filter(e -> e instanceof Map).filter(e -> checkMap((Map) e, (Map) t)).findAny().isPresent());
//            } else {
//                Assert.assertTrue("Checking result existence: " + t, expectedResults.contains(t));
//            }
//        }
//        final Map<T, Long> expectedResultsCount = new HashMap<>();
//        final Map<T, Long> resultsCount = new HashMap<>();
//        Assert.assertEquals("Checking indexing is equivalent", expectedResultsCount.size(), resultsCount.size());
//        expectedResults.forEach(t -> MapHelper.incr(expectedResultsCount, t, 1l));
//        results.forEach(t -> MapHelper.incr(resultsCount, t, 1l));
//        expectedResultsCount.forEach((k, v) -> Assert.assertEquals("Checking result group counts", v, resultsCount.get(k)));
//        Assert.assertFalse(traversal.hasNext());
//    }
//
//    public static <A, B> boolean checkMap(final Map<A, B> expectedMap, final Map<A, B> actualMap) {
//        final List<Map.Entry<A, B>> actualList = actualMap.entrySet().stream().sorted((a, b) -> a.getKey().toString().compareTo(b.getKey().toString())).collect(Collectors.toList());
//        final List<Map.Entry<A, B>> expectedList = expectedMap.entrySet().stream().sorted((a, b) -> a.getKey().toString().compareTo(b.getKey().toString())).collect(Collectors.toList());
//
//        if (expectedList.size() > actualList.size()) {
//            return false;
//        } else if (actualList.size() > expectedList.size()) {
//            return false;
//        }
//
//        for (int i = 0; i < actualList.size(); i++) {
//            if (!actualList.get(i).getKey().equals(expectedList.get(i).getKey())) {
//                return false;
//            }
//            if (!actualList.get(i).getValue().equals(expectedList.get(i).getValue())) {
//                return false;
//            }
//        }
//        return true;
//    }
//
//    public <A, B> List<Map<A, B>> makeMapList(final int size, final Object... keyValues) {
//        final List<Map<A, B>> mapList = new ArrayList<>();
//        for (int i = 0; i < keyValues.length; i = i + (2 * size)) {
//            final Map<A, B> map = new HashMap<>();
//            for (int j = 0; j < (2 * size); j = j + 2) {
//                map.put((A) keyValues[i + j], (B) keyValues[i + j + 1]);
//            }
//            mapList.add(map);
//        }
//        return mapList;
//    }
//
//    @Test
//    public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_2X() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        final Traversal<Vertex, String> traversal = g.traversal()
//                .V().as("a")
//                .out().as("a")
//                .out().as("a")
//                .<List<String>>select("a")
//                .by(__.unfold().values("name").fold())
//                .range(Scope.local, 1, 2);
//        int counter = 0;
//        while (traversal.hasNext()) {
//            final String s = traversal.next();
//            Assert.assertEquals("josh", s);
//            counter++;
//        }
//        Assert.assertEquals(2, counter);
//    }
//
//    @Test
//    public void g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_rangeXlocal_1_2X_Simple() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        final Traversal<Vertex, List<Vertex>> traversal = g.traversal()
//                .V().as("a")
//                .out().as("a")
//                .out().as("a")
//                .select("a");
//        printTraversalForm(traversal);
//        int counter = 0;
//        while (traversal.hasNext()) {
//            final List<Vertex> s = traversal.next();
//            Assert.assertEquals(3, s.size());
//            System.out.println(s);
//            counter++;
//        }
//        Assert.assertEquals(2, counter);
//    }
//
//    @Test
//    public void g_V_hasXinXcreatedX_count_isXgte_2XX_valuesXnameX() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        final Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
//                .V()
//                .where(
//                        __.in("created")
//                                .count()
//                                .is(
//                                        P.gte(2l)
//                                )
//                )
//                .values("name");
//        printTraversalForm(traversal);
//        Assert.assertTrue(traversal.hasNext());
//        Assert.assertEquals("lop", traversal.next());
//        Assert.assertFalse(traversal.hasNext());
//    }
//
//    @Test
//    public void g_VX1X_out_hasXid_2X() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        Object marko = convertToVertexId("marko");
//        Object vadas = convertToVertexId("vadas");
//        final Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V(marko).out().hasId(vadas);
//        printTraversalForm(traversal);
//        Assert.assertThat(traversal.hasNext(), CoreMatchers.is(true));
//        Assert.assertEquals(convertToVertexId("vadas"), traversal.next().id());
//        Assert.assertThat(traversal.hasNext(), CoreMatchers.is(false));
//    }
//
//    @Test
//    public void testHasLabelOut() {
//        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
//        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        a1.addEdge("outB", b1);
//        this.sqlgGraph.tx().commit();
//        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().both().has(T.label, "B");
//        printTraversalForm(traversal);
//        List<Vertex> softwares = traversal.toList();
//        Assert.assertEquals(1, softwares.size());
//        for (Vertex software : softwares) {
//            if (!software.label().equals("B")) {
//                Assert.fail("expected label B found " + software.label());
//            }
//        }
//    }
//
////    @Test
//    public void testSingleCompileWithHasLabelOut() {
//        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
//        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        SqlgVertex b2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        SqlgVertex b3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        SqlgVertex c1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        SqlgVertex c2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        SqlgVertex c3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        a1.addEdge("outB", b1);
//        a1.addEdge("outB", b2);
//        a1.addEdge("outB", b3);
//        a1.addEdge("outC", c1);
//        a1.addEdge("outC", c2);
//        a1.addEdge("outC", c3);
//        this.sqlgGraph.tx().commit();
//
//        GraphTraversal<Vertex, Vertex> traversal = vertexTraversal(a1).out().has(T.label, "B");
//
//        traversal.asAdmin().applyStrategies();
//        final List<Step> temp = new ArrayList<>();
//        Step currentStep = traversal.asAdmin().getStartStep();
//        while (!(currentStep instanceof EmptyStep)) {
//            temp.add(currentStep);
//            currentStep = currentStep.getNextStep();
//        }
//        Assert.assertTrue(temp.get(0) instanceof StartStep);
//        Assert.assertTrue(temp.get(1) instanceof SqlgVertexStepCompiled);
//        SqlgVertexStepCompiled sqlgVertexStepCompiler = (SqlgVertexStepCompiled) temp.get(1);
//        Assert.assertEquals(2, temp.size());
//
//        SchemaTable schemaTable = SchemaTable.of(a1.getSchema(), SchemaManager.VERTEX_PREFIX + a1.getTable());
//        SchemaTableTree schemaTableTree = this.sqlgGraph.getGremlinParser().parse(schemaTable, sqlgVertexStepCompiler.getReplacedSteps());
//
//        Assert.assertEquals(2, schemaTableTree.depth());
//        Assert.assertEquals(3, schemaTableTree.numberOfNodes());
//        Assert.assertEquals(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_A"), schemaTableTree.schemaTableAtDepth(0, 0).getSchemaTable());
//        Assert.assertEquals(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_B"), schemaTableTree.schemaTableAtDepth(1, 1).getSchemaTable());
//        Assert.assertTrue(schemaTableTree.schemaTableAtDepth(1, 1).getHasContainers().isEmpty());
//
//        Assert.assertEquals(this.sqlgGraph.getSqlDialect().getPublicSchema(), schemaTableTree.getSchemaTable().getSchema());
//        Assert.assertEquals("V_A", schemaTableTree.getSchemaTable().getTable());
//        Assert.assertEquals(3, vertexTraversal(a1).out().has(T.label, "B").count().next().intValue());
//    }
//
//    @Test
//    public void testSingleCompileWithHasLabelIn() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
//        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        a1.addEdge("outB", b1);
//        b1.addEdge("outC", c1);
//        a2.addEdge("outB", b2);
//        b2.addEdge("outC", c1);
//        a3.addEdge("outB", b3);
//        b3.addEdge("outC", c1);
//        d1.addEdge("outB", b4);
//        b4.addEdge("outC", c1);
//        this.sqlgGraph.tx().commit();
//        Assert.assertEquals(4, vertexTraversal(c1).in().in().count().next().intValue());
//        Assert.assertEquals(3, vertexTraversal(c1).in().in().has(T.label, "A").count().next().intValue());
//    }
//
////    @Test
//    public void testHasOnProperty() {
//        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
//        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        SqlgVertex b2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        SqlgVertex b3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("outB", b1);
//        a1.addEdge("outB", b2);
//        a1.addEdge("outB", b3);
//        this.sqlgGraph.tx().commit();
//
//        GraphTraversal<Vertex, Vertex> gt = vertexTraversal(a1).out().has("name", P.eq("b2"));
//        gt.asAdmin().applyStrategies();
//        final List<Step> temp = new ArrayList<>();
//        Step currentStep = gt.asAdmin().getStartStep();
//        while (!(currentStep instanceof EmptyStep)) {
//            temp.add(currentStep);
//            currentStep = currentStep.getNextStep();
//        }
//        Assert.assertTrue(temp.get(0) instanceof StartStep);
//        Assert.assertTrue(temp.get(1) instanceof SqlgVertexStepCompiled);
//        SqlgVertexStepCompiled sqlgVertexStepCompiler = (SqlgVertexStepCompiled) temp.get(1);
//        Assert.assertEquals(2, temp.size());
//
//        SchemaTable schemaTable = SchemaTable.of(a1.getSchema(), SchemaManager.VERTEX_PREFIX + a1.getTable());
//        SchemaTableTree schemaTableTree = this.sqlgGraph.getGremlinParser().parse(schemaTable, sqlgVertexStepCompiler.getReplacedSteps());
//
//        Assert.assertEquals(2, schemaTableTree.depth());
//        Assert.assertEquals(3, schemaTableTree.numberOfNodes());
//        Assert.assertEquals(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_A"), schemaTableTree.schemaTableAtDepth(0, 0).getSchemaTable());
//        Assert.assertEquals(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_B"), schemaTableTree.schemaTableAtDepth(1, 1).getSchemaTable());
//        Assert.assertEquals(1, schemaTableTree.schemaTableAtDepth(1, 1).getHasContainers().size());
//
//        Assert.assertEquals(1, vertexTraversal(a1).out().has("name", P.eq("b2")).count().next().intValue());
//    }
//
//    @Test
//    public void testOutHasOutHas() {
//        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
//        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        SqlgVertex b2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        SqlgVertex b3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("outB", b1);
//        a1.addEdge("outB", b2);
//        a1.addEdge("outB", b3);
//        SqlgVertex c1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        SqlgVertex c2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        SqlgVertex c3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        SqlgVertex c4 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
//        SqlgVertex c5 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c5");
//        SqlgVertex c6 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c6");
//        SqlgVertex c7 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c7");
//        SqlgVertex c8 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c8");
//        SqlgVertex c9 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c9");
//        b1.addEdge("outC", c1);
//        b1.addEdge("outC", c2);
//        b1.addEdge("outC", c3);
//        b2.addEdge("outC", c4);
//        b2.addEdge("outC", c5);
//        b2.addEdge("outC", c6);
//        b3.addEdge("outC", c7);
//        b3.addEdge("outC", c8);
//        b3.addEdge("outC", c9);
//        this.sqlgGraph.tx().commit();
//
//        Assert.assertEquals(1, vertexTraversal(a1).out().has("name", "b1").out().has("name", "c1").count().next().intValue());
//        Assert.assertEquals(c1, vertexTraversal(a1).out().has("name", "b1").out().has("name", "c1").next());
//        Assert.assertEquals(1, vertexTraversal(a1).out().has("name", "b2").out().has("name", "c5").count().next().intValue());
//        Assert.assertEquals(c5, vertexTraversal(a1).out().has("name", "b2").out().has("name", "c5").next());
//    }
//
//    @Test
//    public void testOutHasOutHasNotParsed() {
//        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
//        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        SqlgVertex b2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        SqlgVertex b3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("outB", b1);
//        a1.addEdge("outB", b2);
//        a1.addEdge("outB", b3);
//        SqlgVertex c1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        SqlgVertex c2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        SqlgVertex c3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        SqlgVertex c4 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
//        SqlgVertex c5 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c5");
//        SqlgVertex c6 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c6");
//        SqlgVertex c7 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c7");
//        SqlgVertex c8 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c8");
//        SqlgVertex c9 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c9");
//        b1.addEdge("outC", c1);
//        b1.addEdge("outC", c2);
//        b1.addEdge("outC", c3);
//        b2.addEdge("outC", c4);
//        b2.addEdge("outC", c5);
//        b2.addEdge("outC", c6);
//        b3.addEdge("outC", c7);
//        b3.addEdge("outC", c8);
//        b3.addEdge("outC", c9);
//        this.sqlgGraph.tx().commit();
//
//        Assert.assertEquals(1, vertexTraversal(a1).out().has("name", "b1").out().has("name", "c1").count().next().intValue());
//        Assert.assertEquals(c1, vertexTraversal(a1).out().has("name", "b1").out().has("name", "c1").next());
//        Assert.assertEquals(1, vertexTraversal(a1).out().has("name", "b2").out().has("name", "c5").count().next().intValue());
//        Assert.assertEquals(2, vertexTraversal(a1).out().has("name", "b2").has("name", "b2").out().has("name", P.within(Arrays.asList("c5", "c6"))).count().next().intValue());
//        Assert.assertEquals(1, vertexTraversal(a1).out().has("name", "b2").has("name", "b2").out().has("name", P.eq("c5")).count().next().intValue());
//    }
//
//    @Test
//    public void testInOut() {
//        Vertex v1 = sqlgGraph.addVertex();
//        Vertex v2 = sqlgGraph.addVertex();
//        Vertex v3 = sqlgGraph.addVertex();
//        Vertex v4 = sqlgGraph.addVertex();
//        Vertex v5 = sqlgGraph.addVertex();
//        Edge e1 = v1.addEdge("label1", v2);
//        Edge e2 = v2.addEdge("label2", v3);
//        Edge e3 = v3.addEdge("label3", v4);
//        sqlgGraph.tx().commit();
//
//        Assert.assertEquals(1, vertexTraversal(v2).inE().count().next(), 1);
//        Assert.assertEquals(e1, vertexTraversal(v2).inE().next());
//        Assert.assertEquals(1L, edgeTraversal(e1).inV().count().next(), 0);
//        Assert.assertEquals(v2, edgeTraversal(e1).inV().next());
//        Assert.assertEquals(1L, edgeTraversal(e1).outV().count().next(), 0);
//        Assert.assertEquals(0L, edgeTraversal(e1).outV().inE().count().next(), 0);
//        Assert.assertEquals(1L, edgeTraversal(e2).inV().count().next(), 0);
//        Assert.assertEquals(v3, edgeTraversal(e2).inV().next());
//    }
//
//    @Test
//    public void testVertexOutWithHas() {
//        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
//        Vertex bmw1 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw", "cc", 600);
//        Vertex bmw2 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw", "cc", 800);
//        Vertex ktm1 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm", "cc", 200);
//        Vertex ktm2 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm", "cc", 200);
//        Vertex ktm3 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm", "cc", 400);
//        marko.addEdge("drives", bmw1);
//        marko.addEdge("drives", bmw2);
//        marko.addEdge("drives", ktm1);
//        marko.addEdge("drives", ktm2);
//        marko.addEdge("drives", ktm3);
//        this.sqlgGraph.tx().commit();
//        List<Vertex> drivesBmw = vertexTraversal(marko).out("drives").<Vertex>has("name", "bmw").toList();
//        Assert.assertEquals(2L, drivesBmw.size(), 0);
//        List<Vertex> drivesKtm = vertexTraversal(marko).out("drives").<Vertex>has("name", "ktm").toList();
//        Assert.assertEquals(3L, drivesKtm.size(), 0);
//
//        List<Vertex> cc600 = vertexTraversal(marko).out("drives").<Vertex>has("cc", 600).toList();
//        Assert.assertEquals(1L, cc600.size(), 0);
//        List<Vertex> cc800 = vertexTraversal(marko).out("drives").<Vertex>has("cc", 800).toList();
//        Assert.assertEquals(1L, cc800.size(), 0);
//        List<Vertex> cc200 = vertexTraversal(marko).out("drives").<Vertex>has("cc", 200).toList();
//        Assert.assertEquals(2L, cc200.size(), 0);
//        List<Vertex> cc400 = vertexTraversal(marko).out("drives").<Vertex>has("cc", 400).toList();
//        Assert.assertEquals(1L, cc400.size(), 0);
//    }
//
//    @Test
//    public void testg_EX11X_outV_outE_hasXid_10AsStringX() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        final Object edgeId11 = convertToEdgeId(this.sqlgGraph, "josh", "created", "lop");
//        final Object edgeId10 = convertToEdgeId(this.sqlgGraph, "josh", "created", "ripple");
//        final Traversal<Edge, Edge> traversal = g.traversal().E(edgeId11.toString()).outV().outE().has(T.id, edgeId10.toString());
//        printTraversalForm(traversal);
//        Assert.assertTrue(traversal.hasNext());
//        final Edge e = traversal.next();
//        Assert.assertEquals(edgeId10.toString(), e.id().toString());
//        Assert.assertFalse(traversal.hasNext());
//    }
//
//    @Test
//    public void g_V_out_outE_inV_inE_inV_both_name() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//
//        Object id = convertToVertexId(g, "marko");
//        Traversal<Vertex, String> traversal = g.traversal().V(id).out().outE().inV().inE().inV().both().values("name");
//        printTraversalForm(traversal);
//        int counter = 0;
//        final Map<String, Integer> counts = new HashMap<>();
//        while (traversal.hasNext()) {
//            final String key = traversal.next();
//            final int previousCount = counts.getOrDefault(key, 0);
//            counts.put(key, previousCount + 1);
//            counter++;
//        }
//        Assert.assertEquals(3, counts.size());
//        Assert.assertEquals(4, counts.get("josh").intValue());
//        Assert.assertEquals(3, counts.get("marko").intValue());
//        Assert.assertEquals(3, counts.get("peter").intValue());
//
//        Assert.assertEquals(10, counter);
//        Assert.assertFalse(traversal.hasNext());
//    }
//
//    @Test
//    public void testHasWithStringIds() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        String marko = convertToVertexId("marko").toString();
//        String vadas = convertToVertexId("vadas").toString();
//        final Traversal<Vertex, Vertex> traversal = g.traversal().V(marko).out().hasId(vadas);
//        printTraversalForm(traversal);
//        Assert.assertTrue(traversal.hasNext());
//        Assert.assertEquals(convertToVertexId("vadas"), traversal.next().id());
//    }
//
//    @Test
//    public void testHas() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        final Object id2 = convertToVertexId("vadas");
//        final Object id3 = convertToVertexId("lop");
//        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_hasIdX2_3X(g.traversal(), convertToVertexId("marko"), id2.toString(), id3.toString());
//        assert_g_VX1X_out_hasXid_2_3X(id2, id3, traversal);
//    }
//
//    @Test
//    public void g_VX1X_out_hasXid_2AsString_3AsStringX() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        final Object id2 = convertToVertexId("vadas");
//        final Object id3 = convertToVertexId("lop");
//        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_hasIdX2_3X(g.traversal(), convertToVertexId("marko"), id2.toString(), id3.toString());
//        assert_g_VX1X_out_hasXid_2_3X(id2, id3, traversal);
//    }
//
//    @Test
//    public void testX() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        final Object marko = convertToVertexId("marko");
////        final Traversal<Vertex, Vertex> traversal =  g.traversal().V(marko).outE("knows").has("weight", 1.0d).as("here").inV().has("name", "josh").select("here");
//
//        final Traversal<Vertex, Edge> traversal = g.traversal().V(marko).outE("knows").has("weight", 1.0d).as("here").inV().has("name", "josh").select("here");
////        final Traversal<Vertex, Edge> traversal = g.traversal().V(marko).outE("knows").as("here").has("weight", 1.0d).inV().has("name", "josh").<Edge>select("here");
////        final Traversal<Vertex, Edge> traversal = g.traversal().V(marko).outE("knows").as("here").has("weight", 1.0d).as("fake").inV().has("name", "josh").<Edge>select("here");
//
//        printTraversalForm(traversal);
//        Assert.assertTrue(traversal.hasNext());
//        Assert.assertTrue(traversal.hasNext());
//        final Edge edge = traversal.next();
//        Assert.assertEquals("knows", edge.label());
//        Assert.assertEquals(1.0d, edge.<Double>value("weight"), 0.00001d);
//        Assert.assertFalse(traversal.hasNext());
//        Assert.assertFalse(traversal.hasNext());
//    }
//
//    @Test
//    public void g_VX1X_outE_hasXweight_inside_0_06X_inV() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outE_hasXweight_inside_0_06X_inV(g.traversal(), convertToVertexId("marko"));
//        printTraversalForm(traversal);
//        while (traversal.hasNext()) {
//            Vertex vertex = traversal.next();
//            Assert.assertTrue(vertex.value("name").equals("vadas") || vertex.value("name").equals("lop"));
//        }
//        Assert.assertFalse(traversal.hasNext());
//    }
//
//    @Test
//    public void testY() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        assertModernGraph(g, true, false);
//        Object marko = convertToVertexId(g, "marko");
//        final Traversal<Vertex, String> traversal = g.traversal().V(marko).outE("knows").bothV().values("name");
//        printTraversalForm(traversal);
//        final List<String> names = traversal.toList();
//        Assert.assertEquals(4, names.size());
//        Assert.assertTrue(names.contains("marko"));
//        Assert.assertTrue(names.contains("josh"));
//        Assert.assertTrue(names.contains("vadas"));
//        names.remove("marko");
//        Assert.assertEquals(3, names.size());
//        names.remove("marko");
//        Assert.assertEquals(2, names.size());
//        names.remove("josh");
//        Assert.assertEquals(1, names.size());
//        names.remove("vadas");
//        Assert.assertEquals(0, names.size());
//    }
//
//    public Traversal<Vertex, Vertex> get_g_VX1X_outE_hasXweight_inside_0_06X_inV(GraphTraversalSource g, final Object v1Id) {
//        return g.V(v1Id).outE().has("weight", P.inside(0.0d, 0.6d)).inV();
//    }
//
//    public Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2_3X(GraphTraversalSource g, final Object v1Id, final Object v2Id, final Object v3Id) {
//        return g.V(v1Id).out().hasId(v2Id, v3Id);
//    }
//
//
//    protected void assert_g_VX1X_out_hasXid_2_3X(Object id2, Object id3, Traversal<Vertex, Vertex> traversal) {
//        printTraversalForm(traversal);
//        Assert.assertTrue(traversal.hasNext());
//        Assert.assertThat(traversal.next().id(), CoreMatchers.anyOf(CoreMatchers.is(id2), CoreMatchers.is(id3)));
//        Assert.assertThat(traversal.next().id(), CoreMatchers.anyOf(CoreMatchers.is(id2), CoreMatchers.is(id3)));
//        Assert.assertFalse(traversal.hasNext());
//    }
//
//    public Object convertToEdgeId(final Graph graph, final String outVertexName, String edgeLabel, final String inVertexName) {
//        return graph.traversal().V().has("name", outVertexName).outE(edgeLabel).as("e").inV().has("name", inVertexName).<Edge>select("e").next().id();
//    }
}
