package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.strategy.SqlgVertexStepCompiled;
import org.umlg.sqlg.structure.SchemaManager;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SchemaTableTree;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Date: 2015/01/19
 * Time: 6:22 AM
 */
public class TestGremlinCompileWithHas extends BaseTest {

    @Test
    public void testHasLabelOut() {
        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("outB", b1);
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().both().has(T.label, "B");
        printTraversalForm(traversal);
        List<Vertex> softwares = traversal.toList();
        Assert.assertEquals(1, softwares.size());
        for (Vertex software : softwares) {
            if (!software.label().equals("B")) {
                Assert.fail("expected label B found " + software.label());
            }
        }
    }

    @Test
    public void testSingleCompileWithHasLabelOut() {
        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        SqlgVertex b2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        SqlgVertex b3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        SqlgVertex c1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        SqlgVertex c2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        SqlgVertex c3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        a1.addEdge("outB", b1);
        a1.addEdge("outB", b2);
        a1.addEdge("outB", b3);
        a1.addEdge("outC", c1);
        a1.addEdge("outC", c2);
        a1.addEdge("outC", c3);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Vertex> traversal = vertexTraversal(a1).out().has(T.label, "B");

        traversal.asAdmin().applyStrategies();
        final List<Step> temp = new ArrayList<>();
        Step currentStep = traversal.asAdmin().getStartStep();
        while (!(currentStep instanceof EmptyStep)) {
            temp.add(currentStep);
            currentStep = currentStep.getNextStep();
        }
        Assert.assertTrue(temp.get(0) instanceof StartStep);
        Assert.assertTrue(temp.get(1) instanceof SqlgVertexStepCompiled);
        SqlgVertexStepCompiled sqlgVertexStepCompiler = (SqlgVertexStepCompiled) temp.get(1);
        Assert.assertEquals(2, temp.size());

        SchemaTable schemaTable = SchemaTable.of(a1.getSchema(), SchemaManager.VERTEX_PREFIX + a1.getTable());
        SchemaTableTree schemaTableTree = this.sqlgGraph.getGremlinParser().parse(schemaTable, sqlgVertexStepCompiler.getReplacedSteps());

        Assert.assertEquals(2, schemaTableTree.depth());
        Assert.assertEquals(3, schemaTableTree.numberOfNodes());
        Assert.assertEquals(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_A"), schemaTableTree.schemaTableAtDepth(0, 0).getSchemaTable());
        Assert.assertEquals(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_B"), schemaTableTree.schemaTableAtDepth(1, 1).getSchemaTable());
        Assert.assertTrue(schemaTableTree.schemaTableAtDepth(1, 1).getHasContainers().isEmpty());

        Assert.assertEquals(this.sqlgGraph.getSqlDialect().getPublicSchema(), schemaTableTree.getSchemaTable().getSchema());
        Assert.assertEquals("V_A", schemaTableTree.getSchemaTable().getTable());
        Assert.assertEquals(3, vertexTraversal(a1).out().has(T.label, "B").count().next().intValue());
    }

    @Test
    public void testSingleCompileWithHasLabelIn() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("outB", b1);
        b1.addEdge("outC", c1);
        a2.addEdge("outB", b2);
        b2.addEdge("outC", c1);
        a3.addEdge("outB", b3);
        b3.addEdge("outC", c1);
        d1.addEdge("outB", b4);
        b4.addEdge("outC", c1);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(4, vertexTraversal(c1).in().in().count().next().intValue());
        Assert.assertEquals(3, vertexTraversal(c1).in().in().has(T.label, "A").count().next().intValue());
    }

    @Test
    public void testHasOnProperty() {
        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        SqlgVertex b2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        SqlgVertex b3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("outB", b1);
        a1.addEdge("outB", b2);
        a1.addEdge("outB", b3);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Vertex> gt = vertexTraversal(a1).out().has("name", P.eq("b2"));
        gt.asAdmin().applyStrategies();
        final List<Step> temp = new ArrayList<>();
        Step currentStep = gt.asAdmin().getStartStep();
        while (!(currentStep instanceof EmptyStep)) {
            temp.add(currentStep);
            currentStep = currentStep.getNextStep();
        }
        Assert.assertTrue(temp.get(0) instanceof StartStep);
        Assert.assertTrue(temp.get(1) instanceof SqlgVertexStepCompiled);
        SqlgVertexStepCompiled sqlgVertexStepCompiler = (SqlgVertexStepCompiled) temp.get(1);
        Assert.assertEquals(2, temp.size());

        SchemaTable schemaTable = SchemaTable.of(a1.getSchema(), SchemaManager.VERTEX_PREFIX + a1.getTable());
        SchemaTableTree schemaTableTree = this.sqlgGraph.getGremlinParser().parse(schemaTable, sqlgVertexStepCompiler.getReplacedSteps());

        Assert.assertEquals(2, schemaTableTree.depth());
        Assert.assertEquals(3, schemaTableTree.numberOfNodes());
        Assert.assertEquals(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_A"), schemaTableTree.schemaTableAtDepth(0, 0).getSchemaTable());
        Assert.assertEquals(SchemaTable.of(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_B"), schemaTableTree.schemaTableAtDepth(1, 1).getSchemaTable());
        Assert.assertEquals(1, schemaTableTree.schemaTableAtDepth(1, 1).getHasContainers().size());

        Assert.assertEquals(1, vertexTraversal(a1).out().has("name", P.eq("b2")).count().next().intValue());
    }

    @Test
    public void testOutHasOutHas() {
        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        SqlgVertex b2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        SqlgVertex b3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("outB", b1);
        a1.addEdge("outB", b2);
        a1.addEdge("outB", b3);
        SqlgVertex c1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        SqlgVertex c2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        SqlgVertex c3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        SqlgVertex c4 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
        SqlgVertex c5 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c5");
        SqlgVertex c6 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c6");
        SqlgVertex c7 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c7");
        SqlgVertex c8 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c8");
        SqlgVertex c9 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c9");
        b1.addEdge("outC", c1);
        b1.addEdge("outC", c2);
        b1.addEdge("outC", c3);
        b2.addEdge("outC", c4);
        b2.addEdge("outC", c5);
        b2.addEdge("outC", c6);
        b3.addEdge("outC", c7);
        b3.addEdge("outC", c8);
        b3.addEdge("outC", c9);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, vertexTraversal(a1).out().has("name", "b1").out().has("name", "c1").count().next().intValue());
        Assert.assertEquals(c1, vertexTraversal(a1).out().has("name", "b1").out().has("name", "c1").next());
        Assert.assertEquals(1, vertexTraversal(a1).out().has("name", "b2").out().has("name", "c5").count().next().intValue());
        Assert.assertEquals(c5, vertexTraversal(a1).out().has("name", "b2").out().has("name", "c5").next());
    }

    @Test
    public void testOutHasOutHasNotParsed() {
        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        SqlgVertex b2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        SqlgVertex b3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("outB", b1);
        a1.addEdge("outB", b2);
        a1.addEdge("outB", b3);
        SqlgVertex c1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        SqlgVertex c2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        SqlgVertex c3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        SqlgVertex c4 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
        SqlgVertex c5 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c5");
        SqlgVertex c6 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c6");
        SqlgVertex c7 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c7");
        SqlgVertex c8 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c8");
        SqlgVertex c9 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "C", "name", "c9");
        b1.addEdge("outC", c1);
        b1.addEdge("outC", c2);
        b1.addEdge("outC", c3);
        b2.addEdge("outC", c4);
        b2.addEdge("outC", c5);
        b2.addEdge("outC", c6);
        b3.addEdge("outC", c7);
        b3.addEdge("outC", c8);
        b3.addEdge("outC", c9);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, vertexTraversal(a1).out().has("name", "b1").out().has("name", "c1").count().next().intValue());
        Assert.assertEquals(c1, vertexTraversal(a1).out().has("name", "b1").out().has("name", "c1").next());
        Assert.assertEquals(1, vertexTraversal(a1).out().has("name", "b2").out().has("name", "c5").count().next().intValue());
        Assert.assertEquals(2, vertexTraversal(a1).out().has("name", "b2").has("name", "b2").out().has("name", P.within(Arrays.asList("c5", "c6"))).count().next().intValue());
        Assert.assertEquals(1, vertexTraversal(a1).out().has("name", "b2").has("name", "b2").out().has("name", P.eq("c5")).count().next().intValue());
    }

    @Test
    public void testInOut() {
        Vertex v1 = sqlgGraph.addVertex();
        Vertex v2 = sqlgGraph.addVertex();
        Vertex v3 = sqlgGraph.addVertex();
        Vertex v4 = sqlgGraph.addVertex();
        Vertex v5 = sqlgGraph.addVertex();
        Edge e1 = v1.addEdge("label1", v2);
        Edge e2 = v2.addEdge("label2", v3);
        Edge e3 = v3.addEdge("label3", v4);
        sqlgGraph.tx().commit();

        Assert.assertEquals(1, vertexTraversal(v2).inE().count().next(), 1);
        Assert.assertEquals(e1, vertexTraversal(v2).inE().next());
        Assert.assertEquals(1L, edgeTraversal(e1).inV().count().next(), 0);
        Assert.assertEquals(v2, edgeTraversal(e1).inV().next());
        Assert.assertEquals(0L, edgeTraversal(e1).outV().inE().count().next(), 0);
        Assert.assertEquals(1L, edgeTraversal(e2).inV().count().next(), 0);
        Assert.assertEquals(v3, edgeTraversal(e2).inV().next());
    }

    @Test
    public void testVertexOutWithHas() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex bmw1 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw", "cc", 600);
        Vertex bmw2 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw", "cc", 800);
        Vertex ktm1 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm", "cc", 200);
        Vertex ktm2 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm", "cc", 200);
        Vertex ktm3 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm", "cc", 400);
        marko.addEdge("drives", bmw1);
        marko.addEdge("drives", bmw2);
        marko.addEdge("drives", ktm1);
        marko.addEdge("drives", ktm2);
        marko.addEdge("drives", ktm3);
        this.sqlgGraph.tx().commit();
        List<Vertex> drivesBmw = vertexTraversal(marko).out("drives").<Vertex>has("name", "bmw").toList();
        Assert.assertEquals(2L, drivesBmw.size(), 0);
        List<Vertex> drivesKtm = vertexTraversal(marko).out("drives").<Vertex>has("name", "ktm").toList();
        Assert.assertEquals(3L, drivesKtm.size(), 0);

        List<Vertex> cc600 = vertexTraversal(marko).out("drives").<Vertex>has("cc", 600).toList();
        Assert.assertEquals(1L, cc600.size(), 0);
        List<Vertex> cc800 = vertexTraversal(marko).out("drives").<Vertex>has("cc", 800).toList();
        Assert.assertEquals(1L, cc800.size(), 0);
        List<Vertex> cc200 = vertexTraversal(marko).out("drives").<Vertex>has("cc", 200).toList();
        Assert.assertEquals(2L, cc200.size(), 0);
        List<Vertex> cc400 = vertexTraversal(marko).out("drives").<Vertex>has("cc", 400).toList();
        Assert.assertEquals(1L, cc400.size(), 0);
    }

    @Test
    public void testHasWithStringIds() throws IOException {
        Graph g = this.sqlgGraph;
        final GraphReader reader = GryoReader.build()
                .mapper(g.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
            reader.readGraph(stream, g);
        }
        assertModernGraph(g, true, false);
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_hasIdX2X(g.traversal(), convertToVertexId("marko").toString(), convertToVertexId("vadas").toString());
        assert_g_VX1X_out_hasXid_2X(traversal);
    }

    @Test
    public void testHas() throws IOException {
        Graph g = this.sqlgGraph;
        final GraphReader reader = GryoReader.build()
                .mapper(g.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
            reader.readGraph(stream, g);
        }
        assertModernGraph(g, true, false);
        final Object id2 = convertToVertexId("vadas");
        final Object id3 = convertToVertexId("lop");
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_hasIdX2_3X(g.traversal(), convertToVertexId("marko"), id2.toString(), id3.toString());
        assert_g_VX1X_out_hasXid_2_3X(id2, id3, traversal);
    }

    @Test
    public void g_VX1X_out_hasXid_2AsString_3AsStringX() throws IOException {
        Graph g = this.sqlgGraph;
        final GraphReader reader = GryoReader.build()
                .mapper(g.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
            reader.readGraph(stream, g);
        }
        assertModernGraph(g, true, false);
        final Object id2 = convertToVertexId("vadas");
        final Object id3 = convertToVertexId("lop");
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_hasIdX2_3X(g.traversal(), convertToVertexId("marko"), id2.toString(), id3.toString());
        assert_g_VX1X_out_hasXid_2_3X(id2, id3, traversal);
    }

    @Test
    public void g_VX1X_outE_hasXweight_inside_0_06X_inV() throws IOException {
        Graph g = this.sqlgGraph;
        final GraphReader reader = GryoReader.build()
                .mapper(g.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
            reader.readGraph(stream, g);
        }
        assertModernGraph(g, true, false);
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outE_hasXweight_inside_0_06X_inV(g.traversal(), convertToVertexId("marko"));
        printTraversalForm(traversal);
        while (traversal.hasNext()) {
            Vertex vertex = traversal.next();
            Assert.assertTrue(vertex.value("name").equals("vadas") || vertex.value("name").equals("lop"));
        }
        Assert.assertFalse(traversal.hasNext());
    }

    public Traversal<Vertex, Vertex> get_g_VX1X_outE_hasXweight_inside_0_06X_inV(GraphTraversalSource g, final Object v1Id) {
        return g.V(v1Id).outE().has("weight", P.inside(0.0d, 0.6d)).inV();
    }

    public Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2_3X(GraphTraversalSource g, final Object v1Id, final Object v2Id, final Object v3Id) {
        return g.V(v1Id).out().hasId(v2Id, v3Id);
    }

    private void assert_g_VX1X_out_hasXid_2X(final Traversal<Vertex, Vertex> traversal) {
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        Assert.assertEquals(convertToVertexId("vadas"), traversal.next().id());
    }

    public Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2X(GraphTraversalSource g, final Object v1Id, final Object v2Id) {
        return g.V(v1Id).out().hasId(v2Id);
    }

    protected void assert_g_VX1X_out_hasXid_2_3X(Object id2, Object id3, Traversal<Vertex, Vertex> traversal) {
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        Assert.assertThat(traversal.next().id(), CoreMatchers.anyOf(CoreMatchers.is(id2), CoreMatchers.is(id3)));
        Assert.assertThat(traversal.next().id(), CoreMatchers.anyOf(CoreMatchers.is(id2), CoreMatchers.is(id3)));
        Assert.assertFalse(traversal.hasNext());
    }
}
