package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyChangeAction;
import org.umlg.sqlg.structure.TopologyInf;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.*;

@SuppressWarnings("DuplicatedCode")
@RunWith(Parameterized.class)
public class TestTopologyEdgeLabelRenameDistributed extends BaseTest {

    private final List<Triple<TopologyInf, TopologyInf, TopologyChangeAction>> topologyListenerTriple = new ArrayList<>();

    @Parameterized.Parameter
    public String schema1;
    @Parameterized.Parameter(1)
    public String schema2;
    @Parameterized.Parameter(2)
    public boolean rollback;

    @Parameterized.Parameters(name = "{index}: schema1:{0}, schema2:{1}, rollback:{2}")
    public static Collection<Object[]> data() {
        List<Object[]> l = new ArrayList<>();
        String[] schema1s = new String[]{"public", "A"};
        String[] schema2s = new String[]{"public", "B"};
        boolean[] rollback = new boolean[]{true, false};
//        String[] schema1s = new String[]{"public"};
//        String[] schema2s = new String[]{"public"};
//        boolean[] rollback = new boolean[]{true};
        for (String s1 : schema1s) {
            for (String s2 : schema2s) {
                for (boolean r : rollback) {
                    l.add(new Object[]{s1, s2, r});
                }
            }
        }
        return l;
    }
    
    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            Assume.assumeTrue(isPostgres());
            configuration.addProperty("distributed", true);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void before() throws Exception {
        super.before();
        this.topologyListenerTriple.clear();
    }
    
    @Test
    public void testEdgeLabelSimple() throws InterruptedException {
        TestTopologyChangeListener.TopologyListenerTest topologyListenerTest = new TestTopologyChangeListener.TopologyListenerTest(topologyListenerTriple);
        this.sqlgGraph.getTopology().registerListener(topologyListenerTest);
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Schema schema1 = sqlgGraph1.getTopology().ensureSchemaExist(this.schema1);
            Schema schema2 = sqlgGraph1.getTopology().ensureSchemaExist(this.schema2);

            VertexLabel aVertexLabel = schema1.ensureVertexLabelExist("A", new HashMap<>() {{
                put("a", PropertyType.STRING);
            }});
            VertexLabel bVertexLabel = schema2.ensureVertexLabelExist("B", new HashMap<>() {{
                put("a", PropertyType.STRING);
            }});
            EdgeLabel abEdgeLabel = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new HashMap<>() {{
                put("a", PropertyType.STRING);
            }});
            sqlgGraph1.tx().commit();

            Optional<EdgeLabel> edgeLabelOptional = schema1.getEdgeLabel("AB");
            Assert.assertTrue(edgeLabelOptional.isEmpty());
            edgeLabelOptional = schema1.getEdgeLabel("ab");
            Assert.assertTrue(edgeLabelOptional.isPresent());
            EdgeLabel edgeLabel = edgeLabelOptional.get();
            Assert.assertEquals(1, edgeLabel.getInVertexLabels().size());
            Assert.assertEquals(1, edgeLabel.getOutVertexLabels().size());
            Optional<VertexLabel> aVertexLabelOptional = schema1.getVertexLabel("A");
            Assert.assertTrue(aVertexLabelOptional.isPresent());
            Optional<VertexLabel> bVertexLabelOptional = schema2.getVertexLabel("B");
            Assert.assertTrue(bVertexLabelOptional.isPresent());
            aVertexLabel = aVertexLabelOptional.get();
            bVertexLabel = bVertexLabelOptional.get();
            Map<String, EdgeLabel> outEdgeLabels = aVertexLabel.getOutEdgeLabels();
            Assert.assertEquals(1, outEdgeLabels.size());
            Assert.assertTrue(outEdgeLabels.containsKey(schema1.getName() + ".ab"));
            EdgeLabel outEdgeLabel = outEdgeLabels.get(schema1.getName() + ".ab");
            Assert.assertSame(outEdgeLabel, edgeLabelOptional.get());

            Map<String, EdgeLabel> inEdgeLabels = bVertexLabel.getInEdgeLabels();
            Assert.assertTrue(inEdgeLabels.containsKey(schema1.getName() + ".ab"));
            EdgeLabel inEdgeLabel = inEdgeLabels.get(schema1.getName() + ".ab");
            Assert.assertSame(inEdgeLabel, edgeLabelOptional.get());

            List<Vertex> outEdges = sqlgGraph1.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .has(Topology.SQLG_SCHEMA_SCHEMA_NAME, this.schema1)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(1, outEdges.size());
            Assert.assertEquals("ab", outEdges.get(0).<String>property(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME).value());
            List<Vertex> inVertices = sqlgGraph1.topology().V(outEdges.get(0))
                    .in(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                    .toList();
            Assert.assertEquals(1, inVertices.size());

            Vertex a = sqlgGraph1.addVertex(T.label, this.schema1 + ".A", "a", "haloA");
            Vertex b = sqlgGraph1.addVertex(T.label, this.schema2 + ".B", "a", "haloB");
            a.addEdge("ab", b, "a", "halo_ab");
            sqlgGraph1.tx().commit();

            List<Vertex> vertices = sqlgGraph1.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
            Assert.assertEquals(1, vertices.size());

            abEdgeLabel.rename("AB");

            vertices = sqlgGraph1.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
            Assert.assertEquals(0, vertices.size());
            vertices = sqlgGraph1.traversal().V().hasLabel(this.schema1 + ".A").out("AB").toList();
            Assert.assertEquals(1, vertices.size());
            if (sqlgGraph1.getSqlDialect().supportsTransactionalSchema() && this.rollback) {
                sqlgGraph1.tx().rollback();
                Thread.sleep(1000);
                schema1 = this.sqlgGraph.getTopology().getSchema(this.schema1).orElseThrow();
                schema2 = this.sqlgGraph.getTopology().getSchema(this.schema2).orElseThrow();
                vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
                Assert.assertEquals(1, vertices.size());
                vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("AB").toList();
                Assert.assertEquals(0, vertices.size());

                edgeLabelOptional = schema1.getEdgeLabel("AB");
                Assert.assertTrue(edgeLabelOptional.isEmpty());
                edgeLabelOptional = schema1.getEdgeLabel("ab");
                Assert.assertTrue(edgeLabelOptional.isPresent());
                edgeLabel = edgeLabelOptional.get();

                aVertexLabelOptional = schema1.getVertexLabel("A");
                Assert.assertTrue(aVertexLabelOptional.isPresent());
                aVertexLabel = aVertexLabelOptional.get();
                bVertexLabelOptional = schema2.getVertexLabel("B");
                Assert.assertTrue(bVertexLabelOptional.isPresent());
                bVertexLabel = bVertexLabelOptional.get();

                Assert.assertEquals(1, edgeLabel.getOutVertexLabels().size());
                Assert.assertEquals(1, edgeLabel.getInVertexLabels().size());
                Assert.assertSame(edgeLabel.getOutVertexLabels().iterator().next(), aVertexLabel);
                Assert.assertSame(edgeLabel.getInVertexLabels().iterator().next(), bVertexLabel);

                Assert.assertEquals(1, aVertexLabel.getOutEdgeLabels().size());
                Assert.assertEquals(0, aVertexLabel.getInEdgeLabels().size());
                Assert.assertEquals(0, bVertexLabel.getOutEdgeLabels().size());
                Assert.assertEquals(1, bVertexLabel.getInEdgeLabels().size());

                Assert.assertTrue(aVertexLabel.getOutEdgeLabel("AB").isEmpty());
                Assert.assertTrue(aVertexLabel.getOutEdgeLabel("ab").isPresent());
                Assert.assertSame(edgeLabel, aVertexLabel.getOutEdgeLabel("ab").get());
                Assert.assertTrue(bVertexLabel.getInEdgeLabels().containsKey(schema1.getName() + ".ab"));
                Assert.assertSame(edgeLabel, bVertexLabel.getInEdgeLabels().get(schema1.getName() + ".ab"));

                outEdges = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                        .has(Topology.SQLG_SCHEMA_SCHEMA_NAME, this.schema1)
                        .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                        .toList();
                Assert.assertEquals(1, outEdges.size());
                Assert.assertEquals("ab", outEdges.get(0).<String>property(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME).value());
                inVertices = this.sqlgGraph.topology().V(outEdges.get(0))
                        .in(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                        .toList();
                Assert.assertEquals(1, inVertices.size());
            } else {
                sqlgGraph1.tx().commit();
                Thread.sleep(1000);
                schema1 = this.sqlgGraph.getTopology().getSchema(this.schema1).orElseThrow();
                schema2 = this.sqlgGraph.getTopology().getSchema(this.schema2).orElseThrow();
                vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("ab").toList();
                Assert.assertEquals(0, vertices.size());
                vertices = this.sqlgGraph.traversal().V().hasLabel(this.schema1 + ".A").out("AB").toList();
                Assert.assertEquals(1, vertices.size());

                edgeLabelOptional = schema1.getEdgeLabel("ab");
                Assert.assertTrue(edgeLabelOptional.isEmpty());
                edgeLabelOptional = schema1.getEdgeLabel("AB");
                Assert.assertTrue(edgeLabelOptional.isPresent());
                edgeLabel = edgeLabelOptional.get();

                topologyListenerTest.receivedEvent(edgeLabel, TopologyChangeAction.UPDATE);

                aVertexLabelOptional = schema1.getVertexLabel("A");
                Assert.assertTrue(aVertexLabelOptional.isPresent());
                aVertexLabel = aVertexLabelOptional.get();
                bVertexLabelOptional = schema2.getVertexLabel("B");
                Assert.assertTrue(bVertexLabelOptional.isPresent());
                bVertexLabel = bVertexLabelOptional.get();

                Assert.assertEquals(1, edgeLabel.getOutVertexLabels().size());
                Assert.assertEquals(1, edgeLabel.getInVertexLabels().size());
                Assert.assertSame(edgeLabel.getOutVertexLabels().iterator().next(), aVertexLabel);
                Assert.assertSame(edgeLabel.getInVertexLabels().iterator().next(), bVertexLabel);

                Assert.assertEquals(1, aVertexLabel.getOutEdgeLabels().size());
                Assert.assertEquals(0, aVertexLabel.getInEdgeLabels().size());
                Assert.assertEquals(0, bVertexLabel.getOutEdgeLabels().size());
                Assert.assertEquals(1, bVertexLabel.getInEdgeLabels().size());

                Assert.assertTrue(aVertexLabel.getOutEdgeLabel("AB").isPresent());
                Assert.assertSame(edgeLabel, aVertexLabel.getOutEdgeLabel("AB").get());
                Assert.assertTrue(bVertexLabel.getInEdgeLabels().containsKey(schema1.getName() + ".AB"));
                Assert.assertSame(edgeLabel, bVertexLabel.getInEdgeLabels().get(schema1.getName() + ".AB"));

                outEdges = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                        .has(Topology.SQLG_SCHEMA_SCHEMA_NAME, this.schema1)
                        .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                        .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                        .toList();
                Assert.assertEquals(1, outEdges.size());
                Assert.assertEquals("AB", outEdges.get(0).<String>property(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME).value());
                inVertices = this.sqlgGraph.topology().V(outEdges.get(0))
                        .in(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
                        .toList();
                Assert.assertEquals(1, inVertices.size());
            }

        }

    }

}
