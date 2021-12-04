package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

@RunWith(Parameterized.class)
public class TestTopologyVertexLabelRenameDistributed extends BaseTest {

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
//        String[] schema1s = new String[]{"A"};
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

    @Test
    public void renameBeforeCommit() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel aVertexLabel = sqlgGraph1.getTopology().ensureSchemaExist(this.schema1).ensureVertexLabelExist("A", new HashMap<>() {{
                put("a", PropertyType.STRING);
            }});
            VertexLabel bVertexLabel = sqlgGraph1.getTopology().ensureSchemaExist(this.schema2).ensureVertexLabelExist("B", new HashMap<>() {{
                put("a", PropertyType.STRING);
            }});
            aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new HashMap<>() {{
                put("a", PropertyType.STRING);
            }});
            Vertex a = sqlgGraph1.addVertex(T.label, this.schema1 + ".A", "a", "haloA");
            Vertex b = sqlgGraph1.addVertex(T.label, this.schema2 + ".B", "a", "haloB");
            a.addEdge("ab", b, "a", "haloAB");
            sqlgGraph1.tx().commit();

            aVertexLabel.rename("AA");
            bVertexLabel.rename("BB");

            Assert.assertTrue(sqlgGraph1.getTopology().getSchema(this.schema1).orElseThrow().getVertexLabel("A").isEmpty());
            Assert.assertTrue(sqlgGraph1.getTopology().getSchema(this.schema1).orElseThrow().getVertexLabel("AA").isPresent());
            Assert.assertTrue(sqlgGraph1.getTopology().getSchema(this.schema2).orElseThrow().getVertexLabel("B").isEmpty());
            Assert.assertTrue(sqlgGraph1.getTopology().getSchema(this.schema2).orElseThrow().getVertexLabel("BB").isPresent());

            List<Vertex> bbVertices = sqlgGraph1.traversal().V().hasLabel(this.schema1 + ".AA").out("ab").toList();
            Assert.assertEquals(1, bbVertices.size());
            List<Vertex> aaVertices = sqlgGraph1.traversal().V().hasLabel(this.schema2 + ".BB").in("ab").toList();
            Assert.assertEquals(1, aaVertices.size());

            if (this.rollback) {
                sqlgGraph1.tx().rollback();
                Assert.assertTrue(sqlgGraph1.getTopology().getSchema(this.schema1).orElseThrow().getVertexLabel("AA").isEmpty());
                Assert.assertTrue(sqlgGraph1.getTopology().getSchema(this.schema1).orElseThrow().getVertexLabel("A").isPresent());
                Assert.assertTrue(sqlgGraph1.getTopology().getSchema(this.schema2).orElseThrow().getVertexLabel("BB").isEmpty());
                Assert.assertTrue(sqlgGraph1.getTopology().getSchema(this.schema2).orElseThrow().getVertexLabel("B").isPresent());
                Thread.sleep(1_000);
                Assert.assertTrue(this.sqlgGraph.getTopology().getSchema(this.schema1).orElseThrow().getVertexLabel("AA").isEmpty());
                Assert.assertTrue(this.sqlgGraph.getTopology().getSchema(this.schema1).orElseThrow().getVertexLabel("A").isPresent());
                Assert.assertTrue(this.sqlgGraph.getTopology().getSchema(this.schema2).orElseThrow().getVertexLabel("BB").isEmpty());
                Assert.assertTrue(this.sqlgGraph.getTopology().getSchema(this.schema2).orElseThrow().getVertexLabel("B").isPresent());
            } else {
                sqlgGraph1.tx().commit();
                Assert.assertTrue(sqlgGraph1.getTopology().getSchema(this.schema1).orElseThrow().getVertexLabel("A").isEmpty());
                Assert.assertTrue(sqlgGraph1.getTopology().getSchema(this.schema1).orElseThrow().getVertexLabel("AA").isPresent());
                Assert.assertTrue(sqlgGraph1.getTopology().getSchema(this.schema2).orElseThrow().getVertexLabel("B").isEmpty());
                Assert.assertTrue(sqlgGraph1.getTopology().getSchema(this.schema2).orElseThrow().getVertexLabel("BB").isPresent());
                Thread.sleep(1_000);
                Assert.assertTrue(this.sqlgGraph.getTopology().getSchema(this.schema1).orElseThrow().getVertexLabel("A").isEmpty());
                Assert.assertTrue(this.sqlgGraph.getTopology().getSchema(this.schema1).orElseThrow().getVertexLabel("AA").isPresent());
                Assert.assertTrue(this.sqlgGraph.getTopology().getSchema(this.schema2).orElseThrow().getVertexLabel("B").isEmpty());
                Assert.assertTrue(this.sqlgGraph.getTopology().getSchema(this.schema2).orElseThrow().getVertexLabel("BB").isPresent());
            }
        }
    }

//    @Test
//    public void testDistributedNameChange2() throws InterruptedException {
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            sqlgGraph1.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<>() {{
//                put("a", PropertyType.STRING);
//            }});
//            sqlgGraph1.tx().commit();
//            Thread.sleep(1_000);
//            Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
//            Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getProperty("a").isPresent());
//            Assert.assertFalse(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
//
//            VertexLabel aVertexLabel = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
//            Assert.assertTrue(sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
//            Assert.assertFalse(sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
//            Assert.assertTrue(sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getProperty("a").isPresent());
//            aVertexLabel.rename("B");
//            Assert.assertFalse(sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
//            Assert.assertTrue(sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
//            Assert.assertTrue(sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getProperty("a").isPresent());
//            sqlgGraph1.tx().commit();
//            Thread.sleep(1_000);
//            Assert.assertFalse(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
//            Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
//            Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getProperty("a").isPresent());
//        }
//    }
//
//    @Test
//    public void testDistributedNameChangeWithQuery() throws InterruptedException {
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            sqlgGraph1.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<>() {{
//                put("a", PropertyType.STRING);
//            }});
//            sqlgGraph1.addVertex(T.label, "A", "a", "halo");
//            VertexLabel aVertexLabel = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow();
//            Assert.assertTrue(sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
//            Assert.assertFalse(sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
//            Assert.assertTrue(sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getProperty("a").isPresent());
//            Assert.assertEquals(1, sqlgGraph1.traversal().V().hasLabel("A").count().next(), 0);
//            aVertexLabel.rename("B");
//            Assert.assertEquals(0, sqlgGraph1.traversal().V().hasLabel("A").count().next(), 0);
//            Assert.assertEquals(1, sqlgGraph1.traversal().V().hasLabel("B").count().next(), 0);
//            Assert.assertFalse(sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
//            Assert.assertTrue(sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
//            Assert.assertTrue(sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getProperty("a").isPresent());
//            sqlgGraph1.tx().commit();
//            Thread.sleep(1_000);
//            Assert.assertFalse(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").isPresent());
//            Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
//            Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getProperty("a").isPresent());
//            Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
//            Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("B").count().next(), 0);
//        }
//    }
//
//    @Test
//    public void testDistributedVertexLabelRenameAsEdgeRole() throws InterruptedException {
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            VertexLabel aVertexLabel = sqlgGraph1.getTopology().getPublicSchema().ensureVertexLabelExist("A", new HashMap<>() {{
//                put("a", PropertyType.STRING);
//            }});
//            VertexLabel bVertexLabel = sqlgGraph1.getTopology().getPublicSchema().ensureVertexLabelExist("B", new HashMap<>() {{
//                put("a", PropertyType.STRING);
//            }});
//            aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);
//            sqlgGraph1.tx().commit();
//            Thread.sleep(1_000);
//            aVertexLabel.rename("AA");
//            sqlgGraph1.tx().commit();
//            Thread.sleep(2_000);
//
//            EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
//            Set<VertexLabel> outVertexLabels = edgeLabel.getOutVertexLabels();
//            Assert.assertEquals(1, outVertexLabels.size());
//            Assert.assertEquals("AA", new ArrayList<>(outVertexLabels).get(0).getLabel());
//            Set<VertexLabel> inVertexLabels = edgeLabel.getInVertexLabels();
//            Assert.assertEquals(1, inVertexLabels.size());
//            Assert.assertEquals("B", new ArrayList<>(inVertexLabels).get(0).getLabel());
//            Set<EdgeRole> inEdgeRoles = edgeLabel.getInEdgeRoles();
//            Assert.assertEquals(1, inEdgeRoles.size());
//            Set<EdgeRole> outEdgeRoles = edgeLabel.getOutEdgeRoles();
//            Assert.assertEquals(1, outEdgeRoles.size());
//
//            Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").isEmpty());
//            Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("AA").isPresent());
//            Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("AA").orElseThrow().getOutEdgeLabel("ab").isPresent());
//            Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").isPresent());
//            Assert.assertEquals(1, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getInEdgeLabels().size());
//            Assert.assertEquals("ab", this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getInEdgeLabels().values().iterator().next().getLabel());
//
//            List<Vertex> outEdges = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
//                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
//                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "AA")
//                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
//                    .toList();
//            Assert.assertEquals(1, outEdges.size());
//            List<Vertex> inEdges = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
//                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
//                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "AA")
//                    .out(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
//                    .toList();
//            Assert.assertEquals(0, inEdges.size());
//            outEdges = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
//                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
//                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "B")
//                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
//                    .toList();
//            Assert.assertEquals(0, outEdges.size());
//            inEdges = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
//                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
//                    .has(Topology.SQLG_SCHEMA_VERTEX_LABEL_NAME, "B")
//                    .out(Topology.SQLG_SCHEMA_IN_EDGES_EDGE)
//                    .toList();
//            Assert.assertEquals(1, inEdges.size());
//
//        }
//    }
}
