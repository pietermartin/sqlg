package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.*;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.LinkedHashMap;

public class TestEdgeRole extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            Assume.assumeTrue(isPostgres());
            configuration.addProperty(SqlgGraph.DISTRIBUTED, true);
            if (!configuration.containsKey(SqlgGraph.JDBC_URL))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", SqlgGraph.JDBC_URL));

        } catch (ConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void testRemoveOutEdgeRole() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
            VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
            VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("C");
            aVertexLabel.ensureEdgeLabelExist("e", bVertexLabel);
            cVertexLabel.ensureEdgeLabelExist("e", aVertexLabel);
            this.sqlgGraph.tx().commit();

            Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
            Assert.assertTrue(publicSchema.getVertexLabel("A").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("B").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("C").isPresent());
            Assert.assertTrue(publicSchema.getEdgeLabel("e").isPresent());
            Assert.assertEquals(2, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().size());
            Assert.assertEquals(2, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().size());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("C")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("B")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());

            Thread.sleep(1_000);

            publicSchema = sqlgGraph1.getTopology().getPublicSchema();
            Assert.assertTrue(publicSchema.getVertexLabel("A").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("B").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("C").isPresent());
            Assert.assertTrue(publicSchema.getEdgeLabel("e").isPresent());
            Assert.assertEquals(2, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().size());
            Assert.assertEquals(2, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().size());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("C")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("B")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());

            publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
            EdgeRole e_a_edgeRole = publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).findAny().orElseThrow();
            e_a_edgeRole.remove();
            this.sqlgGraph.tx().commit();

            publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
            Assert.assertTrue(publicSchema.getVertexLabel("A").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("B").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("C").isPresent());
            Assert.assertTrue(publicSchema.getEdgeLabel("e").isPresent());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().size());
            Assert.assertEquals(2, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().size());
            Assert.assertEquals(0, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("C")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("B")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());

            Thread.sleep(1_000);

            publicSchema = sqlgGraph1.getTopology().getPublicSchema();
            Assert.assertTrue(publicSchema.getVertexLabel("A").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("B").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("C").isPresent());
            Assert.assertTrue(publicSchema.getEdgeLabel("e").isPresent());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().size());
            Assert.assertEquals(2, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().size());
            Assert.assertEquals(0, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("C")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("B")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());
        }

    }

    @Test
    public void testRemoveInEdgeRole() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A");
            VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B");
            VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("C");
            aVertexLabel.ensureEdgeLabelExist("e", bVertexLabel);
            cVertexLabel.ensureEdgeLabelExist("e", aVertexLabel);
            this.sqlgGraph.tx().commit();

            Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
            Assert.assertTrue(publicSchema.getVertexLabel("A").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("B").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("C").isPresent());
            Assert.assertTrue(publicSchema.getEdgeLabel("e").isPresent());
            Assert.assertEquals(2, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().size());
            Assert.assertEquals(2, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().size());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("C")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("B")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());

            Thread.sleep(1_000);

            publicSchema = sqlgGraph1.getTopology().getPublicSchema();
            Assert.assertTrue(publicSchema.getVertexLabel("A").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("B").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("C").isPresent());
            Assert.assertTrue(publicSchema.getEdgeLabel("e").isPresent());
            Assert.assertEquals(2, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().size());
            Assert.assertEquals(2, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().size());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("C")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("B")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());

            publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
            EdgeRole e_a_edgeRole = publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).findAny().orElseThrow();
            e_a_edgeRole.remove();
            this.sqlgGraph.tx().commit();

            publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
            Assert.assertTrue(publicSchema.getVertexLabel("A").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("B").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("C").isPresent());
            Assert.assertTrue(publicSchema.getEdgeLabel("e").isPresent());
            Assert.assertEquals(2, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().size());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().size());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("C")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("B")).count());
            Assert.assertEquals(0, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());

            Thread.sleep(1_000);

            publicSchema = sqlgGraph1.getTopology().getPublicSchema();
            Assert.assertTrue(publicSchema.getVertexLabel("A").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("B").isPresent());
            Assert.assertTrue(publicSchema.getVertexLabel("C").isPresent());
            Assert.assertTrue(publicSchema.getEdgeLabel("e").isPresent());
            Assert.assertEquals(2, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().size());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().size());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getOutEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("C")).count());
            Assert.assertEquals(1, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("B")).count());
            Assert.assertEquals(0, publicSchema.getEdgeLabel("e").get().getInEdgeRoles().stream().filter(edgeRole -> edgeRole.getVertexLabel().getName().equals("A")).count());
        }

    }

    @Test
    public void testEdgeRoles() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Schema publicSchema = sqlgGraph1.getTopology().getPublicSchema();
            VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A");
            VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B");
            EdgeLabel edgeLabel1 = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);
            sqlgGraph1.tx().commit();
            Thread.sleep(1_000);
            Assert.assertEquals(1, aVertexLabel.getOutEdgeRoles().size());
            Assert.assertEquals(0, aVertexLabel.getInEdgeRoles().size());
            Assert.assertEquals(0, bVertexLabel.getOutEdgeRoles().size());
            Assert.assertEquals(1, bVertexLabel.getInEdgeRoles().size());
            Assert.assertEquals(1, edgeLabel1.getOutEdgeRoles().size());
            Assert.assertEquals(1, edgeLabel1.getInEdgeRoles().size());

            VertexLabel cVertexLabel = publicSchema.ensureVertexLabelExist("C");
            EdgeLabel edgeLabel2 = aVertexLabel.ensureEdgeLabelExist("ab", cVertexLabel);
            sqlgGraph1.tx().commit();

            Thread.sleep(1_000);

            Assert.assertEquals(1, aVertexLabel.getOutEdgeRoles().size());
            Assert.assertEquals(0, aVertexLabel.getInEdgeRoles().size());
            Assert.assertEquals(0, bVertexLabel.getOutEdgeRoles().size());
            Assert.assertEquals(1, bVertexLabel.getInEdgeRoles().size());
            Assert.assertEquals(0, cVertexLabel.getOutEdgeRoles().size());
            Assert.assertEquals(1, cVertexLabel.getInEdgeRoles().size());
            Assert.assertEquals(1, edgeLabel1.getOutEdgeRoles().size());
            Assert.assertEquals(2, edgeLabel1.getInEdgeRoles().size());

            Assert.assertEquals(1, sqlgGraph1.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .count().next(), 0);

        }
    }

    @Test
    public void testEdgeRolesMultipleSchema() {
        Schema aSchema = this.sqlgGraph.getTopology().ensureSchemaExist("A");
        VertexLabel aaVertexLabel = aSchema.ensureVertexLabelExist("A");
        VertexLabel abVertexLabel = aSchema.ensureVertexLabelExist("B");
        EdgeLabel edgeLabelAB_A = aaVertexLabel.ensureEdgeLabelExist("ab", abVertexLabel);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, aaVertexLabel.getOutEdgeRoles().size());
        Assert.assertEquals(0, aaVertexLabel.getInEdgeRoles().size());
        Assert.assertEquals(0, abVertexLabel.getOutEdgeRoles().size());
        Assert.assertEquals(1, abVertexLabel.getInEdgeRoles().size());
        Assert.assertEquals(1, edgeLabelAB_A.getOutEdgeRoles().size());
        Assert.assertEquals(1, edgeLabelAB_A.getInEdgeRoles().size());

        Schema bSchema = this.sqlgGraph.getTopology().ensureSchemaExist("B");
        VertexLabel baVertexLabel = bSchema.ensureVertexLabelExist("A");
        VertexLabel bbVertexLabel = bSchema.ensureVertexLabelExist("B");
        EdgeLabel edgeLabelAB_B = baVertexLabel.ensureEdgeLabelExist("ab", bbVertexLabel);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, aaVertexLabel.getOutEdgeRoles().size());
        Assert.assertEquals(0, aaVertexLabel.getInEdgeRoles().size());
        Assert.assertEquals(0, abVertexLabel.getOutEdgeRoles().size());
        Assert.assertEquals(1, abVertexLabel.getInEdgeRoles().size());

        Assert.assertEquals(1, baVertexLabel.getOutEdgeRoles().size());
        Assert.assertEquals(0, baVertexLabel.getInEdgeRoles().size());
        Assert.assertEquals(0, bbVertexLabel.getOutEdgeRoles().size());
        Assert.assertEquals(1, bbVertexLabel.getInEdgeRoles().size());

        EdgeLabel aEdgeLabel = this.sqlgGraph.getTopology().getSchema("A").orElseThrow().getEdgeLabel("ab").orElseThrow();
        EdgeLabel bEdgeLabel = this.sqlgGraph.getTopology().getSchema("B").orElseThrow().getEdgeLabel("ab").orElseThrow();
        Assert.assertEquals(1, edgeLabelAB_B.getOutEdgeRoles().size());
        Assert.assertEquals(1, edgeLabelAB_B.getInEdgeRoles().size());
        Assert.assertEquals(1, bEdgeLabel.getOutEdgeRoles().size());
        Assert.assertEquals(1, bEdgeLabel.getInEdgeRoles().size());

        aEdgeLabel.ensurePropertiesExist(new LinkedHashMap<>() {{
            put("aname", PropertyDefinition.of(PropertyType.STRING));
        }});
        bEdgeLabel.ensurePropertiesExist(new LinkedHashMap<>() {{
            put("bname", PropertyDefinition.of(PropertyType.STRING));
        }});
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(2, this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                .count().next(), 0);
        Assert.assertEquals(2, this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                .out(Topology.SQLG_SCHEMA_EDGE_PROPERTIES_EDGE)
                .count().next(), 0);
    }

    @Test
    public void testEdgeRolesSameSchema() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Schema aSchema = sqlgGraph1.getTopology().ensureSchemaExist("A");
            VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A");
            VertexLabel bVertexLabel = aSchema.ensureVertexLabelExist("B");
            EdgeLabel edgeLabelTest = aVertexLabel.ensureEdgeLabelExist("test", bVertexLabel);
            VertexLabel cVertexLabel = aSchema.ensureVertexLabelExist("C");
            VertexLabel dVertexLabel = aSchema.ensureVertexLabelExist("D");
            EdgeLabel edgeLabelTest_Again = cVertexLabel.ensureEdgeLabelExist("test", dVertexLabel);
            sqlgGraph1.tx().commit();
            Thread.sleep(1_000);

            Assert.assertEquals(1, aVertexLabel.getOutEdgeRoles().size());
            Assert.assertEquals(0, aVertexLabel.getInEdgeRoles().size());
            Assert.assertEquals(0, bVertexLabel.getOutEdgeRoles().size());
            Assert.assertEquals(1, bVertexLabel.getInEdgeRoles().size());
            Assert.assertEquals(1, cVertexLabel.getOutEdgeRoles().size());
            Assert.assertEquals(0, cVertexLabel.getInEdgeRoles().size());
            Assert.assertEquals(0, dVertexLabel.getOutEdgeRoles().size());
            Assert.assertEquals(1, dVertexLabel.getInEdgeRoles().size());
            Assert.assertEquals(1, sqlgGraph1.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .dedup()
                    .count().next(), 0);

            Assert.assertTrue(this.sqlgGraph.getTopology().getSchema("A").isPresent());
            aSchema = this.sqlgGraph.getTopology().getSchema("A").get();
            Assert.assertTrue(aSchema.getVertexLabel("A").isPresent());
            Assert.assertTrue(aSchema.getVertexLabel("B").isPresent());
            Assert.assertTrue(aSchema.getVertexLabel("C").isPresent());
            Assert.assertTrue(aSchema.getVertexLabel("D").isPresent());
            Assert.assertTrue(aSchema.getEdgeLabel("test").isPresent());
            aVertexLabel = aSchema.getVertexLabel("A").get();
            bVertexLabel = aSchema.getVertexLabel("B").get();
            cVertexLabel = aSchema.getVertexLabel("C").get();
            dVertexLabel = aSchema.getVertexLabel("D").get();
            Assert.assertEquals(1, aVertexLabel.getOutEdgeRoles().size());
            Assert.assertEquals(0, aVertexLabel.getInEdgeRoles().size());
            Assert.assertEquals(0, bVertexLabel.getOutEdgeRoles().size());
            Assert.assertEquals(1, bVertexLabel.getInEdgeRoles().size());
            Assert.assertEquals(1, cVertexLabel.getOutEdgeRoles().size());
            Assert.assertEquals(0, cVertexLabel.getInEdgeRoles().size());
            Assert.assertEquals(0, dVertexLabel.getOutEdgeRoles().size());
            Assert.assertEquals(1, dVertexLabel.getInEdgeRoles().size());

            Assert.assertEquals(1, this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_SCHEMA)
                    .out(Topology.SQLG_SCHEMA_SCHEMA_VERTEX_EDGE)
                    .out(Topology.SQLG_SCHEMA_OUT_EDGES_EDGE)
                    .dedup()
                    .count().next(), 0);

            edgeLabelTest_Again.ensurePropertiesExist(new LinkedHashMap<>() {{
                put("name", PropertyDefinition.of(PropertyType.STRING));
            }});
            sqlgGraph1.tx().commit();
            Thread.sleep(1_000);

            edgeLabelTest_Again = this.sqlgGraph.getTopology().getSchema("A").orElseThrow().getEdgeLabel("test").orElseThrow();
            Assert.assertTrue(edgeLabelTest_Again.getProperty("name").isPresent());
        }
    }

}
