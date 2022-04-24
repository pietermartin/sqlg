package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.EdgeRole;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;

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
    public void testEdgeRoles() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B");
        EdgeLabel edgeLabel1 = aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, aVertexLabel.getOutEdgeRoles().size());
        Assert.assertEquals(0, aVertexLabel.getInEdgeRoles().size());
        Assert.assertEquals(0, bVertexLabel.getOutEdgeRoles().size());
        Assert.assertEquals(1, bVertexLabel.getInEdgeRoles().size());
        Assert.assertEquals(1, edgeLabel1.getOutEdgeRoles().size());
        Assert.assertEquals(1, edgeLabel1.getInEdgeRoles().size());

        VertexLabel cVertexLabel = publicSchema.ensureVertexLabelExist("C");
        EdgeLabel edgeLabel2 = aVertexLabel.ensureEdgeLabelExist("ab", cVertexLabel);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(1, aVertexLabel.getOutEdgeRoles().size());
        Assert.assertEquals(0, aVertexLabel.getInEdgeRoles().size());
        Assert.assertEquals(0, bVertexLabel.getOutEdgeRoles().size());
        Assert.assertEquals(1, bVertexLabel.getInEdgeRoles().size());
        Assert.assertEquals(0, cVertexLabel.getOutEdgeRoles().size());
        Assert.assertEquals(1, cVertexLabel.getInEdgeRoles().size());
        Assert.assertEquals(1, edgeLabel1.getOutEdgeRoles().size());
        Assert.assertEquals(2, edgeLabel1.getInEdgeRoles().size());
    }

}
