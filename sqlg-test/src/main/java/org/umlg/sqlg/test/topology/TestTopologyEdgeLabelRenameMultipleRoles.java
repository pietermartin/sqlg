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
import java.util.Optional;

public class TestTopologyEdgeLabelRenameMultipleRoles extends BaseTest  {

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
    public void testEdgeLabelRenameMultipleRoles() throws InterruptedException {

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Schema aSchema = sqlgGraph1.getTopology().ensureSchemaExist("A");
            Schema bSchema = sqlgGraph1.getTopology().ensureSchemaExist("B");
            sqlgGraph1.tx().commit();
            Thread.sleep(1_000);

            VertexLabel aVertexLabel = aSchema.ensureVertexLabelExist("A");
            VertexLabel otherVertexLabel = aSchema.ensureVertexLabelExist("Other");
            VertexLabel bVertexLabel = bSchema.ensureVertexLabelExist("B");

            aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);
            otherVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);
            sqlgGraph1.tx().commit();

            Thread.sleep(1_000);

            Assert.assertTrue(aVertexLabel.getOutEdgeLabel("ab").isPresent());
            Assert.assertTrue(otherVertexLabel.getOutEdgeLabel("ab").isPresent());
            Assert.assertTrue(sqlgGraph1.getTopology().getEdgeLabel("A", "ab").isPresent());
            EdgeLabel abEdgeLabel = sqlgGraph1.getTopology().getEdgeLabel("A", "ab").get();
            abEdgeLabel.rename("abab");
            sqlgGraph1.tx().commit();

            Thread.sleep(1_000);

            Optional<VertexLabel> aVertexLabelOpt = sqlgGraph1.getTopology().getSchema("A").orElseThrow().getVertexLabel("A");
            Assert.assertTrue(aVertexLabelOpt.isPresent());
            aVertexLabel = aVertexLabelOpt.get();
            Optional<VertexLabel> bVertexLabelOpt = sqlgGraph1.getTopology().getSchema("B").orElseThrow().getVertexLabel("B");
            Assert.assertTrue(bVertexLabelOpt.isPresent());
            bVertexLabel = bVertexLabelOpt.get();
            Optional<VertexLabel> otherVertexLabelOpt = sqlgGraph1.getTopology().getSchema("A").orElseThrow().getVertexLabel("Other");
            Assert.assertTrue(otherVertexLabelOpt.isPresent());
            otherVertexLabel = otherVertexLabelOpt.get();

            Assert.assertTrue(aVertexLabel.getOutEdgeLabel("ab").isEmpty());
            Assert.assertTrue(otherVertexLabel.getOutEdgeLabel("ab").isEmpty());
            Assert.assertTrue(aVertexLabel.getOutEdgeLabel("abab").isPresent());
            Assert.assertTrue(otherVertexLabel.getOutEdgeLabel("abab").isPresent());
            Assert.assertTrue(sqlgGraph1.getTopology().getEdgeLabel("A", "abab").isPresent());

            aVertexLabelOpt = this.sqlgGraph.getTopology().getSchema("A").orElseThrow().getVertexLabel("A");
            Assert.assertTrue(aVertexLabelOpt.isPresent());
            aVertexLabel = aVertexLabelOpt.get();
            bVertexLabelOpt = this.sqlgGraph.getTopology().getSchema("B").orElseThrow().getVertexLabel("B");
            Assert.assertTrue(bVertexLabelOpt.isPresent());
            bVertexLabel = bVertexLabelOpt.get();
            otherVertexLabelOpt = this.sqlgGraph.getTopology().getSchema("A").orElseThrow().getVertexLabel("Other");
            Assert.assertTrue(otherVertexLabelOpt.isPresent());
            otherVertexLabel = otherVertexLabelOpt.get();

            Assert.assertTrue(aVertexLabel.getOutEdgeLabel("ab").isEmpty());
            Assert.assertTrue(otherVertexLabel.getOutEdgeLabel("ab").isEmpty());
            Assert.assertTrue(aVertexLabel.getOutEdgeLabel("abab").isPresent());
            Assert.assertTrue(otherVertexLabel.getOutEdgeLabel("abab").isPresent());
            Assert.assertTrue(this.sqlgGraph.getTopology().getEdgeLabel("A", "abab").isPresent());
            EdgeLabel ababEdgeLabel = this.sqlgGraph.getTopology().getEdgeLabel("A", "abab").get();

            Assert.assertTrue(aVertexLabel.getOutEdgeRoles().containsKey("A.abab"));
            EdgeRole edgeRole = aVertexLabel.getOutEdgeRoles().get("A.abab");
            Assert.assertEquals(aVertexLabel, edgeRole.getVertexLabel());
            Assert.assertEquals(ababEdgeLabel, edgeRole.getEdgeLabel());
        }
    }
}
