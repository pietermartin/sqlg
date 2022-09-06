package org.umlg.sqlg.test.topology.edgeMultiplicity;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.EdgeDefinition;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.EdgeRole;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.HashMap;

public class TestEdgeMultiplicityUniqueDistributed extends BaseTest {

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
    public void testUniqueOneToMany() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A",
                    new HashMap<>() {{
                        put("name", PropertyDefinition.of(PropertyType.STRING));
                    }});
            VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B",
                    new HashMap<>() {{
                        put("name", PropertyDefinition.of(PropertyType.STRING));
                    }});
            aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                    EdgeDefinition.of(
                            Multiplicity.of(0, 1, true),
                            Multiplicity.of(0, -1, true)
                    ));
            this.sqlgGraph.tx().commit();

            Thread.sleep(1_000);

            EdgeLabel edgeLabel = sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
            EdgeRole outEdgeRole = edgeLabel.getOutEdgeRoles(aVertexLabel);
            Assert.assertEquals(Multiplicity.of(0, 1, true), outEdgeRole.getMultiplicity());
            Assert.assertNotNull(outEdgeRole);
            EdgeRole inEdgeRole = edgeLabel.getInEdgeRoles(bVertexLabel);
            Assert.assertEquals(Multiplicity.of(0, -1, true), inEdgeRole.getMultiplicity());
            Assert.assertNotNull(inEdgeRole);
        }
    }

    @Test
    public void testUniqueOneToManyRemoveEdgeRole() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel aVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("A",
                    new HashMap<>() {{
                        put("name", PropertyDefinition.of(PropertyType.STRING));
                    }});
            VertexLabel bVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("B",
                    new HashMap<>() {{
                        put("name", PropertyDefinition.of(PropertyType.STRING));
                    }});
            aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel,
                    EdgeDefinition.of(
                            Multiplicity.of(0, 1, true),
                            Multiplicity.of(0, -1, true)
                    ));
            VertexLabel cVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("C",
                    new HashMap<>() {{
                        put("name", PropertyDefinition.of(PropertyType.STRING));
                    }});
            aVertexLabel.ensureEdgeLabelExist("ab", cVertexLabel,
                    EdgeDefinition.of(
                            Multiplicity.of(0, 1, true),
                            Multiplicity.of(0, -1, false)
                    ));
            this.sqlgGraph.tx().commit();

            Thread.sleep(1_000);

            EdgeLabel edgeLabel = sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
            EdgeRole outEdgeRole = edgeLabel.getOutEdgeRoles(aVertexLabel);
            Assert.assertEquals(Multiplicity.of(0, 1, true), outEdgeRole.getMultiplicity());
            Assert.assertNotNull(outEdgeRole);
            EdgeRole inEdgeRole = edgeLabel.getInEdgeRoles(bVertexLabel);
            Assert.assertEquals(Multiplicity.of(0, -1, true), inEdgeRole.getMultiplicity());
            Assert.assertNotNull(inEdgeRole);

            edgeLabel = sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
            outEdgeRole = edgeLabel.getOutEdgeRoles(aVertexLabel);
            Assert.assertEquals(Multiplicity.of(0, 1, true), outEdgeRole.getMultiplicity());
            Assert.assertNotNull(outEdgeRole);
            inEdgeRole = edgeLabel.getInEdgeRoles(cVertexLabel);
            Assert.assertEquals(Multiplicity.of(0, -1, false), inEdgeRole.getMultiplicity());
            Assert.assertNotNull(inEdgeRole);

            inEdgeRole.remove();
            sqlgGraph1.tx().commit();

            Thread.sleep(1_000);
            edgeLabel = sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
            outEdgeRole = edgeLabel.getOutEdgeRoles(aVertexLabel);
            Assert.assertEquals(Multiplicity.of(0, 1, true), outEdgeRole.getMultiplicity());
            Assert.assertNotNull(outEdgeRole);
            inEdgeRole = edgeLabel.getInEdgeRoles(cVertexLabel);
            Assert.assertNull(inEdgeRole);
        }
    }
}
