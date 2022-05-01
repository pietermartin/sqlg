package org.umlg.sqlg.test.topology.edgeMultiplicity;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.EdgeDefinition;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.EdgeRole;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.Set;

public class TestEdgeMultiplicityDistributed extends BaseTest {

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

//    @Test
//    public void testMultiplicityRemoveInEdgeRole() throws InterruptedException {
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("A");
//            VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("B");
//            VertexLabel cVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("C");
//            aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new EdgeDefinition(Multiplicity.from(2, 2), Multiplicity.from(3, 3)));
//            aVertexLabel.ensureEdgeLabelExist("ab", cVertexLabel, new EdgeDefinition(Multiplicity.from(2, 2), Multiplicity.from(1, 1)));
//            this.sqlgGraph.tx().commit();
//
//            EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
//            Set<EdgeRole> outEdgeRoles = edgeLabel.getOutEdgeRoles(aVertexLabel);
//            Assert.assertEquals(1, outEdgeRoles.size());
//            EdgeRole outEdgeRole = outEdgeRoles.iterator().next();
//            Assert.assertEquals(Multiplicity.from(2, 2), outEdgeRole.getMultiplicity());
//            Set<EdgeRole> inEdgeRoles = edgeLabel.getInEdgeRoles(bVertexLabel);
//            Assert.assertEquals(1, inEdgeRoles.size());
//            Assert.assertEquals(Multiplicity.from(3, 3), inEdgeRoles.iterator().next().getMultiplicity());
//            inEdgeRoles = edgeLabel.getInEdgeRoles(cVertexLabel);
//            Assert.assertEquals(1, inEdgeRoles.size());
//            Assert.assertEquals(Multiplicity.from(1, 1), inEdgeRoles.iterator().next().getMultiplicity());
//
//            Thread.sleep(1_000);
//
//            edgeLabel = sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
//            outEdgeRoles = edgeLabel.getOutEdgeRoles(aVertexLabel);
//            Assert.assertEquals(1, outEdgeRoles.size());
//            Assert.assertEquals(Multiplicity.from(2, 2), outEdgeRoles.iterator().next().getMultiplicity());
//            inEdgeRoles = edgeLabel.getInEdgeRoles(bVertexLabel);
//            Assert.assertEquals(1, inEdgeRoles.size());
//            Assert.assertEquals(Multiplicity.from(3, 3), inEdgeRoles.iterator().next().getMultiplicity());
//            inEdgeRoles = edgeLabel.getInEdgeRoles(cVertexLabel);
//            Assert.assertEquals(1, inEdgeRoles.size());
//            Assert.assertEquals(Multiplicity.from(1, 1), inEdgeRoles.iterator().next().getMultiplicity());
//
//            edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
//            inEdgeRoles = edgeLabel.getInEdgeRoles(bVertexLabel);
//            EdgeRole bInEdgeRole = inEdgeRoles.iterator().next();
//            bInEdgeRole.remove();
//            this.sqlgGraph.tx().commit();
//
//            edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
//            outEdgeRoles = edgeLabel.getOutEdgeRoles(aVertexLabel);
//            Assert.assertEquals(1, outEdgeRoles.size());
//            Assert.assertEquals(Multiplicity.from(2, 2), outEdgeRoles.iterator().next().getMultiplicity());
//            inEdgeRoles = edgeLabel.getInEdgeRoles(bVertexLabel);
//            Assert.assertEquals(0, inEdgeRoles.size());
//            inEdgeRoles = edgeLabel.getInEdgeRoles(cVertexLabel);
//            Assert.assertEquals(1, inEdgeRoles.size());
//            Assert.assertEquals(Multiplicity.from(1, 1), inEdgeRoles.iterator().next().getMultiplicity());
//
//            Thread.sleep(1_000);
//
//            edgeLabel = sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
//            outEdgeRoles = edgeLabel.getOutEdgeRoles(aVertexLabel);
//            Assert.assertEquals(1, outEdgeRoles.size());
//            Assert.assertEquals(Multiplicity.from(2, 2), outEdgeRoles.iterator().next().getMultiplicity());
//            inEdgeRoles = edgeLabel.getInEdgeRoles(bVertexLabel);
//            Assert.assertEquals(0, inEdgeRoles.size());
//            inEdgeRoles = edgeLabel.getInEdgeRoles(cVertexLabel);
//            Assert.assertEquals(1, inEdgeRoles.size());
//            Assert.assertEquals(Multiplicity.from(1, 1), inEdgeRoles.iterator().next().getMultiplicity());
//
//        }
//    }

    @Test
    public void testMultiplicityRemoveOutEdgeRole() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("A");
            VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("B");
            VertexLabel cVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("C");
            bVertexLabel.ensureEdgeLabelExist("ab", aVertexLabel, new EdgeDefinition(Multiplicity.from(2, 2), Multiplicity.from(4, 5)));
            cVertexLabel.ensureEdgeLabelExist("ab", aVertexLabel, new EdgeDefinition(Multiplicity.from(3, 3), Multiplicity.from(4, 5)));
            this.sqlgGraph.tx().commit();

            EdgeLabel edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
            Set<EdgeRole> outEdgeRoles = edgeLabel.getOutEdgeRoles(bVertexLabel);
            Assert.assertEquals(1, outEdgeRoles.size());
            EdgeRole outEdgeRole = outEdgeRoles.iterator().next();
            Assert.assertEquals(Multiplicity.from(2, 2), outEdgeRole.getMultiplicity());
            outEdgeRoles = edgeLabel.getOutEdgeRoles(cVertexLabel);
            Assert.assertEquals(1, outEdgeRoles.size());
            outEdgeRole = outEdgeRoles.iterator().next();
            Assert.assertEquals(Multiplicity.from(3, 3), outEdgeRole.getMultiplicity());

            Set<EdgeRole> inEdgeRoles = edgeLabel.getInEdgeRoles(aVertexLabel);
            Assert.assertEquals(1, inEdgeRoles.size());
            EdgeRole inEdgeRole = inEdgeRoles.iterator().next();
            Assert.assertEquals(Multiplicity.from(4, 5), inEdgeRole.getMultiplicity());

            Thread.sleep(1_000);
            edgeLabel = sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
            outEdgeRoles = edgeLabel.getOutEdgeRoles(bVertexLabel);
            Assert.assertEquals(1, outEdgeRoles.size());
            outEdgeRole = outEdgeRoles.iterator().next();
            Assert.assertEquals(Multiplicity.from(2, 2), outEdgeRole.getMultiplicity());
            outEdgeRoles = edgeLabel.getOutEdgeRoles(cVertexLabel);
            Assert.assertEquals(1, outEdgeRoles.size());
            outEdgeRole = outEdgeRoles.iterator().next();
            Assert.assertEquals(Multiplicity.from(3, 3), outEdgeRole.getMultiplicity());

            edgeLabel = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow();
            inEdgeRoles = edgeLabel.getInEdgeRoles(aVertexLabel);
            Assert.assertEquals(1, inEdgeRoles.size());
            inEdgeRole.remove();
            this.sqlgGraph.tx().commit();

            Assert.assertFalse(this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").isPresent());
            Thread.sleep(1_000);
            Assert.assertFalse(sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").isPresent());
        }
    }
}
