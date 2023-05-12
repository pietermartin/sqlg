package org.umlg.sqlg.test.topology.edgeMultiplicity;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.net.URL;

/**
 * Test that the upgrade fixed the incorrectly defaulted multiplicities.
 * As this test relies on carefully reconstructed data it can not execute as part of the test suite.
 */
public class TestUpgrade301 {

    @Test
    public void testUpgrade() throws ConfigurationException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        Configurations configs = new Configurations();
        Configuration configuration = configs.properties(sqlProperties);
        if (!configuration.containsKey("jdbc.url")) {
            throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
        }
        SqlgGraph sqlgGraph = SqlgGraph.open(configuration);
        Schema aSchema = sqlgGraph.getTopology().ensureSchemaExist("ASchema");
        VertexLabel aVertexLabel = aSchema.getVertexLabel("ATest").orElseThrow();
        VertexLabel bVertexLabel = aSchema.getVertexLabel("BTest").orElseThrow();
        EdgeLabel edgeLabel = aVertexLabel.getOutEdgeLabel("ab").orElseThrow();

        //Array properties lower was -1, should now be 0
        Assert.assertEquals(0, aVertexLabel.getProperty("col3").orElseThrow().getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(0, aVertexLabel.getProperty("col4").orElseThrow().getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(0, bVertexLabel.getProperty("col3").orElseThrow().getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(0, bVertexLabel.getProperty("col4").orElseThrow().getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(0, edgeLabel.getProperty("col3").orElseThrow().getPropertyDefinition().multiplicity().lower());
        Assert.assertEquals(0, edgeLabel.getProperty("col4").orElseThrow().getPropertyDefinition().multiplicity().lower());

        //Non array properties upper was 0, should now be 1
        Assert.assertEquals(1, aVertexLabel.getProperty("col1").orElseThrow().getPropertyDefinition().multiplicity().upper());
        Assert.assertEquals(1, aVertexLabel.getProperty("col2").orElseThrow().getPropertyDefinition().multiplicity().upper());
        Assert.assertEquals(1, bVertexLabel.getProperty("col1").orElseThrow().getPropertyDefinition().multiplicity().upper());
        Assert.assertEquals(1, bVertexLabel.getProperty("col2").orElseThrow().getPropertyDefinition().multiplicity().upper());
        Assert.assertEquals(1, edgeLabel.getProperty("col1").orElseThrow().getPropertyDefinition().multiplicity().upper());
        Assert.assertEquals(1, edgeLabel.getProperty("col2").orElseThrow().getPropertyDefinition().multiplicity().upper());

        sqlgGraph.tx().rollback();
        sqlgGraph.close();
    }

}
