package org.umlg.sqlg.test.topology.propertydefinition;

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
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class TestDefaultValueDistributed extends BaseTest {

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
    public void testRequiredPropertyOnVertexLabel() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
            VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist("A", new HashMap<>() {{
                put("a", PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), "'aaa'"));
            }});
            VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist("B", new HashMap<>() {{
                put("b", PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), "'bbb'"));
            }});
            aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel, new HashMap<>() {{
                put("a", PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), "'ccc'"));
            }});
            this.sqlgGraph.tx().commit();
            Thread.sleep(3_000);
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());
            Assert.assertEquals(
                    PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), "'aaa'"),
                    sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getProperty("a").orElseThrow().getPropertyDefinition()
            );
        }
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());
            Assert.assertEquals(
                    PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), "'aaa'"),
                    sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getProperty("a").orElseThrow().getPropertyDefinition()
            );
            Assert.assertEquals(
                    PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), "'bbb'"),
                    sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("B").orElseThrow().getProperty("b").orElseThrow().getPropertyDefinition()
            );
            Assert.assertEquals(
                    PropertyDefinition.of(PropertyType.STRING, Multiplicity.from(1, 1), "'ccc'"),
                    sqlgGraph1.getTopology().getPublicSchema().getEdgeLabel("ab").orElseThrow().getProperty("a").orElseThrow().getPropertyDefinition()
            );
        }
    }

}
