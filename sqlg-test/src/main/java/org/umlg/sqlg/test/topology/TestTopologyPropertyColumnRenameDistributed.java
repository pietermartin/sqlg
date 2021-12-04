package org.umlg.sqlg.test.topology;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

public class TestTopologyPropertyColumnRenameDistributed extends BaseTest {

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
    public void testDistributedNameChange() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            sqlgGraph1.getTopology().getPublicSchema()
                    .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                        put("column1", PropertyType.STRING);
                    }});
            sqlgGraph1.tx().commit();
            Optional<VertexLabel> aVertexLabelOptional = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A");
            Preconditions.checkState(aVertexLabelOptional.isPresent());
            VertexLabel aVertexLabel = aVertexLabelOptional.get();
            Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("column1");
            Preconditions.checkState(column1Optional.isPresent());
            PropertyColumn column1 = column1Optional.get();
            column1.rename("column2");
            sqlgGraph1.tx().commit();

            Thread.sleep(1_000);
            Assert.assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());
            List<String> propertyNames = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_PROPERTY).<String>values(Topology.SQLG_SCHEMA_PROPERTY_NAME).toList();
            Assert.assertEquals(1, propertyNames.size());
            Assert.assertEquals("column2", propertyNames.get(0));
            Assert.assertTrue(this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").orElseThrow().getProperty("column2").isPresent());
        }
    }

    @Test
    public void testDistributedIdentifierChange() throws InterruptedException {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            sqlgGraph1.getTopology().getPublicSchema()
                    .ensureVertexLabelExist("A", new LinkedHashMap<>() {{
                        put("column1", PropertyType.STRING);
                        put("column2", PropertyType.STRING);
                    }}, ListOrderedSet.listOrderedSet(List.of("column1")));
            sqlgGraph1.tx().commit();

            Optional<VertexLabel> aVertexLabelOptional = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A");
            Preconditions.checkState(aVertexLabelOptional.isPresent());
            VertexLabel aVertexLabel = aVertexLabelOptional.get();
            Optional<PropertyColumn> column1Optional = aVertexLabel.getProperty("column1");
            Preconditions.checkState(column1Optional.isPresent());
            ListOrderedSet<String> identifiers = aVertexLabel.getIdentifiers();
            Assert.assertEquals(1, identifiers.size());
            Assert.assertEquals("column1", identifiers.get(0));
            PropertyColumn column1 = column1Optional.get();
            column1.rename("column1PK");
            sqlgGraph1.tx().commit();

            List<Vertex> identifierProperties = sqlgGraph1.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL)
                    .out(Topology.SQLG_SCHEMA_VERTEX_IDENTIFIER_EDGE)
                    .toList();
            Assert.assertEquals(1, identifierProperties.size());
            Assert.assertEquals("column1PK", identifierProperties.get(0).value(Topology.SQLG_SCHEMA_PROPERTY_NAME));

            aVertexLabelOptional = sqlgGraph1.getTopology().getPublicSchema().getVertexLabel("A");
            Preconditions.checkState(aVertexLabelOptional.isPresent());
            aVertexLabel = aVertexLabelOptional.get();
            column1Optional = aVertexLabel.getProperty("column1PK");
            Preconditions.checkState(column1Optional.isPresent());
            identifiers = aVertexLabel.getIdentifiers();
            Assert.assertEquals(1, identifiers.size());
            Assert.assertEquals("column1PK", identifiers.get(0));

            Thread.sleep(1_000);
            Assert.assertEquals(this.sqlgGraph.getTopology(), sqlgGraph1.getTopology());
            identifierProperties = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_VERTEX_LABEL)
                    .out(Topology.SQLG_SCHEMA_VERTEX_IDENTIFIER_EDGE)
                    .toList();
            Assert.assertEquals(1, identifierProperties.size());
            Assert.assertEquals("column1PK", identifierProperties.get(0).value(Topology.SQLG_SCHEMA_PROPERTY_NAME));

            aVertexLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
            Preconditions.checkState(aVertexLabelOptional.isPresent());
            aVertexLabel = aVertexLabelOptional.get();
            column1Optional = aVertexLabel.getProperty("column1PK");
            Preconditions.checkState(column1Optional.isPresent());
            identifiers = aVertexLabel.getIdentifiers();
            Assert.assertEquals(1, identifiers.size());
            Assert.assertEquals("column1PK", identifiers.get(0));
        }
    }
}
