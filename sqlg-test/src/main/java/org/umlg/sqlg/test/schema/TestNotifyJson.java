package org.umlg.sqlg.test.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.Topology;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2016/11/27
 * Time: 9:42 PM
 */
public class TestNotifyJson extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            Assume.assumeTrue(configuration.getString("jdbc.url").contains("postgresql"));
            configuration.addProperty("distributed", true);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testNotifyJson() {
        Map<String, PropertyType> properties  = new HashMap<>();
        properties.put("name", PropertyType.STRING);
        this.sqlgGraph.getTopology().ensureSchemaExist("A").ensureVertexLabelExist("A", properties);
        this.sqlgGraph.tx().commit();
        List<Vertex> logs = this.sqlgGraph.topology().V().hasLabel(Topology.SQLG_SCHEMA + "." + Topology.SQLG_SCHEMA_LOG).toList();
        assertEquals(1, logs.size());
        Vertex log = logs.get(0);
        JsonNode jsonLog = log.value(Topology.SQLG_SCHEMA_LOG_LOG);
        JsonNode schemas = jsonLog.get("uncommittedSchemas");
        assertNotNull("A", schemas);
        assertTrue(schemas instanceof ArrayNode);
        ArrayNode schemasArray = (ArrayNode)schemas;
        assertEquals(1, schemasArray.size());
        JsonNode aSchema = schemasArray.get(0);
        assertEquals("A", aSchema.get("name").asText());
        JsonNode uncommittedVertexLabels = aSchema.get("uncommittedVertexLabels");
        assertNotNull(uncommittedVertexLabels);
        assertTrue(uncommittedVertexLabels instanceof ArrayNode);
        ArrayNode uncommittedVertexLabelsArray = (ArrayNode)uncommittedVertexLabels;
        assertEquals(1, uncommittedVertexLabelsArray.size());
        JsonNode vertexLabel = uncommittedVertexLabels.get(0);
        assertEquals("A", vertexLabel.get("label").asText());
        JsonNode propertiesJson = vertexLabel.get("uncommittedProperties");
        assertNotNull(propertiesJson);
        assertTrue(propertiesJson instanceof ArrayNode);
        ArrayNode propertiesArray = (ArrayNode)propertiesJson;
        assertEquals(1, propertiesArray.size());



    }
}
