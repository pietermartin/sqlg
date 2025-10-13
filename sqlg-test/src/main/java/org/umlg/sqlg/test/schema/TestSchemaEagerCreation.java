package org.umlg.sqlg.test.schema;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2016/11/23
 * Time: 6:03 PM
 */
@SuppressWarnings("DuplicatedCode")
public class TestSchemaEagerCreation extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsTransactionalSchema());
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testVertexEdgeHasSameName() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name1", "halo1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name2", "halo2");
        a1.addEdge("A", b2, "name3", "halo3");
        this.sqlgGraph.tx().commit();

        Map<String, PropertyColumn> properties = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getProperties();
        assertTrue(properties.containsKey("name1"));

        properties = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B").get().getProperties();
        assertTrue(properties.containsKey("name2"));

        properties = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("A").get().getProperties();
        assertTrue(properties.containsKey("name3"));
    }

    @Test
    public void testModern() {
        createModernSchema();
        this.sqlgGraph.tx().rollback();
        //test nothing is created
        assertTrue(this.sqlgGraph.getTopology().getAllTables().isEmpty());
        createModernSchema();
        this.sqlgGraph.tx().commit();
        assertEquals(4, this.sqlgGraph.getTopology().getAllTables().size());
        assertEquals(2, this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_person").size());
        assertEquals(2, this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_software").size());
        assertEquals(PropertyDefinition.of(PropertyType.STRING), this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_person").get("name"));
        assertEquals(PropertyType.INTEGER, this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_person").get("age").propertyType());
        assertEquals(PropertyDefinition.of(PropertyType.STRING), this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_software").get("name"));
        assertEquals(PropertyDefinition.of(PropertyType.STRING), this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_software").get("lang"));
        //test this by turning sql logging on and watch for create statements.
        loadModern();
    }

    @Test
    public void testAddVertexProperties() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist("A", Collections.emptyMap());
        this.sqlgGraph.tx().commit();
        assertEquals(1, this.sqlgGraph.getTopology().getAllTables().size());
        assertTrue(this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_A").isEmpty());

        Map<String, PropertyDefinition> properties = new HashMap<>();
        properties.put("name", PropertyDefinition.of(PropertyType.STRING));
        properties.put("age", PropertyDefinition.of(PropertyType.INTEGER));
        assertTrue(this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_A").isEmpty());
        this.sqlgGraph.getTopology().ensureVertexLabelPropertiesExist("A", properties);
        this.sqlgGraph.tx().rollback();
        assertTrue(this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_A").isEmpty());
        this.sqlgGraph.getTopology().ensureVertexLabelExist("A", Collections.emptyMap());
        this.sqlgGraph.getTopology().ensureVertexLabelPropertiesExist("A", properties);
        this.sqlgGraph.tx().commit();
        assertEquals(1, this.sqlgGraph.getTopology().getAllTables().size());
        assertEquals(2, this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_A").size());
        assertEquals(PropertyDefinition.of(PropertyType.STRING), this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_A").get("name"));
        assertEquals(PropertyType.INTEGER, this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_A").get("age").propertyType());
    }

    @Test
    public void testAddEdgeProperties() {
        VertexLabel outVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("A");
        VertexLabel inVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("B");
        this.sqlgGraph.getTopology().ensureEdgeLabelExist("ab", outVertexLabel, inVertexLabel, Collections.emptyMap());
        this.sqlgGraph.tx().commit();
        Map<String, PropertyDefinition> properties = new HashMap<>();
        properties.put("name", PropertyDefinition.of(PropertyType.STRING));
        properties.put("age", PropertyDefinition.of(PropertyType.INTEGER));
        assertTrue(this.sqlgGraph.getTopology().getAllTables().entrySet().stream().allMatch((entry) -> entry.getValue().isEmpty()));
        assertTrue(this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".E_ab").isEmpty());
        this.sqlgGraph.getTopology().ensureEdgePropertiesExist("ab", properties);
        this.sqlgGraph.tx().rollback();
        assertTrue(this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".E_ab").isEmpty());
        this.sqlgGraph.getTopology().ensureEdgePropertiesExist("ab", properties);
        this.sqlgGraph.tx().commit();
        Assert.assertFalse(this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".E_ab").isEmpty());
    }

    @Test
    public void testVertexLabelPropertiesViaVertexLabel() {
        Schema schema = this.sqlgGraph.getTopology().getPublicSchema();
        this.sqlgGraph.addVertex(T.label, "Person");
        Optional<VertexLabel> vertexLabel = schema.getVertexLabel("Person");
        assertTrue(vertexLabel.isPresent());
        this.sqlgGraph.tx().rollback();
        vertexLabel = schema.getVertexLabel("Person");
        Assert.assertFalse(vertexLabel.isPresent());

        this.sqlgGraph.addVertex(T.label, "Person");
        vertexLabel = schema.getVertexLabel("Person");
        assertTrue(vertexLabel.isPresent());
        this.sqlgGraph.tx().commit();
        vertexLabel = schema.getVertexLabel("Person");
        assertTrue(vertexLabel.isPresent());

        vertexLabel = schema.getVertexLabel("Person");
        assertTrue(vertexLabel.isPresent());
        Map<String, PropertyDefinition> properties = new HashMap<>();
        properties.put("name", PropertyDefinition.of(PropertyType.STRING));
        properties.put("age", PropertyDefinition.of(PropertyType.INTEGER));
        vertexLabel.get().ensurePropertiesExist(properties);
        assertEquals(2, vertexLabel.get().getProperties().size());
        this.sqlgGraph.tx().rollback();
        assertEquals(0, vertexLabel.get().getProperties().size());

        vertexLabel.get().ensurePropertiesExist(properties);
        this.sqlgGraph.tx().commit();
        assertEquals(2, vertexLabel.get().getProperties().size());
        PropertyColumn propertyColumnName = vertexLabel.get().getProperties().get("name");
        PropertyColumn propertyColumnAge = vertexLabel.get().getProperties().get("age");
        Assert.assertNotNull(propertyColumnName);
        Assert.assertNotNull(propertyColumnAge);
        assertEquals(PropertyDefinition.of(PropertyType.STRING), propertyColumnName.getPropertyDefinition());
        assertEquals(PropertyType.INTEGER, propertyColumnAge.getPropertyType());
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testEdgeLabelsProperties() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();

        Optional<VertexLabel> vertexLabelAOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A");
        assertTrue(vertexLabelAOptional.isPresent());
        Optional<VertexLabel> vertexLabelBOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("B");
        assertTrue(vertexLabelBOptional.isPresent());
        Optional<EdgeLabel> edgeLabelOptional = vertexLabelAOptional.get().getOutEdgeLabel("ab");
        assertTrue(edgeLabelOptional.isPresent());

        Map<String, PropertyDefinition> properties = new HashMap<>();
        properties.put("name", PropertyDefinition.of(PropertyType.STRING));
        properties.put("age", PropertyDefinition.of(PropertyType.INTEGER));
        EdgeLabel edgeLabel = edgeLabelOptional.get();
        edgeLabel.ensurePropertiesExist(properties);
        this.sqlgGraph.tx().rollback();

        edgeLabel = vertexLabelAOptional.get().getOutEdgeLabel("ab").get();
        assertTrue(edgeLabel.getProperties().isEmpty());

        edgeLabel = vertexLabelAOptional.get().getOutEdgeLabel("ab").get();
        edgeLabel.ensurePropertiesExist(properties);
        this.sqlgGraph.tx().commit();

        edgeLabel = vertexLabelAOptional.get().getOutEdgeLabel("ab").get();
        assertEquals(2, edgeLabel.getProperties().size());
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testAddEdgeLabelViaOutVertexLabel() {
        VertexLabel a = this.sqlgGraph.getTopology()
                .ensureSchemaExist("A")
                .ensureVertexLabelExist("A");
        Optional<Schema> schemaOptional = this.sqlgGraph.getTopology().getSchema("A");
        assertTrue(schemaOptional.isPresent());
        VertexLabel b = schemaOptional.get().ensureVertexLabelExist("B");
        a.ensureEdgeLabelExist("ab", b);
        this.sqlgGraph.tx().commit();

        Optional<EdgeLabel> edgeLabel = this.sqlgGraph.getTopology().getSchema("A").get().getEdgeLabel("ab");
        assertTrue(edgeLabel.isPresent());
        assertEquals("ab", edgeLabel.get().getLabel());
    }

    private void createModernSchema() {
        Map<String, PropertyDefinition> properties = new HashMap<>();
        properties.put("name", PropertyDefinition.of(PropertyType.STRING));
        properties.put("age", PropertyDefinition.of(PropertyType.INTEGER));
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("person", properties);
        properties.remove("age");
        properties.put("name", PropertyDefinition.of(PropertyType.STRING));
        properties.put("lang", PropertyDefinition.of(PropertyType.STRING));
        VertexLabel softwareVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("software", properties);
        properties.clear();
        properties.put("weight", PropertyDefinition.of(PropertyType.DOUBLE));
        this.sqlgGraph.getTopology().ensureEdgeLabelExist("knows", personVertexLabel, personVertexLabel, properties);
        this.sqlgGraph.getTopology().ensureEdgeLabelExist("created", personVertexLabel, softwareVertexLabel, properties);
    }

}
