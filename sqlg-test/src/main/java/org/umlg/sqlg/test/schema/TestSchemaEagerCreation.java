package org.umlg.sqlg.test.schema;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Date: 2016/11/23
 * Time: 6:03 PM
 */
public class TestSchemaEagerCreation extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsTransactionalSchema());
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
        assertEquals(PropertyType.STRING, this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_person").get("name"));
        assertEquals(PropertyType.INTEGER, this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_person").get("age"));
        assertEquals(PropertyType.STRING, this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_software").get("name"));
        assertEquals(PropertyType.STRING, this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_software").get("lang"));
        //test this by turning sql logging on and watch for create statements.
        loadModern();
    }

    @Test
    public void testAddVertexProperties() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist("A", Collections.emptyMap());
        this.sqlgGraph.tx().commit();
        assertEquals(1, this.sqlgGraph.getTopology().getAllTables().size());
        assertTrue(this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_A").isEmpty());

        Map<String, PropertyType> properties = new HashMap<>();
        properties.put("name", PropertyType.STRING);
        properties.put("age", PropertyType.INTEGER);
        assertTrue(this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_A").isEmpty());
        this.sqlgGraph.getTopology().ensureVertexLabelPropertiesExist("A", properties);
        this.sqlgGraph.tx().rollback();
        assertTrue(this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_A").isEmpty());
        this.sqlgGraph.getTopology().ensureVertexLabelExist("A", Collections.emptyMap());
        this.sqlgGraph.getTopology().ensureVertexLabelPropertiesExist("A", properties);
        this.sqlgGraph.tx().commit();
        assertEquals(1, this.sqlgGraph.getTopology().getAllTables().size());
        assertEquals(2, this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_A").size());
        assertEquals(PropertyType.STRING, this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_A").get("name"));
        assertEquals(PropertyType.INTEGER, this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".V_A").get("age"));
    }

    @Test
    public void testAddEdgeProperties() {
        VertexLabel outVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("A");
        VertexLabel inVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("B");
        this.sqlgGraph.getTopology().ensureEdgeLabelExist("ab", outVertexLabel, inVertexLabel, Collections.emptyMap());
        this.sqlgGraph.tx().commit();
        Map<String, PropertyType> properties = new HashMap<>();
        properties.put("name", PropertyType.STRING);
        properties.put("age", PropertyType.INTEGER);
        assertTrue(this.sqlgGraph.getTopology().getAllTables().entrySet().stream().allMatch((entry) -> entry.getValue().isEmpty()));
        assertTrue(this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".E_ab").isEmpty());
        this.sqlgGraph.getTopology().ensureEdgePropertiesExist("ab", properties);
        this.sqlgGraph.tx().rollback();
        assertTrue(this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".E_ab").isEmpty());
        this.sqlgGraph.getTopology().ensureEdgePropertiesExist("ab", properties);
        this.sqlgGraph.tx().commit();
        assertFalse(this.sqlgGraph.getTopology().getAllTables().get(this.sqlgGraph.getSqlDialect().getPublicSchema() + ".E_ab").isEmpty());
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testVertexLabelPropertiesViaVertexLabel() {
        Schema schema = this.sqlgGraph.getTopology().getPublicSchema();
        this.sqlgGraph.addVertex(T.label, "Person");
        Optional<VertexLabel> vertexLabel = schema.getVertexLabel("Person");
        assertTrue(vertexLabel.isPresent());
        this.sqlgGraph.tx().rollback();
        vertexLabel = schema.getVertexLabel("Person");
        assertFalse(vertexLabel.isPresent());

        this.sqlgGraph.addVertex(T.label, "Person");
        vertexLabel = schema.getVertexLabel("Person");
        assertTrue(vertexLabel.isPresent());
        this.sqlgGraph.tx().commit();
        vertexLabel = schema.getVertexLabel("Person");
        assertTrue(vertexLabel.isPresent());

        vertexLabel = schema.getVertexLabel("Person");
        assertTrue(vertexLabel.isPresent());
        Map<String, PropertyType> properties = new HashMap<>();
        properties.put("name", PropertyType.STRING);
        properties.put("age", PropertyType.INTEGER);
        vertexLabel.get().ensureColumnsExist(this.sqlgGraph, properties);
        assertEquals(2, vertexLabel.get().getProperties().size());
        this.sqlgGraph.tx().rollback();
        assertEquals(0, vertexLabel.get().getProperties().size());

        vertexLabel.get().ensureColumnsExist(this.sqlgGraph, properties);
        this.sqlgGraph.tx().commit();
        assertEquals(2, vertexLabel.get().getProperties().size());
        PropertyColumn propertyColumnName = vertexLabel.get().getProperties().get("name");
        PropertyColumn propertyColumnAge = vertexLabel.get().getProperties().get("age");
        assertNotNull(propertyColumnName);
        assertNotNull(propertyColumnAge);
        assertEquals(PropertyType.STRING, propertyColumnName.getPropertyType());
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

        Map<String, PropertyType> properties = new HashMap<>();
        properties.put("name", PropertyType.STRING);
        properties.put("age", PropertyType.INTEGER);
        EdgeLabel edgeLabel = edgeLabelOptional.get();
        edgeLabel.ensureColumnsExist(this.sqlgGraph, properties);
        this.sqlgGraph.tx().rollback();

        edgeLabel = vertexLabelAOptional.get().getOutEdgeLabel("ab").get();
        assertTrue(edgeLabel.getProperties().isEmpty());

        edgeLabel = vertexLabelAOptional.get().getOutEdgeLabel("ab").get();
        edgeLabel.ensureColumnsExist(this.sqlgGraph, properties);
        this.sqlgGraph.tx().commit();

        edgeLabel = vertexLabelAOptional.get().getOutEdgeLabel("ab").get();
        assertEquals(2, edgeLabel.getProperties().size());
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testEdgeLabelAddVertexLabels() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();
        List<Map<String, Vertex>> result =  this.sqlgGraph.traversal().V(a).as("a").out("ab").as("b").<Vertex>select("a", "b").toList();


        Optional<EdgeLabel> edgeLabelOptional = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab");
        assertTrue(edgeLabelOptional.isPresent());

        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(this.sqlgGraph, "C");
//        this.sqlgGraph.getTopology().ensuEd
    }

    private void createModernSchema() {
        Map<String, PropertyType> properties = new HashMap<>();
        properties.put("name", PropertyType.STRING);
        properties.put("age", PropertyType.INTEGER);
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("person", properties);
        properties.remove("age");
        properties.put("name", PropertyType.STRING);
        properties.put("lang", PropertyType.STRING);
        VertexLabel softwareVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("software", properties);
        properties.clear();
        properties.put("weight", PropertyType.DOUBLE);
        this.sqlgGraph.getTopology().ensureEdgeLabelExist("knows", personVertexLabel, personVertexLabel, properties);
        this.sqlgGraph.getTopology().ensureEdgeLabelExist("created", personVertexLabel, softwareVertexLabel, properties);
    }
}
