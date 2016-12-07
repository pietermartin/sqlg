package org.umlg.sqlg.test.schema;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.*;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Date: 2016/12/03
 * Time: 9:08 PM
 */
public class TestGlobalUniqueIndex extends BaseTest {

//    @Test
//    public void testGlobalUniqueIndexOnVertex() {
//        Map<String, PropertyType> properties = new HashMap<>();
//        properties.put("namec", PropertyType.STRING);
//        properties.put("namea", PropertyType.STRING);
//        properties.put("nameb", PropertyType.STRING);
//        this.sqlgGraph.getTopology().ensureVertexLabelExist("A", properties);
//        @SuppressWarnings("OptionalGetWithoutIsPresent")
//        Collection<PropertyColumn> propertyColumns = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getProperties().values();
//        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<>(propertyColumns));
//        this.sqlgGraph.tx().commit();
//
//        Schema globalUniqueIndexSchema = this.sqlgGraph.getTopology().getGlobalUniqueIndexSchema();
//        Optional<GlobalUniqueIndex> globalUniqueIndexOptional = globalUniqueIndexSchema.getGlobalUniqueIndex("namea_nameb_namec");
//        assertTrue(globalUniqueIndexOptional.isPresent());
//
//        Optional<PropertyColumn> nameaPropertyColumnOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getProperty("namea");
//        assertTrue(nameaPropertyColumnOptional.isPresent());
//        @SuppressWarnings("OptionalGetWithoutIsPresent")
//        Set<GlobalUniqueIndex> globalUniqueIndices = nameaPropertyColumnOptional.get().getGlobalUniqueIndices();
//        assertEquals(1, globalUniqueIndices.size());
//        assertEquals("namea_nameb_namec", globalUniqueIndices.iterator().next().getName());
//
//        this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
//        this.sqlgGraph.tx().commit();
//        try {
//            this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
//            fail("GlobalUniqueIndex should prevent this from executing");
//        } catch (Exception e) {
//            //swallow
//        }
//        this.sqlgGraph.tx().rollback();
//        this.sqlgGraph.addVertex(T.label, "A", "namea", "aa");
//        this.sqlgGraph.tx().commit();
//    }
//
//    @Test
//    public void testGlobalUniqueIndexOnVertexNormalBatchMode() {
//        Map<String, PropertyType> properties = new HashMap<>();
//        properties.put("namec", PropertyType.STRING);
//        properties.put("namea", PropertyType.STRING);
//        properties.put("nameb", PropertyType.STRING);
//        this.sqlgGraph.getTopology().ensureVertexLabelExist("A", properties);
//        @SuppressWarnings("OptionalGetWithoutIsPresent")
//        Collection<PropertyColumn> propertyColumns = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getProperties().values();
//        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<>(propertyColumns));
//        this.sqlgGraph.tx().commit();
//
//        Schema globalUniqueIndexSchema = this.sqlgGraph.getTopology().getGlobalUniqueIndexSchema();
//        Optional<GlobalUniqueIndex> globalUniqueIndexOptional = globalUniqueIndexSchema.getGlobalUniqueIndex("namea_nameb_namec");
//        assertTrue(globalUniqueIndexOptional.isPresent());
//
//        Optional<PropertyColumn> nameaPropertyColumnOptional = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getProperty("namea");
//        assertTrue(nameaPropertyColumnOptional.isPresent());
//        @SuppressWarnings("OptionalGetWithoutIsPresent")
//        Set<GlobalUniqueIndex> globalUniqueIndices = nameaPropertyColumnOptional.get().getGlobalUniqueIndices();
//        assertEquals(1, globalUniqueIndices.size());
//        assertEquals("namea_nameb_namec", globalUniqueIndices.iterator().next().getName());
//
//        this.sqlgGraph.tx().normalBatchModeOn();
//        this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
//        this.sqlgGraph.tx().commit();
//        try {
//            this.sqlgGraph.tx().normalBatchModeOn();
//            this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
//            this.sqlgGraph.tx().commit();
//            fail("GlobalUniqueIndex should prevent this from executing");
//        } catch (Exception e) {
//            //swallow
//        }
//        this.sqlgGraph.tx().rollback();
//        this.sqlgGraph.tx().normalBatchModeOn();
//        this.sqlgGraph.addVertex(T.label, "A", "namea", "aa");
//        this.sqlgGraph.tx().commit();
//    }

    @Test
    public void testGlobalUniqueIndexOnEdge() {
        Map<String, PropertyType> properties = new HashMap<>();
        properties.put("name", PropertyType.STRING);
        VertexLabel vertexLabelA = this.sqlgGraph.getTopology().ensureVertexLabelExist("A", properties);
        VertexLabel vertexLabelB = this.sqlgGraph.getTopology().ensureVertexLabelExist("B", properties);
        properties.clear();
        properties.put("namea", PropertyType.STRING);
        properties.put("nameb", PropertyType.STRING);
        properties.put("namec", PropertyType.STRING);
        vertexLabelA.ensureEdgeLabelExist(this.sqlgGraph, "ab", vertexLabelB, properties);
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Collection<PropertyColumn> propertyColumns = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getProperties().values();
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<>(propertyColumns));
        this.sqlgGraph.tx().commit();

        Schema globalUniqueIndexSchema = this.sqlgGraph.getTopology().getGlobalUniqueIndexSchema();
        Optional<GlobalUniqueIndex> globalUniqueIndexOptional = globalUniqueIndexSchema.getGlobalUniqueIndex("namea_nameb_namec");
        assertTrue(globalUniqueIndexOptional.isPresent());

        Optional<PropertyColumn> nameaPropertyColumnOptional = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getProperty("namea");
        assertTrue(nameaPropertyColumnOptional.isPresent());
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Set<GlobalUniqueIndex> globalUniqueIndices = nameaPropertyColumnOptional.get().getGlobalUniqueIndices();
        assertEquals(1, globalUniqueIndices.size());
        assertEquals("namea_nameb_namec", globalUniqueIndices.iterator().next().getName());

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        a1.addEdge("ab", b1, "namea", "a", "nameb", "b", "namec", "c");
        this.sqlgGraph.tx().commit();
        try {
            a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
            b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
            a1.addEdge("ab", b1, "namea", "a", "nameb", "b", "namec", "c");
            fail("GlobalUniqueIndex should prevent this from executing");
        } catch (Exception e) {
            //swallow
        }
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.addVertex(T.label, "A", "namea", "aa");
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testGlobalUniqueIndexOnEdgeNormalBatchMode() {
        Map<String, PropertyType> properties = new HashMap<>();
        properties.put("name", PropertyType.STRING);
        VertexLabel vertexLabelA = this.sqlgGraph.getTopology().ensureVertexLabelExist("A", properties);
        VertexLabel vertexLabelB = this.sqlgGraph.getTopology().ensureVertexLabelExist("B", properties);
        properties.clear();
        properties.put("namea", PropertyType.STRING);
        properties.put("nameb", PropertyType.STRING);
        properties.put("namec", PropertyType.STRING);
        vertexLabelA.ensureEdgeLabelExist(this.sqlgGraph, "ab", vertexLabelB, properties);
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Collection<PropertyColumn> propertyColumns = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getProperties().values();
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<>(propertyColumns));
        this.sqlgGraph.tx().commit();

        Schema globalUniqueIndexSchema = this.sqlgGraph.getTopology().getGlobalUniqueIndexSchema();
        Optional<GlobalUniqueIndex> globalUniqueIndexOptional = globalUniqueIndexSchema.getGlobalUniqueIndex("namea_nameb_namec");
        assertTrue(globalUniqueIndexOptional.isPresent());

        Optional<PropertyColumn> nameaPropertyColumnOptional = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("ab").get().getProperty("namea");
        assertTrue(nameaPropertyColumnOptional.isPresent());
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        Set<GlobalUniqueIndex> globalUniqueIndices = nameaPropertyColumnOptional.get().getGlobalUniqueIndices();
        assertEquals(1, globalUniqueIndices.size());
        assertEquals("namea_nameb_namec", globalUniqueIndices.iterator().next().getName());

        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        a1.addEdge("ab", b1, "namea", "a", "nameb", "b", "namec", "c");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        try {
            a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
            b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
            a1.addEdge("ab", b1, "namea", "a", "nameb", "b", "namec", "c");
            this.sqlgGraph.tx().commit();
            fail("GlobalUniqueIndex should prevent this from executing");
        } catch (Exception e) {
            //swallow
        }
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().normalBatchModeOn();
        this.sqlgGraph.addVertex(T.label, "A", "namea", "aa");
        this.sqlgGraph.tx().commit();
    }

    //Lukas's tests
//    @Test
//    public void testVertexSingleLabelUniqueConstraint() throws Exception {
//        this.sqlgGraph.createVertexLabeledIndex("Person", "name", "a");
//        this.sqlgGraph.createVertexUniqueConstraint("name", "Person");
//        this.sqlgGraph.tx().commit();
//
//        this.sqlgGraph.addVertex(T.label, "Person", "name", "Joe");
//        this.sqlgGraph.tx().commit();
//
//        try {
//            this.sqlgGraph.addVertex(T.label, "Person", "name", "Joe");
//            Assert.fail("Should not have been possible to add 2 people with the same name.");
//        } catch (Exception e) {
//            //good
//            this.sqlgGraph.tx().rollback();
//        }
//    }
//
//    @Test
//    public void testVertexMultiLabelUniqueConstraint() throws Exception {
//        this.sqlgGraph.createVertexLabeledIndex("Chocolate", "name", "a");
//        this.sqlgGraph.createVertexLabeledIndex("Candy", "name", "a");
//        this.sqlgGraph.createVertexLabeledIndex("Icecream", "name", "a");
//        this.sqlgGraph.createVertexUniqueConstraint("name", "Chocolate", "Candy");
//
//        this.sqlgGraph.addVertex(T.label, "Chocolate", "name", "Yummy");
//        this.sqlgGraph.tx().commit();
//
//        try {
//            this.sqlgGraph.addVertex(T.label, "Candy", "name", "Yummy");
//            Assert.fail("A chocolate and a candy should not have the same name.");
//        } catch (Exception e) {
//            //good
//            this.sqlgGraph.tx().rollback();
//        }
//
//        this.sqlgGraph.addVertex(T.label, "Icecream", "name", "Yummy");
//        this.sqlgGraph.tx().commit();
//    }
//
//    @Test
//    public void testVertexMultipleConstraintsOnSingleProperty() throws Exception {
//        this.sqlgGraph.createVertexLabeledIndex("Chocolate", "name", "a");
//        this.sqlgGraph.createVertexLabeledIndex("Candy", "name", "a");
//        this.sqlgGraph.createVertexLabeledIndex("Icecream", "name", "a");
//        this.sqlgGraph.createVertexUniqueConstraint("name", "Chocolate");
//        this.sqlgGraph.createVertexUniqueConstraint("name", "Candy");
//
//        this.sqlgGraph.addVertex(T.label, "Chocolate", "name", "Yummy");
//        this.sqlgGraph.addVertex(T.label, "Candy", "name", "Yummy");
//        this.sqlgGraph.addVertex(T.label, "Icecream", "name", "Yummy");
//        this.sqlgGraph.addVertex(T.label, "Icecream", "name", "Yummy");
//        this.sqlgGraph.tx().commit();
//
//        try {
//            this.sqlgGraph.addVertex(T.label, "Chocolate", "name", "Yummy");
//            Assert.fail("Two chocolates should not have the same name.");
//        } catch (Exception e) {
//            //good
//            this.sqlgGraph.tx().rollback();
//        }
//
//        try {
//            this.sqlgGraph.addVertex(T.label, "Candy", "name", "Yummy");
//            Assert.fail("Two candies should not have the same name.");
//        } catch (Exception e) {
//            //good
//            this.sqlgGraph.tx().rollback();
//        }
//    }
//
//    @Test
//    public void testVertexConstraintOnAnyLabel() throws Exception {
//        this.sqlgGraph.createVertexLabeledIndex("Car", "name", "a");
//        this.sqlgGraph.createVertexUniqueConstraint("name");
//        this.sqlgGraph.createVertexLabeledIndex("Chocolate", "name", "a");
//        this.sqlgGraph.createVertexLabeledIndex("Candy", "name", "a");
//
//        this.sqlgGraph.addVertex(T.label, "Chocolate", "name", "Yummy");
//        this.sqlgGraph.tx().commit();
//
//        try {
//            this.sqlgGraph.addVertex(T.label, "Candy", "name", "Yummy");
//            Assert.fail("Should not be able to call a candy a pre-existing name.");
//        } catch (Exception e) {
//            //good
//            this.sqlgGraph.tx().rollback();
//        }
//
//        try {
//            this.sqlgGraph.addVertex(T.label, "Car", "name", "Yummy");
//            Assert.fail("Should not be able to call a car a pre-existing name.");
//        } catch (Exception e) {
//            //good
//            this.sqlgGraph.tx().rollback();
//        }
//    }
//
//    @Test
//    public void testUpdateUniqueProperty() throws Exception {
//        this.sqlgGraph.createVertexLabeledIndex("Person", "name", "a");
//        this.sqlgGraph.createVertexUniqueConstraint("name", "Person");
//        this.sqlgGraph.tx().commit();
//
//        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
//        v1.property("name", "Joseph");
//        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "Joe");
//        this.sqlgGraph.tx().commit();
//        v2 = this.sqlgGraph.v(v2.id());
//
//        try {
//            v2.property("name", "Joseph");
//            Assert.fail("Should not be able to call a person a pre-existing name.");
//        } catch (Exception e) {
//            //good
//        }
//    }
//
//    @Test
//    public void testDeleteUniqueProperty() throws Exception {
//        this.sqlgGraph.createVertexLabeledIndex("Person", "name", "a");
//        this.sqlgGraph.createVertexUniqueConstraint("name", "Person");
//        this.sqlgGraph.tx().commit();
//
//        Vertex v = this.sqlgGraph.addVertex(T.label, "Person", "name", "Joseph");
//        try {
//            this.sqlgGraph.addVertex(T.label, "Person", "name", "Joseph");
//            Assert.fail("Should not be able to call a person a pre-existing name.");
//        } catch (Exception e) {
//            //good
//        }
//
//        this.sqlgGraph.v(v.id()).remove();
//
//        this.sqlgGraph.addVertex(T.label, "Person", "name", "Joseph");
//    }

}
