package org.umlg.sqlg.test.schema;

import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyColumn;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.test.BaseTest;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Date: 2016/12/03
 * Time: 9:08 PM
 */
public class TestGlobalUniqueIndex extends BaseTest {

    @Test
    public void testGlobalUniqueIndex() {
        Map<String, PropertyType> properties = new HashMap<>();
        properties.put("namec", PropertyType.STRING);
        properties.put("namea", PropertyType.STRING);
        properties.put("nameb", PropertyType.STRING);
        this.sqlgGraph.getTopology().ensureVertexLabelExist("A", properties);
        this.sqlgGraph.tx().commit();

        Collection<PropertyColumn> propertyColumns = this.sqlgGraph.getTopology().getPublicSchema().getVertexLabel("A").get().getProperties().values();
        this.sqlgGraph.getTopology().ensureGlobalUniqueIndexExist(new HashSet<>(propertyColumns));
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.addVertex(T.label, "A", "namea", "a");
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
