package org.umlg.sqlg.test.topology2;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/09/04
 * Time: 8:27 PM
 */
public class TestTopology2 extends BaseTest {

//    @BeforeClass
//    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
//        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
//        try {
//            configuration = new PropertiesConfiguration(sqlProperties);
//            configuration.addProperty(SqlgGraph.DISTRIBUTED, true);
//            if (!configuration.containsKey(SqlgGraph.JDBC_URL))
//                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
//
//        } catch (ConfigurationException e) {
//            throw new RuntimeException(e);
//        }
//    }

    @Test
    public void testVertexEdgeAndPropertyCreation() {
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex car = this.sqlgGraph.addVertex(T.label, "Car");
        person.addEdge("drives", car);
        this.sqlgGraph.tx().commit();

        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Person").count().next().intValue());
        assertEquals(person, this.sqlgGraph.traversal().V().hasLabel("Person").next());
        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Car").count().next().intValue());
        assertEquals(car, this.sqlgGraph.traversal().V().hasLabel("Car").next());
        assertEquals(car, this.sqlgGraph.traversal().V(person).out().next());

        person.property("name", "john");
        this.sqlgGraph.tx().commit();

        assertEquals("john", this.sqlgGraph.traversal().V(person.id()).values("name").next());
    }

    @Test
    public void testVertexPlusPropertyCreation() {
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.tx().commit();
        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Person").count().next().intValue());
        assertEquals(person, this.sqlgGraph.traversal().V().hasLabel("Person").next());
        assertEquals("john", this.sqlgGraph.traversal().V(person.id()).values("name").next());
    }

    @Test
    public void testEdgePropertyCreation() {
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex car = this.sqlgGraph.addVertex(T.label, "Car");
        person.addEdge("drives", car, "how", "fast");
        this.sqlgGraph.tx().commit();

        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Person").count().next().intValue());
        assertEquals(person, this.sqlgGraph.traversal().V().hasLabel("Person").next());
        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Car").count().next().intValue());
        assertEquals(car, this.sqlgGraph.traversal().V().hasLabel("Car").next());
        assertEquals(car, this.sqlgGraph.traversal().V(person).out().next());

        person.property("name", "john");
        this.sqlgGraph.tx().commit();

        assertEquals("john", this.sqlgGraph.traversal().V(person.id()).values("name").next());

    }


}
