package org.umlg.sqlg.test.github;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/04/26
 * Time: 5:02 PM
 */
public class TestGithub extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            configuration.setProperty("cache.vertices", true);
            if (!configuration.containsKey("jdbc.url")) {
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
            }
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

//    @Test
    public void edgeUpdate() {
        Vertex a = this.sqlgGraph.addVertex("A");
        Vertex b = this.sqlgGraph.addVertex("B");
        Edge a2b = a.addEdge("a2b", b);
        a2b.property("someKey", "someValue");

        Edge found_a2b = this.sqlgGraph.traversal().E().has("someKey", "someValue").next();
        found_a2b.property("anotherKey", "anotherValue");

        this.sqlgGraph.tx().commit();

        assertEquals("someValue", found_a2b.property("someKey").value());
        assertEquals("anotherValue", found_a2b.property("anotherKey").value());
        assertEquals("someValue", a2b.property("someKey").value());
//        assertEquals("anotherValue", a2b.property("anotherKey").value());
    }

    @Test
    public void testStaleEdge() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a1Again = this.sqlgGraph.traversal().V().hasLabel("A").next();
        this.sqlgGraph.tx().commit();
        a1Again.property("nameAgain", "a1");
        this.sqlgGraph.tx().commit();
        assertEquals("a1", a1.property("name").value());
        assertEquals("a1", a1.property("nameAgain").value());
    }
}
