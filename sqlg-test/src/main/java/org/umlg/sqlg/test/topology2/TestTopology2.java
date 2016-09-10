package org.umlg.sqlg.test.topology2;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/09/04
 * Time: 8:27 PM
 */
public class TestTopology2 extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            configuration.addProperty(SqlgGraph.DISTRIBUTED, true);
            if (!configuration.containsKey(SqlgGraph.JDBC_URL))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testVertexCreation() {
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();

        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Person").count().next().intValue());
        assertEquals(person, this.sqlgGraph.traversal().V().hasLabel("Person").next());
    }


}
