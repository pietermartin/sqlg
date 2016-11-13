package org.umlg.sqlg.test.batch;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/11/11
 * Time: 8:54 PM
 */
public class TestBatchModeMultipleGraphs extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            Assume.assumeTrue(configuration.getString("jdbc.url").contains("postgresql"));
            configuration.addProperty("distributed", true);
            configuration.addProperty("maxPoolSize", 3);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testStreamingBatchModeOnMultipleGraphs() throws Exception {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            this.sqlgGraph.tx().streamingBatchModeOn();
            for (int i = 0; i < 10; i++) {
                this.sqlgGraph.streamVertex(T.label, "Person", "name", "asdasd");
            }
            this.sqlgGraph.tx().flush();
            for (int i = 0; i < 10; i++) {
                this.sqlgGraph.streamVertex(T.label, "Address", "name", "asdasd");
            }
            this.sqlgGraph.tx().commit();
            Thread.sleep(1000);
            assertEquals(this.sqlgGraph.traversal().V().toList(), sqlgGraph1.traversal().V().toList());
        }
    }

    @Test
    public void testNormalBatchModeOnMultipleGraphs() throws Exception {
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            this.sqlgGraph.tx().normalBatchModeOn();
            for (int i = 0; i < 10; i++) {
                Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "asdasd");
                Vertex address = this.sqlgGraph.addVertex(T.label, "Address", "name", "asdasd");
                person.addEdge("address", address, "name", "asdasd");
            }
            this.sqlgGraph.tx().commit();
            Thread.sleep(1000);
            assertEquals(this.sqlgGraph.traversal().V().toList(), sqlgGraph1.traversal().V().toList());
        }
    }
}
