package org.umlg.sqlg.test.topology;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.TopologyChangeAction;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.test.BaseTest;
import org.umlg.sqlg.test.topology.TestTopologyChangeListener.TopologyListenerTest;

import java.net.URL;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/08/04
 */
public class TestTopologySchemaDeleteMultipleGraphs extends BaseTest {

    @SuppressWarnings("Duplicates")
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
    public void testRemoveSchemaWithEdgesAcrossSchemas() throws InterruptedException {

        TopologyListenerTest tlt1 = new TopologyListenerTest();
        TopologyListenerTest tlt2 = new TopologyListenerTest();
        this.sqlgGraph.getTopology().registerListener(tlt1);
        this.sqlgGraph1.getTopology().registerListener(tlt2);

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "halo");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B", "name", "halo");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C.C", "name", "halo");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", c1);
        this.sqlgGraph.tx().commit();

        Thread.sleep(2_000);

        Assert.assertTrue(this.sqlgGraph.getTopology().getAllTables().containsKey("A.V_A"));
        Assert.assertTrue(this.sqlgGraph1.getTopology().getAllTables().containsKey("A.V_A"));

        Schema schemaA = this.sqlgGraph.getTopology().getSchema("A").orElseThrow(IllegalStateException::new);
        schemaA.remove(false);
        this.sqlgGraph.tx().commit();

        Thread.sleep(2_000);

        Assert.assertFalse(this.sqlgGraph.getTopology().getAllTables().containsKey("A.V_A"));
        Assert.assertFalse(this.sqlgGraph1.getTopology().getAllTables().containsKey("A.V_A"));
        Assert.assertTrue(tlt1.receivedEvent(schemaA, TopologyChangeAction.DELETE));
        Assert.assertTrue(tlt2.receivedEvent(schemaA, TopologyChangeAction.DELETE));

    }

}
