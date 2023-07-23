package org.umlg.sqlg.test;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.TopologyChangeAction;
import org.umlg.sqlg.structure.TopologyInf;
import org.umlg.sqlg.structure.TopologyListener;
import org.umlg.sqlg.structure.topology.EdgeLabel;

import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestTest extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            Assume.assumeTrue(isPostgres());
            configuration.addProperty(SqlgGraph.DISTRIBUTED, true);
            if (!configuration.containsKey(SqlgGraph.JDBC_URL))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", SqlgGraph.JDBC_URL));

        } catch (ConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void test() throws InterruptedException {

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            TopologyListenerTest tlt = new TopologyListenerTest();
            sqlgGraph1.getTopology().registerListener(tlt);

            Vertex a = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
            Vertex c = this.sqlgGraph.addVertex(T.label, "C");
            a.addEdge("abc", b);
            a.addEdge("abc", c);
            this.sqlgGraph.tx().commit();
            Thread.sleep(3_000);

            EdgeLabel abc = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("abc").orElseThrow();
            abc.remove();
            this.sqlgGraph.tx().commit();
            Thread.sleep(10_000);

        }

        System.out.println("");
    }

    public static class TopologyListenerTest implements TopologyListener {

        public TopologyListenerTest(List<Triple<TopologyInf, TopologyInf, TopologyChangeAction>> topologyListenerTriple) {
            super();
        }

        public TopologyListenerTest() {

        }

        @Override
        public void change(TopologyInf topologyInf, TopologyInf oldValue, TopologyChangeAction action, boolean beforeCommit) {
            String s = topologyInf.toString();
            assertNotNull(s);
            assertTrue(s + "does not contain " + topologyInf.getName(), s.contains(topologyInf.getName()));
            if (topologyInf instanceof  EdgeLabel edgeLabel && action == TopologyChangeAction.DELETE) {
                System.out.println(edgeLabel.getSchema());
            }
        }

    }
}
