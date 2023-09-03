package org.umlg.sqlg.test;

import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;

import java.net.URL;
import java.util.List;

public class TestTest extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTest.class);

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
    public void testNoOutEdge() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        List<Vertex> bs = this.sqlgGraph.traversal().V().hasLabel("A")
                .out("ab")
                .toList();
        Assert.assertEquals(1, bs.size());
        Assert.assertEquals(b1, bs.get(0));
        List<Vertex> aWithNoBs = this.sqlgGraph.traversal().V().hasLabel("A")
                .not(__.out("ab"))
                .toList();
        Assert.assertEquals(1, aWithNoBs.size());
        Assert.assertEquals(a2, aWithNoBs.get(0));
    }

    //    @Test
    public void testNull() {
        this.sqlgGraph.addVertex(T.label, "A", "name", "John");
        this.sqlgGraph.addVertex(T.label, "A", "name", null);
        this.sqlgGraph.tx().commit();

        List<Vertex> aas = this.sqlgGraph.traversal().V().hasLabel("A").hasNot("name").toList();
        Assert.assertEquals(1, aas.size());
    }


//    @Test
//    public void testValues() {
//        this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "surname", "b1");
//        this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "surname", "b2");
//        this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "surname", "b3");
//        this.sqlgGraph.tx().commit();
//
//        List<Object> whats = this.sqlgGraph.traversal().V().hasLabel("A")
//                .values(T.id.getAccessor())
//                .with(WithOptions.tokens)
//                .toList();
//        for (Object what : whats) {
//            System.out.println(what);
//        }
//
//    }
//
////    @Test
//    public void testTimeout() throws InterruptedException {
//        StopWatch stopWatch = StopWatch.createStarted();
//        this.sqlgGraph.addVertex(T.label, "A");
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
//        for (int i = 0; i < 1000; i++) {
//            Thread.sleep(1_000);
//            stopWatch.split();
//            LOGGER.info("count {}", stopWatch.toSplitString());
//            stopWatch.unsplit();
//        }
//        this.sqlgGraph.tx().rollback();
//    }
//
////    @Test
//    public void test() throws InterruptedException {
//
//        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
//            TopologyListenerTest tlt = new TopologyListenerTest();
//            sqlgGraph1.getTopology().registerListener(tlt);
//
//            Vertex a = this.sqlgGraph.addVertex(T.label, "A");
//            Vertex b = this.sqlgGraph.addVertex(T.label, "B");
//            Vertex c = this.sqlgGraph.addVertex(T.label, "C");
//            a.addEdge("abc", b);
//            a.addEdge("abc", c);
//            this.sqlgGraph.tx().commit();
//            Thread.sleep(3_000);
//
//            EdgeLabel abc = this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabel("abc").orElseThrow();
//            abc.remove();
//            this.sqlgGraph.tx().commit();
//            Thread.sleep(10_000);
//
//        }
//
//        System.out.println("");
//    }
//
//    public static class TopologyListenerTest implements TopologyListener {
//
//        public TopologyListenerTest(List<Triple<TopologyInf, TopologyInf, TopologyChangeAction>> topologyListenerTriple) {
//            super();
//        }
//
//        public TopologyListenerTest() {
//
//        }
//
//        @Override
//        public void change(TopologyInf topologyInf, TopologyInf oldValue, TopologyChangeAction action, boolean beforeCommit) {
//            String s = topologyInf.toString();
//            assertNotNull(s);
//            assertTrue(s + "does not contain " + topologyInf.getName(), s.contains(topologyInf.getName()));
//            if (topologyInf instanceof  EdgeLabel edgeLabel && action == TopologyChangeAction.DELETE) {
//                System.out.println(edgeLabel.getSchema());
//            }
//        }
//
//    }
}
