package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.util.List;

public class TestTest extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTest.class);

    //    @BeforeClass
//    public static void beforeClass() {
//        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
//        try {
//            Configurations configs = new Configurations();
//            configuration = configs.properties(sqlProperties);
//            Assume.assumeTrue(isPostgres());
//            configuration.addProperty(SqlgGraph.DISTRIBUTED, true);
//            if (!configuration.containsKey(SqlgGraph.JDBC_URL))
//                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", SqlgGraph.JDBC_URL));
//
//        } catch (ConfigurationException e) {
//            throw new IllegalStateException(e);
//        }
//    }

//    @Test
//    public void test1() {
//        loadModern();
//        for (EdgeLabel edgeLabel : this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabels().values()) {
//            edgeLabel.remove();
//        }
//        for (VertexLabel vertexLabel : this.sqlgGraph.getTopology().getPublicSchema().getVertexLabels().values()) {
//            vertexLabel.remove();
//        }
//        this.sqlgGraph.tx().commit();
//
//        Vertex a = this.sqlgGraph.addVertex(T.label, "a");
//        Vertex b = this.sqlgGraph.addVertex(T.label, "b");
//        a.addEdge("knows", b);
//        this.sqlgGraph.traversal().V().drop().iterate();
//        this.sqlgGraph.traversal().E().toList();
//    }
//
//    @Test
//    public void test2() {
//        Vertex a = this.sqlgGraph.addVertex(T.label, "a");
//        Vertex b = this.sqlgGraph.addVertex(T.label, "b");
//        a.addEdge("knows", b);
//        this.sqlgGraph.tx().commit();
//        for (EdgeLabel edgeLabel : this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabels().values()) {
//            edgeLabel.remove();
//        }
//        for (VertexLabel vertexLabel : this.sqlgGraph.getTopology().getPublicSchema().getVertexLabels().values()) {
//            vertexLabel.remove();
//        }
//        this.sqlgGraph.tx().commit();
//
//        Vertex alice = this.sqlgGraph.addVertex(T.label, "person", "name", "alice");
//        Vertex bob = this.sqlgGraph.addVertex(T.label, "person", "name", "bob");
//        alice.addEdge("knows", bob, "weight", 1);
//        List<Integer> weights = this.sqlgGraph.traversal().V().bothE().<Integer>values("weight").toList();
//    }

    @Test
    public void test3() {
        loadModern();
        for (EdgeLabel edgeLabel : this.sqlgGraph.getTopology().getPublicSchema().getEdgeLabels().values()) {
            edgeLabel.remove();
        }
        for (VertexLabel vertexLabel : this.sqlgGraph.getTopology().getPublicSchema().getVertexLabels().values()) {
            vertexLabel.remove();
        }
        Vertex a = this.sqlgGraph.addVertex(T.label, "a");
        Vertex b = this.sqlgGraph.addVertex(T.label, "b");
        a.addEdge("knows", b);
        this.sqlgGraph.traversal().V().drop().iterate();
        List<Edge> edges = this.sqlgGraph.traversal().E().hasLabel("knows").toList();
        Assert.assertEquals(1, edges.size());

        this.sqlgGraph.tx().commit();
    }

}
