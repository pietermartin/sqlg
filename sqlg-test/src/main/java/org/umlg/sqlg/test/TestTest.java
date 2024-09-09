package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeDefinition;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;

import java.util.LinkedHashMap;
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

    @Test
    public void testFriendOfFriend() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        }});
        friendVertexLabel.ensureEdgeLabelExist(
                "of",
                friendVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1)
                )
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();

        Vertex john = sqlgGraph.addVertex(T.label, "Friend", "name", "John");
        Vertex peter = sqlgGraph.addVertex(T.label, "Friend", "name", "Peter");
        Vertex dave = sqlgGraph.addVertex(T.label, "Friend", "name", "Dave");
        Vertex mike = sqlgGraph.addVertex(T.label, "Friend", "name", "Mike");
        Vertex luke = sqlgGraph.addVertex(T.label, "Friend", "name", "Luke");
        
        john.addEdge("of", peter);
        peter.addEdge("of", dave);
        dave.addEdge("of", mike);
        mike.addEdge("of", luke);
        this.sqlgGraph.tx().commit();

        List<String> friendsName = this.sqlgGraph.traversal().V().hasLabel("Friend")
                .<String>values("name")
                .toList();
        Assert.assertEquals(4, friendsName.size());
        List<Vertex> friends = this.sqlgGraph.traversal().V().hasLabel("Friend")
                .toList();
        Assert.assertEquals(4, friends.size());
        Vertex friend = friends.get(0);
        Assert.assertEquals("John", friend.property("name").value());

        GraphTraversal<Vertex, Path> graphTraversal = this.sqlgGraph.traversal().V().hasLabel("Friend").has("name", "Peter")
                .repeat(__.both("of").simplePath()).until(__.not(__.both("of").simplePath()))
                .path();
        List<Path> paths = graphTraversal.toList();
        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(peter) && p.get(1).equals(john)));

    }

    //    @Test
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
