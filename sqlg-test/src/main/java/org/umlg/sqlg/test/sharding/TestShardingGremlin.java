package org.umlg.sqlg.test.sharding;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.PropertyColumn;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/04/11
 */
public class TestShardingGremlin extends BaseTest {

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

    @Before
    public void before() throws Exception {
        super.before();
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsSharding());
    }

    @Test
    public void testSinglePath() throws InterruptedException {
        @SuppressWarnings("Duplicates")
        VertexLabel person = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new HashMap<String, PropertyType>(){{
                    put("uid", PropertyType.STRING);
                    put("name", PropertyType.STRING);
                    put("surname", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country"))
        );
        PropertyColumn country = person.getProperty("country").orElseThrow(IllegalStateException::new);
        person.ensureDistributed(4, country);

        VertexLabel address = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Address",
                new HashMap<String, PropertyType>(){{
                    put("uid", PropertyType.STRING);
                    put("street", PropertyType.STRING);
                    put("suburb", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country"))
        );
        country = address.getProperty("country").orElseThrow(IllegalStateException::new);
        address.ensureDistributed(4, country, person);

        @SuppressWarnings("unused")
        EdgeLabel livesAt = person.ensureEdgeLabelExist(
                "livesAt",
                address,
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country"))
        );
        country = livesAt.getProperty("country").orElseThrow(IllegalStateException::new);
        livesAt.ensureDistributed(4, country);
        this.sqlgGraph.tx().commit();

        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "John", "surname", "Smith", "country", "SA");
        Vertex address1 = this.sqlgGraph.addVertex(T.label, "Address", "uid", UUID.randomUUID().toString(), "street", "X", "suburb", "Y", "country", "SA");
        Edge livesAt1 = person1.addEdge("livesAt", address1, "uid", UUID.randomUUID().toString(), "country", "SA");
        this.sqlgGraph.tx().commit();

        List<Edge> livesAtEdges = this.sqlgGraph.traversal().V().hasLabel("Person").outE().toList();
        Assert.assertEquals(1, livesAtEdges.size());
        Assert.assertEquals(livesAt1, livesAtEdges.get(0));

        List<Vertex> livesAts = this.sqlgGraph.traversal().V().hasLabel("Person").out().toList();
        Assert.assertEquals(1, livesAts.size());
        Assert.assertEquals(address1, livesAts.get(0));
        Assert.assertEquals("X", livesAts.get(0).value("street"));
        Assert.assertEquals("Y", livesAts.get(0).value("suburb"));

        List<Map<String, Object>> result = this.sqlgGraph.traversal()
                .V().hasLabel("Person").as("a")
                .outE().as("b")
                .otherV().as("c")
                .select("a", "b", "c")
                .toList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3, result.get(0).size());
        Assert.assertEquals(person1, result.get(0).get("a"));
        Assert.assertEquals(livesAt1, result.get(0).get("b"));
        Assert.assertEquals(address1, result.get(0).get("c"));

        livesAtEdges = this.sqlgGraph.traversal().V().hasLabel("Address").inE().toList();
        Assert.assertEquals(1, livesAtEdges.size());
        Assert.assertEquals(livesAt1, livesAtEdges.get(0));

        List<Vertex> persons = this.sqlgGraph.traversal().V().hasLabel("Address").in().toList();
        Assert.assertEquals(1, persons.size());
        Assert.assertEquals(person1, persons.get(0));
        Assert.assertEquals("John", persons.get(0).value("name"));
        Assert.assertEquals("Smith", persons.get(0).value("surname"));

        result = this.sqlgGraph.traversal()
                .V().hasLabel("Address").as("a")
                .inE().as("b")
                .otherV().as("c")
                .select("a", "b", "c")
                .toList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3, result.get(0).size());
        Assert.assertEquals(address1, result.get(0).get("a"));
        Assert.assertEquals(livesAt1, result.get(0).get("b"));
        Assert.assertEquals(person1, result.get(0).get("c"));

        Thread.sleep(1000);

        result = this.sqlgGraph1.traversal()
                .V().hasLabel("Address").as("a")
                .inE().as("b")
                .otherV().as("c")
                .select("a", "b", "c")
                .toList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(3, result.get(0).size());
        Assert.assertEquals(address1, result.get(0).get("a"));
        Assert.assertEquals(livesAt1, result.get(0).get("b"));
        Assert.assertEquals(person1, result.get(0).get("c"));
    }

    @Test
    public void testDuplicatePath() throws InterruptedException {
        @SuppressWarnings("Duplicates")
        VertexLabel person = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("name", PropertyType.STRING);
                    put("surname", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country"))
        );
        person.ensureDistributed(32, person.getProperty("country").orElseThrow(IllegalStateException::new));
        @SuppressWarnings("unused")
        EdgeLabel livesAt = person.ensureEdgeLabelExist(
                "loves",
                person,
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country"))
        );
        livesAt.ensureDistributed(32, livesAt.getProperty("country").orElseThrow(IllegalStateException::new));
        this.sqlgGraph.tx().commit();

        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "John", "surname", "Smith", "country", "SA");
        Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "Suzi", "surname", "Lovenot", "country", "SA");
        person1.addEdge("loves", person2, "uid", UUID.randomUUID().toString(), "country", "SA");
        person2.addEdge("loves", person1, "uid", UUID.randomUUID().toString(), "country", "SA");
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").out().toList();
        Assert.assertEquals(2, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").in().toList();
        Assert.assertEquals(2, vertices.size());

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").outE().inV().toList();
        Assert.assertEquals(2, vertices.size());

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").outE().outV().toList();
        Assert.assertEquals(2, vertices.size());

        Thread.sleep(1000);

        vertices = this.sqlgGraph1.traversal().V().hasLabel("Person").out().toList();
        Assert.assertEquals(2, vertices.size());
        vertices = this.sqlgGraph1.traversal().V().hasLabel("Person").in().toList();
        Assert.assertEquals(2, vertices.size());

        vertices = this.sqlgGraph1.traversal().V().hasLabel("Person").outE().inV().toList();
        Assert.assertEquals(2, vertices.size());

        vertices = this.sqlgGraph1.traversal().V().hasLabel("Person").outE().outV().toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testDuplicatePath2() throws InterruptedException {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "A",
                new HashMap<String, PropertyType>(){{
                    put("uid", PropertyType.STRING);
                    put("name", PropertyType.STRING);
                    put("name2", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country"))
        );
        aVertexLabel.ensureDistributed(4, aVertexLabel.getProperty("country").orElseThrow(IllegalStateException::new));
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "B",
                new HashMap<String, PropertyType>(){{
                    put("uid", PropertyType.STRING);
                    put("name", PropertyType.STRING);
                    put("name2", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country"))
        );
        bVertexLabel.ensureDistributed(4, bVertexLabel.getProperty("country").orElseThrow(IllegalStateException::new), aVertexLabel);
        @SuppressWarnings("unused")
        EdgeLabel livesAt = aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid", PropertyType.STRING);
                    put("country", PropertyType.STRING);
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country"))
        );
        livesAt.ensureDistributed(4, livesAt.getProperty("country").orElseThrow(IllegalStateException::new));
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a1", "name2", "a11", "country", "SA");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b1", "name2", "b11", "country", "SA");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a2", "name2", "a22", "country", "SA");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b2", "name2", "b22", "country", "SA");
        a1.addEdge("ab", b1, "uid", UUID.randomUUID().toString(), "country", "SA");
        a2.addEdge("ab", b2, "uid", UUID.randomUUID().toString(), "country", "SA");
        this.sqlgGraph.tx().commit();

//        Connection connection = this.sqlgGraph.tx().getConnection();
//        try (Statement statement = connection.createStatement()) {
//            statement.execute("SET citus.enable_repartition_joins = true;");
//        } catch (SQLException e) {
//            Assert.fail(e.getMessage());
//        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V().hasLabel("A").has("country", "SA")
                .out()
                .in()
                .toList();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(a1));
        Assert.assertTrue(vertices.contains(a2));

        vertices = this.sqlgGraph.traversal()
                .V().hasLabel("B").has("country", "SA")
                .in()
                .out()
                .toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b1));
        Assert.assertTrue(vertices.contains(b2));

        Thread.sleep(1000);

        vertices = this.sqlgGraph1.traversal()
                .V().hasLabel("A").has("country", "SA")
                .out()
                .in()
                .toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(a1));
        Assert.assertTrue(vertices.contains(a2));

        vertices = this.sqlgGraph1.traversal()
                .V().hasLabel("B").has("country", "SA")
                .in()
                .out()
                .toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b1));
        Assert.assertTrue(vertices.contains(b2));
    }
}
