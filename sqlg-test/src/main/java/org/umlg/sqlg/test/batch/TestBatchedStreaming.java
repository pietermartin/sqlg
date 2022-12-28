package org.umlg.sqlg.test.batch;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgExceptions;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Date: 2015/10/03
 * Time: 8:53 PM
 */
public class TestBatchedStreaming extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsStreamingBatchMode());
    }

    @Test
    public void testNullProperties() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        //surname is null, that bad-ass means null
        try {
            this.sqlgGraph.streamVertex(T.label, "Person", "name", "John1", "surname", null, "age", 1);
        } catch (SqlgExceptions.InvalidPropertyTypeException e) {
            Assert.assertEquals("Property of type NULL is not supported", e.getMessage());
        }
        try {
            this.sqlgGraph.streamVertex(T.label, "Person", "name", "John2", "surname", "Smith", "age", null);
        } catch (SqlgExceptions.InvalidPropertyTypeException e) {
            Assert.assertEquals("Property of type NULL is not supported", e.getMessage());
        }
        this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist(
                "Person",
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING));
                    put("surname", PropertyDefinition.of(PropertyType.STRING));
                    put("age", PropertyDefinition.of(PropertyType.INTEGER));
                }});
        this.sqlgGraph.streamVertex(T.label, "Person", "name", "John1", "surname", null, "age", 1);
        this.sqlgGraph.streamVertex(T.label, "Person", "name", "John2", "surname", "Smith", "age", null);
        this.sqlgGraph.streamVertex(T.label, "Person", "name", "John3", "surname", "", "age", 1);
        this.sqlgGraph.tx().commit();
        testNullProperties_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testNullProperties_assert(this.sqlgGraph1);
        }
    }

    private void testNullProperties_assert(SqlgGraph sqlgGraph) {
        Vertex john1 = sqlgGraph.traversal().V().hasLabel("Person").has("name", "John1").next();
        assertNull(john1.property("surname").value());
        assertNotNull(john1.property("age").value());
        Vertex john2 = sqlgGraph.traversal().V().hasLabel("Person").has("name", "John2").next();
        assertNotNull(john2.property("surname").value());
        assertNull(john2.property("age").value());
        Vertex john3 = sqlgGraph.traversal().V().hasLabel("Person").has("name", "John3").next();
        assertNotNull(john3.property("surname").value());
        assertEquals("", john3.value("surname"));
    }

    @Test
    public void testStreamingWithBatchSize() throws InterruptedException {
        int BATCH_SIZE = 100;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        this.sqlgGraph.tx().streamingWithLockBatchModeOn();
        List<Pair<Vertex, Vertex>> uids = new ArrayList<>();
        String uuidCache1 = null;
        String uuidCache2 = null;
        for (int i = 1; i <= 1000; i++) {
            String uuid1 = UUID.randomUUID().toString();
            String uuid2 = UUID.randomUUID().toString();
            if (i == 50) {
                uuidCache1 = uuid1;
                uuidCache2 = uuid2;
            }
            properties.put("id", uuid1);
            Vertex v1 = this.sqlgGraph.addVertex("Person", properties);
            properties.put("id", uuid2);
            Vertex v2 = this.sqlgGraph.addVertex("Person", properties);
            uids.add(Pair.of(v1, v2));
            if (i % (BATCH_SIZE / 2) == 0) {
                this.sqlgGraph.tx().flush();
                this.sqlgGraph.tx().streamingWithLockBatchModeOn();
                for (Pair<Vertex, Vertex> uid : uids) {
                    uid.getLeft().addEdge("friend", uid.getRight());
                }
                //This is needed because the number of edges are less than the batch size so it will not be auto flushed
                this.sqlgGraph.tx().flush();
                uids.clear();
                this.sqlgGraph.tx().streamingWithLockBatchModeOn();
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();

        testStreamingWithBatchSize(this.sqlgGraph, uuidCache1, uuidCache2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testStreamingWithBatchSize(this.sqlgGraph1, uuidCache1, uuidCache2);
        }
    }

    private void testStreamingWithBatchSize(SqlgGraph sqlgGraph, String uuidCache1, String uuidCache2) {

        assertEquals(2000, sqlgGraph.traversal().V().hasLabel("Person").count().next(), 0);
        assertEquals(1000, sqlgGraph.traversal().E().hasLabel("friend").count().next(), 0);

        GraphTraversal<Vertex, Vertex> has = sqlgGraph.traversal().V().hasLabel("Person").has("id", uuidCache1);
        assertTrue(has.hasNext());
        Vertex person50 = has.next();

        GraphTraversal<Vertex, Vertex> has1 = sqlgGraph.traversal().V().hasLabel("Person").has("id", uuidCache2);
        assertTrue(has1.hasNext());
        Vertex person250 = has1.next();
        assertTrue(sqlgGraph.traversal().V(person50.id()).out().hasNext());
        Vertex person250Please = sqlgGraph.traversal().V(person50.id()).out().next();
        assertEquals(person250, person250Please);
    }

    @Test
    public void testStreamingWithBatchSizeNonDefaultSchema() throws InterruptedException {
        final int BATCH_SIZE = 1000;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        this.sqlgGraph.tx().streamingWithLockBatchModeOn();
        List<Pair<Vertex, Vertex>> uids = new ArrayList<>();
        String uuidCache1 = null;
        String uuidCache2 = null;
        for (int i = 1; i <= 1000; i++) {
            String uuid1 = UUID.randomUUID().toString();
            String uuid2 = UUID.randomUUID().toString();
            if (i == 50) {
                uuidCache1 = uuid1;
                uuidCache2 = uuid2;
            }
            properties.put("id", uuid1);
            Vertex v1 = this.sqlgGraph.addVertex("A.Person", properties);
            properties.put("id", uuid2);
            Vertex v2 = this.sqlgGraph.addVertex("A.Person", properties);
            uids.add(Pair.of(v1, v2));
            if (i % (BATCH_SIZE / 2) == 0) {
                this.sqlgGraph.tx().flush();
                for (Pair<Vertex, Vertex> uid : uids) {
                    uid.getLeft().addEdge("friend", uid.getRight());
                }
                //This is needed because the number of edges are less than the batch size so it will not be auto flushed
                this.sqlgGraph.tx().flush();
                uids.clear();
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();

        testStreamingWithBatchSizeNonDefaultSchema_assert(this.sqlgGraph, uuidCache1, uuidCache2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testStreamingWithBatchSizeNonDefaultSchema_assert(this.sqlgGraph1, uuidCache1, uuidCache2);
        }
    }

    private void testStreamingWithBatchSizeNonDefaultSchema_assert(SqlgGraph sqlgGraph, String uuidCache1, String uuidCache2) {
        assertEquals(2000, sqlgGraph.traversal().V().hasLabel("A.Person").count().next(), 0);
        assertEquals(1000, sqlgGraph.traversal().E().hasLabel("A.friend").count().next(), 0);

        GraphTraversal<Vertex, Vertex> has = sqlgGraph.traversal().V().hasLabel("A.Person").has("id", uuidCache1);
        assertTrue(has.hasNext());
        Vertex person50 = has.next();

        GraphTraversal<Vertex, Vertex> has1 = sqlgGraph.traversal().V().hasLabel("A.Person").has("id", uuidCache2);
        assertTrue(has1.hasNext());
        Vertex person250 = has1.next();
        assertTrue(sqlgGraph.traversal().V(person50.id()).out().hasNext());
        Vertex person250Please = sqlgGraph.traversal().V(person50.id()).out().next();
        assertEquals(person250, person250Please);
    }

    @Test
    public void testStreamingWithBatchSizeWithCallBack() throws InterruptedException {
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        List<Vertex> persons = new ArrayList<>();
        this.sqlgGraph.tx().streamingWithLockBatchModeOn();
        for (int i = 1; i <= 10; i++) {
            String uuid1 = UUID.randomUUID().toString();
            properties.put("id", uuid1);
            persons.add(this.sqlgGraph.addVertex("Person", properties));
        }
        this.sqlgGraph.tx().flush();
        Vertex previous = null;
        for (Vertex person : persons) {
            if (previous == null) {
                previous = person;
            } else {
                previous.addEdge("friend", person);
            }
        }
        this.sqlgGraph.tx().commit();
        testStreamingWithBatchSizeWithCallBack_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testStreamingWithBatchSizeWithCallBack_assert(this.sqlgGraph1);
        }
    }

    private void testStreamingWithBatchSizeWithCallBack_assert(SqlgGraph sqlgGraph) {
        assertEquals(10, sqlgGraph.traversal().V().hasLabel("Person").count().next(), 0);
        assertEquals(9, sqlgGraph.traversal().E().hasLabel("friend").count().next(), 0);
    }

    @Test
    public void streamJava8StyleWithSchema() throws InterruptedException {
        List<String> uids = Arrays.asList("1", "2", "3", "4", "5");
        this.sqlgGraph.tx().streamingBatchModeOn();
        uids.forEach(u -> this.sqlgGraph.streamVertex(T.label, "R_HG.Person", "name", u));
        this.sqlgGraph.tx().commit();
        streamJava8StyleWithSchema_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            streamJava8StyleWithSchema_assert(this.sqlgGraph1);
        }
    }

    private void streamJava8StyleWithSchema_assert(SqlgGraph sqlgGraph) {
        assertEquals(5, sqlgGraph.traversal().V().hasLabel("R_HG.Person").count().next(), 0L);
    }

    @Test
    public void testBatchContinuations() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Dog");
        v1.addEdge("pet", v2);
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingWithLockBatchModeOn();
        for (int i = 1; i <= 100; i++) {
            this.sqlgGraph.addVertex("Person", new LinkedHashMap<>());
        }
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.streamVertex("Person", new LinkedHashMap<>());
        this.sqlgGraph.tx().commit();
        testBatchContinuations_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchContinuations_assert(this.sqlgGraph1);
        }
    }

    private void testBatchContinuations_assert(SqlgGraph sqlgGraph) {
        assertEquals(102, sqlgGraph.traversal().V().hasLabel("Person").count().next(), 0L);
        assertEquals(1, sqlgGraph.traversal().V().hasLabel("Dog").count().next(), 0L);
    }

    @Test
    public void testBatchWithAttributeWithBackSlashAsLastChar() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.streamVertex(T.label, "Person", "name", "a\\", "test", "b\\");
        this.sqlgGraph.streamVertex(T.label, "Person", "name", "a\\", "test", "b\\");
        this.sqlgGraph.streamVertex(T.label, "Person", "name", "a\\", "test", "b\\");
        this.sqlgGraph.streamVertex(T.label, "Person", "name", "a\\", "test", "b\\");
        this.sqlgGraph.streamVertex(T.label, "Person", "name", "a\\", "test", "b\\");
        this.sqlgGraph.streamVertex(T.label, "Person", "name", "a\\", "test", "b\\");
        this.sqlgGraph.tx().commit();
    }

}
