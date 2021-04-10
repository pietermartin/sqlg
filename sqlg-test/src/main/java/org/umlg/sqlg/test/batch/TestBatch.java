package org.umlg.sqlg.test.batch;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.*;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.time.Duration;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Date: 2014/09/12
 * Time: 5:14 PM
 */
@SuppressWarnings({"UnnecessaryBoxing", "JavaDoc"})
public class TestBatch extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testBatchNormalModeEdgeMultiColumnProperties() {
        this.sqlgGraph.tx().normalBatchModeOn();
        ZonedDateTime now = ZonedDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            now = now.truncatedTo(ChronoUnit.MILLIS);
        }
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "t", now);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        a1.addEdge("ab", a2, "t", now);
        a1.addEdge("ab", a2 );
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").has("t", now).count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").hasNot("t").count().next(), 0);
        Assert.assertEquals(2, this.sqlgGraph.traversal().E().hasLabel("ab").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("ab").has("t", now).count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("ab").hasNot("t").count().next(), 0);
    }

    @Test
    public void testBatchNormalModeMultiColumnProperties() {
        this.sqlgGraph.tx().normalBatchModeOn();
        ZonedDateTime now = ZonedDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            now = now.truncatedTo(ChronoUnit.MILLIS);
        }
        this.sqlgGraph.addVertex(T.label, "A", "t", now);
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").has("t", now).count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").hasNot("t").count().next(), 0);

        this.sqlgGraph.tx().normalBatchModeOn();
        this.sqlgGraph.addVertex(T.label, "A", "d", Duration.ofHours(1));
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(4, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").has("d", Duration.ofHours(1)).count().next(), 0);
        Assert.assertEquals(3, this.sqlgGraph.traversal().V().hasLabel("A").hasNot("d").count().next(), 0);

        this.sqlgGraph.tx().normalBatchModeOn();
        this.sqlgGraph.addVertex(T.label, "A", "p", Period.of(1,1,1));
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(6, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").has("p", Period.of(1,1,1)).count().next(), 0);
        Assert.assertEquals(5, this.sqlgGraph.traversal().V().hasLabel("A").hasNot("p").count().next(), 0);
    }

    @Test
    public void testBatchNormalModeFlushWrapsInQuotes() {
        this.sqlgGraph.tx().normalBatchModeOn();
        String vertexLabel = "schem\"a.tabl\"e";
        String edgeLabel = "edg\"e";
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, vertexLabel, "name", "test1");
        Vertex vertex2 = this.sqlgGraph.addVertex(T.label, vertexLabel, "name", "test2");
        Edge edge = vertex1.addEdge(edgeLabel, vertex2);
        this.sqlgGraph.tx().flush();
        Assert.assertNotNull(vertex1.id());
        Assert.assertNotNull(vertex2.id());
        Assert.assertNotNull(edge.id());
    }

    @Test
    public void testBatchNormalModeFlushWrapsInQuotesSimple() {
        this.sqlgGraph.tx().normalBatchModeOn();
        String vertexLabel = "B\"B.B\"B";
        Vertex vertex1 = this.sqlgGraph.addVertex(T.label, vertexLabel, "name", "test1");
        this.sqlgGraph.tx().commit();
        Assert.assertNotNull(vertex1.id());
    }

    @Test
    public void testBatchNonUTF8Chars() {
        Vertex v = sqlgGraph.addVertex(T.label, "A");
        v.property("name", "<NULL>");
        v.property("value", "Rio’s Supermarket");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 1000; i++) {
            v = sqlgGraph.addVertex(T.label, "A");
            v.property("name", "<NULL>");
            v.property("value", "Rio’s Supermarket");
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1001, this.sqlgGraph.traversal().V().hasLabel("A").has("value", "Rio’s Supermarket").count().next(), 0);
        Assert.assertEquals(1001, this.sqlgGraph.traversal().V().hasLabel("A").has("name", "<NULL>").count().next(), 0);
    }

    @Test
    public void testNullProperties() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "surname", "Smith");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "John1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "surname", "Smith2");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "");
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Person", "name", "\"\"");
        this.sqlgGraph.tx().commit();
        testNullProperties_assert(this.sqlgGraph, v1, v2, v3, v4);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testNullProperties_assert(this.sqlgGraph1, v1, v2, v3, v4);
        }
    }

    private void testNullProperties_assert(SqlgGraph sqlgGraph, Vertex v1, Vertex v2, Vertex v3, Vertex v4) {
        Assert.assertFalse(sqlgGraph.traversal().V(v1.id()).next().property("surname").isPresent());
        Assert.assertFalse(sqlgGraph.traversal().V(v2.id()).next().property("name").isPresent());
        Assert.assertEquals("", sqlgGraph.traversal().V(v3.id()).next().property("name").value());
        Assert.assertEquals("\"\"", sqlgGraph.traversal().V(v4.id()).next().property("name").value());
    }

    @Test
    public void testQueryWhileInserting() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 1; i < 101; i++) {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
            Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b1);
            a.addEdge("ab", b2);
            if (i % 10 == 0) {
                Assert.assertEquals(2, IteratorUtils.count(a.edges(Direction.OUT, "ab")));
                Assert.assertEquals(2, IteratorUtils.count(a.vertices(Direction.OUT, "ab")));
            }
        }
        this.sqlgGraph.tx().commit();
        testQueryWhileInserting_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testQueryWhileInserting_assert(this.sqlgGraph1);
        }
    }

    private void testQueryWhileInserting_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(100, sqlgGraph.traversal().V().hasLabel("A").count().next().intValue());
        Assert.assertEquals(200, sqlgGraph.traversal().V().hasLabel("B").count().next().intValue());
        Assert.assertEquals(200, sqlgGraph.traversal().E().hasLabel("ab").count().next().intValue());
        sqlgGraph.traversal().V().hasLabel("A").forEachRemaining(v -> Assert.assertEquals(2, sqlgGraph.traversal().V(v).out("ab").count().next().intValue()));
    }

    @Test
    public void testRemoveWhileInserting() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Set<Vertex> toRemove = new HashSet<>();
        for (int i = 1; i < 101; i++) {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A");
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
            Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
            a.addEdge("ab", b1);
            a.addEdge("ab", b2);
            if (i % 10 == 0) {
                toRemove.add(a);
            }
        }
        for (Vertex vertex : toRemove) {
            vertex.remove();
        }
        this.sqlgGraph.tx().commit();
        testRemovalWhileInserting_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testRemovalWhileInserting_assert(this.sqlgGraph1);
        }
    }

    private void testRemovalWhileInserting_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(90, sqlgGraph.traversal().V().hasLabel("A").count().next().intValue());
        Assert.assertEquals(200, sqlgGraph.traversal().V().hasLabel("B").count().next().intValue());
        Assert.assertEquals(180, sqlgGraph.traversal().E().hasLabel("ab").count().next().intValue());
        sqlgGraph.traversal().V().hasLabel("A").forEachRemaining(v -> Assert.assertEquals(2, sqlgGraph.traversal().V(v).out("ab").count().next().intValue()));
    }

    @Test
    public void testEscapingCharacters() throws InterruptedException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 10000; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "MO1", "name", "marko" + i, "test1", "\\", "test2", "\nhalo", "test3", "\rhalo", "test4", "\thalo");
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko" + i, "test1", "\\", "test2", "\nhalo", "test3", "\rhalo", "test4", "\thalo");
            v1.addEdge("Friend", v2, "name", "xxx");
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        testEscapingCharacters_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testEscapingCharacters_assert(this.sqlgGraph1);
        }
    }

    private void testEscapingCharacters_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(20000, sqlgGraph.traversal().V().count().next(), 0);
        Assert.assertEquals(10000, sqlgGraph.traversal().E().count().next(), 0);
    }

    @Test
    public void testVerticesBatchOn() throws InterruptedException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 10000; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "MO1", "name", "marko" + i);
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko" + i);
            v1.addEdge("Friend", v2, "name", "xxx");
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        testVerticesBatchOn_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testVerticesBatchOn_assert(this.sqlgGraph1);
        }
    }

    private void testVerticesBatchOn_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(20000, sqlgGraph.traversal().V().count().next(), 0);
        Assert.assertEquals(10000, sqlgGraph.traversal().E().count().next(), 0);
    }

    @Test
    public void testBatchEdgesManyProperties() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        v1.addEdge("Friend", v2, "weight", 1, "test", "a");
        v1.addEdge("Friend", v3, "weight", 2, "test", "b");
        this.sqlgGraph.tx().commit();
        testBatchEdgesManyProperties_assert(this.sqlgGraph, v1, v2, v3);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchEdgesManyProperties_assert(this.sqlgGraph1, v1, v2, v3);
        }
    }

    private void testBatchEdgesManyProperties_assert(SqlgGraph sqlgGraph, Vertex v1, Vertex v2, Vertex v3) {
        Assert.assertEquals(3, sqlgGraph.traversal().V().count().next(), 0);
        Assert.assertEquals(2, sqlgGraph.traversal().V(v1.id()).out("Friend").count().next(), 0);
        Assert.assertTrue(sqlgGraph.traversal().V(v1.id()).out("Friend").toList().contains(v2));
        Assert.assertTrue(sqlgGraph.traversal().V(v1.id()).out("Friend").toList().contains(v3));
        Assert.assertTrue(sqlgGraph.traversal().V(v2.id()).in("Friend").toList().contains(v1));
        Assert.assertTrue(sqlgGraph.traversal().V(v3.id()).in("Friend").toList().contains(v1));
        Assert.assertEquals(1, sqlgGraph.traversal().E().hasLabel("Friend").has("test", "a").count().next(), 0);
        Assert.assertEquals(1, sqlgGraph.traversal().E().hasLabel("Friend").has("test", "b").count().next(), 0);
    }


    @Test
    public void testBatchEdgesDifferentProperties() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        v1.addEdge("Friend", v2, "weight", 1, "test1", "a");
        v1.addEdge("Friend", v3, "weight", 2, "test1", "a", "test2", "b");
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testBatchVertexDifferentProperties() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko", "test1", "a");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter", "test2", "b");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john", "test3", "c", "test4", "d");
        v1.addEdge("Friend", v2, "weight", 1);
        v1.addEdge("Friend", v3, "weight", 2);
        this.sqlgGraph.tx().commit();
        testBatchVertexDifferentProperties_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchVertexDifferentProperties_assert(this.sqlgGraph1);
        }
    }

    private void testBatchVertexDifferentProperties_assert(SqlgGraph sqlgGraph) {
        Vertex marko = sqlgGraph.traversal().V().hasLabel("Person").has("name", "marko").next();
        Assert.assertEquals("a", marko.value("test1"));
        Assert.assertFalse(marko.property("test2").isPresent());
        Assert.assertFalse(marko.property("test3").isPresent());
        Vertex peter = sqlgGraph.traversal().V().hasLabel("Person").has("name", "peter").next();
        Assert.assertEquals("b", peter.value("test2"));
        Assert.assertFalse(peter.property("test1").isPresent());
        Assert.assertFalse(peter.property("test3").isPresent());
        Vertex john = sqlgGraph.traversal().V().hasLabel("Person").has("name", "john").next();
        Assert.assertEquals("c", john.value("test3"));
        Assert.assertFalse(john.property("test1").isPresent());
        Assert.assertFalse(john.property("test2").isPresent());
    }

    @Test
    public void testBatchVertices() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        v1.addEdge("Friend", v2, "weight", 1);
        v1.addEdge("Friend", v3, "weight", 2);
        this.sqlgGraph.tx().commit();
        testBatchVertices_assert(this.sqlgGraph, v1, v2, v3);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchVertices_assert(this.sqlgGraph1, v1, v2, v3);
        }
    }

    private void testBatchVertices_assert(SqlgGraph sqlgGraph, Vertex v1, Vertex v2, Vertex v3) {
        Assert.assertEquals(3, sqlgGraph.traversal().V().count().next(), 0);
        Assert.assertEquals(2, sqlgGraph.traversal().V(v1.id()).out("Friend").count().next(), 0);
        Assert.assertTrue(sqlgGraph.traversal().V(v1.id()).out("Friend").toList().contains(v2));
        Assert.assertTrue(sqlgGraph.traversal().V(v1.id()).out("Friend").toList().contains(v3));
        Assert.assertTrue(sqlgGraph.traversal().V(v2.id()).in("Friend").toList().contains(v1));
        Assert.assertTrue(sqlgGraph.traversal().V(v3.id()).in("Friend").toList().contains(v1));
    }

    @Test
    public void testBatchModeNeedsCleanTransactionPass() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().normalBatchModeOn();
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(4, this.sqlgGraph.traversal().V().count().next(), 0);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            Assert.assertEquals(4, this.sqlgGraph1.traversal().V().count().next(), 0);
        }
    }

    //this test a 'contains' bug in the update of labels batch logic
    @Test
    public void testBatchUpdateOfLabels() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "mike");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw");
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm");
        v1.addEdge("bts_aaaaaa", v2);
        v1.addEdge("bts_btsalmtos", v4);
        v1.addEdge("bts_btsalm", v3);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        try (SqlgGraph sqlgGraph = SqlgGraph.open(configuration)) {
            v1 = sqlgGraph.traversal().V(v1.id()).next();
            Assert.assertEquals(1, sqlgGraph.traversal().V(v1.id()).out("bts_btsalm").count().next().intValue());
            Assert.assertEquals(1, sqlgGraph.traversal().V(v1.id()).out("bts_btsalmtos").count().next().intValue());
        }
    }

    /**
     * The copy command locks the copy to table.
     * This is great as there is no need to worry about the elements not having sequential ids generated.
     * This test that this is indeed the case.
     * Cars only go in after Persons
     *
     * @throws InterruptedException
     */
    @Test
    public void testEdgeCopyHappensInIsolation() throws InterruptedException {

        //This is needed else the schema manager lock on creating schemas
        this.sqlgGraph.addVertex();
        this.sqlgGraph.tx().commit();
        AtomicLong lastPerson = new AtomicLong();
        AtomicLong lastCar = new AtomicLong();


        CountDownLatch firstLatch = new CountDownLatch(1);
        final Thread thread1 = new Thread(() -> {
            try {
                sqlgGraph.tx().normalBatchModeOn();
                for (int i = 0; i < 100000; i++) {
                    Vertex v1 = sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
                    Vertex v2 = sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
                    v1.addEdge("Friend", v2);
                }
                firstLatch.countDown();
                sqlgGraph.tx().commit();
                List<Vertex> persons = sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").toList();
                lastPerson.set(persons.size());
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            } finally {
                sqlgGraph.tx().rollback();
            }

        });
        thread1.start();
        final Thread thread2 = new Thread(() -> {
            try {
                firstLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
            Vertex v1 = sqlgGraph.addVertex(T.label, "Car", "dummy", "a");
            Vertex v2 = sqlgGraph.addVertex(T.label, "Car", "dummy", "a");
            v1.addEdge("Same", v2);
            sqlgGraph.tx().commit();
            List<Vertex> cars = sqlgGraph.traversal().V().<Vertex>has(T.label, "Car").toList();
            lastCar.set(cars.size());
        });
        thread2.start();
        thread1.join();
        thread2.join();
        Assert.assertEquals(200000, lastPerson.get());
        Assert.assertEquals(2, lastCar.get());
    }

    @Test
    public void testVertexProperties() {
        List<Short> shortList = new ArrayList<>();
        List<Integer> integerList = new ArrayList<>();
        List<Long> longList = new ArrayList<>();
        List<Double> doubleList = new ArrayList<>();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "Person",
                    "age2", (short) i,
                    "age3", i,
                    "age4", new Long(i),
                    "age6", new Double(i)
            );
            shortList.add((short) i);
            integerList.add(new Integer(i));
            longList.add(new Long(i));
            doubleList.add(new Double(i));
        }
        Assert.assertEquals(100, shortList.size());
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().toList();
        for (Vertex v : vertices) {
            shortList.remove(v.<Short>value("age2"));
            integerList.remove(v.<Integer>value("age3"));
            longList.remove(v.<Long>value("age4"));
            doubleList.remove(v.<Double>value("age6"));
        }
        Assert.assertTrue(shortList.isEmpty());
        Assert.assertTrue(integerList.isEmpty());
        Assert.assertTrue(longList.isEmpty());
        Assert.assertTrue(doubleList.isEmpty());
    }

    @Test
    public void testEdgeProperties() throws InterruptedException {
        List<Short> shortList = new ArrayList<>();
        List<Integer> integerList = new ArrayList<>();
        List<Long> longList = new ArrayList<>();
        List<Double> doubleList = new ArrayList<>();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 100; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
            v1.addEdge("Friend", v2,
                    "age2", (short) i,
                    "age3", i,
                    "age4", new Long(i),
                    "age6", new Double(i)
            );
            shortList.add((short) i);
            integerList.add(new Integer(i));
            longList.add(new Long(i));
            doubleList.add(new Double(i));
        }
        Assert.assertEquals(100, shortList.size());
        this.sqlgGraph.tx().commit();
        testEdgeProperties_assert(this.sqlgGraph, shortList, integerList, longList, doubleList);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testEdgeProperties_assert(this.sqlgGraph1, shortList, integerList, longList, doubleList);
        }
    }

    private void testEdgeProperties_assert(SqlgGraph sqlgGraph, List<Short> shortList, List<Integer> integerList, List<Long> longList, List<Double> doubleList) {
        List<Edge> edges = sqlgGraph.traversal().E().toList();
        for (Edge e : edges) {
            shortList.remove(e.<Short>value("age2"));
            integerList.remove(e.<Integer>value("age3"));
            longList.remove(e.<Long>value("age4"));
            doubleList.remove(e.<Double>value("age6"));
        }
        Assert.assertTrue(shortList.isEmpty());
        Assert.assertTrue(integerList.isEmpty());
        Assert.assertTrue(longList.isEmpty());
        Assert.assertTrue(doubleList.isEmpty());
    }

    @Test
    public void testUpdateInsertedVertexProperty() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        v1.property("name", "john");
        this.sqlgGraph.tx().commit();
        testUpdateInsertedVertexProperty_assert(this.sqlgGraph, v1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testUpdateInsertedVertexProperty_assert(this.sqlgGraph1, v1);
        }
    }

    private void testUpdateInsertedVertexProperty_assert(SqlgGraph sqlgGraph, Vertex v1) {
        Assert.assertEquals("john", v1.value("name"));
        Assert.assertEquals("john", sqlgGraph.traversal().V().next().value("name"));
        v1 = sqlgGraph.traversal().V(v1.id()).next();
        Assert.assertEquals("john", v1.value("name"));
        Assert.assertEquals("john", sqlgGraph.traversal().V().next().value("name"));
    }

    @Test
    public void testAddPropertyToInsertedVertexProperty() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        v1.property("name", "john");
        v1.property("surname", "aaaa");
        this.sqlgGraph.tx().commit();
        testAddPropertyToInsertVertexProperty_assert(this.sqlgGraph, v1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testAddPropertyToInsertVertexProperty_assert(this.sqlgGraph1, v1);
        }
    }

    private void testAddPropertyToInsertVertexProperty_assert(SqlgGraph sqlgGraph, Vertex v1) {
        v1 = sqlgGraph.traversal().V(v1.id()).next();
        Assert.assertEquals("john", v1.value("name"));
        Assert.assertEquals("aaaa", v1.value("surname"));
        Assert.assertEquals("john", sqlgGraph.traversal().V().next().value("name"));
    }

    @Test
    public void testUpdateInsertedEdgeProperty() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Edge edge = v1.addEdge("Friend", v2, "weight", 1);
        edge.property("weight", 2);
        this.sqlgGraph.tx().commit();
        testUpdateInsertedEdgeProperty_assert(this.sqlgGraph, edge);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testUpdateInsertedEdgeProperty_assert(this.sqlgGraph1, edge);
        }
    }

    private void testUpdateInsertedEdgeProperty_assert(SqlgGraph sqlgGraph, Edge edge) {
        Assert.assertEquals(2, edge.<Integer>value("weight"), 0);
        Assert.assertEquals(2, sqlgGraph.traversal().E().next().<Integer>value("weight"), 0);
    }

    @Test
    //TODO need to deal with missing properties, set them to null
    public void testRemoveProperty() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        marko.addEdge("Friend", john, "weight", 1);
        Edge colleague = marko.addEdge("Colleague", john, "toRemove", "a");
        marko.property("name").remove();
        colleague.property("toRemove").remove();
        this.sqlgGraph.tx().commit();

        testRemoveProperty_assert(this.sqlgGraph, marko);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testRemoveProperty_assert(this.sqlgGraph1, marko);
        }
    }

    @Test
    //TODO need to deal with missing properties, set them to null
    public void testRemovePropertyUserSuppliedPK() throws InterruptedException {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                            put("name", PropertyType.STRING);
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        personVertexLabel.ensureEdgeLabelExist(
                "Friend",
                personVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("weight", PropertyType.INTEGER);
                }}
        );
        personVertexLabel.ensureEdgeLabelExist(
                "Colleague",
                personVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                    put("toRemove", PropertyType.STRING);
                }}
        );
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        marko.addEdge("Friend", john, "weight", 1, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Edge colleague = marko.addEdge("Colleague", john, "toRemove", "a", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        marko.property("name").remove();
        colleague.property("toRemove").remove();
        this.sqlgGraph.tx().commit();

        testRemoveProperty_assert(this.sqlgGraph, marko);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testRemoveProperty_assert(this.sqlgGraph1, marko);
        }
    }

    private void testRemoveProperty_assert(SqlgGraph sqlgGraph, Vertex marko) {
        marko = sqlgGraph.traversal().V(marko.id()).next();
        Assert.assertFalse(marko.property("name").isPresent());
    }

    @Test
    public void testInOutOnEdges() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT", "dummy", "a");
        Vertex god = this.sqlgGraph.addVertex(T.label, "God", "dummy", "a");
        root.addEdge("rootGod", god);
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testGetEdges() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT", "dummy", "a");
        Vertex god = this.sqlgGraph.addVertex(T.label, "God", "dummy", "a");
        Edge sqlgEdge = root.addEdge("rootGod", god);
        Assert.assertNull(sqlgEdge.id());
        Edge rootGodEdge = vertexTraversal(this.sqlgGraph, root).outE("rootGod").next();
        //Querying triggers the cache to be flushed, so the result will have an id
        Assert.assertNotNull(rootGodEdge);
        Assert.assertNotNull(rootGodEdge.id());
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testGetEdgesUserSuppliedPK() {
        VertexLabel rootVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "ROOT",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                            put("dummy", PropertyType.STRING);
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        VertexLabel godVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "God",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                            put("dummy", PropertyType.STRING);
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        rootVertexLabel.ensureEdgeLabelExist(
                "rootGod",
                godVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT", "dummy", "a", "uid1", "root111", "uid2", "root222");
        Vertex god = this.sqlgGraph.addVertex(T.label, "God", "dummy", "a", "uid1", "god111", "uid2", "god222");
        Edge sqlgEdge = root.addEdge("rootGod", god, "uid1", "edge111", "uid2", "edge222");
        Assert.assertNull(sqlgEdge.id());
        this.sqlgGraph.tx().commit();
        Edge rootGodEdge = vertexTraversal(this.sqlgGraph, root).outE("rootGod").next();
        //Querying triggers the cache to be flushed, so the result will have an id
        Assert.assertNotNull(rootGodEdge);
        Assert.assertNotNull(rootGodEdge.id());
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testGetVertices() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT", "dummy", "a");
        Vertex god = this.sqlgGraph.addVertex(T.label, "God", "dummy", "a");
        Vertex human = this.sqlgGraph.addVertex(T.label, "Human", "dummy", "a");
        root.addEdge("rootGod", god);
        root.addEdge("rootHuman", human);
        god.addEdge("rootROOT", root);
        Assert.assertEquals(god, vertexTraversal(this.sqlgGraph, root).out("rootGod").next());
        Assert.assertEquals(human, vertexTraversal(this.sqlgGraph, root).out("rootHuman").next());
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testPerformance() throws InterruptedException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 10000; i++) {
            Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko" + i);
            Vertex spaceTime = this.sqlgGraph.addVertex(T.label, "SpaceTime", "name", "marko" + i);
            Vertex space = this.sqlgGraph.addVertex(T.label, "Space", "name", "marko" + i);
            Vertex time = this.sqlgGraph.addVertex(T.label, "Time", "name", "marko" + i);
            person.addEdge("spaceTime", spaceTime, "context", 1);
            spaceTime.addEdge("space", space, "dimension", 3);
            spaceTime.addEdge("time", time, "dimension", 1);
            if (i != 0 && i % 10000 == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().normalBatchModeOn();
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        testPerformance_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testPerformance_assert(this.sqlgGraph1);
        }
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    private void testPerformance_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(10000, sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
        Assert.assertEquals(10000, sqlgGraph.traversal().V().has(T.label, "SpaceTime").count().next(), 0);
        Assert.assertEquals(10000, sqlgGraph.traversal().V().has(T.label, "Space").count().next(), 0);
        Assert.assertEquals(10000, sqlgGraph.traversal().V().has(T.label, "Time").count().next(), 0);
    }

    @Test
    public void testGetVerticesWithHas() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT", "dummy", "a");
        Vertex jah = this.sqlgGraph.addVertex(T.label, "God", "name", "Jah");
        Vertex jehova = this.sqlgGraph.addVertex(T.label, "God", "name", "Jehova");
        root.addEdge("rootGod", jah);
        root.addEdge("rootGod", jehova);
        List<Vertex> vertices = vertexTraversal(this.sqlgGraph, root).out("rootGod").toList();
        Assert.assertTrue(vertices.contains(jah));
        Assert.assertTrue(vertices.contains(jehova));
        Assert.assertEquals(jah, vertexTraversal(this.sqlgGraph, root).out("rootGod").has("name", "Jah").next());
        Assert.assertEquals(jehova, vertexTraversal(this.sqlgGraph, root).out("rootGod").has("name", "Jehova").next());
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testVertexLabelCache() throws InterruptedException {
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex jah = this.sqlgGraph.addVertex(T.label, "God", "name", "Jah");
        Vertex jehova = this.sqlgGraph.addVertex(T.label, "God", "name", "Jehova");
        root.addEdge("rootGod", jah);
        root.addEdge("rootGod", jehova);
        this.sqlgGraph.tx().commit();
        testVertexLabelCache_assert(this.sqlgGraph, root, jah, jehova);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testVertexLabelCache_assert(this.sqlgGraph1, root, jah, jehova);
        }
    }

    private void testVertexLabelCache_assert(SqlgGraph sqlgGraph, Vertex root, Vertex jah, Vertex jehova) {
        List<Vertex> vertices = vertexTraversal(sqlgGraph, root).out("rootGod").toList();
        Assert.assertTrue(vertices.contains(jah));
        Assert.assertTrue(vertices.contains(jehova));
        Assert.assertEquals(jah, vertexTraversal(sqlgGraph, root).out("rootGod").has("name", "Jah").next());
        Assert.assertEquals(jehova, vertexTraversal(sqlgGraph, root).out("rootGod").has("name", "Jehova").next());
    }

    @Test
    public void testVertexMultipleEdgesLabels() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex car = this.sqlgGraph.addVertex(T.label, "Car", "name", "b");
        Vertex bike = this.sqlgGraph.addVertex(T.label, "Bike", "name", "c");
        person.addEdge("car", car);
        person.addEdge("bike", bike);
        this.sqlgGraph.tx().commit();
        testVertexMulitpleEdgesLabels_assert(this.sqlgGraph, person);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testVertexMulitpleEdgesLabels_assert(this.sqlgGraph1, person);
        }
    }

    private void testVertexMulitpleEdgesLabels_assert(SqlgGraph sqlgGraph, Vertex person) {
        Assert.assertEquals(Long.valueOf(2), sqlgGraph.traversal().V(person).out().count().next());
    }

    @Test
    public void testAddEdgeAccrossSchema() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex person = this.sqlgGraph.addVertex(T.label, "Schema1.Person", "name", "a");
        Vertex car = this.sqlgGraph.addVertex(T.label, "Schema2.Car", "name", "b");
        Vertex bike = this.sqlgGraph.addVertex(T.label, "Schema2.Bike", "name", "c");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        person.addEdge("car", car);
        person.addEdge("bike", bike);
        this.sqlgGraph.tx().commit();
        testAddEdgeAccrossSchema_assert(this.sqlgGraph, person);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testAddEdgeAccrossSchema_assert(this.sqlgGraph1, person);
        }
    }

    private void testAddEdgeAccrossSchema_assert(SqlgGraph sqlgGraph, Vertex person) {
        Assert.assertEquals(Long.valueOf(2), sqlgGraph.traversal().V(person).out().count().next());
    }

    @Test
    public void testCacheAndUpdateVERTICESLabels() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "person");
        List<Vertex> cache = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            cache.add(this.sqlgGraph.addVertex(T.label, "Person", "name", "person" + i));
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (Vertex person2 : cache) {
            person1.addEdge("Friend", person2);
        }
        this.sqlgGraph.tx().commit();
        testCacheAndUpdateVERTICESLabels_assert(this.sqlgGraph, person1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testCacheAndUpdateVERTICESLabels_assert(this.sqlgGraph1, person1);
        }
    }

    private void testCacheAndUpdateVERTICESLabels_assert(SqlgGraph sqlgGraph, Vertex person1) {
        person1 = sqlgGraph.traversal().V(person1.id()).next();
        Assert.assertTrue(vertexTraversal(sqlgGraph, person1).out("Friend").hasNext());
        Assert.assertEquals(Long.valueOf(10000), vertexTraversal(sqlgGraph, person1).out("Friend").count().next());
        List<Vertex> friends = vertexTraversal(sqlgGraph, person1).out("Friend").toList();
        List<String> names = friends.stream().map(v -> v.<String>value("name")).collect(Collectors.toList());
        Assert.assertEquals(10000, names.size(), 0);
        for (int i = 0; i < 10000; i++) {
            Assert.assertTrue(names.contains("person" + i));
        }
    }

    @Test
    public void testBatchInsertDifferentKeys() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "surname", "b");
        this.sqlgGraph.tx().commit();

        testBatchInsertDifferentKeys_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchInsertDifferentKeys_assert(this.sqlgGraph1);
        }
    }

    private void testBatchInsertDifferentKeys_assert(SqlgGraph sqlgGraph) {
        List<Vertex> persons = sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").<Vertex>has("name", "a").toList();
        Assert.assertEquals(1, persons.size());
        Assert.assertFalse(persons.get(0).property("surname").isPresent());

        persons = sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").<Vertex>has("surname", "b").toList();
        Assert.assertEquals(1, persons.size());
        Assert.assertFalse(persons.get(0).property("name").isPresent());

        persons = sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").has("surname", "b").<Vertex>has("name", "a").toList();
        Assert.assertEquals(0, persons.size());
    }

    @Test
    public void testVerticesOutLabelsForPersistentVertices() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "RealWorkspace", "name", "realWorkspace1");
        Vertex softwareVersion = this.sqlgGraph.addVertex(T.label, "SoftwareVersion", "name", "R15");
        Vertex vendorTechnology = this.sqlgGraph.addVertex(T.label, "VendorTechnology", "name", "Huawei_Gsm");
        vendorTechnology.addEdge("vendorTechnology_softwareVersion", softwareVersion);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals("Huawei_Gsm", vertexTraversal(this.sqlgGraph, softwareVersion).in("vendorTechnology_softwareVersion").next().value("name"));

        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex rwe1 = this.sqlgGraph.addVertex(T.label, "RWE", "name", "cell1");
        rwe1.addEdge("workspaceElement_softwareVersion", softwareVersion);
        this.sqlgGraph.tx().commit();

        testVerticesOutLabelsForPersistentVertices_assert(this.sqlgGraph, softwareVersion);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testVerticesOutLabelsForPersistentVertices_assert(this.sqlgGraph1, softwareVersion);
        }
    }

    private void testVerticesOutLabelsForPersistentVertices_assert(SqlgGraph sqlgGraph, Vertex softwareVersion) {
        softwareVersion = sqlgGraph.traversal().V(softwareVersion.id()).next();
        Assert.assertEquals("Huawei_Gsm", vertexTraversal(sqlgGraph, softwareVersion).in("vendorTechnology_softwareVersion").next().value("name"));
    }

    @Test
    public void testVerticesInLabelsForPersistentVertices() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "RealWorkspace", "name", "realWorkspace1");
        Vertex softwareVersion = this.sqlgGraph.addVertex(T.label, "SoftwareVersion", "name", "R15");
        Vertex vendorTechnology = this.sqlgGraph.addVertex(T.label, "VendorTechnology", "name", "Huawei_Gsm");
        softwareVersion.addEdge("softwareVersion_vendorTechnology", vendorTechnology);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals("Huawei_Gsm", vertexTraversal(this.sqlgGraph, softwareVersion).out("softwareVersion_vendorTechnology").next().value("name"));

        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex rwe1 = this.sqlgGraph.addVertex(T.label, "RWE", "name", "cell1");
        rwe1.addEdge("workspaceElement_softwareVersion", softwareVersion);
        this.sqlgGraph.tx().commit();

        testVerticesInLabelsForPersistentVertices_assert(this.sqlgGraph, softwareVersion);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testVerticesInLabelsForPersistentVertices_assert(this.sqlgGraph1, softwareVersion);
        }
    }

    private void testVerticesInLabelsForPersistentVertices_assert(SqlgGraph sqlgGraph, Vertex softwareVersion) {
        softwareVersion = sqlgGraph.traversal().V(softwareVersion.id()).next();
        Assert.assertEquals("Huawei_Gsm", vertexTraversal(sqlgGraph, softwareVersion).out("softwareVersion_vendorTechnology").next().value("name"));
    }

    @Test
    public void testBatchUpdatePersistentVertices() throws InterruptedException {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "surname", "b");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("a", this.sqlgGraph.traversal().V(v1.id()).next().value("name"));
        Assert.assertEquals("b", this.sqlgGraph.traversal().V(v2.id()).next().value("surname"));

        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().normalBatchModeOn();
        v1.property("name", "aa");
        v2.property("surname", "bb");
        this.sqlgGraph.tx().commit();

        testBatchUpdatePersistentVertices_assert(this.sqlgGraph, v1, v2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchUpdatePersistentVertices_assert(this.sqlgGraph1, v1, v2);
        }
    }

    private void testBatchUpdatePersistentVertices_assert(SqlgGraph sqlgGraph, Vertex v1, Vertex v2) {
        Assert.assertEquals("aa", sqlgGraph.traversal().V(v1.id()).next().value("name"));
        Assert.assertEquals("bb", sqlgGraph.traversal().V(v2.id()).next().value("surname"));
    }

    @Test
    public void testBatchUpdatePersistentVerticesAllTypes() throws InterruptedException {

        Assume.assumeTrue(this.sqlgGraph.features().vertex().properties().supportsFloatValues());

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "surname", "b");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("a", this.sqlgGraph.traversal().V(v1.id()).next().value("name"));
        Assert.assertEquals("b", this.sqlgGraph.traversal().V(v2.id()).next().value("surname"));

        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().normalBatchModeOn();
        v1.property("name", "aa");
        v1.property("boolean", true);
        v1.property("short", (short) 1);
        v1.property("integer", 1);
        v1.property("long", 1L);
        v1.property("float", 1F);
        v1.property("double", 1D);

        v2.property("surname", "bb");
        v2.property("boolean", false);
        v2.property("short", (short) 2);
        v2.property("integer", 2);
        v2.property("long", 2L);
        v2.property("float", 2F);
        v2.property("double", 2D);
        this.sqlgGraph.tx().commit();

        testBatchUpdatePersistentVerticesAllTypes_assert(this.sqlgGraph, v1, v2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchUpdatePersistentVerticesAllTypes_assert(this.sqlgGraph1, v1, v2);
        }
    }

    private void testBatchUpdatePersistentVerticesAllTypes_assert(SqlgGraph sqlgGraph, Vertex v1, Vertex v2) {
        Assert.assertEquals("aa", sqlgGraph.traversal().V(v1.id()).next().value("name"));
        Assert.assertEquals(true, sqlgGraph.traversal().V(v1.id()).next().value("boolean"));
        Assert.assertEquals((short) 1, sqlgGraph.traversal().V(v1.id()).next().<Short>value("short").shortValue());
        Assert.assertEquals(1, sqlgGraph.traversal().V(v1.id()).next().<Integer>value("integer").intValue());
        Assert.assertEquals(1L, sqlgGraph.traversal().V(v1.id()).next().<Long>value("long"), 0);
        Assert.assertEquals(1F, sqlgGraph.traversal().V(v1.id()).next().<Float>value("float"), 0);
        Assert.assertEquals(1D, sqlgGraph.traversal().V(v1.id()).next().<Double>value("double"), 0);

        Assert.assertEquals("bb", sqlgGraph.traversal().V(v2.id()).next().value("surname"));
        Assert.assertEquals(false, sqlgGraph.traversal().V(v2.id()).next().value("boolean"));
        Assert.assertEquals((short) 2, sqlgGraph.traversal().V(v2.id()).next().<Short>value("short").shortValue());
        Assert.assertEquals(2, sqlgGraph.traversal().V(v2.id()).next().<Integer>value("integer").intValue());
        Assert.assertEquals(2L, sqlgGraph.traversal().V(v2.id()).next().<Long>value("long"), 0);
        Assert.assertEquals(2F, sqlgGraph.traversal().V(v2.id()).next().<Float>value("float"), 0);
        Assert.assertEquals(2D, sqlgGraph.traversal().V(v2.id()).next().<Double>value("double"), 0);
    }

    @Test
    public void testInsertUpdateQuotedStrings() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "'a'");
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        this.sqlgGraph.tx().normalBatchModeOn();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().toList();
        for (Vertex v : vertices) {
            v.property("name", "'b'");
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    @Test
    public void testBatchRemoveVertex() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        v1.remove();
        v2.remove();
        v3.remove();
        this.sqlgGraph.tx().commit();
        testBatchRemoveVertex_assert();
    }

    private void testBatchRemoveVertex_assert() {
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().count().next().intValue());
    }

    @Test
    public void testBatchRemoveVertexUsersSuppliedPK() {
        this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        v1.remove();
        v2.remove();
        v3.remove();
        this.sqlgGraph.tx().commit();
        testBatchRemoveVertex_assert();
    }

    @Test
    public void testBatchRemoveEdges_UserSuppliedPK() throws InterruptedException {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        personVertexLabel.ensureEdgeLabelExist(
                "test",
                personVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Edge edge1 = v1.addEdge("test", v2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Edge edge2 = v1.addEdge("test", v3, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        edge1.remove();
        edge2.remove();
        this.sqlgGraph.tx().commit();
        testBatchRemoveEdges_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchRemoveEdges_assert(this.sqlgGraph1);
        }
    }

    private void testBatchRemoveEdges_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(3, sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(0, sqlgGraph.traversal().E().count().next().intValue());
    }

    @Test
    public void testBatchRemoveEdges() throws InterruptedException {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        Edge edge1 = v1.addEdge("test", v2);
        Edge edge2 = v1.addEdge("test", v3);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        edge1.remove();
        edge2.remove();
        this.sqlgGraph.tx().commit();
        testBatchRemoveEdges_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchRemoveEdges_assert(this.sqlgGraph1);
        }
    }

    @Test
    public void testBatchRemoveVerticesAndEdgesUserSuppliedPK() throws InterruptedException {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        personVertexLabel.ensureEdgeLabelExist(
                "test",
                personVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Edge edge1 = v1.addEdge("test", v2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Edge edge2 = v1.addEdge("test", v3, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        edge1.remove();
        edge2.remove();
        v1.remove();
        v2.remove();
        v3.remove();
        this.sqlgGraph.tx().commit();
        testBatchRemoveVerticesAndEdges_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchRemoveVerticesAndEdges_assert(this.sqlgGraph1);
        }
    }

    @Test
    public void testBatchRemoveVerticesAndEdges() throws InterruptedException {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        Edge edge1 = v1.addEdge("test", v2);
        Edge edge2 = v1.addEdge("test", v3);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        edge1.remove();
        edge2.remove();
        v1.remove();
        v2.remove();
        v3.remove();
        this.sqlgGraph.tx().commit();
        testBatchRemoveVerticesAndEdges_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchRemoveVerticesAndEdges_assert(this.sqlgGraph1);
        }
    }

    private void testBatchRemoveVerticesAndEdges_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(0, sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(0, sqlgGraph.traversal().E().count().next().intValue());
    }

    @Test
    public void testBatchRemoveVerticesEdgesMustBeGone() throws InterruptedException {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("test", v2);
        v1.addEdge("test", v3);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        v1.remove();
        v2.remove();
        v3.remove();
        this.sqlgGraph.tx().commit();
        testBatchRemoveVerticesEdgesMustBeGone_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchRemoveVerticesEdgesMustBeGone_assert(this.sqlgGraph1);
        }
    }

    @Test
    public void testBatchRemoveVerticesEdgesMustBeGoneUserSuppliedPK() throws InterruptedException {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        personVertexLabel.ensureEdgeLabelExist(
                "test",
                personVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        v1.addEdge("test", v2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        v1.addEdge("test", v3, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        v1.remove();
        v2.remove();
        v3.remove();
        this.sqlgGraph.tx().commit();
        testBatchRemoveVerticesEdgesMustBeGone_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchRemoveVerticesEdgesMustBeGone_assert(this.sqlgGraph1);
        }
    }

    private void testBatchRemoveVerticesEdgesMustBeGone_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(0, sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(0, sqlgGraph.traversal().E().count().next().intValue());
    }

    @Test
    public void testDeletePerformance() throws InterruptedException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().normalBatchModeOn();
        //32767
        int j = 1;
        //createVertexLabel 280 foreign keys
        for (int i = 0; i < 2810; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, this.sqlgGraph.getSqlDialect().getPublicSchema() + ".WorkspaceElement", "name", "workspaceElement" + i);
            if (j == 281) {
                j = 1;
            }
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "huawei.NetworkElement", "name", "networkElement" + i + "_" + j);
            v2.addEdge("WorkspaceElement_NetworkElement" + j, v1);
            j++;
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        List<Vertex> vertexes = this.sqlgGraph.traversal().V().has(T.label, this.sqlgGraph.getSqlDialect().getPublicSchema() + ".WorkspaceElement").toList();
        for (Vertex sqlgVertex : vertexes) {
            sqlgVertex.remove();
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().count().next().intValue());
        vertexes = this.sqlgGraph.traversal().V().<SqlgVertex>has(T.label, "huawei.NetworkElement").toList();
        for (Vertex sqlgVertex : vertexes) {
            sqlgVertex.remove();
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        testDeletePerformance_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testDeletePerformance_assert(this.sqlgGraph1);
        }
    }

    private void testDeletePerformance_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(0, sqlgGraph.traversal().V().count().next().intValue());
    }

    @Test
    public void testDropForeignKeys() {
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex workspaceElementBsc = this.sqlgGraph.addVertex(T.label, "WorkspaceElement", "name", "bsc1");
        Vertex networkElementBsc = this.sqlgGraph.addVertex(T.label, "bsc", "name", "bsc1");
        Vertex workspaceElementCell1 = this.sqlgGraph.addVertex(T.label, "WorkspaceElement", "name", "cell1");
        Vertex networkElementCell1 = this.sqlgGraph.addVertex(T.label, "cell", "name", "cell1");
        Vertex workspaceElementCell2 = this.sqlgGraph.addVertex(T.label, "WorkspaceElement", "name", "cell2");
        Vertex networkElementCell2 = this.sqlgGraph.addVertex(T.label, "cell", "name", "cell2");
        Vertex workspaceElementBsctmr1 = this.sqlgGraph.addVertex(T.label, "WorkspaceElement", "name", "bsctmr1");
        Vertex networkElementBsctmr1 = this.sqlgGraph.addVertex(T.label, "bsctmr", "name", "bsctms1");
        //add edges to workspaceelement
        networkElementBsc.addEdge("bsc_workspaceElement", workspaceElementBsc);
        networkElementCell1.addEdge("cell_workspaceElement", workspaceElementCell1);
        networkElementCell2.addEdge("cell_workspaceElement", workspaceElementCell2);
        networkElementBsctmr1.addEdge("bsctmr_workspaceElement", workspaceElementBsctmr1);

        //add edges to between elements
        networkElementBsc.addEdge("bsc_cell", networkElementCell1);
        networkElementBsc.addEdge("bsc_cell", networkElementCell2);
        networkElementBsc.addEdge("bsc_bsctmr", networkElementBsctmr1);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();

        List<Vertex> vertexes = this.sqlgGraph.traversal().V().<SqlgVertex>has(T.label, "WorkspaceElement").toList();
        for (Vertex sqlgVertex : vertexes) {
            sqlgVertex.remove();
        }
        vertexes = this.sqlgGraph.traversal().V().<SqlgVertex>has(T.label, "bsc").toList();
        for (Vertex sqlgVertex : vertexes) {
            sqlgVertex.remove();
        }
        vertexes = this.sqlgGraph.traversal().V().<SqlgVertex>has(T.label, "cell").toList();
        for (Vertex sqlgVertex : vertexes) {
            sqlgVertex.remove();
        }
        vertexes = this.sqlgGraph.traversal().V().<SqlgVertex>has(T.label, "bsctmr").toList();
        for (Vertex sqlgVertex : vertexes) {
            sqlgVertex.remove();
        }
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testBatchDeleteVertexNewlyAdded() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "test1.Person", "name", "john");
        for (int i = 0; i < 100; i++) {
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "test2.Car", "model", "vw");
            v1.addEdge("car", v2, "bought", 1);
        }
        List<Vertex> cars = vertexTraversal(this.sqlgGraph, v1).out("car").toList();
        for (int i = 0; i < 50; i++) {
            cars.get(i).remove();
        }
        this.sqlgGraph.tx().commit();
        testBatchDeleteVertexNewlyAdded_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchDeleteVertexNewlyAdded_assert(this.sqlgGraph1);
        }
    }

    private void testBatchDeleteVertexNewlyAdded_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(51, sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(50, sqlgGraph.traversal().E().count().next().intValue());
    }

    @Test
    public void testBatchDeleteEdgeNewlyAdded() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "test1.Person", "name", "john");
        for (int i = 0; i < 100; i++) {
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "test2.Car", "model", "vw");
            v1.addEdge("car", v2, "bought", 1);
        }
        List<Edge> cars = vertexTraversal(this.sqlgGraph, v1).outE("car").toList();
        for (int i = 0; i < 50; i++) {
            cars.get(i).remove();
        }
        this.sqlgGraph.tx().commit();
        testBatchDeleteEdgeNewlyAdded_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchDeleteEdgeNewlyAdded_assert(this.sqlgGraph1);
        }
    }

    private void testBatchDeleteEdgeNewlyAdded_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(101, sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(50, sqlgGraph.traversal().E().count().next().intValue());
    }

    @Test
    public void testNullEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        Assert.assertEquals(0, vertexTraversal(this.sqlgGraph, v1).out("cars").count().next().intValue());
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Car", "dummy", "a");
        v1.addEdge("cars", v2);
        Assert.assertEquals(1, vertexTraversal(this.sqlgGraph, v1).out("cars").count().next().intValue());
        this.sqlgGraph.tx().commit();
        testNullEdge_assert(this.sqlgGraph, v1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testNullEdge_assert(this.sqlgGraph1, v1);
        }
    }

    private void testNullEdge_assert(SqlgGraph sqlgGraph, Vertex v1) {
        Assert.assertEquals(1, sqlgGraph.traversal().V(v1).out("cars").count().next().intValue());
    }

    @Test
    public void testBatchModeStuffsUpProperties() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "b");
        Assert.assertEquals("a", v1.value("name"));
        Assert.assertEquals("b", v2.value("name"));
    }

    @Test
    public void testBatchUpdateDifferentPropertiesDifferentRows() throws InterruptedException {

        Vertex sqlgVertex1 = this.sqlgGraph.addVertex(T.label, "Person", "property1", "a1", "property2", "b1", "property3", "c1");
        Vertex sqlgVertex2 = this.sqlgGraph.addVertex(T.label, "Person", "property1", "a2", "property2", "b2", "property3", "c2");
        Vertex sqlgVertex3 = this.sqlgGraph.addVertex(T.label, "Person", "property1", "a3", "property2", "b3", "property3", "c3");
        this.sqlgGraph.tx().commit();

        sqlgVertex1 = this.sqlgGraph.traversal().V(sqlgVertex1.id()).next();
        Assert.assertEquals("a1", sqlgVertex1.value("property1"));
        Assert.assertEquals("b1", sqlgVertex1.value("property2"));
        Assert.assertEquals("c1", sqlgVertex1.value("property3"));

        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().normalBatchModeOn();
        sqlgVertex1 = this.sqlgGraph.traversal().V(sqlgVertex1.id()).next();
        sqlgVertex1.property("property1", "a11");
        sqlgVertex2.property("property2", "b22");
        sqlgVertex3.property("property3", "c33");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals("a11", sqlgVertex1.value("property1"));
        Assert.assertEquals("b1", sqlgVertex1.value("property2"));
        Assert.assertEquals("c1", sqlgVertex1.value("property3"));

        testBatchUpdateDifferentPropertiesDifferentRows_assert(this.sqlgGraph, sqlgVertex1, sqlgVertex2, sqlgVertex3);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchUpdateDifferentPropertiesDifferentRows_assert(this.sqlgGraph1, sqlgVertex1, sqlgVertex2, sqlgVertex3);
        }
    }

    private void testBatchUpdateDifferentPropertiesDifferentRows_assert(SqlgGraph sqlgGraph, Vertex sqlgVertex1, Vertex sqlgVertex2, Vertex sqlgVertex3) {
        sqlgVertex1 = sqlgGraph.traversal().V(sqlgVertex1).next();
        sqlgVertex2 = sqlgGraph.traversal().V(sqlgVertex2).next();
        sqlgVertex3 = sqlgGraph.traversal().V(sqlgVertex3).next();

        Assert.assertEquals("a11", sqlgVertex1.value("property1"));
        Assert.assertEquals("b1", sqlgVertex1.value("property2"));
        Assert.assertEquals("c1", sqlgVertex1.value("property3"));

        sqlgVertex1 = sqlgGraph.traversal().V(sqlgVertex1.id()).next();
        Assert.assertEquals("a11", sqlgVertex1.value("property1"));
        Assert.assertEquals("b1", sqlgVertex1.value("property2"));
        Assert.assertEquals("c1", sqlgVertex1.value("property3"));

        sqlgVertex2 = sqlgGraph.traversal().V(sqlgVertex2.id()).next();
        Assert.assertEquals("a2", sqlgVertex2.value("property1"));
        Assert.assertEquals("b22", sqlgVertex2.value("property2"));
        Assert.assertEquals("c2", sqlgVertex2.value("property3"));

        sqlgVertex3 = sqlgGraph.traversal().V(sqlgVertex3.id()).next();
        Assert.assertEquals("a3", sqlgVertex3.value("property1"));
        Assert.assertEquals("b3", sqlgVertex3.value("property2"));
        Assert.assertEquals("c33", sqlgVertex3.value("property3"));
    }

    @Test
    public void testBatchUpdateNewVertex() throws InterruptedException {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "property1", "a");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v2.property("property2", "bb");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("a", v1.value("property1"));
        Assert.assertFalse(v1.property("property2").isPresent());
        Assert.assertFalse(v2.property("property1").isPresent());
        Assert.assertEquals("bb", v2.value("property2"));
        testBatchUpdateNewVertex_assert(this.sqlgGraph, v1, v2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchUpdateNewVertex_assert(this.sqlgGraph1, v1, v2);
        }
    }

    private void testBatchUpdateNewVertex_assert(SqlgGraph sqlgGraph, Vertex v1, Vertex v2) {
        v1 = sqlgGraph.traversal().V(v1).next();
        v2 = sqlgGraph.traversal().V(v2).next();
        Assert.assertEquals("a", v1.value("property1"));
        Assert.assertFalse(v1.property("property2").isPresent());
        Assert.assertFalse(v2.property("property1").isPresent());
        Assert.assertEquals("bb", v2.value("property2"));
    }

    @Test
    public void testBatchRemoveManyEdgesTestPostgresLimit() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        for (int i = 0; i < 100000; i++) {
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
            v1.addEdge("test", v2);
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(100001, this.sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(100000, this.sqlgGraph.traversal().E().count().next().intValue());
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().normalBatchModeOn();
        vertexTraversal(this.sqlgGraph, v1).outE("test").forEachRemaining(Edge::remove);
        this.sqlgGraph.tx().commit();
        testBatchRemoveManyEdgesTestPostgresLimit_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchRemoveManyEdgesTestPostgresLimit_assert(this.sqlgGraph1);
        }
    }

    @Test
    public void testBatchRemoveManyEdgesTestPostgresLimitUserSuppliedPK() throws InterruptedException {
        Assume.assumeTrue(!isMsSqlServer());
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<String, PropertyType>() {{
                            put("uid1", PropertyType.varChar(100));
                            put("uid2", PropertyType.varChar(100));
                            put("dummy", PropertyType.STRING);
                        }},
                        ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
                );
        personVertexLabel.ensureEdgeLabelExist(
                "test",
                personVertexLabel,
                new HashMap<String, PropertyType>() {{
                    put("uid1", PropertyType.varChar(100));
                    put("uid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid1", "uid2"))
        );
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        for (int i = 0; i < 100000; i++) {
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a", "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
            v1.addEdge("test", v2, "uid1", UUID.randomUUID().toString(), "uid2", UUID.randomUUID().toString());
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(100001, this.sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(100000, this.sqlgGraph.traversal().E().count().next().intValue());
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().normalBatchModeOn();
        vertexTraversal(this.sqlgGraph, v1).outE("test").forEachRemaining(Edge::remove);
        this.sqlgGraph.tx().commit();
        testBatchRemoveManyEdgesTestPostgresLimit_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testBatchRemoveManyEdgesTestPostgresLimit_assert(this.sqlgGraph1);
        }
    }

    private void testBatchRemoveManyEdgesTestPostgresLimit_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(100001, sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(0, sqlgGraph.traversal().E().count().next().intValue());
    }

    @Test
    public void testNoProperties() {
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 0; i < 100; i++) {
            Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person");
            Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person");
            person1.addEdge("friend", person2);
            if (i != 0 && i % 100 == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().normalBatchModeOn();
            }
        }
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testBatchEdgeLoadProperty() {
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge edgeToRoot = root.addEdge("edgeToRoot", god);
        edgeToRoot.property("className", "thisthatandanother");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("GOD").count().next(), 0);
        Assert.assertEquals(god, this.sqlgGraph.traversal().V().hasLabel("GOD").next());
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("edgeToRoot").count().next(), 0);
        Assert.assertEquals(edgeToRoot, this.sqlgGraph.traversal().E().hasLabel("edgeToRoot").next());
        Assert.assertEquals("thisthatandanother", this.sqlgGraph.traversal().E().hasLabel("edgeToRoot").next().value("className"));
    }

    @Test
    public void testEmptyEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "B");
        v1.addEdge("ab", v2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().count().next(), 0);
    }

    @Test
    public void testEmpty() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Empty", "empty", "");
        this.sqlgGraph.tx().commit();
        testEmpty_assert(this.sqlgGraph, person1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testEmpty_assert(this.sqlgGraph1, person1);
        }
    }

    private void testEmpty_assert(SqlgGraph sqlgGraph, Vertex person1) {
        Assert.assertNotNull(person1.id());
        Object o = sqlgGraph.traversal().V().hasLabel("Empty").values("empty").next();
        Assert.assertEquals("", o);
    }

    @Test
    public void testEmpty106() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        this.sqlgGraph.addVertex("A", Collections.singletonMap("emptyProperty", ""));
        this.sqlgGraph.tx().commit();
        testEmpty106_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testEmpty106_assert(this.sqlgGraph1);
        }
    }

    private void testEmpty106_assert(SqlgGraph sqlgGraph) {
        Vertex a = sqlgGraph.traversal().V().hasLabel("A").next();
        Assert.assertEquals("", a.property("emptyProperty").value());
    }

}
