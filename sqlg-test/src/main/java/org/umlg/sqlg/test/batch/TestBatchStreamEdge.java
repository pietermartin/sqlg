package org.umlg.sqlg.test.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 2015/07/18
 * Time: 4:18 PM
 */
public class TestBatchStreamEdge extends BaseTest {

    final private int NUMBER_OF_VERTICES = 1_000;

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

    @Test(expected = IllegalStateException.class)
    public void testCanNotCreateBatchEdgeWhileBatchVertexInProgress() {
        SqlgVertex v1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "Dog");
        SqlgVertex v2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "House");
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "test");
        this.sqlgGraph.streamVertex("A", keyValues);
        this.sqlgGraph.streamVertex("A", keyValues);
        v1.streamEdge("a", v2);
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testEdgeLabelRemainsTheSame() {
        SqlgVertex v1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex v2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        v1.streamEdge("a", v2);
        v1.streamEdge("b", v2);
        Assert.fail();
    }

    @Test
    public void testEdgeFlushAndCloseStream() throws InterruptedException {
        SqlgVertex v1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex v2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        v1.streamEdge("a", v2);
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingBatchModeOn();
        v1.streamEdge("b", v2);
        this.sqlgGraph.tx().commit();
        testEdgeFlushCloseStream_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testEdgeFlushCloseStream_assert(this.sqlgGraph1);
        }
    }

    private void testEdgeFlushCloseStream_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(1, sqlgGraph.traversal().E().hasLabel("a").count().next(), 1);
        Assert.assertEquals(1, sqlgGraph.traversal().E().hasLabel("b").count().next(), 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testEdgePropertiesRemainsTheSame() {
        SqlgVertex v1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex v2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        v1.streamEdge("a", v2, keyValues);
        keyValues.clear();
        keyValues.put("namea", "halo");
        v1.streamEdge("a", v2, keyValues);
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testEdgePropertiesSameOrder() {
        SqlgVertex v1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex v2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("surname", "test");
        v1.streamEdge("a", v2, keyValues);
        keyValues.clear();
        keyValues.put("surname", "test");
        keyValues.put("name", "halo");
        v1.streamEdge("a", v2, keyValues);
        Assert.fail();
    }


    @Test
    public void testStreamingVerticesAndEdges() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("surname", "halo");
        for (int i = 0; i < 1000; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Man", keyValues);
        }
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 1000; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Female", keyValues);
        }
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingBatchModeOn();
        int count = 0;
        List<Vertex> men = this.sqlgGraph.traversal().V().hasLabel("Man").toList();
        List<Vertex> females = this.sqlgGraph.traversal().V().hasLabel("Female").toList();
        for (Vertex man : men) {
            SqlgVertex female = (SqlgVertex) females.get(count++);
            ((SqlgVertex)man).streamEdge("married", female);
        }
        this.sqlgGraph.tx().commit();
        testStreamingVerticesAndEdges_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamingVerticesAndEdges_assert(this.sqlgGraph1);
        }
    }

    private void testStreamingVerticesAndEdges_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(1000, sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(1000, sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(1000, sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
    }

    @Test
    public void testMilCompleteEdges() {
        ArrayList<SqlgVertex> persons = createMilPersonVertex();
        ArrayList<SqlgVertex> cars = createMilCarVertex();
        this.sqlgGraph.tx().commit();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("name2", "halo");
        for (int i = 0; i < NUMBER_OF_VERTICES; i++) {
            SqlgVertex person = persons.get(0);
            SqlgVertex car = cars.get(i);
            person.streamEdge("person_car", car, keyValues);
        }
        this.sqlgGraph.tx().commit();
        int mb = 1024 * 1024;

        // get Runtime instance
        Runtime instance = Runtime.getRuntime();

        System.out.println("***** Heap utilization statistics [MB] *****\n");

        // available memory
        System.out.println("Total Memory: " + instance.totalMemory() / mb);

        // free memory
        System.out.println("Free Memory: " + instance.freeMemory() / mb);

        // used memory
        System.out.println("Used Memory: "
                + (instance.totalMemory() - instance.freeMemory()) / mb);

        // Maximum available memory
        System.out.println("Max Memory: " + instance.maxMemory() / mb);
        Assert.assertEquals(NUMBER_OF_VERTICES, this.sqlgGraph.traversal().V(persons.get(0)).out("person_car").toList().size());
        stopWatch.stop();
        System.out.println("testMilCompleteEdges took " + stopWatch.toString());
    }

    @Test
    public void testEdgeWithProperties() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("surname", "halo");
        for (int i = 0; i < 1000; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Man", keyValues);
        }
        this.sqlgGraph.tx().flush();
        for (int i = 0; i < 1000; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Female", keyValues);
        }
        this.sqlgGraph.tx().flush();
        int count = 0;
        List<Vertex> men = this.sqlgGraph.traversal().V().hasLabel("Man").toList();
        List<Vertex> females = this.sqlgGraph.traversal().V().hasLabel("Female").toList();
        LinkedHashMap<String, Object> edgeKeyValues = new LinkedHashMap<>();
        edgeKeyValues.put("name", "halo");
        edgeKeyValues.put("surname", "halo");
        for (Vertex man : men) {
            SqlgVertex female = (SqlgVertex) females.get(count++);
            ((SqlgVertex)man).streamEdge("married", female, edgeKeyValues);
        }
        this.sqlgGraph.tx().commit();
        testEdgeWithProperties_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testEdgeWithProperties_assert(this.sqlgGraph1);
        }
    }

    private void testEdgeWithProperties_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(1000, sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(1000, sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(1000, sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(1000, sqlgGraph.traversal().E().hasLabel("married").values("name").count().next(), 1);
        Assert.assertEquals(1000, sqlgGraph.traversal().E().hasLabel("married").values("surname").count().next(), 1);
        Assert.assertEquals("halo", sqlgGraph.traversal().E().hasLabel("married").values("name").next());
        Assert.assertEquals("halo", sqlgGraph.traversal().E().hasLabel("married").values("surname").next());
    }

    @Test
    public void testStreamLocalDateTime() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("surname", "halo");
        for (int i = 0; i < 10; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Man", keyValues);
        }
        this.sqlgGraph.tx().flush();
        for (int i = 0; i < 10; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Female", keyValues);
        }
        this.sqlgGraph.tx().flush();
        int count = 0;
        List<Vertex> men = this.sqlgGraph.traversal().V().hasLabel("Man").toList();
        List<Vertex> females = this.sqlgGraph.traversal().V().hasLabel("Female").toList();
        LinkedHashMap<String, Object> edgeKeyValues = new LinkedHashMap<>();
        LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        edgeKeyValues.put("localDateTime", now);
        for (Vertex man : men) {
            SqlgVertex female = (SqlgVertex) females.get(count++);
            ((SqlgVertex)man).streamEdge("married", female, edgeKeyValues);
        }
        this.sqlgGraph.tx().commit();
        testStreamLocalDateTime_assert(this.sqlgGraph, now);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamLocalDateTime_assert(this.sqlgGraph1, now);
        }
    }

    private void testStreamLocalDateTime_assert(SqlgGraph sqlgGraph, LocalDateTime now) {
        Assert.assertEquals(10, sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().E().hasLabel("married").values("localDateTime").count().next(), 1);
        Assert.assertEquals(now, sqlgGraph.traversal().E().hasLabel("married").values("localDateTime").next());
    }

    @Test
    public void testStreamLocalDate() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("surname", "halo");
        for (int i = 0; i < 10; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Man", keyValues);
        }
        this.sqlgGraph.tx().flush();
        for (int i = 0; i < 10; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Female", keyValues);
        }
        this.sqlgGraph.tx().flush();
        int count = 0;
        List<Vertex> men = this.sqlgGraph.traversal().V().hasLabel("Man").toList();
        List<Vertex> females = this.sqlgGraph.traversal().V().hasLabel("Female").toList();
        LinkedHashMap<String, Object> edgeKeyValues = new LinkedHashMap<>();
        LocalDate localDate = LocalDate.now();
        edgeKeyValues.put("localDate", localDate);
        for (Vertex man : men) {
            SqlgVertex female = (SqlgVertex) females.get(count++);
            ((SqlgVertex)man).streamEdge("married", female, edgeKeyValues);
        }
        this.sqlgGraph.tx().commit();
        testStreamLocalDate_assert(this.sqlgGraph, localDate);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamLocalDate_assert(this.sqlgGraph1, localDate);
        }
    }

    private void testStreamLocalDate_assert(SqlgGraph sqlgGraph, LocalDate localDate) {
        Assert.assertEquals(10, sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().E().hasLabel("married").values("localDate").count().next(), 1);
        Assert.assertEquals(localDate, sqlgGraph.traversal().E().hasLabel("married").values("localDate").next());
    }

    @Test
    public void testStreamLocalTime() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("surname", "halo");
        for (int i = 0; i < 10; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Man", keyValues);
        }
        this.sqlgGraph.tx().flush();
        for (int i = 0; i < 10; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Female", keyValues);
        }
        this.sqlgGraph.tx().flush();
        int count = 0;
        List<Vertex> men = this.sqlgGraph.traversal().V().hasLabel("Man").toList();
        List<Vertex> females = this.sqlgGraph.traversal().V().hasLabel("Female").toList();
        LinkedHashMap<String, Object> edgeKeyValues = new LinkedHashMap<>();
        LocalTime localTime = LocalTime.now();
        edgeKeyValues.put("localTime", localTime);
        for (Vertex man : men) {
            SqlgVertex female = (SqlgVertex) females.get(count++);
            ((SqlgVertex)man).streamEdge("married", female, edgeKeyValues);
        }
        this.sqlgGraph.tx().commit();
        testStreamLocalTime_assert(this.sqlgGraph, localTime);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamLocalTime_assert(this.sqlgGraph1, localTime);
        }
    }

    private void testStreamLocalTime_assert(SqlgGraph sqlgGraph, LocalTime localTime) {
        Assert.assertEquals(10, sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().E().hasLabel("married").values("localTime").count().next(), 1);
        Assert.assertEquals(localTime.toSecondOfDay(), sqlgGraph.traversal().E().hasLabel("married").<LocalTime>values("localTime").next().toSecondOfDay());
    }

    @Test
    public void testStreamZonedDateTime() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("surname", "halo");
        for (int i = 0; i < 10; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Man", keyValues);
        }
        this.sqlgGraph.tx().flush();
        for (int i = 0; i < 10; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Female", keyValues);
        }
        this.sqlgGraph.tx().flush();
        int count = 0;
        List<Vertex> men = this.sqlgGraph.traversal().V().hasLabel("Man").toList();
        List<Vertex> females = this.sqlgGraph.traversal().V().hasLabel("Female").toList();
        LinkedHashMap<String, Object> edgeKeyValues = new LinkedHashMap<>();
        ZonedDateTime zonedDateTime = ZonedDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        edgeKeyValues.put("zonedDateTime", zonedDateTime);
        for (Vertex man : men) {
            SqlgVertex female = (SqlgVertex) females.get(count++);
            ((SqlgVertex)man).streamEdge("married", female, edgeKeyValues);
        }
        this.sqlgGraph.tx().commit();
        testStreamZonedDateTime_assert(this.sqlgGraph, zonedDateTime);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamZonedDateTime_assert(this.sqlgGraph1, zonedDateTime);
        }
    }

    private void testStreamZonedDateTime_assert(SqlgGraph sqlgGraph, ZonedDateTime zonedDateTime) {
        Assert.assertEquals(10, sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().E().hasLabel("married").values("zonedDateTime").count().next(), 1);
        Assert.assertEquals(zonedDateTime, sqlgGraph.traversal().E().hasLabel("married").values("zonedDateTime").next());
    }

    @Test
    public void testStreamPeriod() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("surname", "halo");
        for (int i = 0; i < 10; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Man", keyValues);
        }
        this.sqlgGraph.tx().flush();
        for (int i = 0; i < 10; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Female", keyValues);
        }
        this.sqlgGraph.tx().flush();
        int count = 0;
        List<Vertex> men = this.sqlgGraph.traversal().V().hasLabel("Man").toList();
        List<Vertex> females = this.sqlgGraph.traversal().V().hasLabel("Female").toList();
        LinkedHashMap<String, Object> edgeKeyValues = new LinkedHashMap<>();
        Period period = Period.of(1,2,3);
        edgeKeyValues.put("period", period);
        for (Vertex man : men) {
            SqlgVertex female = (SqlgVertex) females.get(count++);
            ((SqlgVertex)man).streamEdge("married", female, edgeKeyValues);
        }
        this.sqlgGraph.tx().commit();
        testStreamPeriod_assert(this.sqlgGraph, period);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamPeriod_assert(this.sqlgGraph1, period);
        }
    }

    private void testStreamPeriod_assert(SqlgGraph sqlgGraph, Period period) {
        Assert.assertEquals(10, sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().E().hasLabel("married").values("period").count().next(), 1);
        Assert.assertEquals(period, sqlgGraph.traversal().E().hasLabel("married").values("period").next());
    }

    @Test
    public void testStreamDuration() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("surname", "halo");
        for (int i = 0; i < 10; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Man", keyValues);
        }
        this.sqlgGraph.tx().flush();
        for (int i = 0; i < 10; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Female", keyValues);
        }
        this.sqlgGraph.tx().flush();
        int count = 0;
        List<Vertex> men = this.sqlgGraph.traversal().V().hasLabel("Man").toList();
        List<Vertex> females = this.sqlgGraph.traversal().V().hasLabel("Female").toList();
        LinkedHashMap<String, Object> edgeKeyValues = new LinkedHashMap<>();
        Duration duration = Duration.ofHours(19);
        edgeKeyValues.put("duration", duration);
        for (Vertex man : men) {
            SqlgVertex female = (SqlgVertex) females.get(count++);
            ((SqlgVertex)man).streamEdge("married", female, edgeKeyValues);
        }
        this.sqlgGraph.tx().commit();
        testStreamDuration_assert(this.sqlgGraph, duration);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamDuration_assert(this.sqlgGraph1, duration);
        }
    }

    private void testStreamDuration_assert(SqlgGraph sqlgGraph, Duration duration) {
        Assert.assertEquals(10, sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().E().hasLabel("married").values("duration").count().next(), 1);
        Assert.assertEquals(duration, sqlgGraph.traversal().E().hasLabel("married").values("duration").next());
    }

    @Test
    public void testStreamJson() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "halo");
        keyValues.put("surname", "halo");
        for (int i = 0; i < 10; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Man", keyValues);
        }
        this.sqlgGraph.tx().flush();
        for (int i = 0; i < 10; i++) {
            keyValues.put("age", i);
            this.sqlgGraph.streamVertex("Female", keyValues);
        }
        this.sqlgGraph.tx().flush();
        int count = 0;
        List<Vertex> men = this.sqlgGraph.traversal().V().hasLabel("Man").toList();
        List<Vertex> females = this.sqlgGraph.traversal().V().hasLabel("Female").toList();
        LinkedHashMap<String, Object> edgeKeyValues = new LinkedHashMap<>();

        ObjectMapper objectMapper =  new ObjectMapper();
        ObjectNode json = new ObjectNode(objectMapper.getNodeFactory());
        json.put("username", "john");

        edgeKeyValues.put("doc", json);
        for (Vertex man : men) {
            SqlgVertex female = (SqlgVertex) females.get(count++);
            ((SqlgVertex)man).streamEdge("married", female, edgeKeyValues);
        }
        this.sqlgGraph.tx().commit();
        testStreamJson_assert(this.sqlgGraph, json);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamJson_assert(this.sqlgGraph1, json);
        }
    }

    private void testStreamJson_assert(SqlgGraph sqlgGraph, ObjectNode json) {
        Assert.assertEquals(10, sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(10, sqlgGraph.traversal().E().hasLabel("married").values("doc").count().next(), 1);
        Assert.assertEquals(json, sqlgGraph.traversal().E().hasLabel("married").values("doc").next());
    }

    private ArrayList<SqlgVertex> createMilPersonVertex() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        ArrayList<SqlgVertex> result = new ArrayList<>();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 1; i < NUMBER_OF_VERTICES + 1; i++) {
            Map<String, Object> keyValue = new LinkedHashMap<>();
            for (int j = 0; j < 100; j++) {
                keyValue.put("name" + j, "aaaaaaaaaa" + i);
            }
            SqlgVertex person = (SqlgVertex) this.sqlgGraph.addVertex("Person", keyValue);
            result.add(person);
            if (i % (NUMBER_OF_VERTICES / 10) == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().normalBatchModeOn();
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println("createMilPersonVertex took " + stopWatch.toString());
        return result;
    }

    private ArrayList<SqlgVertex> createMilCarVertex() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        ArrayList<SqlgVertex> result = new ArrayList<>();
        this.sqlgGraph.tx().normalBatchModeOn();
        for (int i = 1; i < NUMBER_OF_VERTICES + 1; i++) {
            Map<String, Object> keyValue = new LinkedHashMap<>();
            for (int j = 0; j < 100; j++) {
                keyValue.put("name" + j, "aaaaaaaaaa" + i);
            }
            SqlgVertex car = (SqlgVertex) this.sqlgGraph.addVertex("Car", keyValue);
            result.add(car);
            if (i % (NUMBER_OF_VERTICES / 10) == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().normalBatchModeOn();
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println("createMilCarVertex took " + stopWatch.toString());
        return result;
    }

}
