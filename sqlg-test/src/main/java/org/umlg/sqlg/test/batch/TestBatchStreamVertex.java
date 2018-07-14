package org.umlg.sqlg.test.batch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.*;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SqlgExceptions;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Date: 2015/05/19
 * Time: 9:34 PM
 */
public class TestBatchStreamVertex extends BaseTest {

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
    public void testAccessPropertyFromEdgeWhileStreaming() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a2");
        Edge e1 = v1.addEdge("friend", v2);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        for (int i = 0; i < 100; i++) {
            properties.put("name", "aa" + i);
            this.sqlgGraph.streamVertex("Person", properties);
            properties.clear();
        }
        RecordId recordId = (RecordId) e1.id();
//        Assert.assertEquals("a1", SqlgEdge.of(this.sqlgGraph, recordId.getId(), recordId.getSchemaTable().getSchema(), recordId.getSchemaTable().getTable()).value("name"));
        Assert.assertEquals("a1", this.sqlgGraph.traversal().E(recordId).next().value("name"));
        this.sqlgGraph.tx().commit();
    }

     @Test(expected = IllegalStateException.class)
    public void testAccessPropertyFromVertexWhileStreaming() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a2");
        v1.addEdge("friend", v2);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
        for (int i = 0; i < 100; i++) {
            properties.put("name", "aa" + i);
            this.sqlgGraph.streamVertex("Person", properties);
            properties.clear();
        }
        RecordId recordId = (RecordId) v1.id();
        Assert.assertEquals("a1", SqlgVertex.of(
                this.sqlgGraph,
                recordId.sequenceId(),
                recordId.getSchemaTable().getSchema(),
                recordId.getSchemaTable().getTable()).value("name"));
        this.sqlgGraph.tx().commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotQueryWhileStreaming() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 1);
        this.sqlgGraph.tx().commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotQueryFromVertexWhileStreaming() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        Assert.assertEquals(100, this.sqlgGraph.traversal().V(v1).out("friend").count().next(), 1);
        this.sqlgGraph.tx().commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotQueryFromVertexWhileStreaming2() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        Assert.assertEquals(100, IteratorUtils.count(v1.edges(Direction.OUT, "friend")), 1);
        this.sqlgGraph.tx().commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotQueryFromVertexWhileStreaming3() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        Assert.assertEquals(100, IteratorUtils.count(v1.vertices(Direction.OUT, "friend")), 1);
        this.sqlgGraph.tx().commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotQueryFromGraphVerticesWhileStreaming() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        Assert.assertEquals(102, IteratorUtils.count(this.sqlgGraph.vertices()), 1);
        this.sqlgGraph.tx().commit();
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotQueryFromGraphEdgesWhileStreaming() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        Assert.assertEquals(102, IteratorUtils.count(this.sqlgGraph.edges()), 1);
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testVertexWithNoProperties() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            Assert.assertEquals(100, this.sqlgGraph1.traversal().V().hasLabel("Person").count().next(), 1);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCanNotAddVertexOnceStreaming() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValues = new LinkedHashMap<>();
        keyValues.put("name", "test");
        SqlgVertex v2 = (SqlgVertex)this.sqlgGraph.addVertex("A", keyValues);
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testCompleteVertexChecksSingleLabelOnly() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValue = new LinkedHashMap<>();
        keyValue.put("name", "a");
        keyValue.put("surname", "b");
        this.sqlgGraph.streamVertex("Person", keyValue);
        this.sqlgGraph.streamVertex("Persons", keyValue);
        this.sqlgGraph.tx().commit();
        Assert.fail();
    }

    @Test
    public void testCompleteVertexFlushAndCloseStream() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValue = new LinkedHashMap<>();
        keyValue.put("name", "a");
        keyValue.put("surname", "b");
        this.sqlgGraph.streamVertex("Person", keyValue);
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.streamVertex("Persons", keyValue);
        this.sqlgGraph.tx().commit();
        testCompleteVertexFlushAndCloseStream_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testCompleteVertexFlushAndCloseStream_assert(this.sqlgGraph1);
        }
    }

    private void testCompleteVertexFlushAndCloseStream_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(1, sqlgGraph.traversal().V().hasLabel("Person").count().next(), 0L);
        Assert.assertEquals(1, sqlgGraph.traversal().V().hasLabel("Persons").count().next(), 0L);
        Assert.assertEquals("a", sqlgGraph.traversal().V().hasLabel("Person").next().<String>value("name"));
        Assert.assertEquals("b", sqlgGraph.traversal().V().hasLabel("Person").next().<String>value("surname"));
        Assert.assertEquals("a", sqlgGraph.traversal().V().hasLabel("Persons").next().<String>value("name"));
        Assert.assertEquals("b", sqlgGraph.traversal().V().hasLabel("Persons").next().<String>value("surname"));
    }

    @Test(expected = IllegalStateException.class)
    public void testCompleteVertexChecksSameKeys() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValue = new LinkedHashMap<>();
        keyValue.put("name", "a");
        keyValue.put("surname", "b");
        this.sqlgGraph.streamVertex("Person", keyValue);
        keyValue = new LinkedHashMap<>();
        keyValue.put("namea", "a");
        keyValue.put("surname", "b");
        this.sqlgGraph.streamVertex("Person", keyValue);
        this.sqlgGraph.tx().commit();
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testStreamingVertexKeysSameOrder() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValue = new LinkedHashMap<>();
        keyValue.put("name", "a");
        keyValue.put("surname", "b");
        this.sqlgGraph.streamVertex("Person", keyValue);
        keyValue = new LinkedHashMap<>();
        keyValue.put("surname", "b");
        keyValue.put("name", "a");
        this.sqlgGraph.streamVertex("Person", keyValue);
        this.sqlgGraph.tx().commit();
        Assert.fail();
    }

    @Test
    public void testStreamingVertexDifferentSchema() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValue = new LinkedHashMap<>();
        keyValue.put("name", "a");
        keyValue.put("surname", "b");
        this.sqlgGraph.streamVertex("R_HG.Person", keyValue);
        keyValue = new LinkedHashMap<>();
        keyValue.put("name", "a");
        keyValue.put("surname", "b");
        this.sqlgGraph.streamVertex("R_HG.Person", keyValue);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("R_HG.Person").count().next(), 0L);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            Assert.assertEquals(2, this.sqlgGraph1.traversal().V().hasLabel("R_HG.Person").count().next(), 0L);
        }
    }

    @Test
    public void testUsingConnectionDuringResultSetIter() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 1; i < 100_001; i++) {
            LinkedHashMap<String, Object> keyValue = new LinkedHashMap<>();
            for (int j = 0; j < 2; j++) {
                keyValue.put("name" + j, "a" + i);
            }
            this.sqlgGraph.streamVertex("Person", keyValue);
            if (i % 25_000 == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().streamingBatchModeOn();
                System.out.println(i);
            }
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(100_000, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next().intValue());
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            Assert.assertEquals(100_000, this.sqlgGraph1.traversal().V().has(T.label, "Person").count().next().intValue());
        }

    }

    @Test
    public void testMilCompleteVertex() throws InterruptedException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 1; i < 100_001; i++) {
            LinkedHashMap<String, Object> keyValue = new LinkedHashMap<>();
            for (int j = 0; j < 2; j++) {
                keyValue.put("name" + j, "a" + i);
            }
            this.sqlgGraph.streamVertex("Person", keyValue);
            if (i % 25_000 == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().streamingBatchModeOn();
                System.out.println(i);
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        Assert.assertEquals(100_000L, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next().longValue());
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            Assert.assertEquals(100_000L, this.sqlgGraph1.traversal().V().has(T.label, "Person").count().next().longValue());
        }
    }

    @Test
    public void testStreamingRollback() throws InterruptedException {
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
        this.sqlgGraph.tx().rollback();
        testStreamingRollback_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamingRollback_assert(this.sqlgGraph1);
        }
    }

    private void testStreamingRollback_assert(SqlgGraph sqlgGraph) {
        Assert.assertEquals(0, sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(0, sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
    }

    @Test
    public void streamJava8Style() throws InterruptedException {
        List<String> uids = Arrays.asList("1", "2", "3", "4", "5");
        this.sqlgGraph.tx().streamingBatchModeOn();
        uids.forEach(u->this.sqlgGraph.streamVertex(T.label, "Person", "name", u));
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(5, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 0L);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            Assert.assertEquals(5, this.sqlgGraph1.traversal().V().hasLabel("Person").count().next(), 0L);
        }
    }

    @Test
    public void testStreamLocalDateTime() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LocalDateTime now = LocalDateTime.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        testStreamLocalDateTime_assert(this.sqlgGraph, now);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamLocalDateTime_assert(this.sqlgGraph1, now);
        }
    }

    private void testStreamLocalDateTime_assert(SqlgGraph sqlgGraph, LocalDateTime now) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now, vertices.get(0).value("createOn"));
    }

    @Test
    public void testStreamLocalDate() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LocalDate now = LocalDate.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        testStreamLocalDate_assert(this.sqlgGraph, now);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamLocalDate_assert(this.sqlgGraph1, now);
        }
    }

    private void testStreamLocalDate_assert(SqlgGraph sqlgGraph, LocalDate now) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now, vertices.get(0).value("createOn"));
    }

    @Test
    public void testStreamLocalTime() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LocalTime now = LocalTime.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        testStreamLocalTime_assert(this.sqlgGraph, now);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamLocalTime_assert(this.sqlgGraph1, now);
        }
    }

    private void testStreamLocalTime_assert(SqlgGraph sqlgGraph, LocalTime now) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now.toSecondOfDay(), vertices.get(0).<LocalTime>value("createOn").toSecondOfDay());
    }

    @Test
    public void testStreamZonedDateTime() throws InterruptedException {
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "createOn", zonedDateTime);
        }
        this.sqlgGraph.tx().commit();
        testStreamZonedDateTime_assert(this.sqlgGraph, zonedDateTime);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamZonedDateTime_assert(this.sqlgGraph1, zonedDateTime);
        }
    }

    private void testStreamZonedDateTime_assert(SqlgGraph sqlgGraph, ZonedDateTime zonedDateTime) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(zonedDateTime, vertices.get(0).<ZonedDateTime>value("createOn"));
    }

    @Test
    public void testStreamPeriod() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        Period period = Period.of(1,2,3);
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "period", period);
        }
        this.sqlgGraph.tx().commit();
        testSteamPeriod_assert(this.sqlgGraph, period);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testSteamPeriod_assert(this.sqlgGraph1, period);
        }
    }

    private void testSteamPeriod_assert(SqlgGraph sqlgGraph, Period period) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(period, vertices.get(0).<Period>value("period"));
    }

    @Test
    public void testStreamDuration() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        Duration duration = Duration.ofHours(19);
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "duration", duration);
        }
        this.sqlgGraph.tx().commit();
        testStreamDuration_assert(this.sqlgGraph, duration);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamDuration_assert(this.sqlgGraph1, duration);
        }
    }

    private void testStreamDuration_assert(SqlgGraph sqlgGraph, Duration duration) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(duration, vertices.get(0).<Duration>value("duration"));
    }

    @Test
    public void testStreamJson() throws InterruptedException {
        ObjectMapper objectMapper =  new ObjectMapper();
        ObjectNode json = new ObjectNode(objectMapper.getNodeFactory());
        json.put("username", "john");
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "doc", json);
        }
        this.sqlgGraph.tx().commit();
        testStreamJson_assert(this.sqlgGraph, json);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamJson_assert(this.sqlgGraph1, json);
        }
    }

    private void testStreamJson_assert(SqlgGraph sqlgGraph, ObjectNode json) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        JsonNode value = vertices.get(0).value("doc");
        Assert.assertEquals(json, value);
    }

    @Test
    public void testStreamStringArray() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        String[] stringArray = new String[]{"a", "b"};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", stringArray);
        }
        this.sqlgGraph.tx().commit();
        testStreamStringArray_assert(this.sqlgGraph, stringArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamStringArray_assert(this.sqlgGraph1, stringArray);
        }
    }

    private void testStreamStringArray_assert(SqlgGraph sqlgGraph, String[] stringArray) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(stringArray, vertices.get(0).<String[]>value("names"));
    }

    @Test
    public void testStreamBooleanArray() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        boolean[] booleanArray = new boolean[]{true, false};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", booleanArray);
        }
        this.sqlgGraph.tx().commit();
        testSteamBooleanArray_assert(this.sqlgGraph, booleanArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testSteamBooleanArray_assert(this.sqlgGraph1, booleanArray);
        }
    }

    private void testSteamBooleanArray_assert(SqlgGraph sqlgGraph, boolean[] booleanArray) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(booleanArray, vertices.get(0).value("names"));
    }

    @Test
    public void testStreamIntArray() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        int[] intArray = new int[]{11, 22};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", intArray);
        }
        this.sqlgGraph.tx().commit();
        testStreamIntArray_assert(this.sqlgGraph, intArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamIntArray_assert(this.sqlgGraph1, intArray);
        }
    }

    private void testStreamIntArray_assert(SqlgGraph sqlgGraph, int[] intArray) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(intArray, vertices.get(0).value("names"));
    }

    @Test
    public void testStreamLongArray() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        long[] longArray = new long[]{11, 22};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", longArray);
        }
        this.sqlgGraph.tx().commit();
        testStreamLongArray_assert(this.sqlgGraph, longArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamLongArray_assert(this.sqlgGraph1, longArray);
        }
    }

    private void testStreamLongArray_assert(SqlgGraph sqlgGraph, long[] longArray) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(longArray, vertices.get(0).value("names"));
    }

    @Test
    public void testStreamFloatArray() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        float[] floatArray = new float[]{11,11f, 22.22f};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", floatArray);
        }
        this.sqlgGraph.tx().commit();
        testStreamFloatArray_assert(this.sqlgGraph, floatArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamFloatArray_assert(this.sqlgGraph1, floatArray);
        }
    }

    private void testStreamFloatArray_assert(SqlgGraph sqlgGraph, float[] floatArray) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(floatArray, vertices.get(0).value("names"), 0f);
    }

    @Test
    public void testStreamDoubleArray() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        double[] doubleArray = new double[]{11.11d, 22.22d};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", doubleArray);
        }
        this.sqlgGraph.tx().commit();
        testStreamDoubleArray_assert(this.sqlgGraph, doubleArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamDoubleArray_assert(this.sqlgGraph1, doubleArray);
        }
    }

    private void testStreamDoubleArray_assert(SqlgGraph sqlgGraph, double[] doubleArray) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(doubleArray, vertices.get(0).value("names"), 0d);
    }

    @Test
    public void testStreamShortArray() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        short[] shortArray = new short[]{11, 22};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", shortArray);
        }
        this.sqlgGraph.tx().commit();
        testStreamShortArray_assert(this.sqlgGraph, shortArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamShortArray_assert(this.sqlgGraph1, shortArray);
        }
    }

    private void testStreamShortArray_assert(SqlgGraph sqlgGraph, short[] shortArray) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(shortArray, vertices.get(0).value("names"));
    }

    @Test
    public void testStreamByteArray() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        byte[] byteArray = new byte[]{1, 2};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", byteArray);
        }
        this.sqlgGraph.tx().commit();
        testStreamByteArray_assert(this.sqlgGraph, byteArray);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testStreamByteArray_assert(this.sqlgGraph1, byteArray);
        }
    }

    private void testStreamByteArray_assert(SqlgGraph sqlgGraph, byte[] byteArray) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(byteArray, vertices.get(0).value("names"));
    }

    @Test
    public void testLocalDateTimeArray() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LocalDateTime[] localDateTimes = new LocalDateTime[]{LocalDateTime.now().minusDays(1), LocalDateTime.now()};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", localDateTimes);
        }
        this.sqlgGraph.tx().commit();
        testLocalDateTimeArray_assert(this.sqlgGraph, localDateTimes);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testLocalDateTimeArray_assert(this.sqlgGraph1, localDateTimes);
        }
    }

    private void testLocalDateTimeArray_assert(SqlgGraph sqlgGraph, LocalDateTime[] localDateTimes) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(localDateTimes, vertices.get(0).<LocalDateTime[]>value("names"));
    }

    @Test
    public void testLocalDateArray() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LocalDate[] localDates = new LocalDate[]{LocalDate.now().minusDays(1), LocalDate.now()};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", localDates);
        }
        this.sqlgGraph.tx().commit();
        testLocalDateArray_assert(this.sqlgGraph, localDates);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testLocalDateArray_assert(this.sqlgGraph1, localDates);
        }
    }

    private void testLocalDateArray_assert(SqlgGraph sqlgGraph, LocalDate[] localDates) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(localDates, vertices.get(0).<LocalDate[]>value("names"));
    }

    @Test
    public void testLocalTimeArray() throws InterruptedException {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LocalTime[] localTimes = new LocalTime[]{LocalTime.now().minusHours(1), LocalTime.now()};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", localTimes);
        }
        this.sqlgGraph.tx().commit();
        testLocalTimeArray_assert(this.sqlgGraph, localTimes);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testLocalTimeArray_assert(this.sqlgGraph1, localTimes);
        }
    }

    private void testLocalTimeArray_assert(SqlgGraph sqlgGraph, LocalTime[] localTimes) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        List<LocalTime> localTimes1 = new ArrayList<>();
        for (LocalTime localTime : localTimes) {
            localTimes1.add(localTime.minusNanos(localTime.getNano()));
        }
        Assert.assertArrayEquals(localTimes1.toArray(), vertices.get(0).<LocalTime[]>value("names"));
    }

    @Test
    public void testZonedDateTimeArray() throws InterruptedException {
        ZonedDateTime[] zonedDateTimes = new ZonedDateTime[]{ZonedDateTime.now().minusHours(1), ZonedDateTime.now()};
        this.sqlgGraph.addVertex(T.label, "Person", "names", zonedDateTimes);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", zonedDateTimes);
        }
        this.sqlgGraph.tx().commit();
        testZonedDateTimeArray_assert(this.sqlgGraph, zonedDateTimes);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testZonedDateTimeArray_assert(this.sqlgGraph1, zonedDateTimes);
        }
    }

    private void testZonedDateTimeArray_assert(SqlgGraph sqlgGraph, ZonedDateTime[] zonedDateTimes) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(11, vertices.size());
        List<ZonedDateTime> zonedDateTimes1 = new ArrayList<>();
        zonedDateTimes1.addAll(Arrays.asList(zonedDateTimes));
        Assert.assertArrayEquals(zonedDateTimes1.toArray(), vertices.get(0).<ZonedDateTime[]>value("names"));
        Assert.assertArrayEquals(zonedDateTimes1.toArray(), vertices.get(1).<ZonedDateTime[]>value("names"));
        Assert.assertArrayEquals(zonedDateTimes1.toArray(), vertices.get(2).<ZonedDateTime[]>value("names"));
        Assert.assertArrayEquals(zonedDateTimes1.toArray(), vertices.get(3).<ZonedDateTime[]>value("names"));
        Assert.assertArrayEquals(zonedDateTimes1.toArray(), vertices.get(4).<ZonedDateTime[]>value("names"));
        Assert.assertArrayEquals(zonedDateTimes1.toArray(), vertices.get(5).<ZonedDateTime[]>value("names"));
        Assert.assertArrayEquals(zonedDateTimes1.toArray(), vertices.get(6).<ZonedDateTime[]>value("names"));
        Assert.assertArrayEquals(zonedDateTimes1.toArray(), vertices.get(7).<ZonedDateTime[]>value("names"));
        Assert.assertArrayEquals(zonedDateTimes1.toArray(), vertices.get(8).<ZonedDateTime[]>value("names"));
        Assert.assertArrayEquals(zonedDateTimes1.toArray(), vertices.get(9).<ZonedDateTime[]>value("names"));
        Assert.assertArrayEquals(zonedDateTimes1.toArray(), vertices.get(10).<ZonedDateTime[]>value("names"));
    }

    @Test
    public void testDurationArray() throws InterruptedException {
        Duration[] durations = new Duration[]{Duration.ofHours(5), Duration.ofHours(10)};
        this.sqlgGraph.addVertex(T.label, "Person", "names", durations);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", durations);
        }
        this.sqlgGraph.tx().commit();
        testDurationArray_assert(this.sqlgGraph, durations);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testDurationArray_assert(this.sqlgGraph1, durations);
        }
    }

    private void testDurationArray_assert(SqlgGraph sqlgGraph, Duration[] durations) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(11, vertices.size());
        List<Duration> durations1 = new ArrayList<>();
        durations1.addAll(Arrays.asList(durations));
        Assert.assertArrayEquals(durations1.toArray(), vertices.get(0).<Duration[]>value("names"));
        Assert.assertArrayEquals(durations1.toArray(), vertices.get(1).<Duration[]>value("names"));
        Assert.assertArrayEquals(durations1.toArray(), vertices.get(2).<Duration[]>value("names"));
        Assert.assertArrayEquals(durations1.toArray(), vertices.get(3).<Duration[]>value("names"));
        Assert.assertArrayEquals(durations1.toArray(), vertices.get(4).<Duration[]>value("names"));
        Assert.assertArrayEquals(durations1.toArray(), vertices.get(5).<Duration[]>value("names"));
        Assert.assertArrayEquals(durations1.toArray(), vertices.get(6).<Duration[]>value("names"));
        Assert.assertArrayEquals(durations1.toArray(), vertices.get(7).<Duration[]>value("names"));
        Assert.assertArrayEquals(durations1.toArray(), vertices.get(8).<Duration[]>value("names"));
        Assert.assertArrayEquals(durations1.toArray(), vertices.get(9).<Duration[]>value("names"));
        Assert.assertArrayEquals(durations1.toArray(), vertices.get(10).<Duration[]>value("names"));
    }

    @Test
    public void testPeriodArray() throws InterruptedException {
        Period[] periods = new Period[]{Period.of(2016,1,1), Period.of(2017,2,2)};
        this.sqlgGraph.addVertex(T.label, "Person", "names", periods);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", periods);
        }
        this.sqlgGraph.tx().commit();
        testPeriodArray_assert(this.sqlgGraph, periods);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testPeriodArray_assert(this.sqlgGraph1, periods);
        }
    }

    private void testPeriodArray_assert(SqlgGraph sqlgGraph, Period[] periods) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(11, vertices.size());
        List<Period> periods1 = new ArrayList<>();
        periods1.addAll(Arrays.asList(periods));
        Assert.assertArrayEquals(periods1.toArray(), vertices.get(0).<Duration[]>value("names"));
        Assert.assertArrayEquals(periods1.toArray(), vertices.get(1).<Duration[]>value("names"));
        Assert.assertArrayEquals(periods1.toArray(), vertices.get(2).<Duration[]>value("names"));
        Assert.assertArrayEquals(periods1.toArray(), vertices.get(3).<Duration[]>value("names"));
        Assert.assertArrayEquals(periods1.toArray(), vertices.get(4).<Duration[]>value("names"));
        Assert.assertArrayEquals(periods1.toArray(), vertices.get(5).<Duration[]>value("names"));
        Assert.assertArrayEquals(periods1.toArray(), vertices.get(6).<Duration[]>value("names"));
        Assert.assertArrayEquals(periods1.toArray(), vertices.get(7).<Duration[]>value("names"));
        Assert.assertArrayEquals(periods1.toArray(), vertices.get(8).<Duration[]>value("names"));
        Assert.assertArrayEquals(periods1.toArray(), vertices.get(9).<Duration[]>value("names"));
        Assert.assertArrayEquals(periods1.toArray(), vertices.get(10).<Duration[]>value("names"));
    }

    @Test(expected = SqlgExceptions.InvalidPropertyTypeException.class)
    public void testStreamJsonAsArray() {
        ObjectMapper objectMapper =  new ObjectMapper();
        ObjectNode json1 = new ObjectNode(objectMapper.getNodeFactory());
        json1.put("username", "john1");
        ObjectNode json2 = new ObjectNode(objectMapper.getNodeFactory());
        json2.put("username", "john2");

        JsonNode[] jsonNodes = new JsonNode[]{json1};
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "docs", jsonNodes);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        JsonNode[] value = vertices.get(0).value("docs");
        Assert.assertArrayEquals(jsonNodes, value);
    }

}
