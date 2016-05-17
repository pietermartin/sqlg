package org.umlg.sqlg.test.batch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
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
public class TestStreamVertex extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
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
    public void testVertexWithNoProperties() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.streamVertex("Person");
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(100, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 1);
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
    public void testCompleteVertexFlushAndCloseStream() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LinkedHashMap<String, Object> keyValue = new LinkedHashMap<>();
        keyValue.put("name", "a");
        keyValue.put("surname", "b");
        this.sqlgGraph.streamVertex("Person", keyValue);
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingBatchModeOn();
        this.sqlgGraph.streamVertex("Persons", keyValue);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 0l);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("Persons").count().next(), 0l);
        Assert.assertEquals("a", this.sqlgGraph.traversal().V().hasLabel("Person").next().<String>value("name"));
        Assert.assertEquals("b", this.sqlgGraph.traversal().V().hasLabel("Person").next().<String>value("surname"));
        Assert.assertEquals("a", this.sqlgGraph.traversal().V().hasLabel("Persons").next().<String>value("name"));
        Assert.assertEquals("b", this.sqlgGraph.traversal().V().hasLabel("Persons").next().<String>value("surname"));
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
    public void testStreamingVertexDifferentSchema() {
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
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("R_HG.Person").count().next(), 0l);
    }

    @Test
    public void testMilCompleteVertex() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 1; i < 1000001; i++) {
            LinkedHashMap<String, Object> keyValue = new LinkedHashMap<>();
            for (int j = 0; j < 2; j++) {
                keyValue.put("name" + j, "a" + i);
            }
            this.sqlgGraph.streamVertex("Person", keyValue);
            if (i % 250000 == 0) {
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
        Assert.assertEquals(1000000l, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next().longValue());
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    @Test
    public void testStreamingRollback() {
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
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
    }

    @Test
    public void streamJava8Style() {
        List<String> uids = Arrays.asList("1", "2", "3", "4", "5");
        this.sqlgGraph.tx().streamingBatchModeOn();
        uids.stream().forEach(u->this.sqlgGraph.streamVertex(T.label, "Person", "name", u));
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(5, this.sqlgGraph.traversal().V().hasLabel("Person").count().next(), 0l);
    }

    @Test
    public void testStreamLocalDateTime() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LocalDateTime now = LocalDateTime.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now, vertices.get(0).value("createOn"));
    }

    @Test
    public void testStreamLocalDate() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LocalDate now = LocalDate.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now, vertices.get(0).value("createOn"));
    }

    @Test
    public void testStreamLocalTime() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LocalTime now = LocalTime.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now.toSecondOfDay(), vertices.get(0).<LocalTime>value("createOn").toSecondOfDay());
    }

    @Test
    public void testStreamZonedDateTime() {
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "createOn", zonedDateTime);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(zonedDateTime, vertices.get(0).<ZonedDateTime>value("createOn"));
    }

    @Test
    public void testStreamPeriod() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        Period period = Period.of(1,2,3);
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "period", period);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(period, vertices.get(0).<Period>value("period"));
    }

    @Test
    public void testStreamDuration() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        Duration duration = Duration.ofHours(19);
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "duration", duration);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(duration, vertices.get(0).<Period>value("duration"));
    }

    @Test
    public void testStreamJson() {
        ObjectMapper objectMapper =  new ObjectMapper();
        ObjectNode json = new ObjectNode(objectMapper.getNodeFactory());
        json.put("username", "john");
        this.sqlgGraph.tx().streamingBatchModeOn();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "doc", json);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        JsonNode value = vertices.get(0).value("doc");
        Assert.assertEquals(json, value);
    }

    @Test
    public void testStreamStringArray() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        String[] stringArray = new String[]{"a", "b"};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", stringArray);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(stringArray, vertices.get(0).<String[]>value("names"));
    }

    @Test
    public void testStreamBooleanArray() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        boolean[] booleanArray = new boolean[]{true, false};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", booleanArray);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(booleanArray, vertices.get(0).<boolean[]>value("names"));
    }

    @Test
    public void testStreamIntArray() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        int[] intArray = new int[]{11, 22};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", intArray);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(intArray, vertices.get(0).<int[]>value("names"));
    }

    @Test
    public void testStreamLongArray() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        long[] longArray = new long[]{11, 22};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", longArray);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(longArray, vertices.get(0).<long[]>value("names"));
    }

    @Test
    public void testStreamFloatArray() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        float[] floatArray = new float[]{11,11f, 22.22f};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", floatArray);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(floatArray, vertices.get(0).<float[]>value("names"), 0f);
    }
    @Test
    public void testStreamDoubleArray() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        double[] doubleArray = new double[]{11.11d, 22.22d};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", doubleArray);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(doubleArray, vertices.get(0).<double[]>value("names"), 0d);
    }

    @Test
    public void testStreamShortArray() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        short[] shortArray = new short[]{11, 22};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", shortArray);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(shortArray, vertices.get(0).<short[]>value("names"));
    }

    @Test
    public void testStreamByteArray() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        byte[] byteArray = new byte[]{1, 2};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", byteArray);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(byteArray, vertices.get(0).<byte[]>value("names"));
    }

    @Test
    public void testLocalDateTimeArray() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LocalDateTime[] localDateTimes = new LocalDateTime[]{LocalDateTime.now().minusDays(1), LocalDateTime.now()};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", localDateTimes);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(localDateTimes, vertices.get(0).<LocalDateTime[]>value("names"));
    }

    @Test
    public void testLocalDateArray() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LocalDate[] localDates = new LocalDate[]{LocalDate.now().minusDays(1), LocalDate.now()};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", localDates);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertArrayEquals(localDates, vertices.get(0).<LocalDate[]>value("names"));
    }

    @Test
    public void testLocalTimeArray() {
        this.sqlgGraph.tx().streamingBatchModeOn();
        LocalTime[] localTimes = new LocalTime[]{LocalTime.now().minusHours(1), LocalTime.now()};
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.streamVertex(T.label, "Person", "names", localTimes);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        List<LocalTime> localTimes1 = new ArrayList<>();
        for (LocalTime localTime : localTimes) {
            localTimes1.add(localTime.minusNanos(localTime.getNano()));
        }
        Assert.assertArrayEquals(localTimes1.toArray(), vertices.get(0).<LocalTime[]>value("names"));
    }

}
