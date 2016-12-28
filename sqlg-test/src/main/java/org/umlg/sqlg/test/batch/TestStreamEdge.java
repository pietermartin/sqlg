package org.umlg.sqlg.test.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 2015/07/18
 * Time: 4:18 PM
 */
public class TestStreamEdge extends BaseTest {

    final private int NUMBER_OF_VERTICES = 10_000;

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
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
    public void testEdgeFlushAndCloseStream() {
        SqlgVertex v1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        SqlgVertex v2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().streamingBatchModeOn();
        v1.streamEdge("a", v2);
        this.sqlgGraph.tx().flush();
        this.sqlgGraph.tx().streamingBatchModeOn();
        v1.streamEdge("b", v2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("a").count().next(), 1);
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("b").count().next(), 1);
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
    public void testStreamingVerticesAndEdges() {
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
        Assert.assertEquals(1000, this.sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(1000, this.sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(1000, this.sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
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
    public void testEdgeWithProperties() {
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
        Assert.assertEquals(1000, this.sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(1000, this.sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(1000, this.sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(1000, this.sqlgGraph.traversal().E().hasLabel("married").values("name").count().next(), 1);
        Assert.assertEquals(1000, this.sqlgGraph.traversal().E().hasLabel("married").values("surname").count().next(), 1);
        Assert.assertEquals("halo", this.sqlgGraph.traversal().E().hasLabel("married").values("name").next());
        Assert.assertEquals("halo", this.sqlgGraph.traversal().E().hasLabel("married").values("surname").next());
    }

    @Test
    public void testStreamLocalDateTime() {
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
        LocalDateTime now = LocalDateTime.now();
        edgeKeyValues.put("localDateTime", now);
        for (Vertex man : men) {
            SqlgVertex female = (SqlgVertex) females.get(count++);
            ((SqlgVertex)man).streamEdge("married", female, edgeKeyValues);
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().E().hasLabel("married").values("localDateTime").count().next(), 1);
        Assert.assertEquals(now, this.sqlgGraph.traversal().E().hasLabel("married").values("localDateTime").next());
    }

    @Test
    public void testStreamLocalDate() {
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
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().E().hasLabel("married").values("localDate").count().next(), 1);
        Assert.assertEquals(localDate, this.sqlgGraph.traversal().E().hasLabel("married").values("localDate").next());
    }

    @Test
    public void testStreamLocalTime() {
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
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().E().hasLabel("married").values("localTime").count().next(), 1);
        Assert.assertEquals(localTime.toSecondOfDay(), this.sqlgGraph.traversal().E().hasLabel("married").<LocalTime>values("localTime").next().toSecondOfDay());
    }

    @Test
    public void testStreamZonedDateTime() {
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
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        edgeKeyValues.put("zonedDateTime", zonedDateTime);
        for (Vertex man : men) {
            SqlgVertex female = (SqlgVertex) females.get(count++);
            ((SqlgVertex)man).streamEdge("married", female, edgeKeyValues);
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().E().hasLabel("married").values("zonedDateTime").count().next(), 1);
        Assert.assertEquals(zonedDateTime, this.sqlgGraph.traversal().E().hasLabel("married").values("zonedDateTime").next());
    }

    @Test
    public void testStreamPeriod() {
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
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().E().hasLabel("married").values("period").count().next(), 1);
        Assert.assertEquals(period, this.sqlgGraph.traversal().E().hasLabel("married").values("period").next());
    }

    @Test
    public void testStreamDuration() {
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
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().E().hasLabel("married").values("duration").count().next(), 1);
        Assert.assertEquals(duration, this.sqlgGraph.traversal().E().hasLabel("married").values("duration").next());
    }

    @Test
    public void testStreamJson() {
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
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Man").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().V().hasLabel("Female").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().E().hasLabel("married").count().next(), 1);
        Assert.assertEquals(10, this.sqlgGraph.traversal().E().hasLabel("married").values("doc").count().next(), 1);
        Assert.assertEquals(json, this.sqlgGraph.traversal().E().hasLabel("married").values("doc").next());
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
