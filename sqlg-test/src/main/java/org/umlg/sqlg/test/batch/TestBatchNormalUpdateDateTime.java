package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;
import java.util.List;

/**
 * Date: 2016/05/23
 * Time: 8:17 PM
 */
public class TestBatchNormalUpdateDateTime extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testLocalDateTimeUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDateTime localDateTime = LocalDateTime.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localDateTime1", localDateTime);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "localDateTime2", localDateTime);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "localDateTime3", localDateTime);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDateTime localDateTimeAgain = LocalDateTime.now().plusDays(1);
        a1.property("localDateTime1", localDateTimeAgain);
        a2.property("localDateTime2", localDateTimeAgain);
        a3.property("localDateTime3", localDateTimeAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertEquals(localDateTimeAgain, a1.value("localDateTime1"));
        Assert.assertFalse(a1.property("localDateTime2").isPresent());
        Assert.assertFalse(a1.property("localDateTime3").isPresent());

        Assert.assertFalse(a2.property("localDateTime1").isPresent());
        Assert.assertEquals(localDateTimeAgain, a2.value("localDateTime2"));
        Assert.assertFalse(a2.property("localDateTime3").isPresent());

        Assert.assertFalse(a3.property("localDateTime1").isPresent());
        Assert.assertFalse(a3.property("localDateTime2").isPresent());
        Assert.assertEquals(localDateTimeAgain, a3.value("localDateTime3"));

    }

    @Test
    public void testLocalDateUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDate localDate = LocalDate.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localDate1", localDate);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "localDate2", localDate);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "localDate3", localDate);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDate localDateAgain = LocalDate.now().plusDays(1);
        a1.property("localDate1", localDateAgain);
        a2.property("localDate2", localDateAgain);
        a3.property("localDate3", localDateAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertEquals(localDateAgain, a1.value("localDate1"));
        Assert.assertFalse(a1.property("localDate2").isPresent());
        Assert.assertFalse(a1.property("localDate3").isPresent());

        Assert.assertFalse(a2.property("localDate1").isPresent());
        Assert.assertEquals(localDateAgain, a2.value("localDate2"));
        Assert.assertFalse(a2.property("localDate3").isPresent());

        Assert.assertFalse(a3.property("localDate1").isPresent());
        Assert.assertFalse(a3.property("localDate2").isPresent());
        Assert.assertEquals(localDateAgain, a3.value("localDate3"));
    }

    @Test
    public void testLocalTimeUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalTime localTime = LocalTime.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localTime1", localTime);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "localTime2", localTime);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "localTime3", localTime);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        LocalTime localTimeAgain = LocalTime.now().plusHours(1);
        a1.property("localTime1", localTimeAgain);
        a2.property("localTime2", localTimeAgain);
        a3.property("localTime3", localTimeAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertEquals(localTimeAgain.toSecondOfDay(), a1.<LocalTime>value("localTime1").toSecondOfDay());
        Assert.assertFalse(a1.property("localTime2").isPresent());
        Assert.assertFalse(a1.property("localTime3").isPresent());

        Assert.assertFalse(a2.property("localTime1").isPresent());
        Assert.assertEquals(localTimeAgain.toSecondOfDay(), a2.<LocalTime>value("localTime2").toSecondOfDay());
        Assert.assertFalse(a2.property("localTime3").isPresent());

        Assert.assertFalse(a3.property("localTime1").isPresent());
        Assert.assertFalse(a3.property("localTime2").isPresent());
        Assert.assertEquals(localTimeAgain.toSecondOfDay(), a3.<LocalTime>value("localTime3").toSecondOfDay());
    }

    @Test
    public void testZonedDateTimeUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "zonedDateTime1", zonedDateTime);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "zonedDateTime2", zonedDateTime);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "zonedDateTime3", zonedDateTime);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        ZonedDateTime zonedDateTimeAgain = ZonedDateTime.now().plusHours(1);
        a1.property("zonedDateTime1", zonedDateTimeAgain);
        a2.property("zonedDateTime2", zonedDateTimeAgain);
        a3.property("zonedDateTime3", zonedDateTimeAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertEquals(zonedDateTimeAgain, a1.value("zonedDateTime1"));
        Assert.assertFalse(a1.property("zonedDateTime2").isPresent());
        Assert.assertFalse(a1.property("zonedDateTime3").isPresent());

        Assert.assertFalse(a2.property("zonedDateTime1").isPresent());
        Assert.assertEquals(zonedDateTimeAgain, a2.value("zonedDateTime2"));
        Assert.assertFalse(a2.property("zonedDateTime3").isPresent());

        Assert.assertFalse(a3.property("zonedDateTime1").isPresent());
        Assert.assertFalse(a3.property("zonedDateTime2").isPresent());
        Assert.assertEquals(zonedDateTimeAgain, a3.value("zonedDateTime3"));
    }

    @Test
    public void testDurationUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Duration duration = Duration.ofDays(2);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "duration1", duration);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "duration2", duration);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "duration3", duration);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        Duration durationAgain = Duration.ofDays(3);
        a1.property("duration1", durationAgain);
        a2.property("duration2", durationAgain);
        a3.property("duration3", durationAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertEquals(durationAgain, a1.value("duration1"));
        Assert.assertFalse(a1.property("duration2").isPresent());
        Assert.assertFalse(a1.property("duration3").isPresent());

        Assert.assertFalse(a2.property("duration1").isPresent());
        Assert.assertEquals(durationAgain, a2.value("duration2"));
        Assert.assertFalse(a2.property("duration3").isPresent());

        Assert.assertFalse(a3.property("duration1").isPresent());
        Assert.assertFalse(a3.property("duration2").isPresent());
        Assert.assertEquals(durationAgain, a3.value("duration3"));
    }

    @Test
    public void testPeriodUpdateNull() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Period period = Period.of(1, 1, 1);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "period1", period);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "period2", period);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "period3", period);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().normalBatchModeOn();
        Period periodAgain = Period.of(2, 2, 2);
        a1.property("period1", periodAgain);
        a2.property("period2", periodAgain);
        a3.property("period3", periodAgain);
        this.sqlgGraph.tx().commit();

        a1 = this.sqlgGraph.traversal().V(a1.id()).next();
        a2 = this.sqlgGraph.traversal().V(a2.id()).next();
        a3 = this.sqlgGraph.traversal().V(a3.id()).next();
        Assert.assertEquals(periodAgain, a1.value("period1"));
        Assert.assertFalse(a1.property("period2").isPresent());
        Assert.assertFalse(a1.property("period3").isPresent());

        Assert.assertFalse(a2.property("period1").isPresent());
        Assert.assertEquals(periodAgain, a2.value("period2"));
        Assert.assertFalse(a2.property("period3").isPresent());

        Assert.assertFalse(a3.property("period1").isPresent());
        Assert.assertFalse(a3.property("period2").isPresent());
        Assert.assertEquals(periodAgain, a3.value("period3"));
    }

    @Test
    public void batchUpdateLocalDateTime() {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDateTime now = LocalDateTime.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now, vertices.get(0).value("createOn"));
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDateTime nowAgain = LocalDateTime.now();
        for (Vertex vertex : vertices) {
            vertex.property("createOn", nowAgain);
        }
        this.sqlgGraph.tx().commit();
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(nowAgain, vertices.get(0).value("createOn"));
    }

    @Test
    public void batchUpdateLocalDateTimeEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDateTime now = LocalDateTime.now();
        for (int i = 0; i < 10; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
            v1.addEdge("test", v2, "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        List<Edge> edges = this.sqlgGraph.traversal().E().toList();
        Assert.assertEquals(10, edges.size());
        Assert.assertEquals(now, edges.get(0).value("createOn"));
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDateTime nowAgain = LocalDateTime.now();
        for (Edge edge : edges) {
            edge.property("createOn", nowAgain);
        }
        this.sqlgGraph.tx().commit();
        edges = this.sqlgGraph.traversal().E().toList();
        Assert.assertEquals(10, edges.size());
        Assert.assertEquals(nowAgain, edges.get(0).value("createOn"));
    }

    @Test
    public void batchUpdateLocalDate() {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDate now = LocalDate.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now, vertices.get(0).value("createOn"));
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDate nowAgain = LocalDate.now();
        for (Vertex vertex : vertices) {
            vertex.property("createOn", nowAgain);
        }
        this.sqlgGraph.tx().commit();
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(nowAgain, vertices.get(0).value("createOn"));
    }

    @Test
    public void batchUpdateLocalDateEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDate now = LocalDate.now();
        for (int i = 0; i < 10; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
            v1.addEdge("test", v2, "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        List<Edge> edges = this.sqlgGraph.traversal().E().toList();
        Assert.assertEquals(10, edges.size());
        Assert.assertEquals(now, edges.get(0).value("createOn"));
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDate nowAgain = LocalDate.now();
        for (Edge edge : edges) {
            edge.property("createOn", nowAgain);
        }
        this.sqlgGraph.tx().commit();
        edges = this.sqlgGraph.traversal().E().toList();
        Assert.assertEquals(10, edges.size());
        Assert.assertEquals(nowAgain, edges.get(0).value("createOn"));
    }

    @Test
    public void batchUpdateLocalTime() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalTime now = LocalTime.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now.toSecondOfDay(), vertices.get(0).<LocalTime>value("createOn").toSecondOfDay());
        this.sqlgGraph.tx().normalBatchModeOn();
        Thread.sleep(1000);
        LocalTime nowAgain = LocalTime.now();
        for (Vertex vertex : vertices) {
            vertex.property("createOn", nowAgain);
        }
        this.sqlgGraph.tx().commit();
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(nowAgain.toSecondOfDay(), vertices.get(0).<LocalTime>value("createOn").toSecondOfDay());
    }

    @Test
    public void batchUpdateLocalTimeEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalTime now = LocalTime.now();
        for (int i = 0; i < 10; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
            v1.addEdge("test", v2, "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        List<Edge> edges = this.sqlgGraph.traversal().E().toList();
        Assert.assertEquals(10, edges.size());
        Assert.assertEquals(now.toSecondOfDay(), edges.get(0).<LocalTime>value("createOn").toSecondOfDay());
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalTime nowAgain = LocalTime.now().minusHours(3);
        for (Edge edge : edges) {
            edge.property("createOn", nowAgain);
        }
        this.sqlgGraph.tx().commit();
        edges = this.sqlgGraph.traversal().E().toList();
        Assert.assertEquals(10, edges.size());
        Assert.assertEquals(nowAgain.toSecondOfDay(), edges.get(0).<LocalTime>value("createOn").toSecondOfDay());
    }

    @Test
    public void batchUpdateDuration() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Duration duration = Duration.ofHours(5);
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "duration", duration);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(duration, vertices.get(0).<Duration>value("duration"));
        this.sqlgGraph.tx().normalBatchModeOn();
        duration = Duration.ofHours(10);
        for (Vertex vertex : vertices) {
            vertex.property("duration", duration);
        }
        this.sqlgGraph.tx().commit();
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(duration, vertices.get(0).<Duration>value("duration"));
    }

    @Test
    public void batchUpdateDurationEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Duration duration = Duration.ofHours(5);
        for (int i = 0; i < 10; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
            v1.addEdge("test", v2, "duration", duration);
        }
        this.sqlgGraph.tx().commit();
        List<Edge> edges = this.sqlgGraph.traversal().E().toList();
        Assert.assertEquals(10, edges.size());
        Assert.assertEquals(duration, edges.get(0).<Duration>value("duration"));
        this.sqlgGraph.tx().normalBatchModeOn();
        duration = Duration.ofHours(10);
        for (Edge edge : edges) {
            edge.property("duration", duration);
        }
        this.sqlgGraph.tx().commit();
        edges = this.sqlgGraph.traversal().E().toList();
        Assert.assertEquals(10, edges.size());
        Assert.assertEquals(duration, edges.get(0).<Duration>value("duration"));
    }

    @Test
    public void batchUpdatePeriod() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Period period = Period.of(5, 5, 5);
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "period", period);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(period, vertices.get(0).<Period>value("period"));
        this.sqlgGraph.tx().normalBatchModeOn();
        period = Period.of(10, 1, 1);
        for (Vertex vertex : vertices) {
            vertex.property("period", period);
        }
        this.sqlgGraph.tx().commit();
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(period, vertices.get(0).<Period>value("period"));
    }

    @Test
    public void batchUpdatePeriodEdge() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Period period = Period.of(5, 5, 5);
        for (int i = 0; i < 10; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "period", period);
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "period", period);
            v1.addEdge("test", v2, "period", period);
        }
        this.sqlgGraph.tx().commit();
        List<Edge> edges = this.sqlgGraph.traversal().E().toList();
        Assert.assertEquals(10, edges.size());
        Assert.assertEquals(period, edges.get(0).<Period>value("period"));
        this.sqlgGraph.tx().normalBatchModeOn();
        period = Period.of(10, 1, 1);
        for (Edge edge : edges) {
            edge.property("period", period);
        }
        this.sqlgGraph.tx().commit();
        edges = this.sqlgGraph.traversal().E().toList();
        Assert.assertEquals(10, edges.size());
        Assert.assertEquals(period, edges.get(0).<Period>value("period"));
    }

    @Test
    public void batchUpdateZonedlDateTime() {
        this.sqlgGraph.tx().normalBatchModeOn();
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "createOn", zonedDateTime);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(zonedDateTime, vertices.get(0).value("createOn"));
        this.sqlgGraph.tx().normalBatchModeOn();
        zonedDateTime = ZonedDateTime.now().minusDays(1);
        for (Vertex vertex : vertices) {
            vertex.property("createOn", zonedDateTime);
        }
        this.sqlgGraph.tx().commit();
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(zonedDateTime, vertices.get(0).value("createOn"));
    }

    @Test
    public void batchUpdateZonedlDateTimeEdge() {
        this.sqlgGraph.tx().normalBatchModeOn();
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        for (int i = 0; i < 10; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "createOn", zonedDateTime);
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "createOn", zonedDateTime);
            v1.addEdge("test", v2, "createOn", zonedDateTime);
        }
        this.sqlgGraph.tx().commit();
        List<Edge> edges = this.sqlgGraph.traversal().E().toList();
        Assert.assertEquals(10, edges.size());
        Assert.assertEquals(zonedDateTime, edges.get(0).value("createOn"));
        this.sqlgGraph.tx().normalBatchModeOn();
        zonedDateTime = ZonedDateTime.now().minusDays(1);
        for (Edge edge : edges) {
            edge.property("createOn", zonedDateTime);
        }
        this.sqlgGraph.tx().commit();
        edges = this.sqlgGraph.traversal().E().toList();
        Assert.assertEquals(10, edges.size());
        Assert.assertEquals(zonedDateTime, edges.get(0).value("createOn"));
    }
}
