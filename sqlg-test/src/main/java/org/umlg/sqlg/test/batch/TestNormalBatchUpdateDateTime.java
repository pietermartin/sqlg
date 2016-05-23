package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/05/23
 * Time: 8:17 PM
 */
public class TestNormalBatchUpdateDateTime extends BaseTest {

    @Test
    public void batchUpdateLocalDateTime() {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDateTime now = LocalDateTime.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        assertEquals(10, vertices.size());
        assertEquals(now, vertices.get(0).value("createOn"));
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDateTime nowAgain = LocalDateTime.now();
        for (Vertex vertex : vertices) {
            vertex.property("createOn", nowAgain);
        }
        this.sqlgGraph.tx().commit();
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        assertEquals(10, vertices.size());
        assertEquals(nowAgain, vertices.get(0).value("createOn"));
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
        assertEquals(10, edges.size());
        assertEquals(now, edges.get(0).value("createOn"));
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDateTime nowAgain = LocalDateTime.now();
        for (Edge edge : edges) {
            edge.property("createOn", nowAgain);
        }
        this.sqlgGraph.tx().commit();
        edges = this.sqlgGraph.traversal().E().toList();
        assertEquals(10, edges.size());
        assertEquals(nowAgain, edges.get(0).value("createOn"));
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
        assertEquals(10, vertices.size());
        assertEquals(now, vertices.get(0).value("createOn"));
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDate nowAgain = LocalDate.now();
        for (Vertex vertex : vertices) {
            vertex.property("createOn", nowAgain);
        }
        this.sqlgGraph.tx().commit();
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        assertEquals(10, vertices.size());
        assertEquals(nowAgain, vertices.get(0).value("createOn"));
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
        assertEquals(10, edges.size());
        assertEquals(now, edges.get(0).value("createOn"));
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDate nowAgain = LocalDate.now();
        for (Edge edge : edges) {
            edge.property("createOn", nowAgain);
        }
        this.sqlgGraph.tx().commit();
        edges = this.sqlgGraph.traversal().E().toList();
        assertEquals(10, edges.size());
        assertEquals(nowAgain, edges.get(0).value("createOn"));
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
        assertEquals(10, vertices.size());
        assertEquals(now.toSecondOfDay(), vertices.get(0).<LocalTime>value("createOn").toSecondOfDay());
        this.sqlgGraph.tx().normalBatchModeOn();
        Thread.sleep(1000);
        LocalTime nowAgain = LocalTime.now();
        for (Vertex vertex : vertices) {
            vertex.property("createOn", nowAgain);
        }
        this.sqlgGraph.tx().commit();
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        assertEquals(10, vertices.size());
        assertEquals(nowAgain.toSecondOfDay(), vertices.get(0).<LocalTime>value("createOn").toSecondOfDay());
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
        assertEquals(10, edges.size());
        assertEquals(now.toSecondOfDay(), edges.get(0).<LocalTime>value("createOn").toSecondOfDay());
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalTime nowAgain = LocalTime.now().minusHours(3);
        for (Edge edge : edges) {
            edge.property("createOn", nowAgain);
        }
        this.sqlgGraph.tx().commit();
        edges = this.sqlgGraph.traversal().E().toList();
        assertEquals(10, edges.size());
        assertEquals(nowAgain.toSecondOfDay(), edges.get(0).<LocalTime>value("createOn").toSecondOfDay());
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
        assertEquals(10, vertices.size());
        assertEquals(duration, vertices.get(0).<Duration>value("duration"));
        this.sqlgGraph.tx().normalBatchModeOn();
        duration = Duration.ofHours(10);
        for (Vertex vertex : vertices) {
            vertex.property("duration", duration);
        }
        this.sqlgGraph.tx().commit();
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        assertEquals(10, vertices.size());
        assertEquals(duration, vertices.get(0).<Duration>value("duration"));
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
        assertEquals(10, edges.size());
        assertEquals(duration, edges.get(0).<Duration>value("duration"));
        this.sqlgGraph.tx().normalBatchModeOn();
        duration = Duration.ofHours(10);
        for (Edge edge : edges) {
            edge.property("duration", duration);
        }
        this.sqlgGraph.tx().commit();
        edges = this.sqlgGraph.traversal().E().toList();
        assertEquals(10, edges.size());
        assertEquals(duration, edges.get(0).<Duration>value("duration"));
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
        assertEquals(10, vertices.size());
        assertEquals(period, vertices.get(0).<Period>value("period"));
        this.sqlgGraph.tx().normalBatchModeOn();
        period = Period.of(10, 1, 1);
        for (Vertex vertex : vertices) {
            vertex.property("period", period);
        }
        this.sqlgGraph.tx().commit();
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        assertEquals(10, vertices.size());
        assertEquals(period, vertices.get(0).<Period>value("period"));
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
        assertEquals(10, edges.size());
        assertEquals(period, edges.get(0).<Period>value("period"));
        this.sqlgGraph.tx().normalBatchModeOn();
        period = Period.of(10, 1, 1);
        for (Edge edge : edges) {
            edge.property("period", period);
        }
        this.sqlgGraph.tx().commit();
        edges = this.sqlgGraph.traversal().E().toList();
        assertEquals(10, edges.size());
        assertEquals(period, edges.get(0).<Period>value("period"));
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
        assertEquals(10, vertices.size());
        assertEquals(zonedDateTime, vertices.get(0).value("createOn"));
        this.sqlgGraph.tx().normalBatchModeOn();
        zonedDateTime = ZonedDateTime.now().minusDays(1);
        for (Vertex vertex : vertices) {
            vertex.property("createOn", zonedDateTime);
        }
        this.sqlgGraph.tx().commit();
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        assertEquals(10, vertices.size());
        assertEquals(zonedDateTime, vertices.get(0).value("createOn"));
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
        assertEquals(10, edges.size());
        assertEquals(zonedDateTime, edges.get(0).value("createOn"));
        this.sqlgGraph.tx().normalBatchModeOn();
        zonedDateTime = ZonedDateTime.now().minusDays(1);
        for (Edge edge : edges) {
            edge.property("createOn", zonedDateTime);
        }
        this.sqlgGraph.tx().commit();
        edges = this.sqlgGraph.traversal().E().toList();
        assertEquals(10, edges.size());
        assertEquals(zonedDateTime, edges.get(0).value("createOn"));
    }
}
