package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/05/09
 * Time: 8:53 PM
 */
public class TestBatchEdgeDateTime extends BaseTest {

    @Test
    public void testLocalDateTime() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex personA = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex personB = this.sqlgGraph.addVertex(T.label, "Person", "name", "B");
        LocalDateTime localDateTime = LocalDateTime.now();
        Edge e = personA.addEdge("loves", personB, "localDateTime", localDateTime);
        this.sqlgGraph.tx().commit();
        assertEquals(localDateTime, this.sqlgGraph.traversal().E(e).next().value("localDateTime"));
    }

    @Test
    public void testLocalTime() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex personA = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex personB = this.sqlgGraph.addVertex(T.label, "Person", "name", "B");
        LocalTime localTime = LocalTime.now();
        Edge e = personA.addEdge("loves", personB, "localTime", localTime);
        this.sqlgGraph.tx().commit();
        assertEquals(localTime.toSecondOfDay(), this.sqlgGraph.traversal().E(e).next().<LocalTime>value("localTime").toSecondOfDay());
    }

    @Test
    public void testLocalDate() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex personA = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex personB = this.sqlgGraph.addVertex(T.label, "Person", "name", "B");
        LocalDate localDate = LocalDate.now();
        Edge e = personA.addEdge("loves", personB, "localDate", localDate);
        this.sqlgGraph.tx().commit();
        assertEquals(localDate, this.sqlgGraph.traversal().E(e).next().value("localDate"));
    }

    @Test
    public void testZonedDateTime() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex personA = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex personB = this.sqlgGraph.addVertex(T.label, "Person", "name", "B");
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        Edge e = personA.addEdge("loves", personB, "zonedDateTime", zonedDateTime);
        this.sqlgGraph.tx().commit();
        assertEquals(zonedDateTime, this.sqlgGraph.traversal().E(e).next().value("zonedDateTime"));
    }

    @Test
    public void testDuration() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex personA = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex personB = this.sqlgGraph.addVertex(T.label, "Person", "name", "B");
        Duration duration = Duration.ofHours(5);
        Edge e = personA.addEdge("loves", personB, "duration", duration);
        this.sqlgGraph.tx().commit();
        assertEquals(duration, this.sqlgGraph.traversal().E(e).next().value("duration"));
    }

    @Test
    public void testPeriod() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex personA = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex personB = this.sqlgGraph.addVertex(T.label, "Person", "name", "B");
        Period period = Period.of(5, 5, 5);
        Edge e = personA.addEdge("loves", personB, "period", period);
        this.sqlgGraph.tx().commit();
        assertEquals(period, this.sqlgGraph.traversal().E(e).next().value("period"));
    }

}
