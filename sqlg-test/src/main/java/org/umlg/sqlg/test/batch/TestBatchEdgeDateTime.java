package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/05/09
 * Time: 8:53 PM
 */
public class TestBatchEdgeDateTime extends BaseTest {

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
    public void testLocalDateTime() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex personA = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex personB = this.sqlgGraph.addVertex(T.label, "Person", "name", "B");
        LocalDateTime localDateTime = LocalDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            localDateTime = localDateTime.truncatedTo(ChronoUnit.MILLIS);
        }
        Edge e = personA.addEdge("loves", personB, "localDateTime", localDateTime);
        this.sqlgGraph.tx().commit();
        testLocalDate_assert(this.sqlgGraph, localDateTime, e);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testLocalDate_assert(this.sqlgGraph1, localDateTime, e);
        }
    }

    private void testLocalDate_assert(SqlgGraph sqlgGraph, LocalDateTime localDateTime, Edge e) {
        assertEquals(localDateTime, sqlgGraph.traversal().E(e).next().value("localDateTime"));
    }

    @Test
    public void testLocalTime() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex personA = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex personB = this.sqlgGraph.addVertex(T.label, "Person", "name", "B");
        LocalTime localTime = LocalTime.now();
        Edge e = personA.addEdge("loves", personB, "localTime", localTime);
        this.sqlgGraph.tx().commit();
        testLocalTime_assert(this.sqlgGraph, localTime, e);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testLocalTime_assert(this.sqlgGraph1, localTime, e);
        }
    }

    private void testLocalTime_assert(SqlgGraph sqlgGraph, LocalTime localTime, Edge e) {
        assertEquals(localTime.toSecondOfDay(), sqlgGraph.traversal().E(e).next().<LocalTime>value("localTime").toSecondOfDay());
    }

    @Test
    public void testLocalDate() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex personA = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex personB = this.sqlgGraph.addVertex(T.label, "Person", "name", "B");
        LocalDate localDate = LocalDate.now();
        Edge e = personA.addEdge("loves", personB, "localDate", localDate);
        this.sqlgGraph.tx().commit();
        testLocalDate_assert(this.sqlgGraph, localDate, e);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testLocalDate_assert(this.sqlgGraph1, localDate, e);
        }
    }

    private void testLocalDate_assert(SqlgGraph sqlgGraph, LocalDate localDate, Edge e) {
        assertEquals(localDate, sqlgGraph.traversal().E(e).next().value("localDate"));
    }

    @Test
    public void testZonedDateTime() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex personA = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex personB = this.sqlgGraph.addVertex(T.label, "Person", "name", "B");
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            zonedDateTime = zonedDateTime.truncatedTo(ChronoUnit.MILLIS);
        }
        Edge e = personA.addEdge("loves", personB, "zonedDateTime", zonedDateTime);
        this.sqlgGraph.tx().commit();
        testZonedDateTime_assert(this.sqlgGraph, zonedDateTime, e);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testZonedDateTime_assert(this.sqlgGraph1, zonedDateTime, e);
        }
    }

    private void testZonedDateTime_assert(SqlgGraph sqlgGraph, ZonedDateTime zonedDateTime, Edge e) {
        assertEquals(zonedDateTime, sqlgGraph.traversal().E(e).next().value("zonedDateTime"));
    }

    @Test
    public void testDuration() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex personA = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex personB = this.sqlgGraph.addVertex(T.label, "Person", "name", "B");
        Duration duration = Duration.ofHours(5);
        Edge e = personA.addEdge("loves", personB, "duration", duration);
        this.sqlgGraph.tx().commit();
        testDuration_assert(this.sqlgGraph, duration, e);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testDuration_assert(this.sqlgGraph1, duration, e);
        }
    }

    private void testDuration_assert(SqlgGraph sqlgGraph, Duration duration, Edge e) {
        assertEquals(duration, sqlgGraph.traversal().E(e).next().value("duration"));
    }

    @Test
    public void testPeriod() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex personA = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex personB = this.sqlgGraph.addVertex(T.label, "Person", "name", "B");
        Period period = Period.of(5, 5, 5);
        Edge e = personA.addEdge("loves", personB, "period", period);
        this.sqlgGraph.tx().commit();
        testPeriod_assert(this.sqlgGraph, period, e);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testPeriod_assert(this.sqlgGraph1, period, e);
        }
    }

    private void testPeriod_assert(SqlgGraph sqlgGraph, Period period, Edge e) {
        assertEquals(period, sqlgGraph.traversal().E(e).next().value("period"));
    }

}
