package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.BatchManager;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;
import java.util.List;

/**
 * Date: 2016/05/09
 * Time: 8:03 PM
 */
public class TestBatchNormalDateTime extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }


    @Test
    public void testLocalDateTime() {
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        LocalDateTime localDateTime = LocalDateTime.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localDateTime", localDateTime);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(localDateTime, this.sqlgGraph.traversal().V(a1).values("localDateTime").next());
    }

    @Test
    public void testLocalDate() {
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        LocalDate localDate = LocalDate.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localDate", localDate);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(localDate, this.sqlgGraph.traversal().V(a1).values("localDate").next());
    }

    @Test
    public void testLocalTime() {
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        LocalTime localTime = LocalTime.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localTime", localTime);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(localTime.toSecondOfDay(), this.sqlgGraph.traversal().V(a1).<LocalTime>values("localTime").next().toSecondOfDay());
    }

    @Test
    public void testZonedDateTime() {
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "zonedDateTime", zonedDateTime);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(zonedDateTime, this.sqlgGraph.traversal().V(a1).values("zonedDateTime").next());
    }

    @Test
    public void testDuration() {
        Duration duration = Duration.ofHours(5);
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "duration", duration);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(duration, this.sqlgGraph.traversal().V(a1).values("duration").next());
    }

    @Test
    public void testPeriod() {
        Period period = Period.of(5, 5, 5);
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "period", period);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(period, this.sqlgGraph.traversal().V(a1).values("period").next());
    }

    @Test
    public void batchLocalDateTime() {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDateTime now = LocalDateTime.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now, vertices.get(0).value("createOn"));
    }

    @Test
    public void batchLocalDate() {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDate now = LocalDate.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now, vertices.get(0).value("createOn"));
    }

    @Test
    public void batchLocalTime() {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalTime now = LocalTime.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now.toSecondOfDay(), vertices.get(0).<LocalTime>value("createOn").toSecondOfDay());
    }

    @Test
    public void batchDuration() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Duration duration0 = Duration.ofHours(0);
        this.sqlgGraph.addVertex(T.label, "Person", "duration", duration0);
        Duration duration1 = Duration.ofHours(1);
        this.sqlgGraph.addVertex(T.label, "Person", "duration", duration1);
        Duration duration2 = Duration.ofHours(2);
        this.sqlgGraph.addVertex(T.label, "Person", "duration", duration2);
        Duration duration3 = Duration.ofHours(3);
        this.sqlgGraph.addVertex(T.label, "Person", "duration", duration3);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(4, vertices.size());
        Assert.assertEquals(duration0, vertices.get(0).<Duration>value("duration"));
        Assert.assertEquals(duration1, vertices.get(1).<Duration>value("duration"));
        Assert.assertEquals(duration2, vertices.get(2).<Duration>value("duration"));
        Assert.assertEquals(duration3, vertices.get(3).<Duration>value("duration"));
    }

    @Test
    public void batchPeriod() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Period period0 = Period.of(2015, 3, 0);
        this.sqlgGraph.addVertex(T.label, "Person", "period", period0);
        Period period1 = Period.of(2015, 3, 1);
        this.sqlgGraph.addVertex(T.label, "Person", "period", period1);
        Period period2 = Period.of(2015, 3, 2);
        this.sqlgGraph.addVertex(T.label, "Person", "period", period2);
        Period period3 = Period.of(2015, 3, 3);
        this.sqlgGraph.addVertex(T.label, "Person", "period", period3);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(4, vertices.size());
        Assert.assertEquals(period0, vertices.get(0).<Period>value("period"));
        Assert.assertEquals(period1, vertices.get(1).<Period>value("period"));
        Assert.assertEquals(period2, vertices.get(2).<Period>value("period"));
        Assert.assertEquals(period3, vertices.get(3).<Period>value("period"));
    }


}
