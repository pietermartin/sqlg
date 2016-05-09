package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.BatchManager;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/05/09
 * Time: 8:03 PM
 */
public class TestBatchDateTime extends BaseTest {

    @Test
    public void testLocalDateTime() {
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        LocalDateTime localDateTime = LocalDateTime.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localDateTime", localDateTime);
        this.sqlgGraph.tx().commit();
        assertEquals(localDateTime, this.sqlgGraph.traversal().V(a1).values("localDateTime").next());
    }

    @Test
    public void testLocalDate() {
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        LocalDate localDate = LocalDate.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localDate", localDate);
        this.sqlgGraph.tx().commit();
        assertEquals(localDate, this.sqlgGraph.traversal().V(a1).values("localDate").next());
    }

    @Test
    public void testLocalTime() {
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        LocalTime localTime = LocalTime.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localTime", localTime);
        this.sqlgGraph.tx().commit();
        assertEquals(localTime.toSecondOfDay(), this.sqlgGraph.traversal().V(a1).<LocalTime>values("localTime").next().toSecondOfDay());
    }

    @Test
    public void testZonedDateTime() {
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "zonedDateTime", zonedDateTime);
        this.sqlgGraph.tx().commit();
        assertEquals(zonedDateTime, this.sqlgGraph.traversal().V(a1).values("zonedDateTime").next());
    }

    @Test
    public void testDuration() {
        Duration duration = Duration.ofHours(5);
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "duration", duration);
        this.sqlgGraph.tx().commit();
        assertEquals(duration, this.sqlgGraph.traversal().V(a1).values("duration").next());
    }

    @Test
    public void testPeriod() {
        Period period = Period.of(5, 5, 5);
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "period", period);
        this.sqlgGraph.tx().commit();
        assertEquals(period, this.sqlgGraph.traversal().V(a1).values("period").next());
    }

}
