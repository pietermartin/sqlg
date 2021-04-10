package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.umlg.sqlg.structure.BatchManager;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Date: 2016/05/09
 * Time: 8:03 PM
 */
public class TestBatchNormalDateTime extends BaseTest {

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
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        LocalDateTime localDateTime = LocalDateTime.now();
        if (isHsqldb() ||isMariaDb()) {
            localDateTime = localDateTime.truncatedTo(ChronoUnit.MILLIS);
        }
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localDateTime", localDateTime);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(localDateTime, this.sqlgGraph.traversal().V(a1).values("localDateTime").next());
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            Assert.assertEquals(localDateTime, this.sqlgGraph1.traversal().V(a1).values("localDateTime").next());
        }
    }

    @Test
    public void testLocalDate() throws InterruptedException {
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        LocalDate localDate = LocalDate.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localDate", localDate);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(localDate, this.sqlgGraph.traversal().V(a1).values("localDate").next());
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            Assert.assertEquals(localDate, this.sqlgGraph1.traversal().V(a1).values("localDate").next());
        }
    }

    @Test
    public void testLocalTime() throws InterruptedException {
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        LocalTime localTime = LocalTime.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localTime", localTime);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(localTime.toSecondOfDay(), this.sqlgGraph.traversal().V(a1).<LocalTime>values("localTime").next().toSecondOfDay());
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            Assert.assertEquals(localTime.toSecondOfDay(), this.sqlgGraph1.traversal().V(a1).<LocalTime>values("localTime").next().toSecondOfDay());
        }
    }

    @Test
    public void testZonedDateTime() throws InterruptedException {
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            zonedDateTime = zonedDateTime.truncatedTo(ChronoUnit.MILLIS);
        }
        ZonedDateTime zdt2 = ZonedDateTime.of(LocalDateTime.now(), ZoneId.of("+02:00"));
        if (isHsqldb() || isMariaDb()) {
            zdt2 = zdt2.truncatedTo(ChronoUnit.MILLIS);
        }
        // ZoneId corrects +02:00 into GTM+02:00
        ZonedDateTime zdt2Fixed = ZonedDateTime.of(zdt2.toLocalDateTime(), ZoneId.of("GMT+02:00"));
        if (isHsqldb() || isMariaDb()) {
            zdt2Fixed = zdt2Fixed.truncatedTo(ChronoUnit.MILLIS);
        }
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "zonedDateTime", zonedDateTime
                , "zdt2", zdt2
                , "zdt2Fixed", zdt2Fixed);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(zonedDateTime, this.sqlgGraph.traversal().V(a1).values("zonedDateTime").next());
        Assert.assertEquals(zdt2Fixed, this.sqlgGraph.traversal().V(a1).values("zdt2").next());
        Assert.assertEquals(zdt2Fixed, this.sqlgGraph.traversal().V(a1).values("zdt2Fixed").next());
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            Assert.assertEquals(zonedDateTime, this.sqlgGraph1.traversal().V(a1).values("zonedDateTime").next());
            Assert.assertEquals(zdt2Fixed, this.sqlgGraph1.traversal().V(a1).values("zdt2").next());
            Assert.assertEquals(zdt2Fixed, this.sqlgGraph1.traversal().V(a1).values("zdt2Fixed").next());
        }
    }

    @Test
    public void testDuration() throws InterruptedException {
        Duration duration = Duration.ofHours(5);
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "duration", duration);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(duration, this.sqlgGraph.traversal().V(a1).values("duration").next());
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            Assert.assertEquals(duration, this.sqlgGraph1.traversal().V(a1).values("duration").next());
        }
    }

    @Test
    public void testPeriod() throws InterruptedException {
        Period period = Period.of(5, 5, 5);
        this.sqlgGraph.tx().batchMode(BatchManager.BatchModeType.NORMAL);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "period", period);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(period, this.sqlgGraph.traversal().V(a1).values("period").next());
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            Assert.assertEquals(period, this.sqlgGraph1.traversal().V(a1).values("period").next());
        }
    }

    @Test
    public void batchLocalDateTime() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDateTime now = LocalDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            now = now.truncatedTo(ChronoUnit.MILLIS);
        }
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        batchLocalDateTime_assert(this.sqlgGraph, now);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            batchLocalDateTime_assert(this.sqlgGraph1, now);
        }

    }

    private void batchLocalDateTime_assert(SqlgGraph sqlgGraph, LocalDateTime now) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now, vertices.get(0).value("createOn"));
    }

    @Test
    public void batchLocalDate() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalDate now = LocalDate.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        batchLocalDate_assert(this.sqlgGraph, now);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            batchLocalDate_assert(this.sqlgGraph1, now);
        }
    }

    private void batchLocalDate_assert(SqlgGraph sqlgGraph, LocalDate now) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now, vertices.get(0).value("createOn"));
    }

    @Test
    public void batchLocalTime() throws InterruptedException {
        this.sqlgGraph.tx().normalBatchModeOn();
        LocalTime now = LocalTime.now();
        for (int i = 0; i < 10; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "createOn", now);
        }
        this.sqlgGraph.tx().commit();
        batchLocalTime_assert(this.sqlgGraph, now);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            batchLocalTime_assert(this.sqlgGraph1, now);
        }
    }

    private void batchLocalTime_assert(SqlgGraph sqlgGraph, LocalTime now) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(10, vertices.size());
        Assert.assertEquals(now.toSecondOfDay(), vertices.get(0).<LocalTime>value("createOn").toSecondOfDay());
    }

    @Test
    public void batchDuration() throws InterruptedException {
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
        batchDuration_assert(this.sqlgGraph, duration0, duration1, duration2, duration3);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            batchDuration_assert(this.sqlgGraph1, duration0, duration1, duration2, duration3);
        }
    }

    private void batchDuration_assert(SqlgGraph sqlgGraph, Duration duration0, Duration duration1, Duration duration2, Duration duration3) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(4, vertices.size());
        Assert.assertEquals(duration0, vertices.get(0).<Duration>value("duration"));
        Assert.assertEquals(duration1, vertices.get(1).<Duration>value("duration"));
        Assert.assertEquals(duration2, vertices.get(2).<Duration>value("duration"));
        Assert.assertEquals(duration3, vertices.get(3).<Duration>value("duration"));
    }

    @Test
    public void batchPeriod() throws InterruptedException {
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
        batchPeriod_assert(this.sqlgGraph, period0, period1, period2, period3);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            batchPeriod_assert(this.sqlgGraph1, period0, period1, period2, period3);
        }
    }

    private void batchPeriod_assert(SqlgGraph sqlgGraph, Period period0, Period period1, Period period2, Period period3) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").toList();
        Assert.assertEquals(4, vertices.size());
        Assert.assertEquals(period0, vertices.get(0).<Period>value("period"));
        Assert.assertEquals(period1, vertices.get(1).<Period>value("period"));
        Assert.assertEquals(period2, vertices.get(2).<Period>value("period"));
        Assert.assertEquals(period3, vertices.get(3).<Period>value("period"));
    }

}
