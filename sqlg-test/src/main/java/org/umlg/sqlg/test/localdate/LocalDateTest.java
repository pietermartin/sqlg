package org.umlg.sqlg.test.localdate;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;
import java.util.*;

/**
 * Created by pieter on 2015/09/05.
 */
public class LocalDateTest extends BaseTest {

    @Test
    public void testLocalDateTime() throws Exception {
        LocalDateTime now = LocalDateTime.now();
        this.sqlgGraph.addVertex(T.label, "A", "dateTime", now);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property> properties = sqlgGraph1.traversal().V().hasLabel("A").properties("dateTime").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            Assert.assertEquals(now, properties.get(0).value());
        }
    }

    @Test
    public void testLocalDateTimeUpdate() throws Exception {
        LocalDateTime now = LocalDateTime.now();
        Vertex v = this.sqlgGraph.addVertex(T.label, "A", "dateTime", now);
        this.sqlgGraph.tx().commit();
        v.property("dateTime", now.plusHours(1));
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property> properties = sqlgGraph1.traversal().V().hasLabel("A").properties("dateTime").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            Assert.assertEquals(now.plusHours(1), properties.get(0).value());
        }
    }

    @Test
    public void testLocalDateTimeArray() throws Exception {
        LocalDateTime[] localDateTimes = new LocalDateTime[]{LocalDateTime.now(), LocalDateTime.now()};
        this.sqlgGraph.addVertex(T.label, "A", "localDateTimes", localDateTimes);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<LocalDateTime[]>> properties = sqlgGraph1.traversal().V().hasLabel("A").<LocalDateTime[]>properties("localDateTimes").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            LocalDateTime[] localDateTimesFromDb = properties.get(0).value();
            Assert.assertArrayEquals(localDateTimes, localDateTimesFromDb);
        }
    }

    @Test
    public void testLocalDateTimeArrayOnEdge() throws Exception {
        LocalDateTime[] localDateTimes = new LocalDateTime[]{LocalDateTime.now(), LocalDateTime.now()};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localDateTimes", localDateTimes);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "localDateTimes", localDateTimes);
        a1.addEdge("aa", a2, "localDateTimes", localDateTimes);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<LocalDateTime[]>> properties = sqlgGraph1.traversal().E().hasLabel("aa").<LocalDateTime[]>properties("localDateTimes").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            LocalDateTime[] localDateTimesFromDb = properties.get(0).value();
            Assert.assertArrayEquals(localDateTimes, localDateTimesFromDb);
        }
    }

    @Test
    public void testLocalDate() throws Exception {
        LocalDate now = LocalDate.now();
        this.sqlgGraph.addVertex(T.label, "A", "date", now);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property> properties = sqlgGraph1.traversal().V().hasLabel("A").properties("date").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            Assert.assertEquals(now, properties.get(0).value());
        }
    }

    @Test
    public void testLocalDateArray() throws Exception {
        LocalDate[] localDates = new LocalDate[]{LocalDate.now(), LocalDate.now().minusDays(1)};
        this.sqlgGraph.addVertex(T.label, "A", "localDates", localDates);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<LocalDate[]>> properties = sqlgGraph1.traversal().V().hasLabel("A").<LocalDate[]>properties("localDates").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            Assert.assertArrayEquals(localDates, properties.get(0).value());
        }
    }

    @Test
    public void testLocalDateArrayOnEdge() throws Exception {
        LocalDate[] localDates = new LocalDate[]{LocalDate.now(), LocalDate.now().minusDays(1)};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localDates", localDates);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "localDates", localDates);
        a1.addEdge("aa", a2, "localDates", localDates);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<LocalDate[]>> properties = sqlgGraph1.traversal().E().hasLabel("aa").<LocalDate[]>properties("localDates").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            Assert.assertArrayEquals(localDates, properties.get(0).value());
        }
    }

    @Test
    public void testLocalTime() throws Exception {
        LocalTime now = LocalTime.now();
        this.sqlgGraph.addVertex(T.label, "A", "time", now);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<LocalTime>> properties = sqlgGraph1.traversal().V().hasLabel("A").<LocalTime>properties("time").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            LocalTime value = properties.get(0).<LocalTime>value();
            Assert.assertEquals(now.toSecondOfDay(), value.toSecondOfDay());
        }
    }

    @Test
    public void testLocalTimeArray() throws Exception {
        LocalTime[] localTimes = new LocalTime[]{LocalTime.now(), LocalTime.now().minusHours(1)};
        this.sqlgGraph.addVertex(T.label, "A", "localTimes", localTimes);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<LocalTime[]>> properties = sqlgGraph1.traversal().V().hasLabel("A").<LocalTime[]>properties("localTimes").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            LocalTime[] value = properties.get(0).<LocalTime[]>value();
            List<LocalTime> localTimes1 = new ArrayList<>();
            for (LocalTime localTime : value) {
                localTimes1.add(localTime.minusNanos(localTime.getNano()));
            }
            Assert.assertArrayEquals(localTimes1.toArray(), value);
        }
    }

    @Test
    public void testLocalTimeArrayOnEdge() throws Exception {
        LocalTime[] localTimes = new LocalTime[]{LocalTime.now(), LocalTime.now().minusHours(1)};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "localTimes", localTimes);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "localTimes", localTimes);
        a1.addEdge("aa", a2, "localTimes", localTimes);
        this.sqlgGraph.tx().commit();
        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<LocalTime[]>> properties = sqlgGraph1.traversal().E().hasLabel("aa").<LocalTime[]>properties("localTimes").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            LocalTime[] value = properties.get(0).<LocalTime[]>value();
            List<LocalTime> localTimes1 = new ArrayList<>();
            for (LocalTime localTime : value) {
                localTimes1.add(localTime.minusNanos(localTime.getNano()));
            }
            Assert.assertArrayEquals(localTimes1.toArray(), value);
        }
    }

    @Test
    public void testZonedDateTime() throws Exception {
        ZoneId zoneIdShanghai = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(LocalDateTime.now(), zoneIdShanghai);
        this.sqlgGraph.addVertex(T.label, "A", "zonedDateTime", zonedDateTimeAGT);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<ZonedDateTime>> properties = sqlgGraph1.traversal().V().hasLabel("A").<ZonedDateTime>properties("zonedDateTime").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            ZonedDateTime value = properties.get(0).<ZonedDateTime>value();
            Assert.assertEquals(zonedDateTimeAGT, value);
        }
    }

    @Test
    public void testZonedDateTimeArray() throws Exception {
        ZoneId zoneIdShanghai = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(LocalDateTime.now(), zoneIdShanghai);
        ZoneId zoneIdHarare = ZoneId.of("Africa/Harare");
        ZonedDateTime zonedDateTimeAGTHarare = ZonedDateTime.of(LocalDateTime.now(), zoneIdHarare);
        ZonedDateTime[] zonedDateTimes = {zonedDateTimeAGT, zonedDateTimeAGTHarare};
        this.sqlgGraph.addVertex(T.label, "A", "zonedDateTimes", zonedDateTimes);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        //noinspection Duplicates
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<ZonedDateTime[]>> properties = sqlgGraph1.traversal().V().hasLabel("A").<ZonedDateTime[]>properties("zonedDateTimes").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            ZonedDateTime[] value = properties.get(0).<ZonedDateTime[]>value();
            Assert.assertArrayEquals(zonedDateTimes, value);
        }
    }

    @Test
    public void testZonedDateTimeArrayOnEdge() throws Exception {
        ZoneId zoneIdShanghai = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(LocalDateTime.now(), zoneIdShanghai);
        ZoneId zoneIdHarare = ZoneId.of("Africa/Harare");
        ZonedDateTime zonedDateTimeAGTHarare = ZonedDateTime.of(LocalDateTime.now(), zoneIdHarare);
        ZonedDateTime[] zonedDateTimes = {zonedDateTimeAGT, zonedDateTimeAGTHarare};

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "zonedDateTimes", zonedDateTimes);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "zonedDateTimes", zonedDateTimes);
        a1.addEdge("aa", a2, "zonedDateTimes", zonedDateTimes);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<ZonedDateTime[]>> properties = sqlgGraph1.traversal().E().hasLabel("aa").<ZonedDateTime[]>properties("zonedDateTimes").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            ZonedDateTime[] localDateTimesFromDb = properties.get(0).value();
            Assert.assertArrayEquals(zonedDateTimes, localDateTimesFromDb);
        }
    }

    @Test
    public void testDuration() throws Exception {
        Duration duration = Duration.ofHours(5);
        this.sqlgGraph.addVertex(T.label, "A", "duration", duration);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property> properties = sqlgGraph1.traversal().V().hasLabel("A").properties("duration").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            Assert.assertEquals(duration, properties.get(0).value());
        }
    }

    @Test
    public void testDurationArray() throws Exception {
        Duration[] durations = new Duration[]{Duration.ofHours(1), Duration.ofHours(5)};
        this.sqlgGraph.addVertex(T.label, "A", "durations", durations);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<Duration[]>> properties = sqlgGraph1.traversal().V().hasLabel("A").<Duration[]>properties("durations").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            Duration[] durationsFromDb = properties.get(0).value();
            Assert.assertArrayEquals(durations, durationsFromDb);
        }
    }

    @Test
    public void testDurationArrayOnEdge() throws Exception {
        Duration[] durations = new Duration[]{Duration.ofHours(1), Duration.ofHours(5)};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "duration", durations);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "duration", durations);
        a1.addEdge("aa", a2, "duration", durations);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<Duration[]>> properties = sqlgGraph1.traversal().E().hasLabel("aa").<Duration[]>properties("duration").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            Duration[] durationsFromDb = properties.get(0).value();
            Assert.assertArrayEquals(durations, durationsFromDb);
        }
    }

    @Test
    public void testPeriod() throws Exception {
        Period period = Period.of(2016, 5, 5);
        this.sqlgGraph.addVertex(T.label, "A", "period", period);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property> properties = sqlgGraph1.traversal().V().hasLabel("A").properties("period").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            Assert.assertEquals(period, properties.get(0).value());
        }
    }

    @Test
    public void testPeriodArray() throws Exception {
        Period[] periods = new Period[]{Period.of(2016, 5, 5), Period.of(2015, 4, 4)};
        this.sqlgGraph.addVertex(T.label, "A", "periods", periods);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<Period[]>> properties = sqlgGraph1.traversal().V().hasLabel("A").<Period[]>properties("periods").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            Period[] periodsFromDb = properties.get(0).value();
            Assert.assertArrayEquals(periods, periodsFromDb);
        }
    }

    @Test
    public void testPeriodArrayOnEdge() throws Exception {
        Period[] periods = new Period[]{Period.of(2016, 5, 5), Period.of(2015, 4, 4)};
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "periods", periods);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "periods", periods);
        a1.addEdge("aa", a2, "periods", periods);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<Period[]>> properties = sqlgGraph1.traversal().E().hasLabel("aa").<Period[]>properties("periods").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            Period[] periodsFromDb = properties.get(0).value();
            Assert.assertArrayEquals(periods, periodsFromDb);
        }
    }

    @Test
    public void testLocalDateVertex() {
        ZoneId zoneIdShanghai = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(LocalDateTime.now(), zoneIdShanghai);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "andReBornAgain", zonedDateTimeAGT);
        LocalDate now = LocalDate.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "born", now);
        LocalDateTime now1 = LocalDateTime.now();
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "bornAgain", now1);
        ZoneId zoneIdHarare = ZoneId.of("Africa/Harare");
        ZonedDateTime zonedDateTimeAGTHarare = ZonedDateTime.of(LocalDateTime.now(), zoneIdHarare);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "andBornAgain", zonedDateTimeAGTHarare);
        LocalTime now2 = LocalTime.now();
        Vertex a5 = this.sqlgGraph.addVertex(T.label, "A", "time", now2);
        this.sqlgGraph.tx().commit();

        LocalDate ld = this.sqlgGraph.traversal().V(a1.id()).next().value("born");
        Assert.assertEquals(now, ld);
        LocalDateTime ldt = this.sqlgGraph.traversal().V(a2.id()).next().value("bornAgain");
        Assert.assertEquals(now1, ldt);
        ZonedDateTime zonedDateTime = this.sqlgGraph.traversal().V(a3.id()).next().value("andBornAgain");
        Assert.assertEquals(zonedDateTimeAGTHarare, zonedDateTime);
        zonedDateTime = this.sqlgGraph.traversal().V(a4.id()).next().value("andReBornAgain");
        Assert.assertEquals(zonedDateTimeAGT, zonedDateTime);
        LocalTime localTime = this.sqlgGraph.traversal().V(a5.id()).next().value("time");
        Assert.assertEquals(now2.toSecondOfDay(), localTime.toSecondOfDay());
    }

    @Test
    public void testZonedDateTimeArray2() throws Exception {
        ZoneId zoneIdShanghai = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(LocalDateTime.now(), zoneIdShanghai);
        ZoneId zoneIdHarare = ZoneId.of("Africa/Harare");
        ZonedDateTime zonedDateTimeAGTHarare = ZonedDateTime.of(LocalDateTime.now(), zoneIdHarare);
        ZonedDateTime[] zonedDateTimes = new ZonedDateTime[]{zonedDateTimeAGT, zonedDateTimeAGTHarare};
        this.sqlgGraph.addVertex(T.label, "A", "zonedDateTimes", zonedDateTimes);
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        //noinspection Duplicates
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<ZonedDateTime[]>> properties = sqlgGraph1.traversal().V().hasLabel("A").<ZonedDateTime[]>properties("zonedDateTimes").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            ZonedDateTime[] value = properties.get(0).<ZonedDateTime[]>value();
            Assert.assertArrayEquals(zonedDateTimes, value);
        }
    }

    @Test
    public void testLocalDateManyTimes() {
        ZoneId zoneIdShanghai = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(LocalDateTime.now(), zoneIdShanghai);
        ZoneId zoneIdHarare = ZoneId.of("Africa/Harare");
        ZonedDateTime zonedDateTimeAGTHarare = ZonedDateTime.of(LocalDateTime.now(), zoneIdHarare);
        LocalDate now = LocalDate.now();
        LocalDateTime now1 = LocalDateTime.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A",
                "created1", now,
                "created2", now1,
                "created3", zonedDateTimeAGT,
                "created4", zonedDateTimeAGTHarare
        );
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(now, this.sqlgGraph.traversal().V(a1.id()).next().value("created1"));
        Assert.assertEquals(now1, this.sqlgGraph.traversal().V(a1.id()).next().value("created2"));
        Assert.assertEquals(zonedDateTimeAGT, this.sqlgGraph.traversal().V(a1.id()).next().value("created3"));
        Assert.assertEquals(zonedDateTimeAGTHarare, this.sqlgGraph.traversal().V(a1.id()).next().value("created4"));
    }

    @Test
    public void testLocalDateEdge() {
        ZoneId zoneIdShanghai = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(LocalDateTime.now(), zoneIdShanghai);
        ZoneId zoneIdHarare = ZoneId.of("Africa/Harare");
        ZonedDateTime zonedDateTimeAGTHarare = ZonedDateTime.of(LocalDateTime.now(), zoneIdHarare);
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "born", LocalDate.now());
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "born", LocalDate.now());
        LocalDate now = LocalDate.now();
        LocalDateTime now1 = LocalDateTime.now();
        LocalTime time = LocalTime.now();
        Edge e1 = a1.addEdge("halo", a2,
                "created1", now,
                "created2", now1,
                "created3", zonedDateTimeAGT,
                "created4", zonedDateTimeAGTHarare,
                "created5", time
        );
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(now, this.sqlgGraph.traversal().E(e1.id()).next().value("created1"));
        Assert.assertEquals(now1, this.sqlgGraph.traversal().E(e1.id()).next().value("created2"));
        Assert.assertEquals(zonedDateTimeAGT, this.sqlgGraph.traversal().E(e1.id()).next().value("created3"));
        Assert.assertEquals(zonedDateTimeAGTHarare, this.sqlgGraph.traversal().E(e1.id()).next().value("created4"));
        Assert.assertEquals(time.toSecondOfDay(), this.sqlgGraph.traversal().E(e1.id()).next().<LocalTime>value("created5").toSecondOfDay());
    }

    @Test
    public void testPeriod2() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "period", Period.of(1, 1, 1));
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "period", Period.of(11, 11, 11));
        a1.addEdge("test", a2, "period", Period.of(22, 10, 22));
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(Period.of(1,1,1), this.sqlgGraph.traversal().V(a1.id()).next().value("period"));
        Assert.assertEquals(Period.of(11,11,11), this.sqlgGraph.traversal().V(a2.id()).next().value("period"));
        Assert.assertEquals(Period.of(22,10,22), this.sqlgGraph.traversal().V(a1.id()).outE().next().value("period"));
        Assert.assertEquals(Period.of(11, 11, 11), this.sqlgGraph.traversal().V(a1.id()).out().next().value("period"));
    }

    @Test
    public void testDuration2() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "duration", Duration.ofSeconds(1, 1));
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "duration", Duration.ofSeconds(1, 1));
        a1.addEdge("test", a2, "duration", Duration.ofSeconds(2, 2));
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(Duration.ofSeconds(1, 1), this.sqlgGraph.traversal().V(a1.id()).next().value("duration"));
        Assert.assertEquals(Duration.ofSeconds(1, 1), this.sqlgGraph.traversal().V(a2.id()).next().value("duration"));
        Assert.assertEquals(Duration.ofSeconds(2, 2), this.sqlgGraph.traversal().V(a1.id()).outE().next().value("duration"));
        Assert.assertEquals(Duration.ofSeconds(1, 1), this.sqlgGraph.traversal().V(a1.id()).out().next().value("duration"));
    }

    @Test
    public void testLabelledZonedDate() throws InterruptedException {
        ZonedDateTime now = ZonedDateTime.now();
        Thread.sleep(1000);
        ZonedDateTime now1 = ZonedDateTime.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "now", now);
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "now", now);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "now", now);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "now", now);
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "now", now);
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "now", now);
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "now", now);
        a1.addEdge("ab", b1, "now1", now1);
        a1.addEdge("ab", b2, "now1", now1);
        b1.addEdge("bc", c1, "now1", now1);
        b1.addEdge("bc", c2, "now1", now1);
        b2.addEdge("bc", c3, "now1", now1);
        b2.addEdge("bc", c4, "now1", now1);
        this.sqlgGraph.tx().commit();

        List<Map<String, Vertex>> result = this.sqlgGraph.traversal().V().hasLabel("A").as("a").out().as("b").out().as("c").<Vertex>select("a", "b", "c").toList();
        Assert.assertEquals(4, result.size());

        //Check all 4 c are found
        List<Vertex> cs = Arrays.asList(result.get(0).get("c"), result.get(1).get("c"), result.get(2).get("c"), result.get(3).get("c"));
        Assert.assertTrue(cs.containsAll(Arrays.asList(c1, c2, c3, c4)));
        Assert.assertEquals(now, result.get(0).get("c").value("now"));
        Assert.assertEquals(now, result.get(1).get("c").value("now"));
        Assert.assertEquals(now, result.get(2).get("c").value("now"));
        Assert.assertEquals(now, result.get(3).get("c").value("now"));

        Assert.assertEquals(now, result.get(0).get("b").value("now"));
        Assert.assertEquals(now, result.get(1).get("b").value("now"));
        Assert.assertEquals(now, result.get(2).get("b").value("now"));
        Assert.assertEquals(now, result.get(3).get("b").value("now"));

        Assert.assertEquals(now, result.get(0).get("a").value("now"));
        Assert.assertEquals(now, result.get(1).get("a").value("now"));
        Assert.assertEquals(now, result.get(2).get("a").value("now"));
        Assert.assertEquals(now, result.get(3).get("a").value("now"));
    }

    @Test
    public void testLabelledZonedDateOnEdge() throws InterruptedException {
        ZonedDateTime now = ZonedDateTime.now();
        Thread.sleep(1000);
        ZonedDateTime now1 = ZonedDateTime.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "now", now);
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "now", now);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "now", now);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "now", now);
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "now", now);
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "now", now);
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "now", now);
        Edge e1 = a1.addEdge("ab", b1, "now1", now1);
        Edge e2 = a1.addEdge("ab", b2, "now1", now1);
        Edge e3 = b1.addEdge("bc", c1, "now1", now1);
        Edge e4 = b1.addEdge("bc", c2, "now1", now1);
        Edge e5 = b2.addEdge("bc", c3, "now1", now1);
        Edge e6 = b2.addEdge("bc", c4, "now1", now1);
        this.sqlgGraph.tx().commit();

        List<Map<String, Object>> result = this.sqlgGraph.traversal().V()
                .hasLabel("A").as("a")
                .outE().as("ab")
                .inV()
                .outE().as("bc")
                .inV().as("c").select("a", "ab", "bc", "c").toList();
        Assert.assertEquals(4, result.size());

        //Check all 4 c are found
        List<Object> cs = Arrays.asList(result.get(0).get("c"), result.get(1).get("c"), result.get(2).get("c"), result.get(3).get("c"));
        Assert.assertTrue(cs.containsAll(Arrays.asList(c1, c2, c3, c4)));
        List<Object> bc = Arrays.asList(result.get(0).get("bc"), result.get(1).get("bc"), result.get(2).get("bc"), result.get(3).get("bc"));
        Assert.assertTrue(bc.containsAll(Arrays.asList(e3, e4, e5, e6)));
        Set<Object> ab = new HashSet<>(Arrays.asList(result.get(0).get("ab"), result.get(1).get("ab"), result.get(2).get("ab"), result.get(3).get("ab")));
        Assert.assertEquals(2, ab.size());
        Assert.assertTrue(ab.containsAll(Arrays.asList(e1, e2)));
    }

    @Test
    public void testMultipleLabelledZonedDate() throws InterruptedException {
        ZonedDateTime now = ZonedDateTime.now();
        Thread.sleep(1000);
        ZonedDateTime now1 = ZonedDateTime.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "now", now);
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "now", now);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "now", now);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "now", now);
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "now", now);
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "now", now);
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "now", now);
        a1.addEdge("ab", b1, "now1", now1);
        a1.addEdge("ab", b2, "now1", now1);
        b1.addEdge("bc", c1, "now1", now1);
        b1.addEdge("bc", c2, "now1", now1);
        b2.addEdge("bc", c3, "now1", now1);
        b2.addEdge("bc", c4, "now1", now1);
        this.sqlgGraph.tx().commit();

        List<Map<String, Vertex>> result = this.sqlgGraph.traversal().V()
                .hasLabel("A").as("a1", "a2", "a3")
                .out().as("b1", "b2", "b3")
                .out().as("c1", "c2", "c3")
                .<Vertex>select("a1", "a2", "a3", "b1", "b2", "b3", "c1", "c2", "c3").toList();
        Assert.assertEquals(4, result.size());

        //Check all 4 c are found
        List<Vertex> cs = Arrays.asList(result.get(0).get("c1"), result.get(0).get("c2"), result.get(0).get("c3"));
        Assert.assertEquals(3, cs.size());
        Set<Vertex> csSet = new HashSet<>(cs);
        Assert.assertEquals(1, csSet.size());
        Assert.assertTrue(csSet.contains(c1) || csSet.contains(c2) || csSet.contains(c3));
    }

    @Test
    public void testLoadDateTypes() throws Exception {

        LocalDateTime localDateTime = LocalDateTime.now();
        LocalDate localDate = LocalDate.now();
        LocalTime localTime = LocalTime.now();
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        Period period = Period.of(12, 13, 14);
        Duration duration = Duration.ofSeconds(2);
        this.sqlgGraph.addVertex(T.label, "Person",
                "dateTime", localDateTime,
                "date", localDate,
                "time", localTime,
                "zonedDateTime", zonedDateTime,
                "period", period,
                "duration", duration
        );
        this.sqlgGraph.tx().commit();

        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertTrue(sqlgGraph1.traversal().V().hasLabel("Person").hasNext());
            Vertex v = sqlgGraph1.traversal().V().hasLabel("Person").next();
            Assert.assertEquals(localDateTime, v.value("dateTime"));
            Assert.assertEquals(localDate, v.value("date"));
            Assert.assertEquals(localTime.toSecondOfDay(), v.<LocalTime>value("time").toSecondOfDay());
            Assert.assertEquals(zonedDateTime, v.value("zonedDateTime"));
            Assert.assertEquals(period, v.value("period"));
            Assert.assertEquals(duration, v.value("duration"));
        }
    }
}
