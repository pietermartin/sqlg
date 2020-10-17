package org.umlg.sqlg.test.localdate;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/07/15
 */
public class TestLocalDateArray extends BaseTest {

    @Test
    public void testLocalDateTimeArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalDateTimeArrayValues());
        LocalDateTime[] localDateTimes = new LocalDateTime[]{
                isHsqldb() ? LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS) : LocalDateTime.now(),
                isHsqldb() ? LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS) : LocalDateTime.now()
        };
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
    public void testLocalDateTimeArrayOnEdge() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalDateTimeArrayValues());
        LocalDateTime[] localDateTimes = new LocalDateTime[]{
                isHsqldb() ? LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS) : LocalDateTime.now(),
                isHsqldb() ? LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS) : LocalDateTime.now()
        };
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
    public void testLocalDateArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalDateArrayValues());
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
    public void testLocalDateArrayOnEdge() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalDateArrayValues());
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
    public void testLocalTimeArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalTimeArrayValues());
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
    public void testLocalTimeArrayOnEdge() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalTimeArrayValues());
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
    public void testZonedDateTimeArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsZonedDateTimeArrayValues());
        ZoneId zoneIdShanghai = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(isHsqldb() ? LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS) : LocalDateTime.now(), zoneIdShanghai);
        ZoneId zoneIdHarare = ZoneId.of("Africa/Harare");
        ZonedDateTime zonedDateTimeAGTHarare = ZonedDateTime.of(isHsqldb() ? LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS) : LocalDateTime.now(), zoneIdHarare);
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
    public void testZonedDateTimeArrayOnEdge() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsZonedDateTimeArrayValues());
        ZoneId zoneIdShanghai = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(isHsqldb() ? LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS) : LocalDateTime.now(), zoneIdShanghai);
        ZoneId zoneIdHarare = ZoneId.of("Africa/Harare");
        ZonedDateTime zonedDateTimeAGTHarare = ZonedDateTime.of(isHsqldb() ? LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS) : LocalDateTime.now(), zoneIdHarare);
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
    public void testDurationArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
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
    public void testDurationArrayOnEdge() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
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
    public void testPeriodArray() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
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
    public void testPeriodArrayOnEdge() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsIntegerArrayValues());
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
    public void testZonedDateTimeArray2() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsLocalDateTimeArrayValues());
        ZoneId zoneIdShanghai = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(isHsqldb() ? LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS) : LocalDateTime.now(), zoneIdShanghai);
        ZoneId zoneIdHarare = ZoneId.of("Africa/Harare");
        ZonedDateTime zonedDateTimeAGTHarare = ZonedDateTime.of(isHsqldb() ? LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS) : LocalDateTime.now(), zoneIdHarare);
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

}
