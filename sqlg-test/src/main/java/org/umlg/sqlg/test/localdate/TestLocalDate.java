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
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Created by pieter on 2015/09/05.
 */
public class TestLocalDate extends BaseTest {

    @Test
    public void testLocalDateTime() {
        LocalDateTime now = LocalDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            now = now.truncatedTo(ChronoUnit.MILLIS);
        }
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
    public void testLocalDateTimeUpdate() {
        LocalDateTime now = LocalDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            now = now.truncatedTo(ChronoUnit.MILLIS);
        }
        Vertex v = this.sqlgGraph.addVertex(T.label, "A", "dateTime", now);
        this.sqlgGraph.tx().commit();
        v.property("dateTime", now.plusHours(1));
        this.sqlgGraph.tx().commit();

        //Create a new sqlgGraph
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            List<? extends Property<?>> properties = sqlgGraph1.traversal().V().hasLabel("A").properties("dateTime").toList();
            Assert.assertEquals(1, properties.size());
            Assert.assertTrue(properties.get(0).isPresent());
            Assert.assertEquals(now.plusHours(1), properties.get(0).value());
        }
    }

    @Test
    public void testLocalDate() {
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
    public void testLocalTime() {
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
    public void testZonedDateTime() {
        ZoneId zoneIdShanghai = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(LocalDateTime.now(), zoneIdShanghai);
        if (isHsqldb() || isMariaDb()) {
            zonedDateTimeAGT = zonedDateTimeAGT.truncatedTo(ChronoUnit.MILLIS);
        }
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
    public void testDuration() {
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
    public void testPeriod() {
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
    public void testLocalDateVertex() {
        ZoneId zoneIdShanghai = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(LocalDateTime.now(), zoneIdShanghai);
        if (isHsqldb() || isMariaDb()) {
            zonedDateTimeAGT = zonedDateTimeAGT.truncatedTo(ChronoUnit.MILLIS);
        }

        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "andReBornAgain", zonedDateTimeAGT);
        LocalDate now = LocalDate.now();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "born", now);
        LocalDateTime now1 = LocalDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            now1 = now1.truncatedTo(ChronoUnit.MILLIS);
        }
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "bornAgain", now1);
        ZoneId zoneIdHarare = ZoneId.of("Africa/Harare");
        ZonedDateTime zonedDateTimeAGTHarare = ZonedDateTime.of(LocalDateTime.now(), zoneIdHarare);
        if (isHsqldb() || isMariaDb()) {
            zonedDateTimeAGTHarare = zonedDateTimeAGTHarare.truncatedTo(ChronoUnit.MILLIS);
        }
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "andBornAgain", zonedDateTimeAGTHarare);
        LocalTime now2 = LocalTime.now();
        if (isHsqldb() || isMariaDb()) {
            now2 = now2.truncatedTo(ChronoUnit.MILLIS);
        }
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
    public void testLocalDateManyTimes() {
        ZoneId zoneIdShanghai = ZoneId.of("Asia/Shanghai");
        ZonedDateTime zonedDateTimeAGT = ZonedDateTime.of(LocalDateTime.now(), zoneIdShanghai);
        if (isHsqldb() || isMariaDb()) {
            zonedDateTimeAGT = zonedDateTimeAGT.truncatedTo(ChronoUnit.MILLIS);
        }
        ZoneId zoneIdHarare = ZoneId.of("Africa/Harare");
        ZonedDateTime zonedDateTimeAGTHarare = ZonedDateTime.of(LocalDateTime.now(), zoneIdHarare);
        if (isHsqldb() || isMariaDb()) {
            zonedDateTimeAGTHarare = zonedDateTimeAGTHarare.truncatedTo(ChronoUnit.MILLIS);
        }
        LocalDate now = LocalDate.now();
        LocalDateTime now1 = LocalDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            now1 = now1.truncatedTo(ChronoUnit.MILLIS);
        }

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
        if (isHsqldb() || isMariaDb()) {
            zonedDateTimeAGT = zonedDateTimeAGT.truncatedTo(ChronoUnit.MILLIS);
        }
        ZoneId zoneIdHarare = ZoneId.of("Africa/Harare");
        ZonedDateTime zonedDateTimeAGTHarare = ZonedDateTime.of(LocalDateTime.now(), zoneIdHarare);
        if (isHsqldb() || isMariaDb()) {
            zonedDateTimeAGTHarare = zonedDateTimeAGTHarare.truncatedTo(ChronoUnit.MILLIS);
        }
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "born", LocalDate.now());
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "born", LocalDate.now());
        LocalDate now = LocalDate.now();
        LocalDateTime now1 = LocalDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            now1 = now1.truncatedTo(ChronoUnit.MILLIS);
        }
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
        if (isHsqldb() || isMariaDb()) {
            now = now.truncatedTo(ChronoUnit.MILLIS);
        }
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
    public void testLabelledZonedDateWithDuplicatedEntitiesInPath() {
        ZonedDateTime now = ZonedDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            now = now.truncatedTo(ChronoUnit.MILLIS);
        }
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "now", now);
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "now", now);
        Vertex c = this.sqlgGraph.addVertex(T.label, "C", "now", now);
        a.addEdge("ab", b);
        b.addEdge("bc", c);
        c.addEdge("ca", a);
        this.sqlgGraph.tx().commit();

        List<Map<String, Vertex>> result = this.sqlgGraph.traversal().V()
                .hasLabel("A").as("a")
                .out().as("b")
                .out().as("c")
                .out().as("a_again")
                .<Vertex>select("a", "b", "c", "a_again").toList();
        Assert.assertEquals(1, result.size());
        Map<String, Vertex> path = result.get(0);

        // Check all entities are found
        List<Vertex> entities = Arrays.asList(path.get("a"), path.get("b"), path.get("c"), path.get("a_again"));
        Assert.assertEquals(Arrays.asList(a, b, c, a), entities);

        Assert.assertEquals(now, path.get("a").value("now"));
        Assert.assertEquals(now, path.get("b").value("now"));
        Assert.assertEquals(now, path.get("c").value("now"));
        Assert.assertEquals(now, path.get("a_again").value("now"));
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
    public void testLoadDateTypes() {

        LocalDateTime localDateTime = LocalDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            localDateTime = localDateTime.truncatedTo(ChronoUnit.MILLIS);
        }
        LocalDate localDate = LocalDate.now();
        LocalTime localTime = LocalTime.now();
        if (isHsqldb() || isMariaDb()) {
            localTime = localTime.truncatedTo(ChronoUnit.MILLIS);
        }
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        if (isHsqldb() || isMariaDb()) {
            zonedDateTime = zonedDateTime.truncatedTo(ChronoUnit.MILLIS);
        }
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
