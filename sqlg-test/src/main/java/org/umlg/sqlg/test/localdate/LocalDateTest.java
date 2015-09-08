package org.umlg.sqlg.test.localdate;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;

/**
 * Created by pieter on 2015/09/05.
 */
public class LocalDateTest extends BaseTest {

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

        LocalDate ld = this.sqlgGraph.traversal().V(a1.id()).next().<LocalDate>value("born");
        Assert.assertEquals(now, ld);
        LocalDateTime ldt = this.sqlgGraph.traversal().V(a2.id()).next().<LocalDateTime>value("bornAgain");
        Assert.assertEquals(now1, ldt);
        ZonedDateTime zonedDateTime = this.sqlgGraph.traversal().V(a3.id()).next().<ZonedDateTime>value("andBornAgain");
        Assert.assertEquals(zonedDateTimeAGTHarare, zonedDateTime);
        zonedDateTime = this.sqlgGraph.traversal().V(a4.id()).next().<ZonedDateTime>value("andReBornAgain");
        Assert.assertEquals(zonedDateTimeAGT, zonedDateTime);
        LocalTime localTime = this.sqlgGraph.traversal().V(a5.id()).next().<LocalTime>value("time");
        Assert.assertEquals(now2.toSecondOfDay(), localTime.toSecondOfDay());
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
    public void testPeriod() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "period", Period.of(1,1,1));
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "period", Period.of(11,11,11));
        a1.addEdge("test", a2, "period", Period.of(22,10,22));
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(Period.of(1,1,1), this.sqlgGraph.traversal().V(a1.id()).next().value("period"));
        Assert.assertEquals(Period.of(11,11,11), this.sqlgGraph.traversal().V(a2.id()).next().value("period"));
        Assert.assertEquals(Period.of(22,10,22), this.sqlgGraph.traversal().V(a1.id()).outE().next().value("period"));
        Assert.assertEquals(Period.of(11,11,11), this.sqlgGraph.traversal().V(a1.id()).out().next().value("period"));
    }

    public static void main(String[] args) {
        ZoneId zoneIdParis = ZoneId.of("Europe/Paris");
        LocalDateTime parisDateTime = LocalDateTime.now(zoneIdParis);
        System.out.println("paris time " + parisDateTime);

        ZoneId zoneIdTokoy = ZoneId.of("Asia/Tokyo");
        LocalDateTime tokoyDateTime = LocalDateTime.now(zoneIdTokoy);
        System.out.println("harare time " + tokoyDateTime);
    }
}
