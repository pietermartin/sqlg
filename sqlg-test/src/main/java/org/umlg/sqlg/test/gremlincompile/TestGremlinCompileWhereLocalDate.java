package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.time.*;
import java.util.List;

/**
 * Created by pieter on 2015/08/03.
 */
public class TestGremlinCompileWhereLocalDate extends BaseTest {

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        BaseTest.beforeClass();
        if (configuration.getString("jdbc.url").contains("postgresql")) {
            configuration.addProperty("distributed", true);
        }
    }

    @Test
    public void testEqualsLocalDate() throws InterruptedException {
        LocalDate born1 = LocalDate.of(1990, 1, 1);
        LocalDate born2 = LocalDate.of(1991, 1, 1);
        LocalDate born3 = LocalDate.of(1992, 1, 1);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "johnny", "born", born1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pietie", "born", born2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "koosie", "born", born3);
        this.sqlgGraph.tx().commit();
        testEqualsLocalDate_assert(this.sqlgGraph, born1, born2, born3, v1, v2, v3);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testEqualsLocalDate_assert(this.sqlgGraph1, born1, born2, born3, v1, v2, v3);
        }
    }

    private void testEqualsLocalDate_assert(SqlgGraph sqlgGraph, LocalDate born1, LocalDate born2, LocalDate born3, Vertex v1, Vertex v2, Vertex v3) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born1)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v1, vertices.get(0));
        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born2)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v2, vertices.get(0));

        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.lt(born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));
        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.gt(born1)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v2));
        Assert.assertTrue(vertices.contains(v3));

        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.between(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.within(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v3));
    }

    @Test
    public void testEqualsLocalDateTime() throws InterruptedException {
        LocalDateTime born1 = LocalDateTime.of(1990, 1, 1, 1, 1, 1);
        LocalDateTime born2 = LocalDateTime.of(1990, 1, 1, 1, 1, 2);
        LocalDateTime born3 = LocalDateTime.of(1990, 1, 1, 1, 1, 3);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "johnny", "born", born1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pietie", "born", born2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "koosie", "born", born3);
        this.sqlgGraph.tx().commit();
        testEqualsLocalDateTime_assert(this.sqlgGraph, born1, born2, born3, v1, v2, v3);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testEqualsLocalDateTime_assert(this.sqlgGraph1, born1, born2, born3, v1, v2, v3);
        }
    }

    private void testEqualsLocalDateTime_assert(SqlgGraph sqlgGraph, LocalDateTime born1, LocalDateTime born2, LocalDateTime born3, Vertex v1, Vertex v2, Vertex v3) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born1)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v1, vertices.get(0));
        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born2)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v2, vertices.get(0));

        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.lt(born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));
        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.gt(born1)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v2));
        Assert.assertTrue(vertices.contains(v3));

        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.between(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.within(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v3));
    }


    //ZonedDateTime comparisons happens in java not on the db
    @Test
    public void testEqualsZonedDateTime() throws InterruptedException {
        ZoneId zoneId = ZoneId.of("Africa/Harare");
        ZonedDateTime born1 = ZonedDateTime.of(1999, 1, 1, 1, 1, 1, 0, zoneId);
        ZonedDateTime born2 = ZonedDateTime.of(1999, 1, 1, 1, 1, 2, 0, zoneId);
        ZonedDateTime born3 = ZonedDateTime.of(1999, 1, 1, 1, 1, 3, 0, zoneId);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "johnny", "born", born1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pietie", "born", born2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "koosie", "born", born3);
        this.sqlgGraph.tx().commit();
        testEqualsZonedDateTime_assert(this.sqlgGraph, born1, born2, born3, v1, v2, v3);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testEqualsZonedDateTime_assert(this.sqlgGraph1, born1, born2, born3, v1, v2, v3);
        }

    }

    private void testEqualsZonedDateTime_assert(SqlgGraph sqlgGraph, ZonedDateTime born1, ZonedDateTime born2, ZonedDateTime born3, Vertex v1, Vertex v2, Vertex v3) {
        //The zone is ignored in the query, how about that????
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born1)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v1, vertices.get(0));
        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born2)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v2, vertices.get(0));

        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.lt(born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));
        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.gt(born1)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v2));
        Assert.assertTrue(vertices.contains(v3));

        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.between(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.within(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v3));
    }

    //ZonedDateTime comparisons happens in java not on the db
    @Test
    public void testEqualsZonedDateTime2() throws InterruptedException {
        ZoneId zoneId1 = ZoneId.of("Africa/Harare");
        ZonedDateTime born1 = ZonedDateTime.of(1999, 1, 1, 1, 1, 1, 0, zoneId1);
        ZoneId zoneId2 = ZoneId.of("Asia/Tokyo");
        ZonedDateTime born2 = ZonedDateTime.of(1999, 1, 1, 1, 1, 1, 0, zoneId2);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "johnny", "born", born1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pietie", "born", born2);
        this.sqlgGraph.tx().commit();
        testEqualsZonedDateTime2_assert(this.sqlgGraph, born2);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testEqualsZonedDateTime2_assert(this.sqlgGraph1, born2);
        }
    }

    private void testEqualsZonedDateTime2_assert(SqlgGraph sqlgGraph, ZonedDateTime born2) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.gt(born2)).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testEqualsDuration() throws InterruptedException {
        Duration born1 = Duration.ofSeconds(1, 1);
        Duration born2 = Duration.ofSeconds(2, 1);
        Duration born3 = Duration.ofSeconds(3, 1);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "johnny", "born", born1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pietie", "born", born2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "koosie", "born", born3);
        this.sqlgGraph.tx().commit();
        testEqualsDuration_assert(this.sqlgGraph, born1, born2, born3, v1, v2, v3);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testEqualsDuration_assert(this.sqlgGraph1, born1, born2, born3, v1, v2, v3);
        }
    }

    private void testEqualsDuration_assert(SqlgGraph sqlgGraph, Duration born1, Duration born2, Duration born3, Vertex v1, Vertex v2, Vertex v3) {
        List<Vertex> vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born1)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v1, vertices.get(0));
        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born2)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v2, vertices.get(0));

        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.lt(born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));
        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.gt(born1)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v2));
        Assert.assertTrue(vertices.contains(v3));

        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.between(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        vertices = sqlgGraph.traversal().V().hasLabel("Person").has("born", P.within(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v3));
    }

}
