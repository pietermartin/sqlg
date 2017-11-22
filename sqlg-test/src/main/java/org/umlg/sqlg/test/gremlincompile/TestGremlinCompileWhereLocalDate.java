package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
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
        if (isPostgres()) {
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
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V()
                .hasLabel("Person").has("born", P.eq(born1));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v1, vertices.get(0));

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.eq(born2));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v2, vertices.get(0));

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.lt(born3));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.gt(born1));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v2));
        Assert.assertTrue(vertices.contains(v3));

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.between(born1, born3));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        DefaultGraphTraversal<Vertex, Vertex> traversal5 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.within(born1, born3));
        Assert.assertEquals(2, traversal5.getSteps().size());
        vertices = traversal5.toList();
        Assert.assertEquals(1, traversal5.getSteps().size());
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
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.eq(born1));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v1, vertices.get(0));

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.eq(born2));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v2, vertices.get(0));

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.lt(born3));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        DefaultGraphTraversal<Vertex, Vertex> traversal2bis = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name","johnny").has("born", P.lt(born3));
        Assert.assertEquals(2, traversal2bis.getSteps().size());
        vertices = traversal2bis.toList();
        Assert.assertEquals(traversal2bis.toString(),1, traversal2bis.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertFalse(vertices.contains(v2));

        DefaultGraphTraversal<Vertex, Vertex> traversal2ter = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name").has("born", P.lt(born3));
        Assert.assertEquals(4, traversal2ter.getSteps().size());
        vertices = traversal2ter.toList();
        Assert.assertEquals(traversal2ter.toString(),1, traversal2ter.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        DefaultGraphTraversal<Vertex, Vertex> traversal2cuatro = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").hasNot("name").has("born", P.lt(born3));
        Assert.assertEquals(4, traversal2cuatro.getSteps().size());
        vertices = traversal2cuatro.toList();
        Assert.assertEquals(traversal2cuatro.toString(),1, traversal2cuatro.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2cinquo = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").hasNot("unknown").has("born", P.lt(born3));
        Assert.assertEquals(4, traversal2cinquo.getSteps().size());
        vertices = traversal2cinquo.toList();
        Assert.assertEquals(traversal2cinquo.toString(),1, traversal2cinquo.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.gt(born1));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v2));
        Assert.assertTrue(vertices.contains(v3));

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.between(born1, born3));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        DefaultGraphTraversal<Vertex, Vertex> traversal5 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.within(born1, born3));
        Assert.assertEquals(2, traversal5.getSteps().size());
        vertices = traversal5.toList();
        Assert.assertEquals(1, traversal5.getSteps().size());
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
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.eq(born1));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v1, vertices.get(0));

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.eq(born2));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(2, traversal1.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v2, vertices.get(0));

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V().hasLabel("Person").has("born", P.lt(born3));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(2, traversal2.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.gt(born1));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(2, traversal3.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v2));
        Assert.assertTrue(vertices.contains(v3));

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.between(born1, born3));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(2, traversal4.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        DefaultGraphTraversal<Vertex, Vertex> traversal5 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.within(born1, born3));
        Assert.assertEquals(2, traversal5.getSteps().size());
        vertices = traversal5.toList();
        Assert.assertEquals(2, traversal5.getSteps().size());
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

        System.out.println(born1.isAfter(born2));

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
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.gt(born2));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
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
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.eq(born1));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v1, vertices.get(0));

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.eq(born2));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(2, traversal1.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v2, vertices.get(0));

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.lt(born3));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(2, traversal2.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.gt(born1));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(2, traversal3.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v2));
        Assert.assertTrue(vertices.contains(v3));

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.between(born1, born3));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertEquals(2, traversal4.getSteps().size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        DefaultGraphTraversal<Vertex, Vertex> traversal5 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("born", P.within(born1, born3));
        Assert.assertEquals(2, traversal5.getSteps().size());
        vertices = traversal5.toList();
        Assert.assertEquals(2, traversal5.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v3));
    }

}
