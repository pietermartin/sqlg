package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.time.*;
import java.util.List;

/**
 * Created by pieter on 2015/08/03.
 */
public class TestGremlinCompileWhereLocalDate extends BaseTest {

    @Test
    public void testEqualsLocalDate() {
        LocalDate born1 = LocalDate.of(1990, 1, 1);
        LocalDate born2 = LocalDate.of(1991, 1, 1);
        LocalDate born3 = LocalDate.of(1992, 1, 1);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "johnny", "born", born1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pietie", "born", born2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "koosie", "born", born3);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born1)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v1, vertices.get(0));
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born2)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v2, vertices.get(0));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.lt(born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.gt(born1)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v2));
        Assert.assertTrue(vertices.contains(v3));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.between(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.within(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v3));
    }

    @Test
    public void testEqualsLocalDateTime() {
        LocalDateTime born1 = LocalDateTime.of(1990, 1, 1, 1, 1, 1);
        LocalDateTime born2 = LocalDateTime.of(1990, 1, 1, 1, 1, 2);
        LocalDateTime born3 = LocalDateTime.of(1990, 1, 1, 1, 1, 3);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "johnny", "born", born1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pietie", "born", born2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "koosie", "born", born3);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born1)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v1, vertices.get(0));
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born2)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v2, vertices.get(0));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.lt(born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.gt(born1)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v2));
        Assert.assertTrue(vertices.contains(v3));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.between(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.within(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v3));
    }


    //ZonedDateTime comparisons happens in java not on the db
    @Test
    public void testEqualsZonedDateTime() {
        ZoneId zoneId = ZoneId.of("Africa/Harare");
        ZonedDateTime born1 = ZonedDateTime.of(1999, 1, 1, 1, 1, 1, 0, zoneId);
        ZonedDateTime born2 = ZonedDateTime.of(1999, 1, 1, 1, 1, 2, 0, zoneId);
        ZonedDateTime born3 = ZonedDateTime.of(1999, 1, 1, 1, 1, 3, 0, zoneId);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "johnny", "born", born1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pietie", "born", born2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "koosie", "born", born3);
        this.sqlgGraph.tx().commit();
        //The zone is ignored in the query, how about that????
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born1)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v1, vertices.get(0));
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born2)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v2, vertices.get(0));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.lt(born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.gt(born1)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v2));
        Assert.assertTrue(vertices.contains(v3));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.between(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.within(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v3));
    }

    //ZonedDateTime comparisons happens in java not on the db
    @Test
    public void testEqualsZonedDateTime2() {
        ZoneId zoneId1 = ZoneId.of("Africa/Harare");
        ZonedDateTime born1 = ZonedDateTime.of(1999, 1, 1, 1, 1, 1, 0, zoneId1);
        ZoneId zoneId2 = ZoneId.of("Asia/Tokyo");
        ZonedDateTime born2 = ZonedDateTime.of(1999, 1, 1, 1, 1, 1, 0, zoneId2);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "johnny", "born", born1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pietie", "born", born2);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.gt(born2)).toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testEqualsDuration() {
        Duration born1 = Duration.ofSeconds(1, 1);
        Duration born2 = Duration.ofSeconds(2, 1);
        Duration born3 = Duration.ofSeconds(3, 1);
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "johnny", "born", born1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "pietie", "born", born2);
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "koosie", "born", born3);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born1)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v1, vertices.get(0));
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.eq(born2)).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(v2, vertices.get(0));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.lt(born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.gt(born1)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v2));
        Assert.assertTrue(vertices.contains(v3));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.between(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v2));

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("born", P.within(born1, born3)).toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(v1));
        Assert.assertTrue(vertices.contains(v3));
    }

}
