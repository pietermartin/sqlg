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

import java.util.Collections;
import java.util.List;

/**
 * Created by pieter on 2015/08/03.
 */
public class TestGremlinCompileWhere extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Test
    public void testEquals() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "johnny");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "pietie");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "koosie");
        this.sqlgGraph.tx().commit();
        testEquals_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testEquals_assert(this.sqlgGraph1);
        }
    }

    private void testEquals_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", P.eq("johnny"));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", P.eq("johnnyxxx"));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testNotEquals() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "johnny");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "pietie");
        this.sqlgGraph.addVertex(T.label, "Person", "name", "koosie");
        this.sqlgGraph.tx().commit();
        testNotEquals_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testNotEquals_assert(this.sqlgGraph1);
        }
    }

    private void testNotEquals_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", P.neq("johnny"));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("name", P.neq("johnnyxxx"));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testBiggerThan() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.tx().commit();
        testBiggerThan_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBiggerThan_assert(this.sqlgGraph1);
        }
    }

    private void testBiggerThan_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.gt(0));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.gt(1));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V().hasLabel("Person").has("age", P.gt(2));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.gt(3));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testBiggerEqualsTo() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.tx().commit();
        testBiggerEqualsTo_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBiggerEqualsTo_assert(this.sqlgGraph1);
        }
    }

    private void testBiggerEqualsTo_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.gte(0));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.gte(1));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.gte(2));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.gte(3));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.gte(4));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testSmallerThan() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.tx().commit();
        testSmallerThan_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testSmallerThan_assert(this.sqlgGraph1);
        }
    }

    private void testSmallerThan_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.lt(0));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.lt(1));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.lt(2));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.lt(3));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.lt(4));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testLessThanEqualsTo() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.tx().commit();
        testLessThanEqualsTo_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testLessThanEqualsTo_assert(this.sqlgGraph1);
        }
    }

    private void testLessThanEqualsTo_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.lte(0));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.lte(1));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.lte(2));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V().hasLabel("Person").has("age", P.lte(3));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.lte(4));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(3, vertices.size());
    }

    //Note gremlin between is >= and <
    @Test
    public void testBetween() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.tx().commit();
        testBetween_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testBetween_assert(this.sqlgGraph1);
        }
    }

    private void testBetween_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().
                V().hasLabel("Person").has("age", P.between(0, 4));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.between(1, 4));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.between(1, 3));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.between(1, 1));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testInside() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.tx().commit();
        testInside_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testInside_assert(this.sqlgGraph1);
        }
    }

    private void testInside_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.inside(0, 4));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.inside(1, 4));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V().hasLabel("Person").has("age", P.inside(1, 3));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.inside(1, 1));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testOutside() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.tx().commit();
        testOutside_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testOutside_assert(this.sqlgGraph1);
        }
    }

    private void testOutside_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.outside(0, 4));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.outside(1, 4));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.outside(1, 3));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.outside(1, 1));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testWithin() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.tx().commit();
        testWithin_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testWithin_assert(this.sqlgGraph1);
        }
    }

    private void testWithin_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.within(1, 2, 3));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(3, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.within(0, 1));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.within(1, 3));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.within(1, 1));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.within(3, 4));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal5 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.within(4, 5));
        Assert.assertEquals(2, traversal5.getSteps().size());
        vertices = traversal5.toList();
        Assert.assertEquals(1, traversal5.getSteps().size());
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testWithout() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.tx().commit();
        testWithout_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testWithout_assert(this.sqlgGraph1);
        }
    }

    private void testWithout_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.without(1, 2, 3));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.without(0, 1));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().
                V().hasLabel("Person").has("age", P.without(1, 3));
        Assert.assertEquals(2, traversal2.getSteps().size());
        vertices = traversal2.toList();
        Assert.assertEquals(1, traversal2.getSteps().size());
        Assert.assertEquals(1, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.without(1, 1));
        Assert.assertEquals(2, traversal3.getSteps().size());
        vertices = traversal3.toList();
        Assert.assertEquals(1, traversal3.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal4 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.without(3, 4));
        Assert.assertEquals(2, traversal4.getSteps().size());
        vertices = traversal4.toList();
        Assert.assertEquals(1, traversal4.getSteps().size());
        Assert.assertEquals(2, vertices.size());

        DefaultGraphTraversal<Vertex, Vertex> traversal5 = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.without(4, 5));
        Assert.assertEquals(2, traversal5.getSteps().size());
        vertices = traversal5.toList();
        Assert.assertEquals(1, traversal5.getSteps().size());
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testEmptyWithin() throws InterruptedException {
        this.sqlgGraph.addVertex(T.label, "Person", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "age", 3);
        this.sqlgGraph.tx().commit();
        testEmptyWithin_assert(this.sqlgGraph);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testEmptyWithin_assert(this.sqlgGraph1);
        }
    }

    private void testEmptyWithin_assert(SqlgGraph sqlgGraph) {
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal()
                .V().hasLabel("Person").has("age", P.within(Collections.emptyList()));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(0, vertices.size());
    }

}
