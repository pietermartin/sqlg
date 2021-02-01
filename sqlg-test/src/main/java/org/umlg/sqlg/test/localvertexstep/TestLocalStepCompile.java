package org.umlg.sqlg.test.localvertexstep;

import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.step.barrier.SqlgLocalStepBarrier;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

/**
 * Date: 2016/05/07
 * Time: 1:46 PM
 */
@SuppressWarnings("rawtypes")
public class TestLocalStepCompile extends BaseTest {

    @Test
    public void testLocalStepCompile() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) gt
                .V(a1)
                .local(
                        __.out().out()
                ).path();

        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(1) instanceof LocalStep);
        LocalStep<?, ?> localStep = (LocalStep) traversal.getSteps().get(1);
        Assert.assertEquals(1, localStep.getLocalChildren().size());
        Traversal.Admin<?, ?> traversal1 = localStep.getLocalChildren().get(0);
        Assert.assertEquals(2, traversal1.getSteps().size());

        List<Path> paths = traversal.toList();
        Assert.assertEquals(1, paths.size());

        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgLocalStepBarrier);
        SqlgLocalStepBarrier<?,?> sqlgLocalStepBarrier = (SqlgLocalStepBarrier) traversal.getSteps().get(1);
        Assert.assertEquals(1, sqlgLocalStepBarrier.getLocalChildren().size());
        traversal1 = sqlgLocalStepBarrier.getLocalChildren().get(0);
        Assert.assertEquals(1, traversal1.getSteps().size());
    }

    //the limit is executed on the db
    @Test
    public void testLocalStepWithLimitOnDb() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c11 = this.sqlgGraph.addVertex(T.label, "C", "name", "c11");
        Vertex c12 = this.sqlgGraph.addVertex(T.label, "C", "name", "c12");
        Vertex c13 = this.sqlgGraph.addVertex(T.label, "C", "name", "c13");
        Vertex c21 = this.sqlgGraph.addVertex(T.label, "C", "name", "c21");
        Vertex c22 = this.sqlgGraph.addVertex(T.label, "C", "name", "c22");
        Vertex c23 = this.sqlgGraph.addVertex(T.label, "C", "name", "c23");
        Vertex c31 = this.sqlgGraph.addVertex(T.label, "C", "name", "c31");
        Vertex c32 = this.sqlgGraph.addVertex(T.label, "C", "name", "c32");
        Vertex c33 = this.sqlgGraph.addVertex(T.label, "C", "name", "c33");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        b1.addEdge("bc", c11);
        b1.addEdge("bc", c12);
        b1.addEdge("bc", c13);
        b2.addEdge("bc", c21);
        b2.addEdge("bc", c22);
        b2.addEdge("bc", c23);
        b3.addEdge("bc", c31);
        b3.addEdge("bc", c32);
        b3.addEdge("bc", c33);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1)
                .local(
                        __.out().limit(1).out()
                );
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(3, vertices.size());

    }

    //the limit is NOT executed on the db
    @Test
    public void testLocalStepWithLimitNotOnDb() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex bb1 = this.sqlgGraph.addVertex(T.label, "BB", "name", "bb1");
        Vertex bb2 = this.sqlgGraph.addVertex(T.label, "BB", "name", "bb2");
        Vertex bb3 = this.sqlgGraph.addVertex(T.label, "BB", "name", "bb3");
        Vertex c11 = this.sqlgGraph.addVertex(T.label, "C", "name", "c11");
        Vertex c12 = this.sqlgGraph.addVertex(T.label, "C", "name", "c12");
        Vertex c13 = this.sqlgGraph.addVertex(T.label, "C", "name", "c13");
        Vertex c21 = this.sqlgGraph.addVertex(T.label, "C", "name", "c21");
        Vertex c22 = this.sqlgGraph.addVertex(T.label, "C", "name", "c22");
        Vertex c23 = this.sqlgGraph.addVertex(T.label, "C", "name", "c23");
        Vertex c31 = this.sqlgGraph.addVertex(T.label, "C", "name", "c31");
        Vertex c32 = this.sqlgGraph.addVertex(T.label, "C", "name", "c32");
        Vertex c33 = this.sqlgGraph.addVertex(T.label, "C", "name", "c33");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        a1.addEdge("abb", bb1);
        a1.addEdge("abb", bb2);
        a1.addEdge("abb", bb3);

        b1.addEdge("bc", c11);
        b1.addEdge("bc", c12);
        b1.addEdge("bc", c13);
        b2.addEdge("bc", c21);
        b2.addEdge("bc", c22);
        b2.addEdge("bc", c23);
        b3.addEdge("bc", c31);
        b3.addEdge("bc", c32);
        b3.addEdge("bc", c33);

        bb1.addEdge("bc", c11);
        bb1.addEdge("bc", c12);
        bb1.addEdge("bc", c13);
        bb2.addEdge("bc", c21);
        bb2.addEdge("bc", c22);
        bb2.addEdge("bc", c23);
        bb3.addEdge("bc", c31);
        bb3.addEdge("bc", c32);
        bb3.addEdge("bc", c33);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1)
                .local(
                        __.out().limit(1).out()
                );
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testSelectBeforeOrder() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1, "weight", 3);
        a1.addEdge("ab", b2, "weight", 2);
        a1.addEdge("ab", b3, "weight", 1);
        this.sqlgGraph.tx().commit();
        List<String> names = this.sqlgGraph.traversal()
                .V()
                .local(
                        __.outE().as("e")
                                .inV().as("v")
                                .select("e")
                                .order().by("weight", Order.asc)
                                .select("v")
                                .<String>values("name")
                                .dedup())
                .toList();
        Assert.assertEquals(3, names.size());
        Assert.assertEquals("b3", names.get(0));
        Assert.assertEquals("b2", names.get(1));
        Assert.assertEquals("b1", names.get(2));
    }


    @Test
    public void testOrderby() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "a");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "c");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1)
                .out()
                .order()
                .by("name", Order.asc);
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> result = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(b1, result.get(0));
        Assert.assertEquals(b2, result.get(1));
        Assert.assertEquals(b3, result.get(2));

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1)
                .out()
                .order()
                .by("name", Order.desc);
        Assert.assertEquals(3, traversal1.getSteps().size());
        result = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(b1, result.get(2));
        Assert.assertEquals(b2, result.get(1));
        Assert.assertEquals(b3, result.get(0));
    }

    @Test
    public void testOrderbyDuplicatePath() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "aa");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "a");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "c");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "ab");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "ac");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        b1.addEdge("ba", a2);
        b1.addEdge("ba", a3);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1)
                .out()
                .out()
                .order()
                .by("name", Order.desc);
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Vertex> result = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(a3, result.get(0));
        Assert.assertEquals(a2, result.get(1));
    }

    @Test
    public void testOrderByDuplicatePathLabelled() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "aa");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "a");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "c");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "ab");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "ac");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        b1.addEdge("ba", a2);
        b1.addEdge("ba", a3);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1)
                .out()
                .out().as("x")
                .<Vertex>select("x")
                .order()
                .by(__.select("x").by("name"), Order.desc);
        Assert.assertEquals(5, traversal.getSteps().size());
        List<Vertex> result = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(a3, result.get(0));
        Assert.assertEquals(a2, result.get(1));
    }

    @Test
    public void testOrderbyDuplicatePathOrderInMemory() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "aa");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "a");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "c");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "ab");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "ac");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "ca");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        b1.addEdge("ba", a2);
        b1.addEdge("ba", a3);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1)
                .out()
                .out()
                .order()
                .by("name", Order.asc);
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Vertex> result = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(c1, result.get(2));
        Assert.assertEquals(a3, result.get(1));
        Assert.assertEquals(a2, result.get(0));
    }

    @Test
    public void testOrderOnEdge() {
        Vertex god = this.sqlgGraph.addVertex(T.label, "God");
        Vertex fantasy1 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan1");
        Vertex fantasy2 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan2");
        Vertex fantasy3 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan3");
        Vertex fantasy4 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan4");
        god.addEdge("godDream", fantasy1, "sequence", 1);
        god.addEdge("godDream", fantasy2, "sequence", 2);
        god.addEdge("godDream", fantasy3, "sequence", 3);
        god.addEdge("godDream", fantasy4, "sequence", 4);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V(god)
                .outE("godDream").as("e")
                .inV().as("v")
                .select("e", "v")
                .order().by(__.select("e").by("sequence"), Order.desc)
                .map(m -> (Vertex) m.get().get("v"));
        Assert.assertEquals(6, traversal.getSteps().size());
        List<Vertex> result = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertEquals(fantasy4, result.get(0));
        Assert.assertEquals(fantasy3, result.get(1));
        Assert.assertEquals(fantasy2, result.get(2));
        Assert.assertEquals(fantasy1, result.get(3));
    }

    @Test
    public void testSelectVertexAndEdgeOrderByEdge() {
        Vertex god = this.sqlgGraph.addVertex(T.label, "God");
        Vertex fantasy1 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan1");
        Vertex fantasy2 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan2");
        Vertex fantasy3 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan3");
        Vertex fantasy4 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan4");
        Edge e1 = god.addEdge("godDream", fantasy1, "sequence", 1);
        Edge e2 = god.addEdge("godDream", fantasy2, "sequence", 2);
        Edge e3 = god.addEdge("godDream", fantasy3, "sequence", 3);
        Edge e4 = god.addEdge("godDream", fantasy4, "sequence", 4);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Map<String, Object>> traversal = (DefaultGraphTraversal<Vertex, Map<String, Object>>) this.sqlgGraph.traversal().V(god)
                .outE("godDream").as("e")
                .inV().as("v")
                .select("e", "v")
                .order().by(__.select("e").by("sequence"), Order.desc);
        Assert.assertEquals(5, traversal.getSteps().size());
        List<Map<String, Object>> result = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());

        Assert.assertEquals(4, result.size());
        Assert.assertEquals(fantasy4, result.get(0).get("v"));
        Assert.assertEquals(fantasy3, result.get(1).get("v"));
        Assert.assertEquals(fantasy2, result.get(2).get("v"));
        Assert.assertEquals(fantasy1, result.get(3).get("v"));

        Assert.assertEquals(e4, result.get(0).get("e"));
        Assert.assertEquals(e3, result.get(1).get("e"));
        Assert.assertEquals(e2, result.get(2).get("e"));
        Assert.assertEquals(e1, result.get(3).get("e"));
    }

    @Test
    public void testOrderByToSelf() {

        Vertex root = this.sqlgGraph.addVertex(T.label, "Root");
        Vertex folder1 = this.sqlgGraph.addVertex(T.label, "Folder");
        Vertex folder2 = this.sqlgGraph.addVertex(T.label, "Folder");
        root.addEdge("rootFolder", folder1);
        folder1.addEdge("subFolder", folder2, "sequence", 1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V(folder1)
                .outE("subFolder").as("e")
                .inV().as("v")
                .select("e", "v")
                .order().by(__.select("e").by("sequence"), Order.asc)
                .map(m -> (Vertex) m.get().get("v"));
        Assert.assertEquals(6, traversal.getSteps().size());
        List<Vertex> result = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());

        Assert.assertEquals(1, result.size());
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void testOrderRangeAs() {
        Vertex god = this.sqlgGraph.addVertex(T.label, "God");
        Vertex fantasy1 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan1");
        Vertex fantasy2 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan2");
        Vertex fantasy3 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan3");
        Vertex fantasy4 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan4");
        god.addEdge("godDream", fantasy1, "sequence", 1);
        god.addEdge("godDream", fantasy2, "sequence", 2);
        god.addEdge("godDream", fantasy3, "sequence", 3);
        god.addEdge("godDream", fantasy4, "sequence", 4);

        this.sqlgGraph.tx().commit();
        Traversal<Vertex, Map<String, Object>> traversal = this.sqlgGraph.traversal().V()
                .hasLabel("Fantasy")
                .order().by("name")
                .as("f")
                .in("godDream").as("g").select("f", "g");
        List<Map<String, Object>> l = traversal.toList();
        Assert.assertEquals(4, l.size());
        Set<Vertex> vs = new HashSet<>();
        for (Map<String, Object> m : l) {
            Assert.assertEquals(god, m.get("g"));
            vs.add((Vertex) m.get("f"));
        }
        Assert.assertEquals(4, vs.size());
        Assert.assertTrue(vs.contains(fantasy1));
        Assert.assertTrue(vs.contains(fantasy2));
        Assert.assertTrue(vs.contains(fantasy3));
        Assert.assertTrue(vs.contains(fantasy4));

        traversal = this.sqlgGraph.traversal().V()
                .hasLabel("Fantasy")
                .order().by("name").range(0, 2)
                .as("f")
                .in("godDream").as("g").select("f", "g");
        l = traversal.toList();
        Assert.assertEquals(2, l.size());
        vs = new HashSet<>();
        for (Map<String, Object> m : l) {
            Assert.assertEquals(god, m.get("g"));
            vs.add((Vertex) m.get("f"));
        }
        Assert.assertEquals(2, vs.size());
        Assert.assertTrue(vs.contains(fantasy1));
        Assert.assertTrue(vs.contains(fantasy2));
        Assert.assertFalse(vs.contains(fantasy3));
        Assert.assertFalse(vs.contains(fantasy4));
    }

    @Test
    public void testLimitOnVertexLabels() {
        loadModern();

        Traversal<Vertex, Map<String, Map<String, Map<String, Object>>>> traversal = this.sqlgGraph.traversal()
                .V().hasLabel("person").filter(__.outE("created")).aggregate("p").as("p1").values("name").as("p1n")
                .select("p").unfold().where(P.neq("p1")).as("p2").values("name").as("p2n").select("p2")
                .out("created").choose(__.in("created").where(P.eq("p1")), __.values("name"), __.constant(Collections.emptySet()))
                .<String, Map<String, Map<String, Object>>>group().by(__.select("p1n")).
                        by(__.group().by(__.select("p2n")).
                                by(__.unfold().fold().project("numCoCreated", "coCreated").by(__.count(Scope.local)).by()));

        Assert.assertTrue(traversal.hasNext());
    }

}
