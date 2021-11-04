package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.step.SqlgVertexStep;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

/**
 * Date: 2015/10/21
 * Time: 8:18 PM
 */
@SuppressWarnings("DuplicatedCode")
public class TestRepeatStepGraphBoth extends BaseTest {

    @Test
    public void testEmitRepeatWithVertexStepAfter() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        @SuppressWarnings("unused")
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c2);
        b2.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A")
                .repeat(__.out()).emit().times(3)
                .out();
        printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgVertexStep);

        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(c1, c2, c3)));

    }

    @Test
    public void g_V_untilXout_outX_repeatXin_asXaX_in_asXbXX_selectXa_bX_byXnameX() {
        loadModern();

        final Traversal<Vertex, Map<String, String>> traversal = this.sqlgGraph.traversal()
                .V()
                .until(__.out().out())
                .repeat(__.in().as("a").in().as("b"))
                .<String>select("a", "b").by("name");
        Map<String, String> result = traversal.next();
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("josh", result.get("a"));
        Assert.assertEquals("marko", result.get("b"));
        result = traversal.next();
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("josh", result.get("a"));
        Assert.assertEquals("marko", result.get("b"));
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_V_asXaX_repeatXbothX_timesX3X_emit_asXbX_group_byXselectXaXX_byXselectXbX_dedup_order_byXidX_foldX_selectXvaluesX_unfold_dedup() {
        loadModern();
        final Traversal<Vertex, Collection<String>> traversal = this.sqlgGraph.traversal()
                .V().as("a")
                .repeat(__.both()).times(3).emit().values("name").as("b")
                .group()
                .by(__.select("a"))
                .by(__.select("b").dedup().order().fold())
                .select(Column.values).<Collection<String>>unfold().dedup();
        final List<String> vertices = new ArrayList<>(traversal.next());
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(6, vertices.size());
        Assert.assertEquals("josh", vertices.get(0));
        Assert.assertEquals("lop", vertices.get(1));
        Assert.assertEquals("marko", vertices.get(2));
        Assert.assertEquals("peter", vertices.get(3));
        Assert.assertEquals("ripple", vertices.get(4));
        Assert.assertEquals("vadas", vertices.get(5));
    }

    @Test
    public void testRepeatBoth() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Map<String, Vertex>> traversal = (DefaultGraphTraversal<Vertex, Map<String, Vertex>>) this.sqlgGraph.traversal()
                .V(b1).as("a")
                .repeat(__.both()).times(3).emit().as("b")
                .<Vertex>select("a", "b");
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Map<String, Vertex>> vertexList = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());

        Assert.assertEquals(8, vertexList.size());

        List<Vertex> aVertices = new ArrayList<>();
        List<Vertex> bVertices = new ArrayList<>();
        for (Map<String, Vertex> stringVertexMap : vertexList) {
            System.out.println(stringVertexMap);
            Assert.assertEquals(2, stringVertexMap.size());
            aVertices.add(stringVertexMap.get("a"));
            bVertices.add(stringVertexMap.get("b"));
        }
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.remove(b1));
        Assert.assertTrue(aVertices.isEmpty());

        Assert.assertTrue(bVertices.remove(c1));
        Assert.assertTrue(bVertices.remove(c1));
        Assert.assertTrue(bVertices.remove(c1));
        Assert.assertTrue(bVertices.remove(a1));
        Assert.assertTrue(bVertices.remove(a1));
        Assert.assertTrue(bVertices.remove(a1));
        Assert.assertTrue(bVertices.remove(b1));
        Assert.assertTrue(bVertices.remove(b1));
        Assert.assertTrue(bVertices.isEmpty());
    }

    @Test
    public void testGroupByByFailureWithoutDedup() {
        loadModern();
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Collection<Vertex>> traversal = (DefaultGraphTraversal<Vertex, Collection<Vertex>>) this.sqlgGraph.traversal()
                .V().as("a")
                .repeat(__.both()).times(3).emit().as("b")
                .group()
                .by(__.select("a"))
                .by(__.select("b").dedup().order().by(T.id).fold())
                .select(Column.values)
                .<Collection<Vertex>>unfold();

        final List<Collection<Vertex>> result = traversal.toList();
        Assert.assertEquals(6, result.size());
        Assert.assertTrue(result.stream().allMatch(v -> v.size() == 6));
    }

    @Test
    public void testGroupByByFailure() {
        loadModern();
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Collection<Vertex>> traversal = (DefaultGraphTraversal<Vertex, Collection<Vertex>>) this.sqlgGraph.traversal()
                .V().as("a")
                .repeat(__.both()).times(3).emit().as("b")
                .group()
                .by(__.select("a"))
                .by(__.select("b").dedup().order().by(T.id).fold())
                .select(Column.values)
                .<Collection<Vertex>>unfold()
                .dedup();

        Assert.assertEquals(6, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(5, traversal.getSteps().size());
        final Collection<Vertex> vertices = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(6, vertices.size());
        Assert.assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "marko")));
        Assert.assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "vadas")));
        Assert.assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "josh")));
        Assert.assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "peter")));
        Assert.assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "lop")));
        Assert.assertTrue(vertices.contains(convertToVertex(this.sqlgGraph, "ripple")));
    }

    @Test
    public void testRepeatAndOut() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().repeat(__.both()).times(1).out();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(b1, vertices.get(0));
    }

    @Test
    public void testRepeatBoth2() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(b1).repeat(__.both()).times(3);
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testRepeatBothE() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(b1).repeat(__.bothE().otherV()).times(3);
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testRepeatBothEWithAggregate() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Edge e1 = a1.addEdge("ab", b1);
        Edge e2 = b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(b1)
                .repeat(
                        __.bothE()
                                .where(P.without("e"))
                                .aggregate("e")
                                .otherV()
                )
                .emit()
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertEquals(2, paths.size());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(b1) && p.get(1).equals(e1) && p.get(2).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(b1) && p.get(1).equals(e1) && p.get(2).equals(a1)).findAny().orElseThrow());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(b1) && p.get(1).equals(e2) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(b1) && p.get(1).equals(e2) && p.get(2).equals(c1)).findAny().orElseThrow());
        Assert.assertTrue(paths.isEmpty());
    }
}
