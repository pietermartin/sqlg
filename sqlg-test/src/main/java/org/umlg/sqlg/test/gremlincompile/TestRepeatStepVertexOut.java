package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

/**
 * Date: 2015/10/21
 * Time: 8:18 PM
 */
public class TestRepeatStepVertexOut extends BaseTest {

    @Test
    //This is not optimized
    public void testUntilRepeat() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1.id())
                .repeat(
                        __.out()
                ).until(__.hasLabel("C"));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> t = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(3, t.size());
        Assert.assertTrue(t.remove(c1));
        Assert.assertTrue(t.remove(c2));
        Assert.assertTrue(t.remove(c3));
        Assert.assertTrue(t.isEmpty());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1.id())
                .until(__.hasLabel("C")).repeat(__.out());
        Assert.assertEquals(2, traversal1.getSteps().size());
        t = traversal1.toList();
        Assert.assertEquals(2, traversal1.getSteps().size());
        Assert.assertEquals(3, t.size());
        Assert.assertTrue(t.remove(c1));
        Assert.assertTrue(t.remove(c2));
        Assert.assertTrue(t.remove(c3));

        DefaultGraphTraversal<Vertex, Vertex> traversal2 = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1).repeat(__.out()).until(__.hasLabel("C"));
        t = traversal2.toList();
        Assert.assertEquals(2, traversal2.getSteps().size());
        Assert.assertEquals(3, t.size());
        Assert.assertEquals(2, traversal2.getSteps().size());
        Assert.assertTrue(t.remove(c1));
        Assert.assertTrue(t.remove(c2));
        Assert.assertTrue(t.remove(c3));
        Assert.assertTrue(t.isEmpty());

        DefaultGraphTraversal<Vertex, Vertex> traversal3 = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1).until(__.hasLabel("C")).repeat(__.out());
        Assert.assertEquals(2, traversal3.getSteps().size());
        t = traversal3.toList();
        Assert.assertEquals(2, traversal3.getSteps().size());
        Assert.assertEquals(3, t.size());
        Assert.assertTrue(t.remove(c1));
        Assert.assertTrue(t.remove(c2));
        Assert.assertTrue(t.remove(c3));
    }

    @Test
    //This is not optimized
    public void testRepeatWithUnoptimizableInternalSteps() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Map<String, Long>> traversal = (DefaultGraphTraversal<Vertex, Map<String, Long>>) this.sqlgGraph.traversal()
                .V(a1).repeat(__.groupCount("m").by("name").out()).times(2).<Map<String, Long>>cap("m");
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Map<String, Long>> t = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertEquals(1, t.size());
        Assert.assertEquals(4, t.get(0).size());
        Assert.assertTrue(t.get(0).containsKey("a1"));
        Assert.assertTrue(t.get(0).containsKey("b1"));
        Assert.assertTrue(t.get(0).containsKey("b2"));
        Assert.assertTrue(t.get(0).containsKey("b3"));

        DefaultGraphTraversal<Vertex, Map<String, Long>> traversal1 = (DefaultGraphTraversal<Vertex, Map<String, Long>>) this.sqlgGraph.traversal()
                .V(a1.id()).repeat(__.groupCount("m").by("name").out()).times(2).<Map<String, Long>>cap("m");
        Assert.assertEquals(3, traversal1.getSteps().size());
        t = traversal1.toList();
        Assert.assertEquals(3, traversal1.getSteps().size());
        Assert.assertEquals(1, t.size());
        Assert.assertEquals(4, t.get(0).size());
        Assert.assertTrue(t.get(0).containsKey("a1"));
        Assert.assertTrue(t.get(0).containsKey("b1"));
        Assert.assertTrue(t.get(0).containsKey("b2"));
        Assert.assertTrue(t.get(0).containsKey("b3"));
    }

    @Test
    //This is not optimized
    public void testRepeatNoLimit() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1).repeat(__.out("ab").out("bc"));
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(vertices.isEmpty());

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1.id()).repeat(__.out("ab").out("bc"));
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(2, traversal1.getSteps().size());
        Assert.assertTrue(vertices.isEmpty());
    }

    @Test
    public void testRepeat() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1).repeat(__.out("ab").out("bc")).times(1);
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(c1));
        Assert.assertTrue(vertices.contains(c2));
        Assert.assertTrue(vertices.contains(c3));

        DefaultGraphTraversal<Vertex, Vertex> traversal1 = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1).repeat(__.out("ab", "bc")).times(2);
        Assert.assertEquals(2, traversal1.getSteps().size());
        vertices = traversal1.toList();
        Assert.assertEquals(1, traversal1.getSteps().size());
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(c1));
        Assert.assertTrue(vertices.contains(c2));
        Assert.assertTrue(vertices.contains(c3));
    }

    @Test
    public void testSmallerRepeatWithEmitFirst() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1.id()).emit().repeat(__.out("ab", "bc")).times(2).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(4, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testSmallerRepeatWithEmitFirst2() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1.id()).emit().repeat(__.out("ab", "bc")).times(2).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(4, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testRepeatWithEmitFirst() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
        c1.addEdge("cd", d1);
        c1.addEdge("cd", d2);
        c1.addEdge("cd", d3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1).emit().repeat(__.out("ab", "bc", "cd")).times(3).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(14, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
    }

    @Test
    public void testRepeatWithEmitFirstPath() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
        c1.addEdge("cd", d1);
        c1.addEdge("cd", d2);
        c1.addEdge("cd", d3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1).emit().repeat(__.out("ab", "bc", "cd")).times(4).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(14, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
    }

    @Test
    public void testRepeatWithEmitLast() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
        c1.addEdge("cd", d1);
        c1.addEdge("cd", d2);
        c1.addEdge("cd", d3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1).repeat(__.out("ab", "bc", "cd")).emit().times(3).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(13, paths.size());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)).findAny().get());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)).findAny().get());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)).findAny().get());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)).findAny().get());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)).findAny().get());

        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testRepeatWithEmitLastShouldNotLeftJoinFirstDegree() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
        c1.addEdge("cd", d1);
        c1.addEdge("cd", d2);
        c1.addEdge("cd", d3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1).repeat(__.out("ab", "bc", "cd")).emit().times(3);
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(13, vertices.size());
        Assert.assertTrue(vertices.remove(b1));
        Assert.assertTrue(vertices.remove(b2));
        Assert.assertTrue(vertices.remove(b3));
        Assert.assertTrue(vertices.remove(c1));
        Assert.assertTrue(vertices.remove(c1));
        Assert.assertTrue(vertices.remove(c2));
        Assert.assertTrue(vertices.remove(c3));
        Assert.assertTrue(vertices.remove(d1));
        Assert.assertTrue(vertices.remove(d2));
        Assert.assertTrue(vertices.remove(d3));
        Assert.assertTrue(vertices.remove(d1));
        Assert.assertTrue(vertices.remove(d2));
        Assert.assertTrue(vertices.remove(d3));
        Assert.assertTrue(vertices.isEmpty());
    }

    @Test
    public void testRepeatWithEmitLastWithTimesFirst() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
        c1.addEdge("cd", d1);
        c1.addEdge("cd", d2);
        c1.addEdge("cd", d3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1).times(3).repeat(__.out("ab", "bc", "cd")).emit();
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(19, vertices.size());
        Assert.assertTrue(vertices.remove(b1));
        Assert.assertTrue(vertices.remove(b2));
        Assert.assertTrue(vertices.remove(b3));
        Assert.assertTrue(vertices.remove(c1));
        Assert.assertTrue(vertices.remove(c1));
        Assert.assertTrue(vertices.remove(c2));
        Assert.assertTrue(vertices.remove(c3));
        Assert.assertTrue(vertices.remove(d1));
        Assert.assertTrue(vertices.remove(d2));
        Assert.assertTrue(vertices.remove(d3));
        Assert.assertTrue(vertices.remove(d1));
        Assert.assertTrue(vertices.remove(d2));
        Assert.assertTrue(vertices.remove(d3));
        Assert.assertTrue(vertices.remove(d1));
        Assert.assertTrue(vertices.remove(d2));
        Assert.assertTrue(vertices.remove(d3));
        Assert.assertTrue(vertices.remove(d1));
        Assert.assertTrue(vertices.remove(d2));
        Assert.assertTrue(vertices.remove(d3));
        Assert.assertTrue(vertices.isEmpty());
    }

    @Test
    public void testGremlinLeftJoin() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex josh = this.sqlgGraph.addVertex(T.label, "Person", "name", "josh");
        Vertex lop = this.sqlgGraph.addVertex(T.label, "Software", "name", "lop");
        marko.addEdge("knows", josh);
        marko.addEdge("created", lop);
        josh.addEdge("created", lop);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(marko, josh, lop).repeat(__.out()).times(2).emit().path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(marko) && p.get(1).equals(lop)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(marko) && p.get(1).equals(lop)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(josh) && p.get(1).equals(lop)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(josh) && p.get(1).equals(lop)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(marko) && p.get(1).equals(josh)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(marko) && p.get(1).equals(josh)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(marko) && p.get(1).equals(josh) && p.get(2).equals(lop)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(marko) && p.get(1).equals(josh) && p.get(2).equals(lop)).findAny().get());
        Assert.assertTrue(paths.isEmpty());

        List<Vertex> vertices = this.sqlgGraph.traversal().V().repeat(__.out()).times(2).emit().toList();
        Assert.assertEquals(4, vertices.size());
        Assert.assertTrue(vertices.remove(josh));
        Assert.assertTrue(vertices.remove(lop));
        Assert.assertTrue(vertices.remove(lop));
        Assert.assertTrue(vertices.remove(lop));
        Assert.assertTrue(vertices.isEmpty());
    }

    @Test
    public void testDuplicatePathToSelf() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");

        a1.addEdge("knows", a2);
        a2.addEdge("knows", a3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1, a2, a3).repeat(__.out("knows")).times(2).emit();
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.remove(a2));
        Assert.assertTrue(vertices.remove(a3));
        Assert.assertTrue(vertices.remove(a3));
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Path> traversal1 = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1, a2, a3).repeat(__.out("knows")).emit().times(2).path();
        Assert.assertEquals(3, traversal1.getSteps().size());
        List<Path> paths = traversal1.toList();
        Assert.assertEquals(2, traversal1.getSteps().size());
        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(a3)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(a3)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(a3)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(a3)).findAny().get());
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testDuplicatePathToSelfEmitFirst() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");

        a1.addEdge("knows", a2);
        a2.addEdge("knows", a3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1, a2, a3).emit().repeat(__.out("knows")).times(2);
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(6, vertices.size());
        Assert.assertTrue(vertices.remove(a1));
        Assert.assertTrue(vertices.remove(a2));
        Assert.assertTrue(vertices.remove(a3));
        Assert.assertTrue(vertices.remove(a2));
        Assert.assertTrue(vertices.remove(a3));
        Assert.assertTrue(vertices.remove(a3));
        Assert.assertEquals(0, vertices.size());

        DefaultGraphTraversal<Vertex, Path> traversal1 = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1, a2, a3).emit().repeat(__.out("knows")).times(2).path();
        Assert.assertEquals(3, traversal1.getSteps().size());
        List<Path> paths = traversal1.toList();
        Assert.assertEquals(2, traversal1.getSteps().size());
        Assert.assertEquals(6, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(a3)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(a3)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(a3)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(a3)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a3)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a3)).findAny().get());
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testOnLeftJoinOnLeaveNode() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1, a2).emit().repeat(__.out("ab")).times(1);
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testOnDuplicatePaths() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        b1.addEdge("ba", a2);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1, a2).emit().repeat(__.out("ab", "ba")).times(2);
        Assert.assertEquals(2, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(4, vertices.size());
    }

    @Test
    public void testOnDuplicatePathsFromVertexTimesAfter() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        b1.addEdge("ba", a2);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1, a2).emit().repeat(__.out("ab", "ba")).times(3).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(4, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)).findAny().get());
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testOnDuplicatePathsFromVertexTimes1After() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        b1.addEdge("ba", a2);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1, a2, b1).emit().repeat(__.out("ab", "ba")).times(2).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());

        Assert.assertEquals(6, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)).findAny().get());
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testOnDuplicatePathsFromVertexTimes1EmitAfter() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        b1.addEdge("ba", a2);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1, a2, b1).repeat(__.out("ab", "ba")).emit().times(2).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());

        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)).findAny().get());
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testRepeatWithTimesBefore() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1, b1).emit().times(2).repeat(__.out()).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());

        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testPathToSelfTreeValidatedTakingTheRootIntoAccount() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("aa", a2);
        a2.addEdge("ba", b1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1, a2).repeat(__.out()).emit().times(2).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(b1)).findAny().get());
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testEmitAfterTimesAfterAndBefore() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        c1.addEdge("cd", d1);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1).repeat(__.out()).emit().times(2).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        Assert.assertTrue(paths.isEmpty());

        DefaultGraphTraversal<Vertex, Path> traversal1 = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1).repeat(__.out()).emit().times(3).path();
        Assert.assertEquals(3, traversal1.getSteps().size());
        paths = traversal1.toList();
        Assert.assertEquals(2, traversal1.getSteps().size());
        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
        Assert.assertTrue(paths.isEmpty());

        DefaultGraphTraversal<Vertex, Path> traversal2 = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1).times(2).repeat(__.out()).emit().path();
        Assert.assertEquals(3, traversal2.getSteps().size());
        paths = traversal2.toList();
        Assert.assertEquals(2, traversal2.getSteps().size());
        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testTimesBeforeAfterFirstNoEmit() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        c1.addEdge("cd", d1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1, a2).times(3).repeat(__.out()).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());

        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));

        DefaultGraphTraversal<Vertex, Path> traversal1 = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1, a2).repeat(__.out()).times(3).path();
        Assert.assertEquals(3, traversal1.getSteps().size());
        paths = traversal1.toList();
        Assert.assertEquals(2, traversal1.getSteps().size());
        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
    }

    @Test
    public void testEmitTimesBeforeAfter() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(b1).emit().times(2).repeat(__.out()).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
        Assert.assertTrue(paths.isEmpty());

        DefaultGraphTraversal<Vertex, Path> traversal1 = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1, b1, c1).emit().times(2).repeat(__.out()).path();
        Assert.assertEquals(3, traversal1.getSteps().size());
        paths = traversal1.toList();
        Assert.assertEquals(2, traversal1.getSteps().size());
        Assert.assertEquals(6, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(c1)).findAny().get());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
        Assert.assertTrue(paths.isEmpty());

        DefaultGraphTraversal<Vertex, Path> traversal2 = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1, b1, c1).emit().times(3).repeat(__.out()).path();
        Assert.assertEquals(3, traversal2.getSteps().size());
        paths = traversal2.toList();
        Assert.assertEquals(2, traversal2.getSteps().size());
        Assert.assertEquals(6, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(c1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
        Assert.assertTrue(paths.isEmpty());

        DefaultGraphTraversal<Vertex, Path> traversal3 = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1, b1, c1).emit().repeat(__.out()).times(3).path();
        Assert.assertEquals(3, traversal3.getSteps().size());
        paths = traversal3.toList();
        Assert.assertEquals(2, traversal3.getSteps().size());
        Assert.assertEquals(6, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(c1)).findAny().get());
        Assert.assertTrue(paths.isEmpty());

        DefaultGraphTraversal<Vertex, Path> traversal4 = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1, b1, c1).emit().repeat(__.out()).times(2).path();
        Assert.assertEquals(3, traversal4.getSteps().size());
        paths = traversal4.toList();
        Assert.assertEquals(2, traversal4.getSteps().size());
        Assert.assertEquals(6, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(c1)).findAny().get());
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testEmitTimes2MultiplePaths() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        a1.addEdge("aa", a2);
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a2.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(a1, a2, b1).emit().times(2).repeat(__.out()).path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(6, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        Assert.assertTrue(paths.isEmpty());
    }
}
