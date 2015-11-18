package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
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

//    @Test
//    //This is not optimized
//    public void testUntilRepeat() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c2);
//        b1.addEdge("bc", c3);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> t = this.sqlgGraph.traversal().V(a1.id()).repeat(__.out()).until(__.hasLabel("C")).toList();
//        Assert.assertEquals(3, t.size());
//        Assert.assertTrue(t.remove(c1));
//        Assert.assertTrue(t.remove(c2));
//        Assert.assertTrue(t.remove(c3));
//        Assert.assertTrue(t.isEmpty());
//
//        t = this.sqlgGraph.traversal().V(a1.id()).until(__.hasLabel("C")).repeat(__.out()).toList();
//        Assert.assertEquals(3, t.size());
//        Assert.assertTrue(t.remove(c1));
//        Assert.assertTrue(t.remove(c2));
//        Assert.assertTrue(t.remove(c3));
//
//        t = this.sqlgGraph.traversal().V(a1).repeat(__.out()).until(__.hasLabel("C")).toList();
//        Assert.assertEquals(3, t.size());
//        Assert.assertTrue(t.remove(c1));
//        Assert.assertTrue(t.remove(c2));
//        Assert.assertTrue(t.remove(c3));
//        Assert.assertTrue(t.isEmpty());
//
//        t = this.sqlgGraph.traversal().V(a1).until(__.hasLabel("C")).repeat(__.out()).toList();
//        Assert.assertEquals(3, t.size());
//        Assert.assertTrue(t.remove(c1));
//        Assert.assertTrue(t.remove(c2));
//        Assert.assertTrue(t.remove(c3));
//    }
//
//    @Test
//    //This is not optimized
//    public void testRepeatWithUnoptimizableInternalSteps() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c2);
//        b1.addEdge("bc", c3);
//        this.sqlgGraph.tx().commit();
//
//        List<Map<String, Long>> t = this.sqlgGraph.traversal().V(a1).repeat(__.groupCount("m").by("name").out()).times(2).<Map<String, Long>>cap("m").toList();
//        Assert.assertEquals(1, t.size());
//        Assert.assertEquals(4, t.get(0).size());
//        Assert.assertTrue(t.get(0).containsKey("a1"));
//        Assert.assertTrue(t.get(0).containsKey("b1"));
//        Assert.assertTrue(t.get(0).containsKey("b2"));
//        Assert.assertTrue(t.get(0).containsKey("b3"));
//
//        t = this.sqlgGraph.traversal().V(a1.id()).repeat(__.groupCount("m").by("name").out()).times(2).<Map<String, Long>>cap("m").toList();
//        Assert.assertEquals(1, t.size());
//        Assert.assertEquals(4, t.get(0).size());
//        Assert.assertTrue(t.get(0).containsKey("a1"));
//        Assert.assertTrue(t.get(0).containsKey("b1"));
//        Assert.assertTrue(t.get(0).containsKey("b2"));
//        Assert.assertTrue(t.get(0).containsKey("b3"));
//    }
//
//    @Test
//    //This is not optimized
//    public void testRepeatNoLimit() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c2);
//        b1.addEdge("bc", c3);
//        this.sqlgGraph.tx().commit();
//        List<Vertex> vertices = this.sqlgGraph.traversal().V(a1).repeat(__.out("ab").out("bc")).toList();
//        Assert.assertTrue(vertices.isEmpty());
//
//        vertices = this.sqlgGraph.traversal().V(a1.id()).repeat(__.out("ab").out("bc")).toList();
//        Assert.assertTrue(vertices.isEmpty());
//    }
//
//    @Test
//    public void testRepeat() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c2);
//        b1.addEdge("bc", c3);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V(a1).repeat(__.out("ab").out("bc")).times(0).toList();
//        Assert.assertEquals(3, vertices.size());
//        Assert.assertTrue(vertices.contains(c1));
//        Assert.assertTrue(vertices.contains(c2));
//        Assert.assertTrue(vertices.contains(c3));
//
//        vertices = this.sqlgGraph.traversal().V(a1).repeat(__.out("ab", "bc")).times(1).toList();
//        Assert.assertEquals(3, vertices.size());
//        Assert.assertTrue(vertices.contains(c1));
//        Assert.assertTrue(vertices.contains(c2));
//        Assert.assertTrue(vertices.contains(c3));
//    }

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
        List<Path> paths = this.sqlgGraph.traversal().V(a1.id()).emit().repeat(__.out("ab", "bc")).times(1).path().toList();
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

//
//    @Test
//    public void testRepeatWithEmitFirst() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c2);
//        b1.addEdge("bc", c3);
//        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
//        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
//        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
//        c1.addEdge("cd", d1);
//        c1.addEdge("cd", d2);
//        c1.addEdge("cd", d3);
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V(a1).emit().repeat(__.out("ab", "bc", "cd")).times(2).path().toList();
//        Assert.assertEquals(14, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
//    }
//
//    @Test
//    public void testRepeatWithEmitFirstPath() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c2);
//        b1.addEdge("bc", c3);
//        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
//        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
//        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
//        c1.addEdge("cd", d1);
//        c1.addEdge("cd", d2);
//        c1.addEdge("cd", d3);
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "bc", "cd")).times(3).path().toList();
//        Assert.assertEquals(14, paths.size());
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
//
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)).findAny().get());
//
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)).findAny().get());
//
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testRepeatWithEmitLast() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c2);
//        b1.addEdge("bc", c3);
//        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
//        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
//        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
//        c1.addEdge("cd", d1);
//        c1.addEdge("cd", d2);
//        c1.addEdge("cd", d3);
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out("ab", "bc", "cd")).emit().times(3).path().toList();
//        for (Path path : paths) {
//            System.out.println(path.toString());
//        }
//        Assert.assertEquals(13, paths.size());
//
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)).findAny().get());
//
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)).findAny().get());
//
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
//
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
//
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)).findAny().get());
//
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)).findAny().get());
//
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)).findAny().get());
//
//        Assert.assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testRepeatWithEmitLastShouldNotLeftJoinFirstDegree() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c2);
//        b1.addEdge("bc", c3);
//        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
//        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
//        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
//        c1.addEdge("cd", d1);
//        c1.addEdge("cd", d2);
//        c1.addEdge("cd", d3);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out("ab", "bc", "cd")).emit().times(3).toList();
//        for (Vertex vertex : vertices) {
//            System.out.println(vertex.value("name").toString());
//        }
//        Assert.assertEquals(13, vertices.size());
//        Assert.assertTrue(vertices.remove(b1));
//        Assert.assertTrue(vertices.remove(b2));
//        Assert.assertTrue(vertices.remove(b3));
//        Assert.assertTrue(vertices.remove(c1));
//        Assert.assertTrue(vertices.remove(c1));
//        Assert.assertTrue(vertices.remove(c2));
//        Assert.assertTrue(vertices.remove(c3));
//        Assert.assertTrue(vertices.remove(d1));
//        Assert.assertTrue(vertices.remove(d2));
//        Assert.assertTrue(vertices.remove(d3));
//        Assert.assertTrue(vertices.remove(d1));
//        Assert.assertTrue(vertices.remove(d2));
//        Assert.assertTrue(vertices.remove(d3));
//        Assert.assertTrue(vertices.isEmpty());
//    }
//
//    @Test
//    public void testRepeatWithEmitLastWithTimesFirst() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c2);
//        b1.addEdge("bc", c3);
//        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
//        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
//        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
//        c1.addEdge("cd", d1);
//        c1.addEdge("cd", d2);
//        c1.addEdge("cd", d3);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").times(3).repeat(__.out("ab", "bc", "cd")).emit().toList();
//        for (Vertex vertex : vertices) {
//            System.out.println(vertex.value("name").toString());
//        }
//        Assert.assertEquals(19, vertices.size());
//        Assert.assertTrue(vertices.remove(b1));
//        Assert.assertTrue(vertices.remove(b2));
//        Assert.assertTrue(vertices.remove(b3));
//        Assert.assertTrue(vertices.remove(c1));
//        Assert.assertTrue(vertices.remove(c1));
//        Assert.assertTrue(vertices.remove(c2));
//        Assert.assertTrue(vertices.remove(c3));
//        Assert.assertTrue(vertices.remove(d1));
//        Assert.assertTrue(vertices.remove(d2));
//        Assert.assertTrue(vertices.remove(d3));
//        Assert.assertTrue(vertices.remove(d1));
//        Assert.assertTrue(vertices.remove(d2));
//        Assert.assertTrue(vertices.remove(d3));
//        Assert.assertTrue(vertices.remove(d1));
//        Assert.assertTrue(vertices.remove(d2));
//        Assert.assertTrue(vertices.remove(d3));
//        Assert.assertTrue(vertices.remove(d1));
//        Assert.assertTrue(vertices.remove(d2));
//        Assert.assertTrue(vertices.remove(d3));
//        Assert.assertTrue(vertices.isEmpty());
//    }
//
////    tp3 bug
//    @Test
//    public void g_V_repeatXoutX_timesX2X() throws IOException {
//        final List<Traversal<Vertex, Vertex>> traversals = new ArrayList<>();
//        Graph graph = this.sqlgGraph;
//        graph.io(GryoIo.build()).readGraph("../sqlg-test/src/main/resources/tinkerpop-modern.kryo");
//        assertModernGraph(graph, true, false);
//
//        Traversal t = graph.traversal().V().repeat(__.out()).times(2);
//        traversals.add(t);
//        traversals.forEach(traversal -> {
//            printTraversalForm(traversal);
//            int counter = 0;
//            while (traversal.hasNext()) {
//                counter++;
//                Vertex vertex = traversal.next();
////                Assert.assertTrue(vertex.value("name").equals("lop") || vertex.value("name").equals("ripple"));
//            }
//            Assert.assertEquals(0, counter);
//            Assert.assertFalse(traversal.hasNext());
//        });
//    }
//
//    //tp3 bug
//    @Test
//    public void g_V_repeatXoutX_timesX2X_path_byXitX_byXnameX_byXlangX() throws IOException {
//        Graph graph = this.sqlgGraph;
//        graph.io(GryoIo.build()).readGraph("../sqlg-test/src/main/resources/tinkerpop-modern.kryo");
//        assertModernGraph(graph, true, false);
//        final Traversal<Vertex, Path> traversal =  graph.traversal().V().repeat(__.out()).times(2).path().by().by("name").by("lang");
//        printTraversalForm(traversal);
//        int counter = 0;
//        while (traversal.hasNext()) {
//            counter++;
//            final Path path = traversal.next();
////            Assert.assertEquals(3, path.size());
////            Assert.assertEquals("marko", ((Vertex) path.get(0)).<String>value("name"));
////            Assert.assertEquals("josh", path.<String>get(1));
////            Assert.assertEquals("java", path.<String>get(2));
//        }
////        Assert.assertEquals(2, counter);
//        Assert.assertEquals(0, counter);
//    }
//
//
//    @Test
//    public void g_V_repeatXoutX_timesX2X_emit_path() throws IOException {
//        Graph graph = this.sqlgGraph;
//        graph.io(GryoIo.build()).readGraph("../sqlg-test/src/main/resources/tinkerpop-modern.kryo");
//        assertModernGraph(graph, true, false);
//        GraphTraversalSource g = graph.traversal();
//        final List<Traversal<Vertex, Path>> traversals = new ArrayList<>();
//        Traversal t = g.V().repeat(__.out()).emit().times(2).path();
//        traversals.add(t);
//        traversals.forEach(traversal -> {
//            printTraversalForm(traversal);
//            final Map<Integer, Long> pathLengths = new HashMap<>();
//            int counter = 0;
//            while (traversal.hasNext()) {
//                counter++;
//                MapHelper.incr(pathLengths, traversal.next().size(), 1l);
//            }
//            Assert.assertEquals(2, pathLengths.size());
//            Assert.assertEquals(8, counter);
//            Assert.assertEquals(new Long(6), pathLengths.get(2));
//            Assert.assertEquals(new Long(2), pathLengths.get(3));
//        });
//    }
//
//    @Test
//    public void g_V_emit_timesX2X_repeatXoutX_path() throws IOException {
//        Graph graph = this.sqlgGraph;
//        graph.io(GryoIo.build()).readGraph("../sqlg-test/src/main/resources/tinkerpop-modern.kryo");
//        GraphTraversalSource g = graph.traversal();
//        final List<Traversal<Vertex, Path>> traversals = Arrays.asList(
//                g.V().emit().times(2).repeat(__.out()).path(),
//                g.V().emit().repeat(__.out()).times(2).path()
//        );
//        traversals.forEach(traversal -> {
//            int path1 = 0;
//            int path2 = 0;
//            int path3 = 0;
//            while (traversal.hasNext()) {
//                final Path path = traversal.next();
//                System.out.println(path);
//                if (path.size() == 1) {
//                    path1++;
//                } else if (path.size() == 2) {
//                    path2++;
//                } else if (path.size() == 3) {
//                    path3++;
//                } else {
//                    Assert.fail("Only path lengths of 1, 2, or 3 should be seen");
//                }
//            }
//            Assert.assertEquals(6, path1);
//            Assert.assertEquals(6, path2);
//            Assert.assertEquals(2, path3);
//        });
//    }
//
//    @Test
//    public void testGremlinLeftJoin() {
//        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
//        Vertex josh = this.sqlgGraph.addVertex(T.label, "Person", "name", "josh");
//        Vertex lop = this.sqlgGraph.addVertex(T.label, "Software", "name", "lop");
//        marko.addEdge("knows", josh);
//        marko.addEdge("created", lop);
//        josh.addEdge("created", lop);
//        this.sqlgGraph.tx().commit();
//        List<Path> paths = this.sqlgGraph.traversal().V().repeat(__.out()).times(2).emit().path().toList();
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(marko) && p.get(1).equals(lop)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(marko) && p.get(1).equals(lop)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(josh) && p.get(1).equals(lop)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(josh) && p.get(1).equals(lop)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(marko) && p.get(1).equals(josh)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(marko) && p.get(1).equals(josh)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(marko) && p.get(1).equals(josh) && p.get(2).equals(lop)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(marko) && p.get(1).equals(josh) && p.get(2).equals(lop)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().repeat(__.out()).times(2).emit().toList();
//        Assert.assertEquals(4, vertices.size());
//        Assert.assertTrue(vertices.remove(josh));
//        Assert.assertTrue(vertices.remove(lop));
//        Assert.assertTrue(vertices.remove(lop));
//        Assert.assertTrue(vertices.remove(lop));
//        Assert.assertTrue(vertices.isEmpty());
//    }
//
//    @Test
//    public void testDuplicatePathToSelf() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
//
//        a1.addEdge("knows", a2);
//        a2.addEdge("knows", a3);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().repeat(__.out("knows")).times(2).emit().toList();
//        Assert.assertEquals(3, vertices.size());
//        Assert.assertTrue(vertices.remove(a2));
//        Assert.assertTrue(vertices.remove(a3));
//        Assert.assertTrue(vertices.remove(a3));
//        Assert.assertEquals(0, vertices.size());
//
//        List<Path> paths = this.sqlgGraph.traversal().V().repeat(__.out("knows")).emit().times(2).path().toList();
//        Assert.assertEquals(3, paths.size());
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(a3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(a3)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(a3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(a3)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testDuplicatePathToSelfEmitFirst() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
//
//        a1.addEdge("knows", a2);
//        a2.addEdge("knows", a3);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().emit().repeat(__.out("knows")).times(2).toList();
//        Assert.assertEquals(6, vertices.size());
//        Assert.assertTrue(vertices.remove(a1));
//        Assert.assertTrue(vertices.remove(a2));
//        Assert.assertTrue(vertices.remove(a3));
//        Assert.assertTrue(vertices.remove(a2));
//        Assert.assertTrue(vertices.remove(a3));
//        Assert.assertTrue(vertices.remove(a3));
//        Assert.assertEquals(0, vertices.size());
//
//        List<Path> paths = this.sqlgGraph.traversal().V().emit().repeat(__.out("knows")).times(2).path().toList();
//        Assert.assertEquals(6, paths.size());
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(a3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(a3)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(a3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(a3)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a3)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testOnLeftJoinOnLeaveNode() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        a1.addEdge("ab", b1);
//        a2.addEdge("ab", b1);
//        this.sqlgGraph.tx().commit();
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab")).times(1).toList();
//        Assert.assertEquals(4, vertices.size());
//    }
//
//    @Test
//    public void testOnDuplicatePaths() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        a1.addEdge("ab", b1);
//        b1.addEdge("ba", a2);
//        this.sqlgGraph.tx().commit();
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "ba")).times(2).toList();
//        Assert.assertEquals(4, vertices.size());
//    }
//
//    @Test
//    public void testOnDuplicatePathsFromVertexTimesAfter() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        a1.addEdge("ab", b1);
//        b1.addEdge("ba", a2);
//        this.sqlgGraph.tx().commit();
//        List<Path> paths = this.sqlgGraph.traversal().V().emit().repeat(__.out("ab", "ba")).times(2).path().toList();
//        Assert.assertEquals(6, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testOnDuplicatePathsFromVertexTimes1After() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        a1.addEdge("ab", b1);
//        b1.addEdge("ba", a2);
//        this.sqlgGraph.tx().commit();
//        List<Path> paths = this.sqlgGraph.traversal().V().emit().repeat(__.out("ab", "ba")).times(1).path().toList();
//        Assert.assertEquals(6, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testOnDuplicatePathsFromVertexTimes1EmitAfter() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        a1.addEdge("ab", b1);
//        b1.addEdge("ba", a2);
//        this.sqlgGraph.tx().commit();
//        List<Path> paths = this.sqlgGraph.traversal().V().repeat(__.out("ab", "ba")).emit().times(1).path().toList();
//        Assert.assertEquals(4, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testRepeatWithTimesBefore() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        a1.addEdge("ab", b1);
//        this.sqlgGraph.tx().commit();
//        List<Path> paths = this.sqlgGraph.traversal().V().emit().times(2).repeat(__.out()).path().toList();
//        Assert.assertEquals(3, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testPathToSelfTreeValidatedTakingTheRootIntoAccount() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        a1.addEdge("aa", a2);
//        a2.addEdge("ba", b1);
//        this.sqlgGraph.tx().commit();
//        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out()).emit().times(2).path().toList();
//        Assert.assertEquals(3, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testEmitAfterTimesAfterAndBefore() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
//        a1.addEdge("ab", b1);
//        b1.addEdge("bc", c1);
//        c1.addEdge("cd", d1);
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out()).emit().times(2).path().toList();
//    Assert.assertEquals(4, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//
//        paths = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out()).emit().times(3).path().toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//
//        paths = this.sqlgGraph.traversal().V().hasLabel("A").times(2).repeat(__.out()).emit().path().toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//    }
//
//
//    @Test
//    public void testTimesBeforeAfterFirstNoEmit() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
//        a1.addEdge("ab", b1);
//        b1.addEdge("bc", c1);
//        c1.addEdge("cd", d1);
//        this.sqlgGraph.tx().commit();
//        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").times(3).repeat(__.out()).path().toList();
//        Assert.assertEquals(1, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
//        System.out.println("--------------------");
//        paths = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out()).times(3).path().toList();
//        Assert.assertEquals(0, paths.size());
//    }
//
//    @Test
//    public void testEmitTimesBeforeAfter() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        a1.addEdge("ab", b1);
//        b1.addEdge("bc", c1);
//        this.sqlgGraph.tx().commit();
//        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("B").emit().times(2).repeat(__.out()).path().toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//        Assert.assertEquals(2, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//
//        paths = this.sqlgGraph.traversal().V().emit().times(2).repeat(__.out()).path().toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//        Assert.assertEquals(6, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//        System.out.println("-----------------");
//
//        paths = this.sqlgGraph.traversal().V().emit().times(3).repeat(__.out()).path().toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//        Assert.assertEquals(6, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//
//        System.out.println("-----------------");
//        paths = this.sqlgGraph.traversal().V().emit().repeat(__.out()).times(3).path().toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//        Assert.assertEquals(6, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//        System.out.println("-----------------");
//        paths = this.sqlgGraph.traversal().V().emit().repeat(__.out()).times(2).path().toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//        Assert.assertEquals(6, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testEmitTimes2MultiplePaths() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        a1.addEdge("aa", a2);
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        a2.addEdge("ab", b1);
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V().emit().times(2).repeat(__.out()).path().toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//        Assert.assertEquals(6, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//    }
}
