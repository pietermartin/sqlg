package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 2015/10/21
 * Time: 8:18 PM
 */
public class TestRepeatStep extends BaseTest {

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
//        List<Vertex> t = this.sqlgGraph.traversal().V().repeat(__.out()).until(__.hasLabel("C")).toList();
//        Assert.assertEquals(6, t.size());
//        Assert.assertTrue(t.remove(c1));
//        Assert.assertTrue(t.remove(c1));
//        Assert.assertTrue(t.remove(c2));
//        Assert.assertTrue(t.remove(c2));
//        Assert.assertTrue(t.remove(c3));
//        Assert.assertTrue(t.remove(c3));
//        Assert.assertTrue(t.isEmpty());
//
//        t = this.sqlgGraph.traversal().V().until(__.hasLabel("C")).repeat(__.out()).toList();
//        Assert.assertEquals(9, t.size());
//        Assert.assertTrue(t.remove(c1));
//        Assert.assertTrue(t.remove(c1));
//        Assert.assertTrue(t.remove(c1));
//        Assert.assertTrue(t.remove(c2));
//        Assert.assertTrue(t.remove(c2));
//        Assert.assertTrue(t.remove(c2));
//        Assert.assertTrue(t.remove(c3));
//        Assert.assertTrue(t.remove(c3));
//        Assert.assertTrue(t.remove(c3));
//        Assert.assertTrue(t.isEmpty());
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
//        List<Map<String, Vertex>> t = this.sqlgGraph.traversal().V().repeat(__.groupCount("m").by("name").out()).times(2).<Map<String, Vertex>>cap("m").toList();
//        Assert.assertEquals(1, t.size());
//        Assert.assertTrue(t.get(0).containsKey("a1"));
//        Assert.assertTrue(t.get(0).containsKey("b1"));
//        Assert.assertTrue(t.get(0).containsKey("b1"));
//        Assert.assertTrue(t.get(0).containsKey("b1"));
//        Assert.assertTrue(t.get(0).containsKey("c1"));
//        Assert.assertTrue(t.get(0).containsKey("c1"));
//        Assert.assertTrue(t.get(0).containsKey("c1"));
//
//        Assert.assertEquals(1l, t.get(0).get("a1"));
//        Assert.assertEquals(2l, t.get(0).get("b1"));
//        Assert.assertEquals(2l, t.get(0).get("b2"));
//        Assert.assertEquals(2l, t.get(0).get("b3"));
//        Assert.assertEquals(2l, t.get(0).get("c1"));
//        Assert.assertEquals(2l, t.get(0).get("c2"));
//        Assert.assertEquals(2l, t.get(0).get("c3"));
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
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().repeat(__.out("ab").out("bc")).toList();
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
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out("ab").out("bc")).times(1).toList();
//        Assert.assertEquals(3, vertices.size());
//        Assert.assertTrue(vertices.contains(c1));
//        Assert.assertTrue(vertices.contains(c2));
//        Assert.assertTrue(vertices.contains(c3));
//
//        vertices = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out("ab", "bc")).times(2).toList();
//        Assert.assertEquals(3, vertices.size());
//        Assert.assertTrue(vertices.contains(c1));
//        Assert.assertTrue(vertices.contains(c2));
//        Assert.assertTrue(vertices.contains(c3));
//
////        GraphTraversal<Vertex, Vertex> gt = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out("ab", "bc")).emit().times(2);
////        GraphTraversal<Vertex, Vertex> gt = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out("ab", "bc")).times(2).emit();
////        GraphTraversal<Vertex, Vertex> gt = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "bc")).times(2);
//    }
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
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "bc", "cd")).times(3).toList();
//        for (Vertex vertex : vertices) {
//            System.out.println(vertex.value("name").toString());
//        }
//        Assert.assertEquals(14, vertices.size());
//        Assert.assertTrue(vertices.remove(a1));
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
    @Test
    public void g_V_repeatXoutX_timesX2X_emit_path() throws IOException {
        Graph graph = this.sqlgGraph;
        final GraphReader reader = GryoReader.build()
                .mapper(graph.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
            reader.readGraph(stream, graph);
        }
        assertModernGraph(graph, true, false);
        GraphTraversalSource g = graph.traversal();
        final List<Traversal<Vertex, Path>> traversals = new ArrayList<>();
        Traversal t = g.V().repeat(__.out()).times(2).emit().path();
        traversals.add(t);
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Map<Integer, Long> pathLengths = new HashMap<>();
            int counter = 0;
            while (traversal.hasNext()) {
                counter++;
                MapHelper.incr(pathLengths, traversal.next().size(), 1l);
            }
            Assert.assertEquals(2, pathLengths.size());
            Assert.assertEquals(8, counter);
            Assert.assertEquals(new Long(6), pathLengths.get(2));
            Assert.assertEquals(new Long(2), pathLengths.get(3));
        });
    }

//    @Test
//    public void testDuplicatePath() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//
//        a1.addEdge("ab", b1);
//        b1.addEdge("ba", a2);
//        a2.addEdge("ab", b2);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().repeat(__.out("ab", "ba")).times(2).emit().toList();
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

}
