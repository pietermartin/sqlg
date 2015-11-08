package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2015/10/21
 * Time: 8:18 PM
 */
public class TestRepeatStepGraphIn extends BaseTest {

//    @Test
//    public void testEmitAfterTimesAfterAndBeforeIn() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
//        a1.addEdge("ab", b1);
//        b1.addEdge("bc", c1);
//        c1.addEdge("cd", d1);
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("D").repeat(__.in()).emit().times(2).path().toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//        Assert.assertEquals(4, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(d1) && p.get(1).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(d1) && p.get(1).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(d1) && p.get(1).equals(c1) && p.get(2).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(d1) && p.get(1).equals(c1) && p.get(2).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(d1) && p.get(1).equals(c1) && p.get(2).equals(b1) && p.get(3).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(d1) && p.get(1).equals(c1) && p.get(2).equals(b1) && p.get(3).equals(a1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(d1) && p.get(1).equals(c1) && p.get(2).equals(b1) && p.get(3).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(d1) && p.get(1).equals(c1) && p.get(2).equals(b1) && p.get(3).equals(a1)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//
//        paths = this.sqlgGraph.traversal().V().hasLabel("D").repeat(__.in()).emit().times(3).path().toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//        Assert.assertEquals(3, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(d1) && p.get(1).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(d1) && p.get(1).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(d1) && p.get(1).equals(c1) && p.get(2).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(d1) && p.get(1).equals(c1) && p.get(2).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(d1) && p.get(1).equals(c1) && p.get(2).equals(b1) && p.get(3).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(d1) && p.get(1).equals(c1) && p.get(2).equals(b1) && p.get(3).equals(a1)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//
//        paths = this.sqlgGraph.traversal().V().hasLabel("D").times(2).repeat(__.in()).emit().path().toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//        Assert.assertEquals(3, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(d1) && p.get(1).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(d1) && p.get(1).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(d1) && p.get(1).equals(c1) && p.get(2).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(d1) && p.get(1).equals(c1) && p.get(2).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(d1) && p.get(1).equals(c1) && p.get(2).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(d1) && p.get(1).equals(c1) && p.get(2).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testEmitTimesRepeatInSinglePathQuery() {
//        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        d1.addEdge("cd", c1);
//        c1.addEdge("cb", b1);
//        c2.addEdge("cb", b2);
//        b1.addEdge("ab", a1);
//        b2.addEdge("ab", a1);
//        b3.addEdge("ab", a1);
//        this.sqlgGraph.tx().commit();
//        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").emit().times(3).repeat(__.in()).path().toList();
//        Assert.assertEquals(7, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b2) && p.get(2).equals(c2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b2) && p.get(2).equals(c2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testEmitTimesRepeatInDuplicatePathQuery() {
//
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
//        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
//        b4.addEdge("ba", a2);
//        a2.addEdge("ab", b1);
//        b1.addEdge("ba", a1);
//        a3.addEdge("ab", b2);
//        b2.addEdge("ba", a1);
//        b3.addEdge("ba", a1);
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").emit().times(3).repeat(__.in()).path().toList();
//        Assert.assertEquals(10, paths.size());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2) && p.get(3).equals(b4)));
//        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2) && p.get(3).equals(b4)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b2) && p.get(2).equals(a3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b2) && p.get(2).equals(a3)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b4)));
//        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b4)).findAny().get());
//        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a3)));
//        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a3)).findAny().get());
//        Assert.assertTrue(paths.isEmpty());
//    }

    @Test
    public void g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX() throws IOException {
        Graph graph = this.sqlgGraph;
        graph.io(GryoIo.build()).readGraph("../sqlg-test/src/main/resources/grateful-dead.kryo");

        final Traversal<Vertex, Map<String, Long>> traversal = graph.traversal().V().repeat(__.both("followedBy")).times(2).<String, Long>group().by("songType").by(__.count());
        AbstractGremlinProcessTest.checkMap(new HashMap<String, Long>() {{
            put("original", 771317l);
            put("", 160968l);
            put("cover", 368579l);
        }}, traversal.next());
        Assert.assertFalse(traversal.hasNext());
    }
}
