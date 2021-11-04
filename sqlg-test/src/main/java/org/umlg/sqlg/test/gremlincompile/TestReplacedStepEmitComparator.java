package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/03/11
 */
public class TestReplacedStepEmitComparator extends BaseTest {

    @Test
    public void testOrderFollowedByVertexStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        a2.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        a2.addEdge("ab", b3);
        a3.addEdge("ab", b1);
        a3.addEdge("ab", b2);
        a3.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal()
                .V().hasLabel("A").order().by("name", Order.desc)
                .out("ab")
                .path()
                .toList();
        Assert.assertEquals(9, paths.size());
        Assert.assertEquals(a3, paths.get(0).objects().get(0));
        Assert.assertEquals(a3, paths.get(1).objects().get(0));
        Assert.assertEquals(a3, paths.get(2).objects().get(0));
        Assert.assertEquals(a2, paths.get(3).objects().get(0));
        Assert.assertEquals(a2, paths.get(4).objects().get(0));
        Assert.assertEquals(a2, paths.get(5).objects().get(0));
        Assert.assertEquals(a1, paths.get(6).objects().get(0));
        Assert.assertEquals(a1, paths.get(7).objects().get(0));
        Assert.assertEquals(a1, paths.get(8).objects().get(0));
    }

    @Test
    public void testOrderRangeOrderAgain() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "a");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "c");

        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);

        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "a");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "b");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "d");
        Vertex c5 = this.sqlgGraph.addVertex(T.label, "C", "name", "e");
        Vertex c6 = this.sqlgGraph.addVertex(T.label, "C", "name", "f");
        Vertex c7 = this.sqlgGraph.addVertex(T.label, "C", "name", "g");
        Vertex c8 = this.sqlgGraph.addVertex(T.label, "C", "name", "h");
        Vertex c9 = this.sqlgGraph.addVertex(T.label, "C", "name", "i");

        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        b2.addEdge("bc", c4);
        b2.addEdge("bc", c5);
        b2.addEdge("bc", c6);
        b3.addEdge("bc", c7);
        b3.addEdge("bc", c8);
        b3.addEdge("bc", c9);

        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = sqlgGraph.traversal()
                .V().hasLabel("A")
                .out("ab").order().by("name", Order.desc).limit(1)
                .out("bc").order().by("name", Order.desc)
                .toList();

        Assert.assertEquals(3, vertices.size());
        Assert.assertEquals(c9, vertices.get(0));
        Assert.assertEquals(c8, vertices.get(1));
        Assert.assertEquals(c7, vertices.get(2));

    }

    @Test
    public void testVertexStepAfterRange() {
        loadModern();
        Object v1Id = convertToVertexId("marko");
        final Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V(v1Id).out("created").inE("created").range(1, 3).outV();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String name = traversal.next().value("name");
            Assert.assertTrue(name.equals("marko") || name.equals("josh") || name.equals("peter"));
        }
        Assert.assertEquals(2, counter);
    }

    @Test
    public void testComparatorViolations() {
        loadGratefulDead();
        final Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().has("song", "name", "OH BOY").out("followedBy").out("followedBy").order().by("performances").by("songType", Order.desc);
        printTraversalForm(traversal);
        int counter = 0;
        String lastSongType = "a";
        int lastPerformances = Integer.MIN_VALUE;
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
            final String currentSongType = vertex.value("songType");
            final int currentPerformances = vertex.value("performances");
            Assert.assertTrue(currentPerformances == lastPerformances || currentPerformances > lastPerformances);
            if (currentPerformances == lastPerformances)
                Assert.assertTrue(currentSongType.equals(lastSongType) || currentSongType.compareTo(lastSongType) < 0);
            lastSongType = currentSongType;
            lastPerformances = currentPerformances;
            counter++;
        }
        Assert.assertEquals(144, counter);
    }

    @Test
    public void testStepsAfterRangeNotOptimized() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "d");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "c");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "b");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");

        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "surname", "b");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "surname", "b");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "surname", "b");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "surname", "b");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        a3.addEdge("ab", b3);
        a4.addEdge("ab", b4);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V().hasLabel("A").order().by("name").limit(2)
                .out("ab").toList();

        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b3));
        Assert.assertTrue(vertices.contains(b4));
    }

    @Test
    public void testOptionalWithOrder() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "aa");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "aaa");

        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "d");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "c");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex bb1 = this.sqlgGraph.addVertex(T.label, "BB", "name", "g");
        Vertex bb2 = this.sqlgGraph.addVertex(T.label, "BB", "name", "f");
        Vertex bb3 = this.sqlgGraph.addVertex(T.label, "BB", "name", "e");

        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "h");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "i");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "j");
        Vertex cc1 = this.sqlgGraph.addVertex(T.label, "CC", "name", "k");
        Vertex cc2 = this.sqlgGraph.addVertex(T.label, "CC", "name", "l");
        Vertex cc3 = this.sqlgGraph.addVertex(T.label, "CC", "name", "m");

        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        a1.addEdge("abb", bb1);
        a1.addEdge("abb", bb2);
        a1.addEdge("abb", bb3);

        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        b2.addEdge("bcc", cc1);
        b2.addEdge("bcc", cc2);
        b2.addEdge("bcc", cc3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .optional(
                        __.out().order().by("name").optional(
                                __.out().order().by("name", Order.desc)
                        )
                )
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        for (Path next : paths) {
            for (Object o : next) {
                Vertex v = (Vertex) o;
                System.out.print("[");
                System.out.print(v.<String>value("name"));
                System.out.print("], ");
            }
            System.out.print("\n");
        }
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(12, paths.size());

        //assert the order
        //all the paths of length 2 and 3 must be sorted
        List<Path> pathsOfLength3 = paths.stream().filter(p -> p.size() == 3).collect(Collectors.toList());
        Vertex v = (Vertex) pathsOfLength3.get(5).objects().get(2);
        Assert.assertEquals("h", v.value("name"));
        v = (Vertex) pathsOfLength3.get(4).objects().get(2);
        Assert.assertEquals("i", v.value("name"));
        v = (Vertex) pathsOfLength3.get(3).objects().get(2);
        Assert.assertEquals("j", v.value("name"));
        v = (Vertex) pathsOfLength3.get(2).objects().get(2);
        Assert.assertEquals("k", v.value("name"));
        v = (Vertex) pathsOfLength3.get(1).objects().get(2);
        Assert.assertEquals("l", v.value("name"));
        v = (Vertex) pathsOfLength3.get(0).objects().get(2);
        Assert.assertEquals("m", v.value("name"));

        List<Path> pathsOfLength2 = paths.stream().filter(p -> p.size() == 2).collect(Collectors.toList());
        v = (Vertex) pathsOfLength2.get(0).objects().get(1);
        Assert.assertEquals("b", v.value("name"));
        v = (Vertex) pathsOfLength2.get(1).objects().get(1);
        Assert.assertEquals("e", v.value("name"));
        v = (Vertex) pathsOfLength2.get(2).objects().get(1);
        Assert.assertEquals("f", v.value("name"));
        v = (Vertex) pathsOfLength2.get(3).objects().get(1);
        Assert.assertEquals("g", v.value("name"));

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b2) && p.get(2).equals(cc1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b2) && p.get(2).equals(cc1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b2) && p.get(2).equals(cc2)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b2) && p.get(2).equals(cc2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b2) && p.get(2).equals(cc3)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b2) && p.get(2).equals(cc3)).findAny().get());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(bb1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(bb1)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(bb2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(bb2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(bb3)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(bb3)).findAny().get());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)).findAny().get());

        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a3)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a3)).findAny().get());

        Assert.assertTrue(paths.isEmpty());

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
        List<String> names =  this.sqlgGraph.traversal()
                .V()
                .outE().as("e")
                .inV().as("v")
                .select("e")
                .order().by("weight", Order.asc)
                .select("v")
                .<String>values("name")
                .dedup()
                .toList();
        Assert.assertEquals(3, names.size());
        Assert.assertEquals("b3", names.get(0));
        Assert.assertEquals("b2", names.get(1));
        Assert.assertEquals("b1", names.get(2));
    }
}
