package org.umlg.sqlg.test.repeatstep;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/06/06
 */
public class TestUnoptimizedRepeatStep extends BaseTest {

    @Test
    public void testHubSites() {
        Vertex a0 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", true);

        Vertex a10 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
        Vertex a11 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
        Vertex a12 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
        Vertex a13 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
        Vertex a14 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", true);
        a0.addEdge("a", a10);
        a0.addEdge("a", a11);
        a0.addEdge("a", a13);
        a0.addEdge("a", a14);

        Vertex a20 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", true);
        Vertex a21 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
        Vertex a22 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);
        Vertex a23 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);

        a10.addEdge("a", a20);
        a11.addEdge("a", a21);
        a12.addEdge("a", a22);
        a13.addEdge("a", a23);

        Vertex a30 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", true);
        Vertex a31 = this.sqlgGraph.addVertex(T.label, "A", "hubSite", false);

        a21.addEdge("a", a30);
        a22.addEdge("a", a31);

        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V(a0)
                .repeat(__.out())
//                .until(__.or(__.loops().is(P.gt(3)), __.has("hubSite", true)))
                .until(__.has("hubSite", true))
                .toList();
        Assert.assertEquals(3, vertices.size());

        List<Path> paths = this.sqlgGraph.traversal()
                .V(a0)
                .repeat(__.out())
                .until(__.or(__.loops().is(P.gt(10)), __.has("hubSite", true)))
                .path()
                .toList();
        Assert.assertEquals(3, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a0) && p.get(1).equals(a10) && p.get(2).equals(a20),
                p -> p.size() == 4 && p.get(0).equals(a0) && p.get(1).equals(a11) && p.get(2).equals(a21) && p.get(3).equals(a30),
                p -> p.size() == 2 && p.get(0).equals(a0) && p.get(1).equals(a14)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

//    @Test
//    public void testUnoptimizedRepeatStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
//        a1.addEdge("ab", b1);
//        a2.addEdge("ab", b2);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").until(__.has(T.label, "B")).repeat(__.out()).toList();
//        Assert.assertEquals(2, vertices.size());
//        Assert.assertTrue(vertices.contains(b1) && vertices.contains(b2));
//
//        vertices = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out()).until(__.has(T.label, "B")).toList();
//        Assert.assertEquals(2, vertices.size());
//        Assert.assertTrue(vertices.contains(b1) && vertices.contains(b2));
//    }
//
//    @Test
//    public void testUnoptimizedRepeatStepUntilHasProperty() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1", "hub", true);
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2", "hub", true);
//        a1.addEdge("ab", b1);
//        a2.addEdge("ab", b2);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out()).until(__.has("hub", true)).toList();
//        Assert.assertEquals(2, vertices.size());
//        Assert.assertTrue(vertices.contains(b1) && vertices.contains(b2));
//    }
//
//    @Test
//    public void testUnoptimizedRepeatDeep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "hub", true);
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "hub", false);
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "hub", false);
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "hub", false);
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        Vertex c11 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
//        Vertex c12 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
//        Vertex c13 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
//        b1.addEdge("bc", c11);
//        b1.addEdge("bc", c12);
//        b1.addEdge("bc", c13);
//        Vertex c21 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
//        Vertex c22 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
//        Vertex c23 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
//        b2.addEdge("bc", c21);
//        b2.addEdge("bc", c22);
//        b2.addEdge("bc", c23);
//        Vertex c31 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
//        Vertex c32 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
//        Vertex c33 = this.sqlgGraph.addVertex(T.label, "C", "hub", false);
//        b3.addEdge("bc", c31);
//        b3.addEdge("bc", c32);
//        b3.addEdge("bc", c33);
//        Vertex d = this.sqlgGraph.addVertex(T.label, "D", "hub", true);
//        c11.addEdge("cd", d);
//        c12.addEdge("cd", d);
//        c13.addEdge("cd", d);
//        c21.addEdge("cd", d);
//        c22.addEdge("cd", d);
//        c23.addEdge("cd", d);
//        c31.addEdge("cd", d);
//        c32.addEdge("cd", d);
//        c33.addEdge("cd", d);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal()
//                .V().hasLabel("A")
//                .repeat(
//                        __.out()
//                )
//                .until(__.has("hub", true))
//                .toList();
//
//        Assert.assertEquals(9, vertices.size());
//
//    }
//
//    @Test
//    public void testUnoptimizedRepeatStepOnGraphyGremlin() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "hub", true);
//        Vertex a21 = this.sqlgGraph.addVertex(T.label, "A", "hub", false);
//        Vertex a22 = this.sqlgGraph.addVertex(T.label, "A", "hub", false);
//        Vertex a23 = this.sqlgGraph.addVertex(T.label, "A", "hub", false);
//        Vertex a31 = this.sqlgGraph.addVertex(T.label, "A", "hub", false);
//        Vertex a32 = this.sqlgGraph.addVertex(T.label, "A", "hub", false);
//        Vertex a33 = this.sqlgGraph.addVertex(T.label, "A", "hub", false);
//        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "hub", false);
//        Vertex a5 = this.sqlgGraph.addVertex(T.label, "A", "hub", true);
//        a1.addEdge("aa", a21);
//        a1.addEdge("aa", a22);
//        a1.addEdge("aa", a23);
//
//        a21.addEdge("aa", a31);
//        a21.addEdge("aa", a32);
//        a21.addEdge("aa", a33);
//
//        a31.addEdge("aa", a4);
//        a4.addEdge("aa", a5);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal()
//                .V().hasLabel("A")
//                .repeat(
//                        __.out()
//                ).until(
//                        __.has("hub", true)
//                )
//                .toList();
//        Assert.assertEquals(4, vertices.size());
//        Assert.assertEquals(a5, vertices.get(0));
//        Assert.assertEquals(a5, vertices.get(1));
//        Assert.assertEquals(a5, vertices.get(2));
//        Assert.assertEquals(a5, vertices.get(3));
//    }
}
