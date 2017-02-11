package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/05/31
 * Time: 7:32 PM
 */
public class TestOptionalWithOrder extends BaseTest {

    //unoptimized
    @Test
    public void testOptionalWithOrder() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "order", 3);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "order", 2);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "order", 1);
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a1.id()).as("a").optional(outE().as("e").otherV().as("v")).order().by("order");
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> vertices =  traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());
        assertEquals(3, vertices.size());
    }

    //TODO unoptimized, this is needed for UMLG, optimize!!!
    @Test
    public void testOptionalWithOrderBy2() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
        Edge ab1 = a1.addEdge("ab", b1, "order", 3);
        Edge ab2 = a1.addEdge("ab", b2, "order", 2);
        Edge ab3 = a1.addEdge("ab", b3, "order", 1);
        Edge bc1 = b1.addEdge("bc", c1, "order", 3);
        Edge bc2 = b1.addEdge("bc", c2, "order", 2);
        Edge bc3 = b1.addEdge("bc", c3, "order", 1);
        Edge cd1 = c1.addEdge("cd", d1, "order", 3);
        Edge cd2 = c1.addEdge("cd", d2, "order", 2);
        Edge cd3 = c1.addEdge("cd", d3, "order", 1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a1.id())
                .optional(
                        __.outE("ab").as("ab").otherV().as("vb").order().by(__.select("ab").by("order"), Order.incr)
                                .optional(
                                        __.outE("bc").as("bc").otherV().as("vc").order().by(__.select("bc").by("order"), Order.incr)
                                                .optional(
                                                        __.outE("cd").as("cd").inV().as("vd").order().by(__.select("cd").by("order"), Order.incr)
                                                )
                                )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());

        Assert.assertEquals(7, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(
                p -> p.size() == 5 &&
                        p.get(0).equals(a1) &&
                        p.get(1).equals(ab1) &&
                        p.get(2).equals(b1) &&
                        p.get(3).equals(bc3) &&
                        p.get(4).equals(c3)
        ));
        paths.remove(paths.stream().filter(
                p -> p.size() == 5 &&
                        p.get(0).equals(a1) &&
                        p.get(1).equals(ab1) &&
                        p.get(2).equals(b1) &&
                        p.get(3).equals(bc3) &&
                        p.get(4).equals(c3)
        ).findAny().get());
        Assert.assertEquals(6, paths.size());

        Assert.assertTrue(paths.stream().anyMatch(
                p -> p.size() == 5 &&
                        p.get(0).equals(a1) &&
                        p.get(1).equals(ab1) &&
                        p.get(2).equals(b1) &&
                        p.get(3).equals(bc2) &&
                        p.get(4).equals(c2)
        ));
        paths.remove(paths.stream().filter(
                p -> p.size() == 5 &&
                        p.get(0).equals(a1) &&
                        p.get(1).equals(ab1) &&
                        p.get(2).equals(b1) &&
                        p.get(3).equals(bc2) &&
                        p.get(4).equals(c2)
        ).findAny().get());
        Assert.assertEquals(5, paths.size());

        Assert.assertTrue(paths.stream().anyMatch(
                p -> p.size() == 7 &&
                        p.get(0).equals(a1) &&
                        p.get(1).equals(ab1) &&
                        p.get(2).equals(b1) &&
                        p.get(3).equals(bc1) &&
                        p.get(4).equals(c1) &&
                        p.get(5).equals(cd1) &&
                        p.get(6).equals(d1)
        ));
        paths.remove(paths.stream().filter(
                p -> p.size() == 7 &&
                        p.get(0).equals(a1) &&
                        p.get(1).equals(ab1) &&
                        p.get(2).equals(b1) &&
                        p.get(3).equals(bc1) &&
                        p.get(4).equals(c1) &&
                        p.get(5).equals(cd1) &&
                        p.get(6).equals(d1)
        ).findAny().get());
        Assert.assertEquals(4, paths.size());

        Assert.assertTrue(paths.stream().anyMatch(
                p -> p.size() == 7 &&
                        p.get(0).equals(a1) &&
                        p.get(1).equals(ab1) &&
                        p.get(2).equals(b1) &&
                        p.get(3).equals(bc1) &&
                        p.get(4).equals(c1) &&
                        p.get(5).equals(cd2) &&
                        p.get(6).equals(d2)
        ));
        paths.remove(paths.stream().filter(
                p -> p.size() == 7 &&
                        p.get(0).equals(a1) &&
                        p.get(1).equals(ab1) &&
                        p.get(2).equals(b1) &&
                        p.get(3).equals(bc1) &&
                        p.get(4).equals(c1) &&
                        p.get(5).equals(cd2) &&
                        p.get(6).equals(d2)
        ).findAny().get());
        Assert.assertEquals(3, paths.size());

        Assert.assertTrue(paths.stream().anyMatch(
                p -> p.size() == 7 &&
                        p.get(0).equals(a1) &&
                        p.get(1).equals(ab1) &&
                        p.get(2).equals(b1) &&
                        p.get(3).equals(bc1) &&
                        p.get(4).equals(c1) &&
                        p.get(5).equals(cd3) &&
                        p.get(6).equals(d3)
        ));
        paths.remove(paths.stream().filter(
                p -> p.size() == 7 &&
                        p.get(0).equals(a1) &&
                        p.get(1).equals(ab1) &&
                        p.get(2).equals(b1) &&
                        p.get(3).equals(bc1) &&
                        p.get(4).equals(c1) &&
                        p.get(5).equals(cd3) &&
                        p.get(6).equals(d3)
        ).findAny().get());
        Assert.assertEquals(2, paths.size());

        Assert.assertTrue(paths.stream().anyMatch(
                p -> p.size() == 3 &&
                        p.get(0).equals(a1) &&
                        p.get(1).equals(ab2) &&
                        p.get(2).equals(b2)
        ));
        paths.remove(paths.stream().filter(
                p -> p.size() == 3 &&
                        p.get(0).equals(a1) &&
                        p.get(1).equals(ab2) &&
                        p.get(2).equals(b2)
        ).findAny().get());
        Assert.assertEquals(1, paths.size());

        Assert.assertTrue(paths.stream().anyMatch(
                p -> p.size() == 3 &&
                        p.get(0).equals(a1) &&
                        p.get(1).equals(ab3) &&
                        p.get(2).equals(b3)
        ));
        paths.remove(paths.stream().filter(
                p -> p.size() == 3 &&
                        p.get(0).equals(a1) &&
                        p.get(1).equals(ab3) &&
                        p.get(2).equals(b3)
        ).findAny().get());
        Assert.assertEquals(0, paths.size());
    }

}
