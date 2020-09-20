package org.umlg.sqlg.test.branchstep;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/04/30
 */
public class TestSqlgBranchStep extends BaseTest {

    @Test
    public void testSqlgBranchStepKeepsStartElementsOrder() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "order", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "order", 2);
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
        a1.addEdge("ab", b1, "order", 1);
        a1.addEdge("ab", b2, "order", 2);
        a1.addEdge("ab", b3, "order", 3);
        a1.addEdge("ab", b4, "order", 0);
        a2.addEdge("ab", b1, "order", 1);
        a2.addEdge("ab", b2, "order", 2);
        a2.addEdge("ab", b3, "order", 3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1", "order", 1);
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2", "order", 2);
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3", "order", 3);
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4", "order", 1);
        Vertex c5 = this.sqlgGraph.addVertex(T.label, "C", "name", "c5", "order", 2);
        Vertex c6 = this.sqlgGraph.addVertex(T.label, "C", "name", "c6", "order", 3);
        Vertex c7 = this.sqlgGraph.addVertex(T.label, "C", "name", "c7", "order", 1);
        Vertex c8 = this.sqlgGraph.addVertex(T.label, "C", "name", "c8", "order", 2);
        Vertex c9 = this.sqlgGraph.addVertex(T.label, "C", "name", "c9", "order", 3);
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

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V().hasLabel("A").order().by("order")
                .local(
                        __.optional(
                                __.outE("ab").order().by("order", Order.desc).inV()
                                        .local(
                                                __.optional(
                                                        __.out("bc").order().by("order", Order.desc)
                                                )
                                        )
                        )
                )
                .path();
        List<Path> paths = traversal.toList();
        for (Path path : paths) {
            if (path.objects().size() > 3) {
                System.out.println(path.toString() + " " + ((Vertex) path.objects().get(3)).<String>value("name"));
            } else {
                System.out.println(path.toString() + " " + ((Vertex) path.objects().get(2)).<String>value("name"));
            }
        }

        Assert.assertEquals(19, paths.size());
        Assert.assertEquals(a1, paths.get(0).objects().get(0));
        Assert.assertEquals(c9, paths.get(0).objects().get(3));
        Assert.assertEquals(a1, paths.get(1).objects().get(0));
        Assert.assertEquals(c8, paths.get(1).objects().get(3));
        Assert.assertEquals(a1, paths.get(2).objects().get(0));
        Assert.assertEquals(c7, paths.get(2).objects().get(3));

        Assert.assertEquals(a1, paths.get(3).objects().get(0));
        Assert.assertEquals(c6, paths.get(3).objects().get(3));
        Assert.assertEquals(a1, paths.get(4).objects().get(0));
        Assert.assertEquals(c5, paths.get(4).objects().get(3));
        Assert.assertEquals(a1, paths.get(5).objects().get(0));
        Assert.assertEquals(c4, paths.get(5).objects().get(3));

        Assert.assertEquals(a1, paths.get(6).objects().get(0));
        Assert.assertEquals(c3, paths.get(6).objects().get(3));
        Assert.assertEquals(a1, paths.get(7).objects().get(0));
        Assert.assertEquals(c2, paths.get(7).objects().get(3));
        Assert.assertEquals(a1, paths.get(8).objects().get(0));
        Assert.assertEquals(c1, paths.get(8).objects().get(3));

        Assert.assertEquals(a1, paths.get(9).objects().get(0));
        Assert.assertEquals(b4, paths.get(9).objects().get(2));

        Assert.assertEquals(a2, paths.get(10).objects().get(0));
        Assert.assertEquals(c9, paths.get(10).objects().get(3));
        Assert.assertEquals(a2, paths.get(11).objects().get(0));
        Assert.assertEquals(c8, paths.get(11).objects().get(3));
        Assert.assertEquals(a2, paths.get(12).objects().get(0));
        Assert.assertEquals(c7, paths.get(12).objects().get(3));

        Assert.assertEquals(a2, paths.get(13).objects().get(0));
        Assert.assertEquals(c6, paths.get(13).objects().get(3));
        Assert.assertEquals(a2, paths.get(14).objects().get(0));
        Assert.assertEquals(c5, paths.get(14).objects().get(3));
        Assert.assertEquals(a2, paths.get(15).objects().get(0));
        Assert.assertEquals(c4, paths.get(15).objects().get(3));

        Assert.assertEquals(a2, paths.get(16).objects().get(0));
        Assert.assertEquals(c3, paths.get(16).objects().get(3));
        Assert.assertEquals(a2, paths.get(17).objects().get(0));
        Assert.assertEquals(c2, paths.get(17).objects().get(3));
        Assert.assertEquals(a2, paths.get(18).objects().get(0));
        Assert.assertEquals(c1, paths.get(18).objects().get(3));
    }

    @Test
    public void testBranchStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal traversal = (DefaultGraphTraversal) this.sqlgGraph.traversal()
                .V(a1)
                .branch(__.out().values("name"))
                .option(P.eq("b2"), __.out());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(0, vertices.size());

        traversal = (DefaultGraphTraversal) this.sqlgGraph.traversal()
                .V(a1)
                .branch(__.out().values("name"))
                .option(P.eq("b1"), __.out());
        vertices = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(2, vertices.size());
    }

}
