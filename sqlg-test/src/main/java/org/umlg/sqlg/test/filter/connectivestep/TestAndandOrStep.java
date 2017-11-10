package org.umlg.sqlg.test.filter.connectivestep;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/11/05
 */
public class TestAndandOrStep extends BaseTest {

    @Test
    public void testAndandOr() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "surname", "s1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "surname", "s2", "age", 2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "surname", "s3", "age", 3);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "surname", "ss3", "age", 3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A")
                .or(
                        __.has("name", "a1"),
                        __.has("name", "a2"),
                        __.and(
                                __.has("name", "a3"),
                                __.has("surname", "s3")
                        )
                );
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testAndOr() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "surname", "s1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "surname", "s2", "age", 2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "surname", "s3", "age", 3);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "surname", "ss3", "age", 3);
        Vertex a5 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "surname", "s4", "age", 4);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A")
                .or(
                        __.and(
                                __.has("name", "a1"),
                                __.has("age", 1)
                        ),
                        __.and(
                                __.has("name", "a2"),
                                __.has("age", 3)
                        ),
                        __.or(
                                __.has("name", "a3"),
                                __.has("name", "a4")
                        )

                );
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(4, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(a1, a3, a4, a5)));
    }

    @Test
    public void testAndOrNested() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "surname", "s1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "surname", "s2", "age", 2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "surname", "s3", "age", 3);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "surname", "ss3", "age", 3);
        Vertex a5 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "surname", "s4", "age", 4);
        this.sqlgGraph.tx().commit();


        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A")
                .or(
                        __.and(
                                __.has("name", "a1"),
                                __.has("age", 1)
                        ),
                        __.and(
                                __.has("name", "a2"),
                                __.has("age", 3)
                        ),
                        __.or(
                                __.or(
                                        __.and(
                                                __.has("name", "a1"),
                                                __.has("name", "a1")
                                        ),
                                        __.and(
                                                __.has("name", "a2"),
                                                __.has("age",2)
                                        )
                                ),
                                __.or(
                                        __.has("name", "b1"),
                                        __.has("name", "b1"),
                                        __.has("name", "b1")
                                )
                        )
                );

        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(a1, a2)));

    }
}
