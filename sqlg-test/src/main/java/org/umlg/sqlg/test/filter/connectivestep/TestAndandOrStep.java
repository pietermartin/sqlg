package org.umlg.sqlg.test.filter.connectivestep;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
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
@SuppressWarnings("unused")
public class TestAndandOrStep extends BaseTest {

    @SuppressWarnings("unused")
    @Test
    public void testAndWithWithin() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "surname", "s1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "surname", "s2", "age", 2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "surname", "s3", "age", 3);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "surname", "ss3", "age", 3);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .and(
                        __.has("name", P.eq("a1")),
                        __.has("name", P.eq("a3"))
                ).toList();
        Assert.assertEquals(0, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .and(
                        __.has("name", P.within("a1", "a2")),
                        __.has("name", P.within("a2", "a4"))
                ).toList();
        Assert.assertEquals(1, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .and(
                        __.has("name", P.without("a1", "a2")),
                        __.has("name", P.within("a3", "a4"))
                ).toList();
        Assert.assertEquals(2, vertices.size());
    }

    /**
     * For simple HasContainers, sqlg optimizes within via a join statement.
     * This does not work with nested AND/OR, so we skip the bulk logic
     */
    @SuppressWarnings("unused")
    @Test
    public void testAndWithWithinNotTriggeringBulkWithin() {
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", "a" +  i, "surname", "s" + i, "age", i);
        }
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .and(
                        __.has("name", P.eq("a1")),
                        __.has("name", P.eq("a3"))
                ).toList();
        Assert.assertEquals(0, vertices.size());

        vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .and(
                        __.has("name", P.within("a1", "a2", "a3", "a4")),
                        __.has("name", P.within("a2", "a3", "a4", "a5"))
                ).toList();
        Assert.assertEquals(3, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .and(
                        __.has("name", P.without("a1", "a2", "a3")),
                        __.has("name", P.within("a3", "a4", "a5"))
                ).toList();
        Assert.assertEquals(2, vertices.size());
    }

    /**
     * For simple HasContainers, sqlg optimizes within via a join statement.
     * This does not work with nested AND/OR, so we skip the bulk logic
     */
    @SuppressWarnings("unused")
    @Test
    public void testOrWithWithinNotTriggeringBulkWithin() {
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", "a" +  i, "surname", "s" + i, "age", i);
        }
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .or(
                        __.has("name", P.eq("a1")),
                        __.has("name", P.eq("a3"))
                ).toList();
        Assert.assertEquals(2, vertices.size());

        vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .or(
                        __.has("name", P.within("a1", "a2", "a3", "a4")),
                        __.has("name", P.within("a2", "a3", "a4", "a5"))
                ).toList();
        Assert.assertEquals(5, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .or(
                        __.has("name", P.without("a1", "a2", "a3")),
                        __.has("name", P.within("a3", "a4", "a5"))
                ).toList();
        Assert.assertEquals(98, vertices.size());
    }

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

    @Test
    public void testAndWithHasNot() {
        Vertex v = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "surname", "s2");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A")
                .and(
                        __.hasNot("surname"),
                        __.has("name", "a1")
                );
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(v));
    }

    @Test
    public void testAndOrNestedWithHasAndHasNot() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "surname", "s2");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A")
                .or(
                        __.and(
                                __.hasNot("surname"),
                                __.has("name", "a1")
                        ),
                        __.and(
                                __.has("surname"),
                                __.has("name", "a2")
                        )
                );
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(a1, a2)));
    }

    @Test
    public void testHasBeforeOr() {
        Vertex v1 = this.sqlgGraph.traversal().addV("V").property("start", 0).property("end", 10).next();
        this.sqlgGraph.traversal().addV("V").property("start", 1).next();
        this.sqlgGraph.traversal().tx().commit();

        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("V")
                .or(
                        __.has("start", P.lte(-1))
                                .or(
                                        __.hasNot("end"),
                                        __.has("end", P.gte(-1))
                                ),
                        __.has("start", P.lte(0))
                                .or(
                                        __.hasNot("end"),
                                        __.has("end", P.gte(0))
                                )
                );
        printTraversalForm(traversal);
        List<Vertex> result = traversal.toList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(v1, result.get(0));
    }
}
