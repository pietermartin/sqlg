package org.umlg.sqlg.test.vertexout;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.step.barrier.SqlgNotStepBarrier;
import org.umlg.sqlg.structure.DefaultSqlgTraversal;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings("unchecked")
public class TestNotOutStep extends BaseTest {

    @Test
    public void testProcessTestsFailure() {
        loadModern();
        //this traversal gets optimized into a Not traversal by TinkerPop
        GraphTraversal<Vertex, String> traversal = this.sqlgGraph.traversal().V()
                .where(
                        __.in("knows")
                                .out("created")
                                .count().is(0)
                ).values("name");
        checkResults(Arrays.asList("marko", "lop", "ripple", "peter"), traversal);
    }

    @Test
    public void testNotOut() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a3.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A")
                .not(__.out("ab"));
        String sql = getSQL(traversal);
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        if (isPostgres()) {
            Assert.assertEquals("""
                    SELECT
                    \t"public"."V_A"."ID" AS "alias1",
                    \t"public"."V_A"."name" AS "alias2"
                    FROM
                    \t"public"."V_A" LEFT OUTER JOIN
                    \t"public"."E_ab" ON "public"."V_A"."ID" = "public"."E_ab"."public.A__O" LEFT OUTER JOIN
                    \t"public"."V_B" ON "public"."E_ab"."public.B__I" = "public"."V_B"."ID"
                    WHERE
                    \t"public"."E_ab"."ID" IS NULL""", sql);
        }

        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(a2));
        Assert.assertTrue(vertices.contains(a4));
    }

    @Test
    public void testNotOutEdge() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a3.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A")
                .not(__.outE("ab"));
        String sql = getSQL(traversal);
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        if (isPostgres()) {
            Assert.assertEquals("""
                    SELECT
                    	"public"."E_ab"."public.A__O" AS "alias1",
                    	"public"."E_ab"."public.B__I" AS "alias2",
                    	"public"."V_A"."ID" AS "alias3",
                    	"public"."V_A"."name" AS "alias4"
                    FROM
                    	"public"."V_A" LEFT OUTER JOIN
                    	"public"."E_ab" ON "public"."V_A"."ID" = "public"."E_ab"."public.A__O"
                    WHERE
                    	"public"."E_ab"."ID" IS NULL""", sql);
        }

        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(a2));
        Assert.assertTrue(vertices.contains(a4));
    }

    @Test
    public void testNotOutWithHas() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        a3.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        DefaultTraversal<Vertex, Vertex> traversal = (DefaultTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A")
                .not(__.out("ab").has("name", "b1"));
        printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgNotStepBarrier<?>);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(a2));
    }
}
