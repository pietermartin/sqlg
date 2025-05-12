package org.umlg.sqlg.test.filter.or.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.step.barrier.SqlgOrStepBarrier;
import org.umlg.sqlg.step.barrier.SqlgRepeatStepBarrier;
import org.umlg.sqlg.structure.DefaultSqlgTraversal;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/10/27
 */
public class TestOrStepBarrier extends BaseTest {

    @Test
    public void g_V_emitXhasXname_markoX_or_loops_isX2XX_repeatXoutX_valuesXnameX() {
        loadModern();
        final DefaultSqlgTraversal<Vertex, String> traversal = (DefaultSqlgTraversal<Vertex, String>) this.sqlgGraph.traversal()
                .V()
                .emit(
                        __.has("name", "marko")
                                .or()
                                .loops().is(2)
                )
                .repeat(__.out())
                .<String>values("name");
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "ripple", "lop"), traversal);
        List<SqlgRepeatStepBarrier> sqlgRepeatStepBarriers = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgRepeatStepBarrier.class, traversal);
        Assert.assertEquals(1, sqlgRepeatStepBarriers.size());
        List<SqlgOrStepBarrier> sqlgOrStepBarriers = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgOrStepBarrier.class, traversal);
        Assert.assertEquals(1, sqlgOrStepBarriers.size());
    }

    @Test
    public void testOrStepBarrier3Ors() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        a2.addEdge("abb", b2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B");
        a3.addEdge("abbb", b3);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B");
        a4.addEdge("abbbb", b4);


        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .or(
                        __.out("ab"),
                        __.out("abb"),
                        __.out("abbb")
                );
        printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(a1, a2, a3)));
        List<SqlgOrStepBarrier> sqlgOrStepBarriers = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgOrStepBarrier.class, traversal);
        Assert.assertEquals(1, sqlgOrStepBarriers.size());
    }

    @Test
    public void testOrStepBarrier() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        a2.addEdge("abb", b2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B");
        a3.addEdge("abbb", b3);


        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .or(
                        __.out("ab"),
                        __.out("abb")
                );
        printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(a1, a2)));
        List<SqlgOrStepBarrier> sqlgOrStepBarriers = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgOrStepBarrier.class, traversal);
        Assert.assertEquals(1, sqlgOrStepBarriers.size());
    }

    @Test
    public void test() {
        this.sqlgGraph.traversal().V().hasLabel("A").or();
    }
}
