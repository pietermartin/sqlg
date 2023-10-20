package org.umlg.sqlg.test.filter.not.barrier;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.step.SqlgVertexStep;
import org.umlg.sqlg.step.barrier.SqlgNotStepBarrier;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/10/28
 */
public class TestNotStepBarrier extends BaseTest {

    @Test
    public void g_VX1X_repeatXoutX_untilXoutE_count_isX0XX_name() {
        loadModern();
        DefaultGraphTraversal<Vertex, String> traversal = (DefaultGraphTraversal<Vertex, String>)this.sqlgGraph.traversal()
                .V(convertToVertexId("marko"))
                .repeat(__.out())
                .until(__.outE().count().is(0))
                .<String>values("name");

        printTraversalForm(traversal);
        checkResults(Arrays.asList("lop", "lop", "ripple", "vadas"), traversal);

        List<SqlgNotStepBarrier> steps = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgNotStepBarrier.class, traversal);
        Assert.assertEquals(1, steps.size());

    }

    @Test
    public void testNotStepBarrier() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V()
                .hasLabel("A")
                .not(__.out().has("name", "b1"));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(a2, vertices.get(0));
        List<Step> steps = traversal.getSteps();
        Assert.assertEquals(2, steps.size());
        Assert.assertTrue(steps.get(1) instanceof SqlgNotStepBarrier);
        SqlgNotStepBarrier sqlgNotStepBarrier = (SqlgNotStepBarrier) steps.get(1);
        Assert.assertEquals(1, sqlgNotStepBarrier.getLocalChildren().size());
        Traversal.Admin t = (Traversal.Admin) sqlgNotStepBarrier.getLocalChildren().get(0);
        Assert.assertEquals(1, t.getSteps().size());
        Assert.assertTrue(t.getSteps().get(0) instanceof SqlgVertexStep);
    }
}
