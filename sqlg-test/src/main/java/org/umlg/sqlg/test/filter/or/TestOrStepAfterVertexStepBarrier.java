package org.umlg.sqlg.test.filter.or;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.step.barrier.SqlgLocalStepBarrier;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2017/11/06
 */
public class TestOrStepAfterVertexStepBarrier extends BaseTest {

    @Test
    public void testOrStepAfterVertexStepBarrier() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A")
                .local(
                        __.out().or(
                                __.has("name", "b2"),
                                __.has("name", "b3")
                        )
                );
        List<Vertex> vertices = traversal.toList();
        List<Step> steps = traversal.getSteps();
        Assert.assertEquals(2, steps.size());
        Assert.assertTrue(steps.get(1) instanceof SqlgLocalStepBarrier);
        SqlgLocalStepBarrier sqlgLocalStepBarrier = (SqlgLocalStepBarrier)steps.get(1);
        Assert.assertEquals(1, sqlgLocalStepBarrier.getLocalChildren().size());
        Traversal.Admin t = (Traversal.Admin) sqlgLocalStepBarrier.getLocalChildren().get(0);
        //or step is collapsed into the vertex step.
        Assert.assertEquals(1, t.getSteps().size());
        Assert.assertEquals(2, vertices.size());
    }

}
