package org.umlg.sqlg.test.orstep;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
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
 * Date: 2017/10/27
 */
public class TestOrStepBarrier extends BaseTest {

    @Test
    public void g_V_emitXhasXname_markoX_or_loops_isX2XX_repeatXoutX_valuesXnameX() {
        loadModern();
        final Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
                .V()
                .emit(
                        __.has("name", "marko")
                                .or()
                                .loops().is(2)
                )
                .repeat(__.out())
                .values("name");
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "ripple", "lop"), traversal);
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


        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("A").or(
                __.out("ab"),
                __.out("abb")
        );
        printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.containsAll(Arrays.asList(a1, a2)));
    }
}
