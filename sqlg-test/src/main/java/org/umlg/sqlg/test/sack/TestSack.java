package org.umlg.sqlg.test.sack;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.Collections;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 *         Date: 2017/05/02
 */
public class TestSack extends BaseTest {

    @Test
    public void g_withSackX1_sumX_VX1X_localXoutXknowsX_barrierXnormSackXX_inXknowsX_barrier_sack() {
        loadModern();
        final Traversal<Vertex, Double> traversal = this.sqlgGraph.traversal()
                .withSack(1.0d, Operator.sum)
                .V(convertToVertexId("marko"))
                .local(
                        __.out("knows").barrier(SackFunctions.Barrier.normSack)
                )
                .in("knows")
                .barrier()
                .sack();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(1.0d, 1.0d), traversal);
    }

    @Test
    public void g_withBulkXfalseX_withSackX1_sumX_VX1X_localXoutEXknowsX_barrierXnormSackX_inVX_inXknowsX_barrier_sack() {
        loadModern();
        final Traversal<Vertex, Double> traversal = this.sqlgGraph.traversal()
                .withBulk(false)
                .withSack(1.0d, Operator.sum)
                .V(convertToVertexId("marko"))
                .local(
                        __.outE("knows").barrier(SackFunctions.Barrier.normSack).inV()
                )
                .in("knows")
                .barrier()
                .sack();
        printTraversalForm(traversal);
        checkResults(Collections.singletonList(1.0d), traversal);
    }

    @Test
    public void g_withBulkXfalseX_withSackX1_sumX_V_out_barrier_sack() {
        loadModern();
        final Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal()
                .withBulk(false)
                .withSack(1, Operator.sum)
                .V()
                .out()
                .barrier()
                .sack();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(1, 1, 1, 3), traversal); // josh, vadas, ripple, lop
    }
}
