package org.umlg.sqlg.test.fold;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.function.BinaryOperator;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@SuppressWarnings("unchecked")
public class TestFoldStep extends BaseTest {

    @Test
    public void g_V_age_foldX0_plusX() {
        loadModern();
        final Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V().<Integer>values("age").fold(0, (BinaryOperator) Operator.sum);
        printTraversalForm(traversal);
        final Integer ageSum = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals(Integer.valueOf(123), ageSum);
    }

    @Test
    public void shouldTriggerAddVertexAndPropertyUpdateWithCoalescePattern() {
        Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().has("some","thing").fold().coalesce(unfold(), addV()).property("some", "thing");
        traversal.iterate();
        this.sqlgGraph.tx().commit();
        assertEquals(1, IteratorUtils.count(this.sqlgGraph.traversal().V().has("some", "thing")));
    }

}
