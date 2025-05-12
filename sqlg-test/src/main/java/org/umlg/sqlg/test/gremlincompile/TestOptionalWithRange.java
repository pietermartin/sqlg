package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.DefaultSqlgTraversal;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2017/04/05
 * Time: 6:16 PM
 */
public class TestOptionalWithRange extends BaseTest {

    @Test
    public void testSimpleOrderWithRange() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "order", 1);
        for (int i = 0; i < 10; i++) {
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "order", i);
            a1.addEdge("ab", b);
            for (int j = 0; j < 10; j++) {
                Vertex c = this.sqlgGraph.addVertex(T.label, "C", "order", i);
                b.addEdge("bc", c);
            }
        }
        this.sqlgGraph.tx().commit();

        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .optional(
                        __.out().optional(
                                __.out().range(5, 6)
                        )
                );
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());


    }
}
