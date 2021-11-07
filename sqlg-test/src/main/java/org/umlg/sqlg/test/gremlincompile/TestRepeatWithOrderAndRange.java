package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2017/04/07
 * Time: 10:31 AM
 */
public class TestRepeatWithOrderAndRange extends BaseTest {

    @Test
    public void testRepeatWithOrder() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1", "order", 1);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2", "order", 2);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3", "order", 3);
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1", "order", 4);
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2", "order", 5);
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3", "order", 6);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .repeat(
                        __.out().order().by("order", Order.desc)
                ).times(2);
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(3, vertices.size());
        Assert.assertEquals(c3, vertices.get(0));
        Assert.assertEquals(c2, vertices.get(1));
        Assert.assertEquals(c1, vertices.get(2));
    }
}
