package org.umlg.sqlg.test.tinkerpopjira;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Date: 2017/01/26
 * Time: 9:14 PM
 */
public class TestTinkerPopJira extends BaseTest {

    //This return 3 for Sqlg and 2 on TinkerGraph
    @SuppressWarnings("unchecked")
    @Test
    public void testLazy1AddE() {
        final Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        final Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        final Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        a1.addEdge("ab", b1);
        a1.addEdge("ac", c1);
        this.sqlgGraph.tx().commit();

        GraphTraversal t = this.sqlgGraph.traversal().V(a1).both().addE("ab").from(a1).to(b1);
        printTraversalForm(t);
        List<Edge> edges = t.toList();
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(3, edges.size());
    }

    @Test
    public void testLazy1() {
        final Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        final Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        final Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        a1.addEdge("ab", b1);
        a1.addEdge("ac", c1);
        this.sqlgGraph.tx().commit();

        AtomicInteger count = new AtomicInteger(0);
        this.sqlgGraph.traversal().V(a1).bothE().forEachRemaining(edge -> {
            a1.addEdge("ab", b1);
            c1.addEdge("ac", a1);
            count.getAndIncrement();
        });
        Assert.assertEquals(3, count.get());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testLazy2() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("A");
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("B");
        VertexLabel cVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("C");
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);
        aVertexLabel.ensureEdgeLabelExist("ac", cVertexLabel);
        cVertexLabel.ensureEdgeLabelExist("ac", aVertexLabel);


        final Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        final Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        final Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        a1.addEdge("ab", b1);
        a1.addEdge("ac", c1);
        this.sqlgGraph.tx().commit();

        AtomicInteger count = new AtomicInteger(0);
        this.sqlgGraph.traversal().V(a1).union(__.outE(), __.inE()).forEachRemaining(edge -> {
            a1.addEdge("ab", b1);
            c1.addEdge("ac", a1);
            count.getAndIncrement();
        });
        Assert.assertEquals(4, count.get());
    }
}
