package org.umlg.sqlg.test.vertex;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Date: 2016/12/13
 * Time: 4:36 PM
 */
public class TestVertexEdges extends BaseTest {

    @Test
    public void testVertexEdges() {
        final Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        final Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        final Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
        a1.addEdge("ab", b1);
        a1.addEdge("ac", c1);
        this.sqlgGraph.tx().commit();

        AtomicInteger count = new AtomicInteger(0);
        a1.edges(Direction.BOTH).forEachRemaining(e->count.incrementAndGet());
        Assert.assertEquals(2, count.get());
        count.set(0);
        a1.edges(Direction.BOTH).forEachRemaining(edge -> {
            a1.addEdge("ab", b1);
            count.getAndIncrement();
        });
        Assert.assertEquals(2, count.get());
    }

    @Test
    public void testBothEOnEdgeToSelf() {
        final Vertex v1 = this.sqlgGraph.addVertex("name", "marko");
        final Vertex v2 = this.sqlgGraph.addVertex("name", "puppy");
        v1.addEdge("knows", v2, "since", 2010);
        v1.addEdge("pets", v2);
        v1.addEdge("walks", v2, "location", "arroyo");
        v2.addEdge("knows", v1, "since", 2010);
        Assert.assertEquals(4, vertexTraversal(v1).bothE().count().next().intValue());
        Assert.assertEquals(4, vertexTraversal(v2).bothE().count().next().intValue());
        v1.edges(Direction.BOTH).forEachRemaining(edge -> {
            v1.addEdge("livesWith", v2);
            v1.addEdge("walks", v2, "location", "river");
            edge.remove();
        });

        this.sqlgGraph.tx().commit();

        List<Edge> edgeList = vertexTraversal(v1).outE().toList();
        for (Edge edge : edgeList) {
            System.out.println(edge);
        }

        Assert.assertEquals(8, vertexTraversal(v1).outE().count().next().intValue());
        Assert.assertEquals(0, vertexTraversal(v2).outE().count().next().intValue());
        v1.edges(Direction.BOTH).forEachRemaining(Edge::remove);
        Assert.assertEquals(0, vertexTraversal(v1).bothE().count().next().intValue());
        Assert.assertEquals(0, vertexTraversal(v2).bothE().count().next().intValue());
    }
}
