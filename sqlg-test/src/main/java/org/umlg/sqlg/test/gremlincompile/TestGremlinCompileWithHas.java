package org.umlg.sqlg.test.gremlincompile;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/01/19
 * Time: 6:22 AM
 */
public class TestGremlinCompileWithHas extends BaseTest {

    @Test
    public void testSingleCompileWithHasLabelOut() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        a1.addEdge("outB", b1);
        a1.addEdge("outB", b2);
        a1.addEdge("outB", b3);
        a1.addEdge("outC", c1);
        a1.addEdge("outC", c2);
        a1.addEdge("outC", c3);
        this.sqlgGraph.tx().commit();
        GraphTraversal traversal = a1.out().has(T.label, "B");
        traversal.asAdmin().applyStrategies(TraversalEngine.STANDARD);
        Assert.assertEquals(3, a1.out().has(T.label, "B").count().next().intValue());
    }

    @Test
    public void testSingleCompileWithHasLabelIn() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("outB", b1);
        b1.addEdge("outC", c1);
        a2.addEdge("outB", b2);
        b2.addEdge("outC", c1);
        a3.addEdge("outB", b3);
        b3.addEdge("outC", c1);
        d1.addEdge("outB", b4);
        b4.addEdge("outC", c1);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(4, c1.in().in().count().next().intValue());
        Assert.assertEquals(3, c1.in().in().has(T.label, "A").count().next().intValue());
    }
}
