package org.umlg.sqlg.test.gremlincompile;

import com.tinkerpop.gremlin.process.T;
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
    public void testSingleCompileWithHas() {
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

        Assert.assertEquals(3,a1.out().has(T.label, "B").count().next().intValue());
    }
}
