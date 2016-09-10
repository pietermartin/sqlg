package org.umlg.sqlg.test.edges;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/09/10
 * Time: 3:39 PM
 */
public class TestEdgeFromDifferentSchema extends BaseTest {

    @Test
    public void test() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A.A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B.B");
        a1.addEdge("eee", b1);
        this.sqlgGraph.tx().commit();
        assertEquals(1, this.sqlgGraph.traversal().E().hasLabel("eee").count().next().intValue());
    }
}
