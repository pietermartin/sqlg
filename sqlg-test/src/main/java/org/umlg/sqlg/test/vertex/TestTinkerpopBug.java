package org.umlg.sqlg.test.vertex;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2014/10/19
 * Time: 8:55 AM
 */
public class TestTinkerpopBug extends BaseTest {

    @Test(expected = IllegalStateException.class)
    public void hasNextCountBug() {
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> gt = this.sqlgGraph.V().has(T.label, "Person");
        assertTrue(gt.hasNext());
        assertEquals(3, gt.count().next().intValue());
    }

}
