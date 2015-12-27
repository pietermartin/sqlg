package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2015/12/27
 * Time: 8:21 AM
 */
public class TestBatchEdge extends BaseTest {

    @Test
    public void testBatchEdgeDoesNotLooseProperties() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex personA = this.sqlgGraph.addVertex(T.label, "Person", "name", "A");
        Vertex personB = this.sqlgGraph.addVertex(T.label, "Person", "name", "B");
        Edge e = personA.addEdge("loves", personB);
        e.property("from", "nowish");
        this.sqlgGraph.tx().commit();
    }
}
