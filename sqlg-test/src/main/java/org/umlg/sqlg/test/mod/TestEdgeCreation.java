package org.umlg.sqlg.test.mod;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.topology.Topology;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/07/12
 * Time: 5:44 PM
 */
public class TestEdgeCreation extends BaseTest {

    @Test
    public void testCreateEdge() {
        Vertex v1 = sqlgGraph.addVertex();
        Vertex v2 = sqlgGraph.addVertex();
        v1.addEdge("label1", v2, "name", "marko");
        sqlgGraph.tx().commit();
        assertDb(Topology.EDGE_PREFIX +  "label1", 1);
        assertDb(Topology.VERTEX_PREFIX + "vertex", 2);
    }

    @Test
    public void testCreateEdgeWithProperties() {
        Vertex v1 = sqlgGraph.addVertex();
        Vertex v2 = sqlgGraph.addVertex();
        v1.addEdge("label1", v2, "name", "marko");
        sqlgGraph.tx().commit();
        assertDb(Topology.EDGE_PREFIX  + "label1", 1);
        assertDb(Topology.VERTEX_PREFIX + "vertex", 2);
    }
}
