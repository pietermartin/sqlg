package org.umlg.sqlg.test.edges;

import org.apache.tinkerpop.gremlin.process.traversal.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/09/16
 * Time: 8:22 PM
 */
public class TestEdgeSchemaCreation extends BaseTest {

    @Test
    public void testEdgeSchemaCreation() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "b");
        v1.addEdge("label1", v2);
        this.sqlgGraph.tx().commit();
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Product", "name", "c");
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Product", "name", "d");
        v1.addEdge("label1", v3);
        v1.addEdge("label1", v4);
        this.sqlgGraph.tx().commit();

    }
}
