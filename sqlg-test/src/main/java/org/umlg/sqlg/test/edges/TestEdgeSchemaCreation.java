package org.umlg.sqlg.test.edges;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/09/16
 * Time: 8:22 PM
 */
public class TestEdgeSchemaCreation extends BaseTest {

    @Test
    public void testEdgeSchemaCreation() {
        Vertex v1 = this.sqlG.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlG.addVertex(T.label, "Person", "name", "b");
        v1.addEdge("label1", v2);
        this.sqlG.tx().commit();
        Vertex v3 = this.sqlG.addVertex(T.label, "Product", "name", "c");
        Vertex v4 = this.sqlG.addVertex(T.label, "Product", "name", "d");
        v1.addEdge("label1", v3);
        v1.addEdge("label1", v4);
        this.sqlG.tx().commit();

    }
}
