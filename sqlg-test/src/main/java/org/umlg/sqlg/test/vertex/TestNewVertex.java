package org.umlg.sqlg.test.vertex;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/10/04
 * Time: 10:17 AM
 */
public class TestNewVertex extends BaseTest {

    @Test
    public void testNewVertexDoesNotQueryLabels() {
        Vertex v1 = this.sqlG.addVertex(T.label, "Person", "name", "john1");
        Vertex v2 = this.sqlG.addVertex(T.label, "Person", "name", "john2");
        v1.addEdge("friend", v2, "weight", 1);
        this.sqlG.tx().commit();

    }
}
