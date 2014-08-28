package org.umlg.sqlg.test.mod;

import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.SchemaManager;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Date: 2014/07/12
 * Time: 5:44 PM
 */
public class TestEdgeCreation extends BaseTest {

    @Test
    public void testCreateEdge() throws Exception {
        Vertex v1 = sqlG.addVertex();
        Vertex v2 = sqlG.addVertex();
        v1.addEdge("label1", v2, "name", "marko");
        sqlG.tx().commit();
        assertDb(SchemaManager.EDGE_PREFIX +  "label1", 1);
        assertDb(SchemaManager.VERTEX_PREFIX + "vertex", 2);
    }

    @Test
    public void testCreateEdgeWithProperties() {
        Vertex v1 = sqlG.addVertex();
        Vertex v2 = sqlG.addVertex();
        v1.addEdge("label1", v2, "name", "marko");
        sqlG.tx().commit();
        assertDb(SchemaManager.EDGE_PREFIX  + "label1", 1);
        assertDb(SchemaManager.VERTEX_PREFIX + "vertex", 2);
    }
}
