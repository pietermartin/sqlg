package org.umlg.sqlgraph.h2database.test;

import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlgraph.structure.SqlGraph;

/**
 * Date: 2014/07/12
 * Time: 5:44 PM
 */
public class TestEdgeCreation extends BaseTest {

    @Test
    public void testCreateEdge() {
        SqlGraph sqlGraph = new SqlGraph();
        Vertex v1 = sqlGraph.addVertex();
        Vertex v2 = sqlGraph.addVertex();
        v1.addEdge("label1", v2);
        sqlGraph.tx().commit();
    }
}
