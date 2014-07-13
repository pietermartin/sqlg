package org.umlg.sqlgraph.h2database.test;

import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlgraph.sql.impl.SqlGraphDataSource;
import org.umlg.sqlgraph.structure.SqlGraph;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Date: 2014/07/12
 * Time: 5:44 PM
 */
public class TestEdgeCreation extends BaseTest {

    @Test
    public void testCreateEdge() throws Exception {
        Vertex v1 = sqlGraph.addVertex();
        Vertex v2 = sqlGraph.addVertex();
        v1.addEdge("label1", v2, "name", "marko");
        sqlGraph.tx().commit();
        assertDb("label1", 1);
        assertDb("vertex", 2);
    }

    @Test
    public void testCreateEdgeWithProperties() {
        Vertex v1 = sqlGraph.addVertex();
        Vertex v2 = sqlGraph.addVertex();
        v1.addEdge("label1", v2, "name", "marko");
        sqlGraph.tx().commit();
        assertDb("label1", 1);
        assertDb("vertex", 2);
    }
}
