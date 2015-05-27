package org.umlg.sqlg.test.edges;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

/**
 * Date: 2015/05/27
 * Time: 9:15 PM
 */
public class TestForeignKeysAreOptional extends BaseTest {

    @Test
    public void testForeignKeysOn() throws Exception {
        Configuration conf = new PropertiesConfiguration();
        conf.setProperty("jdbc.url", "jdbc:postgresql://localhost:5432/sqlgraphdb");
        conf.setProperty("jdbc.username", "postgres");
        conf.setProperty("jdbc.password", "postgres");
        conf.setProperty("implement.foreign.keys", "true");
        SqlgGraph g = SqlgGraph.open(conf);
        Vertex v1 = g.addVertex(T.label, "Person");
        Vertex v2 = g.addVertex(T.label, "Person");
        v1.addEdge("Edge1", v2);
        g.tx().commit();
        Connection conn = g.tx().getConnection();
        DatabaseMetaData dm = conn.getMetaData();
        ResultSet rs = dm.getImportedKeys("sqlggraphdb", "public", "E_Edge1");
        Assert.assertTrue(rs.next());
        g.close();
    }

    @Test
    public void testForeignKeysOff() throws Exception {
        Configuration conf = new PropertiesConfiguration();
        conf.setProperty("jdbc.url", "jdbc:postgresql://localhost:5432/sqlgraphdb");
        conf.setProperty("jdbc.username", "postgres");
        conf.setProperty("jdbc.password", "postgres");
        conf.setProperty("implement.foreign.keys", "false");
        SqlgGraph g = SqlgGraph.open(conf);
        Vertex v1 = g.addVertex(T.label, "Person");
        Vertex v2 = g.addVertex(T.label, "Person");
        v1.addEdge("Edge1", v2);
        g.tx().commit();
        Connection conn = g.tx().getConnection();
        DatabaseMetaData dm = conn.getMetaData();
        ResultSet rs = dm.getImportedKeys("sqlggraphdb", "public", "E_Edge1");
        Assert.assertFalse(rs.next());
        g.close();
    }
}
