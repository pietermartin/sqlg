package org.umlg.sqlg.test.edges;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Date: 2015/05/27
 * Time: 9:15 PM
 */
public class TestForeignKeysAreOptional extends BaseTest {

	
    @Test
    public void testForeignKeysOnPostgres() throws Exception {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres"));
        Configuration conf=getConfigurationClone();
        conf.setProperty("implement.foreign.keys", "true");
        try (SqlgGraph g = SqlgGraph.open(conf)) {
            Vertex v1 = g.addVertex(T.label, "Person");
            Vertex v2 = g.addVertex(T.label, "Person");
            v1.addEdge("Edge1", v2);
            g.tx().commit();
            Connection conn = g.tx().getConnection();
            DatabaseMetaData dm = conn.getMetaData();
            ResultSet rs = dm.getImportedKeys("sqlggraphdb", "public", "E_Edge1");
            Assert.assertTrue(rs.next());
        }
    }

    @Test
    public void testForeignKeysOnHsqldb() throws Exception {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Hsqldb"));
        Configuration conf=getConfigurationClone();
        conf.setProperty("implement.foreign.keys", "true");
        try (SqlgGraph g = SqlgGraph.open(conf)) {
            Vertex v1 = g.addVertex(T.label, "Person");
            Vertex v2 = g.addVertex(T.label, "Person");
            v1.addEdge("Edge1", v2);
            g.tx().commit();
            Connection conn = g.tx().getConnection();
            try (PreparedStatement preparedStatement = conn.prepareStatement("select * from information_schema.constraint_table_usage where CONSTRAINT_NAME like '%FK%'")) {
                ResultSet rs = preparedStatement.executeQuery();
                Assert.assertTrue(rs.next());
            }
        }
    }

    @Test
    public void testForeignKeysOffPostgres() throws Exception {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres"));
        Configuration conf=getConfigurationClone();
        conf.setProperty("implement.foreign.keys", "false");
        try (SqlgGraph g = SqlgGraph.open(conf)) {
            Vertex v1 = g.addVertex(T.label, "Person");
            Vertex v2 = g.addVertex(T.label, "Person");
            v1.addEdge("Edge1", v2);
            g.tx().commit();
            Connection conn = g.tx().getConnection();
            DatabaseMetaData dm = conn.getMetaData();
            ResultSet rs = dm.getImportedKeys("sqlgraphdb", "public", "E_Edge1");
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testForeignKeysOffHsqldb() throws Exception {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Hsqldb"));
        Configuration conf=getConfigurationClone();
        conf.setProperty("implement.foreign.keys", "false");
        try (SqlgGraph g = SqlgGraph.open(conf)) {
            Vertex v1 = g.addVertex(T.label, "Person");
            Vertex v2 = g.addVertex(T.label, "Person");
            v1.addEdge("Edge1", v2);
            g.tx().commit();
            Connection conn = g.tx().getConnection();
            try (PreparedStatement preparedStatement = conn.prepareStatement(
                    "select * from information_schema.constraint_table_usage " +
                            "where TABLE_NAME = 'E_Edge1' and CONSTRAINT_NAME like '%FK%'")) {
                ResultSet rs = preparedStatement.executeQuery();
                Assert.assertFalse(rs.next());
            }
        }
    }
}
