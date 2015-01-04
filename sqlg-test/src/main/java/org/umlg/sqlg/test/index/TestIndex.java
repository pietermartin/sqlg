package org.umlg.sqlg.test.index;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgDataSource;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Date: 2014/08/17
 * Time: 2:43 PM
 */
public class TestIndex extends BaseTest {

    @Test
    public void testIndexOnInteger() {
        this.sqlgGraph.createVertexLabeledIndex("Person", "name1", "dummy", "age", 1);
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 5000; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "john" + i, "age", i);
        }
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testIndexOnVertex2() throws SQLException {
        this.sqlgGraph.createVertexLabeledIndex("Person", "name", "dummy");
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 5000; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "john" + i);
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.V().has(T.label, "Person").has("name", "john50").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.V().has(T.label, "Person").has("name", Compare.eq, "john50").count().next().intValue());

        //Check if the index is being used
        Connection conn = this.sqlgGraph.tx().getConnection();
        Statement statement = conn.createStatement();
        if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
            ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"V_Person\" a WHERE a.\"name\" = 'john50'");
            Assert.assertTrue(rs.next());
            String result = rs.getString(1);
            System.out.println(result);
            Assert.assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
            statement.close();
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testIndexOnVertex() throws SQLException {
        this.sqlgGraph.createVertexLabeledIndex("Person", "name1", "dummy", "name2", "dummy", "name3", "dummy");
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 5000; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name1", "john" + i, "name2", "tom" + i, "name3", "piet" + i);
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.V().has(T.label, "Person").has("name1", "john50").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.V().has(T.label, "Person").has("name1", Compare.eq, "john50").count().next().intValue());

        //Check if the index is being used
        Connection conn = this.sqlgGraph.tx().getConnection();
        Statement statement = conn.createStatement();
        if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
            ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"V_Person\" a WHERE a.\"name1\" = 'john50'");
            Assert.assertTrue(rs.next());
            String result = rs.getString(1);
            System.out.println(result);
            Assert.assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
            statement.close();
        }
        this.sqlgGraph.tx().rollback();
    }

//    @Test
    public void testIndexOnVertex1() throws SQLException {
        this.sqlgGraph.createVertexLabeledIndex("Person", "name1", "dummy", "name2", "dummy", "name3", "dummy");
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 5000; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name1", "john" + i, "name2", "tom" + i, "name3", "piet" + i);
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.V().has(T.label, "Person").has("name1", "john50").count().next(), 0);
        Connection conn = SqlgDataSource.INSTANCE.get(this.sqlgGraph.getJdbcUrl()).getConnection();
        Statement statement = conn.createStatement();
        if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
            ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"V_Person\" a WHERE a.\"name1\" = 'john50'");
            Assert.assertTrue(rs.next());
            String result = rs.getString(1);
            System.out.println(result);
            Assert.assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
            statement.close();
            conn.close();
        }
    }

    @Test
    public void testIndexOnEdge() throws SQLException {
        this.sqlgGraph.createEdgeLabeledIndex("Schema0.edge", "name1", "dummy", "name2", "dummy", "name3", "dummy");
        this.sqlgGraph.tx().commit();
        Vertex previous = null;
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 1000; j++) {
                Vertex v = this.sqlgGraph.addVertex(T.label, "Schema" + i + ".Person", "name1", "n" + j, "name2", "n" + j);
                if (previous != null) {
                    previous.addEdge("edge", v, "name1", "n" + j, "name2", "n" + j);
                }
                previous = v;
            }
            this.sqlgGraph.tx().commit();
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.E().has(T.label, "Schema0.edge").has("name1", "n500").count().next(), 0);
        if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
            Connection conn = SqlgDataSource.INSTANCE.get(this.sqlgGraph.getJdbcUrl()).getConnection();
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"Schema0\".\"E_edge\" a WHERE a.\"name1\" = 'n50'");
            Assert.assertTrue(rs.next());
            String result = rs.getString(1);
            System.out.println(result);
            Assert.assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
            statement.close();
            conn.close();
        }
    }

    @Test
    public void testIndexExist() {
        this.sqlgGraph.createVertexLabeledIndex("Person", "name", "a");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.createVertexLabeledIndex("Person", "name", "a");
        this.sqlgGraph.createVertexLabeledIndex("Person", "name", "a");
        this.sqlgGraph.tx().commit();
    }

}
