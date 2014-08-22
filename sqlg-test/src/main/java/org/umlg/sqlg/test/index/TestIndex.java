package org.umlg.sqlg.test.index;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.lang.time.StopWatch;
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
    public void testIndexOnVertex() throws SQLException {
        this.sqlG.createLabeledIndex("Person", "name1", "dummy", "name2", "dummy", "name3", "dummy");
        this.sqlG.tx().commit();
        for (int i = 0; i < 5000; i++) {
            this.sqlG.addVertex(Element.LABEL, "Person", "name1", "john" + i, "name2", "tom" + i, "name3", "piet" + i);
        }
        this.sqlG.tx().commit();
        Assert.assertEquals(1, this.sqlG.V().has(Element.LABEL, "Person").has("name1", "john50").count().next(), 0);
        Connection conn = SqlgDataSource.INSTANCE.get(this.sqlG.getJdbcUrl()).getConnection();
        Statement statement = conn.createStatement();
        if (this.sqlG.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
            ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"V_Person\" a WHERE a.\"name1\" = 'john50'");
            Assert.assertTrue(rs.next());
            String result = rs.getString(1);
            System.out.println(result);
            Assert.assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
            statement.close();
            conn.close();
        }
    }

//    @Test
//    public void testIndexOnVertex1() throws SQLException {
//        this.sqlG.createLabeledIndex("Person", "name1", "dummy", "name2", "dummy", "name3", "dummy");
//        this.sqlG.tx().commit();
//        for (int i = 0; i < 5000; i++) {
//            this.sqlG.addVertex(Element.LABEL, "Person", "name1", "john" + i, "name2", "tom" + i, "name3", "piet" + i);
//        }
//        this.sqlG.tx().commit();
//        Assert.assertEquals(1, this.sqlG.V().has(Element.LABEL, "Person").has("name1", "john50").count().next(), 0);
//        Connection conn = SqlgDataSource.INSTANCE.get(this.sqlG.getJdbcUrl()).getConnection();
//        Statement statement = conn.createStatement();
//        if (this.sqlG.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
//            ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"V_Person\" a WHERE a.\"name1\" = 'john50'");
//            Assert.assertTrue(rs.next());
//            String result = rs.getString(1);
//            System.out.println(result);
//            Assert.assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
//            statement.close();
//            conn.close();
//        }
//    }
//
//    @Test
//    public void testIndexOnEdge() throws SQLException {
//        this.sqlG.createLabeledIndex("Schema0.edge", "name1", "dummy", "name2", "dummy", "name3", "dummy");
//        this.sqlG.tx().commit();
//        Vertex previous = null;
//        for (int i = 0; i < 5; i++) {
//            for (int j = 0; j < 1000; j++) {
//                Vertex v = this.sqlG.addVertex(Element.LABEL, "Schema" + i + ".Person", "name1", "n" + j, "name2", "n" + j);
//                if (previous != null) {
//                    previous.addEdge("edge", v, "name1", "n" + j, "name2", "n" + j);
//                }
//                previous = v;
//            }
//            this.sqlG.tx().commit();
//        }
//        this.sqlG.tx().commit();
//        Assert.assertEquals(1, this.sqlG.E().has(Element.LABEL, "Schema0.edge").has("name1", "n500").count().next(), 0);
//        if (this.sqlG.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
//            Connection conn = SqlgDataSource.INSTANCE.get(this.sqlG.getJdbcUrl()).getConnection();
//            Statement statement = conn.createStatement();
//            ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"Schema0\".\"E_edge\" a WHERE a.\"name1\" = 'n50'");
//            Assert.assertTrue(rs.next());
//            String result = rs.getString(1);
//            System.out.println(result);
//            Assert.assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
//            statement.close();
//            conn.close();
//        }
//    }
//
//    @Test
//    public void testIndexExist() {
//        this.sqlG.createLabeledIndex("Person", "name", "a");
//        this.sqlG.tx().commit();
//        this.sqlG.createLabeledIndex("Person", "name", "a");
//        this.sqlG.createLabeledIndex("Person", "name", "a");
//        this.sqlG.tx().commit();
//    }

}
