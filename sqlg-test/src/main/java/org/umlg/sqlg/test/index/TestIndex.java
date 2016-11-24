package org.umlg.sqlg.test.index;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.structure.Index;
import org.umlg.sqlg.structure.PropertyColumn;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Date: 2014/08/17
 * Time: 2:43 PM
 */
public class TestIndex extends BaseTest {

    @Test
    public void testIndexViaTopology() {
        for (int i = 0; i < 5000; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        }
        this.sqlgGraph.tx().commit();
        Optional<PropertyColumn> propertyOptional = this.sqlgGraph.getTopology().getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "Person").get().getProperty("name");
        assertTrue(propertyOptional.isPresent());
        assertTrue(propertyOptional.get().getIndex()==Index.NONE);
        propertyOptional.get().ensureIndexExist(this.sqlgGraph, Index.NON_UNIQUE);
        this.sqlgGraph.tx().rollback();
        propertyOptional = this.sqlgGraph.getTopology().getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "Person").get().getProperty("name");
        assertTrue(propertyOptional.isPresent());
        assertTrue(propertyOptional.get().getIndex()==Index.NONE);
        propertyOptional.get().ensureIndexExist(this.sqlgGraph, Index.NON_UNIQUE);
        this.sqlgGraph.tx().commit();
        assertTrue(propertyOptional.get().getIndex()==Index.NON_UNIQUE);
        //Check if the index is being used
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
                ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"V_Person\" a WHERE a.\"name\" = 'john'");
                assertTrue(rs.next());
                String result = rs.getString(1);
                System.out.println(result);
                assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
            }
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testUniqueIndexViaTopolgy() {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        this.sqlgGraph.tx().commit();
        Optional<PropertyColumn> propertyOptional = this.sqlgGraph.getTopology().getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "Person").get().getProperty("name");
        assertTrue(propertyOptional.isPresent());
        propertyOptional.get().ensureIndexExist(this.sqlgGraph, Index.UNIQUE);
        this.sqlgGraph.tx().commit();
        try {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
            fail("Unique index did not work.");
        } catch (RuntimeException e) {

        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testIndexOnInteger() {
        this.sqlgGraph.createVertexLabeledIndex("Person", "name", "dummy", "age", 1);
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 5000; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "john" + i, "age", i);
        }
        this.sqlgGraph.tx().commit();
        //Check if the index is being used
        Connection conn = this.sqlgGraph.tx().getConnection();
        try (Statement statement = conn.createStatement()) {
            if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
                ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"V_Person\" a WHERE a.\"name\" = 'john50'");
                assertTrue(rs.next());
                String result = rs.getString(1);
                System.out.println(result);
                assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
                statement.close();
            }
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testIndexOnVertex2() throws SQLException {
        this.sqlgGraph.createVertexLabeledIndex("Person", "name", "dummy");
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 5000; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "john" + i);
        }
        this.sqlgGraph.tx().commit();
        assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "Person").has("name", "john50").count().next(), 0);
        assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "Person").has("name", P.eq("john50")).count().next().intValue());

        //Check if the index is being used
        Connection conn = this.sqlgGraph.tx().getConnection();
        Statement statement = conn.createStatement();
        if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
            ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"V_Person\" a WHERE a.\"name\" = 'john50'");
            assertTrue(rs.next());
            String result = rs.getString(1);
            System.out.println(result);
            assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
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
        assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "Person").has("name1", "john50").count().next(), 0);
        assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "Person").has("name1", P.eq("john50")).count().next().intValue());

        //Check if the index is being used
        Connection conn = this.sqlgGraph.tx().getConnection();
        Statement statement = conn.createStatement();
        if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
            ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"V_Person\" a WHERE a.\"name1\" = 'john50'");
            assertTrue(rs.next());
            String result = rs.getString(1);
            System.out.println(result);
            assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
            statement.close();
        }
        this.sqlgGraph.tx().rollback();
    }

    @Test
    public void testIndexOnVertex1() throws SQLException {
        //This is for postgres only
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres"));
        this.sqlgGraph.createVertexLabeledIndex("Person", "name1", "dummy", "name2", "dummy", "name3", "dummy");
        this.sqlgGraph.tx().commit();
        for (int i = 0; i < 5000; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name1", "john" + i, "name2", "tom" + i, "name3", "piet" + i);
        }
        this.sqlgGraph.tx().commit();
        assertEquals(1, this.sqlgGraph.traversal().V().has(T.label, "Person").has("name1", "john50").count().next(), 0);
        Connection conn = this.sqlgGraph.getSqlgDataSource().get(this.sqlgGraph.getJdbcUrl()).getConnection();
        Statement statement = conn.createStatement();
        if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
            ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"V_Person\" a WHERE a.\"name1\" = 'john50'");
            assertTrue(rs.next());
            String result = rs.getString(1);
            System.out.println(result);
            assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
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

    @Test
    public void testIndexOnEdge() throws SQLException {
        Map<String, PropertyType> columns = new HashMap<>();
        columns.put("name", PropertyType.STRING);

        String publicSchema = this.sqlgGraph.getSqlDialect().getPublicSchema();
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Person", columns);
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Address", columns);
        this.sqlgGraph.getTopology().ensureEdgeLabelExist("person_address", SchemaTable.of(publicSchema, "Person"), SchemaTable.of(publicSchema, "Address"), columns);
        this.sqlgGraph.getTopology().getEdgeLabel(publicSchema, "person_address").get().getProperty("name").get().ensureIndexExist(this.sqlgGraph, Index.UNIQUE);
        this.sqlgGraph.tx().commit();

        for (int i = 0; i < 5000; i++) {
            Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
            Vertex address1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
            person1.addEdge("person_address", address1, "name", "address" + i);
        }
        this.sqlgGraph.tx().commit();

        if (this.sqlgGraph.getSqlDialect().getClass().getSimpleName().contains("Postgres")) {
            Connection conn = this.sqlgGraph.getSqlgDataSource().get(this.sqlgGraph.getJdbcUrl()).getConnection();
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery("explain analyze SELECT * FROM \"public\".\"E_person_address\" a WHERE a.\"name\" = 'address1001'");
            assertTrue(rs.next());
            String result = rs.getString(1);
            System.out.println(result);
            assertTrue(result.contains("Index Scan") || result.contains("Bitmap Heap Scan"));
            statement.close();
            conn.close();
        }
    }

    @Test
    public void testIndexOnEdgeAcrossSchemas() {
        Vertex aa1 = this.sqlgGraph.addVertex(T.label, "A.A", "name", "aa");
        Vertex bb1 = this.sqlgGraph.addVertex(T.label, "B.B", "name", "bb");
        Vertex cc1 = this.sqlgGraph.addVertex(T.label, "C.C", "name", "cc");
        aa1.addEdge("test", bb1, "name", "ola");
        bb1.addEdge("test", cc1, "name", "ola");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.getTopology().getEdgeLabel("A", "test").get().getProperty("name").ifPresent(p -> p.ensureIndexExist(this.sqlgGraph, Index.UNIQUE));
        this.sqlgGraph.tx().commit();

        assertEquals(Index.UNIQUE, this.sqlgGraph.getTopology().getEdgeLabel("A", "test").get().getProperty("name").get().getIndex());
        assertEquals(Index.NONE, this.sqlgGraph.getTopology().getEdgeLabel("B", "test").get().getProperty("name").get().getIndex());

        try {
            aa1.addEdge("test", bb1, "name", "ola");
            fail("Unique constraint should prevent this from happening");
        } catch (Exception e) {
            //swallow
        }
        this.sqlgGraph.tx().rollback();

        //this one is ok, no unique constraint
        bb1.addEdge("test", cc1, "name", "ola");
        this.sqlgGraph.tx().commit();
    }

}
