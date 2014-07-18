package org.umlg.sqlgraph.test;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlgraph.structure.SqlGraphDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Date: 2014/07/12
 * Time: 2:26 PM
 */
public class TestVertexCreation extends BaseTest {

    @Test
    public void testCreateEmptyVertex() throws SQLException {
        sqlGraph.addVertex();
        sqlGraph.tx().commit();
        try (Connection conn = SqlGraphDataSource.INSTANCE.get(this.sqlGraph.getJdbcUrl()).getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                StringBuilder sql = new StringBuilder("SELECT * FROM ");
                sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("vertex"));
                sql.append(";");
                ResultSet rs = stmt.executeQuery(sql.toString());
                int countRows = 0;
                while (rs.next()) {
                    countRows++;
                }
                assertEquals(1, countRows);
                rs.close();
            }
        }
    }

    @Test
    public void testCreateVertexWithProperties() throws SQLException {
        sqlGraph.addVertex(Element.LABEL, "Person",
                "boolean1", true,
                "byte1", (byte) 1,
                "short1", (short) 1,
                "integer1", 1,
                "long1", 1L,
                "float1", 1F,
                "double1", 1D,
                "name", "marko"
        );
        sqlGraph.addVertex(Element.LABEL, "Person",
                "boolean1", true,
                "byte1", (byte) 1,
                "short1", (short) 1,
                "integer1", 1,
                "long1", 1L,
                "float1", 1F,
                "double1", 1D,
                "name", "marko"
        );
        sqlGraph.tx().commit();

        try (Connection conn = SqlGraphDataSource.INSTANCE.get(this.sqlGraph.getJdbcUrl()).getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                StringBuilder sql = new StringBuilder("SELECT * FROM ");
                sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes("Person"));
                sql.append(";");
                ResultSet rs = stmt.executeQuery(sql.toString());
                int countRows = 0;
                boolean boolean1 = false;
                byte byte1 = (byte) -1;
                short short1 = (short) -1;
                int integer1 = -1;
                long long1 = -1L;
                float float1 = -1F;
                double double1 = -1D;
                String name = "";
                while (rs.next()) {
                    boolean1 = rs.getBoolean("boolean1");
                    byte1 = rs.getByte("byte1");
                    short1 = rs.getShort("short1");
                    integer1 = rs.getInt("integer1");
                    long1 = rs.getLong("long1");
                    float1 = rs.getFloat("float1");
                    double1 = rs.getDouble("double1");
                    name = rs.getString("name");
                    countRows++;
                }
                assertEquals(2, countRows);
                assertEquals(boolean1, true);
                assertEquals(byte1, (byte) 1);
                assertEquals(short1, (short) 1);
                assertEquals(integer1, 1);
                assertEquals(long1, 1L);
                assertEquals(float1, 1F, 0);
                assertEquals(double1, 1D, 0);
                assertEquals("marko", name);
                rs.close();
            }
        }
    }

    @Test
    public void testAndColumns() {
        Vertex v1 = this.sqlGraph.addVertex(Element.LABEL, "Person", "name1", "marko");
        this.sqlGraph.tx().commit();
        assertEquals(1, this.sqlGraph.V().count().next(), 0);
        assertEquals(v1, this.sqlGraph.v(v1.id()));
        assertEquals(1, v1.properties().size());
        Vertex v2 = this.sqlGraph.addVertex(Element.LABEL, "Person", "name2", "john");
        assertEquals(2, this.sqlGraph.V().count().next(), 0);
        assertEquals(v2, this.sqlGraph.v(v2.id()));
        assertEquals(1, v2.properties().size());
    }

}
