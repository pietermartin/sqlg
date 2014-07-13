package org.umlg.sqlgraph.h2database.test;

import org.junit.Test;
import org.umlg.sqlgraph.sql.impl.SqlGraphDataSource;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Date: 2014/07/12
 * Time: 6:06 AM
 */
public class TestH2Database extends BaseTest {

    @Test
    public void testH2Database() {
        Connection conn = null;
        try {
            conn = SqlGraphDataSource.INSTANCE.get(DB_URL).getConnection();
            conn.close();
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                fail(se.getMessage());
            }
        }
    }

    @Test
    public void testCreateTable() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = SqlGraphDataSource.INSTANCE.get(DB_URL).getConnection();
            stmt = conn.createStatement();

            StringBuilder sql = new StringBuilder("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
            sql.append("INSERT INTO TEST VALUES(1, 'Hello');");
            stmt.execute(sql.toString());

            stmt.close();
            conn.close();

            conn = DriverManager.getConnection(DB_URL);
            stmt = conn.createStatement();

            sql.setLength(0);
            sql.append("SELECT * FROM TEST;");

            int id = -1;
            String name = null;
            ResultSet rs = stmt.executeQuery(sql.toString());
            while(rs.next()){
                id  = rs.getInt("ID");
                name = rs.getString("NAME");
            }
            assertEquals(1, id);
            assertEquals("Hello", name);
            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                fail(se.getMessage());
            }
        }
    }
}
