package org.umlg.sqlg.test;

import junit.framework.Assert;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;

/**
 * Date: 2014/07/19
 * Time: 11:20 AM
 */
public class JDBC {

    /**
     * Helper class whose <code>equals()</code> method returns
     * <code>true</code> for all strings on this format: SQL061021105830900
     */
    public static class GeneratedId {
        public boolean equals(Object o) {
            // unless JSR169, use String.matches...
            if (JDBC.vmSupportsJDBC3()) {
                return o instanceof String &&
                        ((String) o).matches("SQL[0-9]{15}");
            } else {
                String tmpstr = (String) o;
                boolean b = true;
                if (!(o instanceof String))
                    b = false;
                if (!(tmpstr.startsWith("SQL")))
                    b = false;
                if (tmpstr.length() != 18)
                    b = false;
                for (int i = 3; i < 18; i++) {
                    if (Character.isDigit(tmpstr.charAt(i)))
                        continue;
                    else {
                        b = false;
                        break;
                    }
                }
                return b;
            }
        }

        public String toString() {
            return "xxxxGENERATED-IDxxxx";
        }
    }

    /**
     * Constant to pass to DatabaseMetaData.getTables() to fetch
     * just tables.
     */
    public static final String[] GET_TABLES_TABLE = new String[]{"TABLE"};
    /**
     * Constant to pass to DatabaseMetaData.getTables() to fetch
     * just views.
     */
    public static final String[] GET_TABLES_VIEW = new String[]{"VIEW"};
    /**
     * Constant to pass to DatabaseMetaData.getTables() to fetch
     * just synonyms.
     */
    public static final String[] GET_TABLES_SYNONYM =
            new String[]{"SYNONYM"};

    /**
     * Types.SQLXML value without having to compile with JDBC4.
     */
    public static final int SQLXML = 2009;

    /**
     * Tell if we are allowed to use DriverManager to create database
     * connections.
     */
    private static final boolean HAVE_DRIVER
            = haveClass("java.sql.Driver");

    /**
     * Does the Savepoint class exist, indicates
     * JDBC 3 (or JSR 169).
     */
    private static final boolean HAVE_SAVEPOINT
            = haveClass("java.sql.Savepoint");

    /**
     * Does the java.sql.SQLXML class exist, indicates JDBC 4.
     */
    private static final boolean HAVE_SQLXML
            = haveClass("java.sql.SQLXML");

    /**
     * Can we load a specific class, use this to determine JDBC level.
     *
     * @param className Class to attempt load on.
     * @return true if class can be loaded, false otherwise.
     */
    static boolean haveClass(String className) {
        try {
            Class.forName(className);
            return true;
        } catch (Throwable e) {
            return false;
        }
    }

    /**
     * Return true if the virtual machine environment
     * supports JDBC4 or later. JDBC 4 is a superset
     * of JDBC 3 and of JSR169.
     * <BR>
     * This method returns true in a JDBC 4 environment
     * and false in a JDBC 3 or JSR 169 environment.
     */
    public static boolean vmSupportsJDBC4() {
        return HAVE_DRIVER
                && HAVE_SQLXML;
    }

    /**
     * Return true if the virtual machine environment
     * supports JDBC3 or later. JDBC 3 is a super-set of JSR169
     * and a subset of JDBC 4.
     * <BR>
     * This method will return true in a JDBC 3 or JDBC 4
     * environment, but false in a JSR169 environment.
     */
    public static boolean vmSupportsJDBC3() {
        return HAVE_DRIVER
                && HAVE_SAVEPOINT;
    }

    /**
     * Return true if the virtual machine environment
     * supports JSR169. JSR169 is a subset of JDBC 3
     * and hence a subset of JDBC 4 as well.
     * <BR>
     * This method returns true only in a JSR 169
     * environment.
     */
    public static boolean vmSupportsJSR169() {
        return !HAVE_DRIVER
                && HAVE_SAVEPOINT;
    }

    /**
     * Rollback and close a connection for cleanup.
     * Test code that is expecting Connection.close to succeed
     * normally should just call conn.close().
     * <p>
     * <p>
     * If conn is not-null and isClosed() returns false
     * then both rollback and close will be called.
     * If both methods throw exceptions
     * then they will be chained together and thrown.
     *
     * @throws java.sql.SQLException Error closing connection.
     */
    public static void cleanup(Connection conn) throws SQLException {
        if (conn == null)
            return;
        if (conn.isClosed())
            return;

        SQLException sqle = null;
        try {
            conn.rollback();
        } catch (SQLException e) {
            sqle = e;
        }

        try {
            conn.close();
        } catch (SQLException e) {
            if (sqle == null)
                sqle = e;
            else
                sqle.setNextException(e);
            throw sqle;
        }
    }

    /**
     * Drop a database schema by dropping all objects in it
     * and then executing DROP SCHEMA. If the schema is
     * APP it is cleaned but DROP SCHEMA is not executed.
     * <p>
     * TODO: Handle dependencies by looping in some intelligent
     * way until everything can be dropped.
     *
     * @param dmd    DatabaseMetaData object for database
     * @param schema Name of the schema
     * @throws java.sql.SQLException database error
     */
    public static void dropSchema(DatabaseMetaData dmd, String schema) throws SQLException {
        Connection conn = dmd.getConnection();
        Assert.assertFalse(conn.getAutoCommit());
        Statement s = dmd.getConnection().createStatement();

        // Functions - not supported by JDBC meta data until JDBC 4
        // Need to use the CHAR() function on A.ALIASTYPE
        // so that the compare will work in any schema.
        PreparedStatement psf = conn.prepareStatement(
                "SELECT ALIAS FROM SYS.SYSALIASES A, SYS.SYSSCHEMAS S" +
                        " WHERE A.SCHEMAID = S.SCHEMAID " +
                        " AND CHAR(A.ALIASTYPE) = ? " +
                        " AND S.SCHEMANAME = ?");
        psf.setString(1, "F");
        psf.setString(2, schema);
        ResultSet rs = psf.executeQuery();
        dropUsingDMD(s, rs, schema, "ALIAS", "FUNCTION");

        // Procedures
        rs = dmd.getProcedures((String) null,
                schema, (String) null);

        dropUsingDMD(s, rs, schema, "PROCEDURE_NAME", "PROCEDURE");

        // Views
        rs = dmd.getTables((String) null, schema, (String) null,
                GET_TABLES_VIEW);

        dropUsingDMD(s, rs, schema, "TABLE_NAME", "VIEW");

        // Tables
        rs = dmd.getTables((String) null, schema, (String) null,
                GET_TABLES_TABLE);

        dropUsingDMD(s, rs, schema, "TABLE_NAME", "TABLE");

        // At this point there may be tables left due to
        // foreign key constraints leading to a dependency loop.
        // Drop any constraints that remain and then drop the tables.
        // If there are no tables then this should be a quick no-op.
        ResultSet table_rs = dmd.getTables((String) null, schema, (String) null,
                GET_TABLES_TABLE);

        while (table_rs.next()) {
            String tablename = table_rs.getString("TABLE_NAME");
            rs = dmd.getExportedKeys((String) null, schema, tablename);
            while (rs.next()) {
                short keyPosition = rs.getShort("KEY_SEQ");
                if (keyPosition != 1)
                    continue;
                String fkName = rs.getString("FK_NAME");
                // No name, probably can't happen but couldn't drop it anyway.
                if (fkName == null)
                    continue;
                String fkSchema = rs.getString("FKTABLE_SCHEM");
                String fkTable = rs.getString("FKTABLE_NAME");

                String ddl = "ALTER TABLE " +
                        JDBC.escape(fkSchema, fkTable) +
                        " DROP FOREIGN KEY " +
                        JDBC.escape(fkName);
                s.executeUpdate(ddl);
            }
            rs.close();
        }
        table_rs.close();
        conn.commit();

        // Tables (again)
        rs = dmd.getTables((String) null, schema, (String) null,
                GET_TABLES_TABLE);
        dropUsingDMD(s, rs, schema, "TABLE_NAME", "TABLE");

        // drop UDTs
        psf.setString(1, "A");
        psf.setString(2, schema);
        rs = psf.executeQuery();
        dropUsingDMD(s, rs, schema, "ALIAS", "TYPE");
        psf.close();

        // Synonyms - need work around for DERBY-1790 where
        // passing a table type of SYNONYM fails.
        rs = dmd.getTables((String) null, schema, (String) null,
                GET_TABLES_SYNONYM);

        dropUsingDMD(s, rs, schema, "TABLE_NAME", "SYNONYM");

        // sequences
        if (sysSequencesExists(conn)) {
            psf = conn.prepareStatement
                    (
                            "SELECT SEQUENCENAME FROM SYS.SYSSEQUENCES A, SYS.SYSSCHEMAS S" +
                                    " WHERE A.SCHEMAID = S.SCHEMAID " +
                                    " AND S.SCHEMANAME = ?");
            psf.setString(1, schema);
            rs = psf.executeQuery();
            dropUsingDMD(s, rs, schema, "SEQUENCENAME", "SEQUENCE");
            psf.close();
        }

        // Finally drop the schema if it is not APP
        if (!schema.equals("APP")) {
            s.executeUpdate("DROP SCHEMA " + JDBC.escape(schema) + " RESTRICT");
        }
        conn.commit();
        s.close();
    }

    /**
     * Return true if the SYSSEQUENCES table exists.
     */
    private static boolean sysSequencesExists(Connection conn) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement
                    (
                            "select count(*) from sys.systables t, sys.sysschemas s\n" +
                                    "where t.schemaid = s.schemaid\n" +
                                    "and ( cast(s.schemaname as varchar(128)))= 'SYS'\n" +
                                    "and ( cast(t.tablename as varchar(128))) = 'SYSSEQUENCES'");
            rs = ps.executeQuery();
            rs.next();
            return (rs.getInt(1) > 0);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
        }
    }

    /**
     * DROP a set of objects based upon a ResultSet from a
     * DatabaseMetaData call.
     * <p>
     * TODO: Handle errors to ensure all objects are dropped,
     * probably requires interaction with its caller.
     *
     * @param s        Statement object used to execute the DROP commands.
     * @param rs       DatabaseMetaData ResultSet
     * @param schema   Schema the objects are contained in
     * @param mdColumn The column name used to extract the object's
     *                 name from rs
     * @param dropType The keyword to use after DROP in the SQL statement
     * @throws java.sql.SQLException database errors.
     */
    private static void dropUsingDMD(
            Statement s, ResultSet rs, String schema,
            String mdColumn,
            String dropType) throws SQLException {
        String dropLeadIn = "DROP " + dropType + " ";

        // First collect the set of DROP SQL statements.
        ArrayList ddl = new ArrayList();
        while (rs.next()) {
            String objectName = rs.getString(mdColumn);
            String raw = dropLeadIn + JDBC.escape(schema, objectName);
            if ("TYPE".equals(dropType) || "SEQUENCE".equals(dropType)) {
                raw = raw + " restrict ";
            }
            ddl.add(raw);
        }
        rs.close();
        if (ddl.isEmpty())
            return;

        // Execute them as a complete batch, hoping they will all succeed.
        s.clearBatch();
        int batchCount = 0;
        for (Iterator i = ddl.iterator(); i.hasNext(); ) {
            Object sql = i.next();
            if (sql != null) {
                s.addBatch(sql.toString());
                batchCount++;
            }
        }

        int[] results;
        boolean hadError;
        try {
            results = s.executeBatch();
            Assert.assertNotNull(results);
            Assert.assertEquals("Incorrect result length from executeBatch",
                    batchCount, results.length);
            hadError = false;
        } catch (BatchUpdateException batchException) {
            results = batchException.getUpdateCounts();
            Assert.assertNotNull(results);
            Assert.assertTrue("Too many results in BatchUpdateException",
                    results.length <= batchCount);
            hadError = true;
        }

        // Remove any statements from the list that succeeded.
        boolean didDrop = false;
        for (int i = 0; i < results.length; i++) {
            int result = results[i];
            if (result == Statement.EXECUTE_FAILED)
                hadError = true;
            else if (result == Statement.SUCCESS_NO_INFO || result >= 0) {
                didDrop = true;
                ddl.set(i, null);
            } else
                Assert.fail("Negative executeBatch status");
        }
        s.clearBatch();
        if (didDrop) {
            // Commit any work we did do.
            s.getConnection().commit();
        }

        // If we had failures drop them as individual statements
        // until there are none left or none succeed. We need to
        // do this because the batch processing stops at the first
        // error. This copes with the simple case where there
        // are objects of the same type that depend on each other
        // and a different drop order will allow all or most
        // to be dropped.
        if (hadError) {
            do {
                hadError = false;
                didDrop = false;
                for (ListIterator i = ddl.listIterator(); i.hasNext(); ) {
                    Object sql = i.next();
                    if (sql != null) {
                        try {
                            s.executeUpdate(sql.toString());
                            i.set(null);
                            didDrop = true;
                        } catch (SQLException e) {
                            hadError = true;
                        }
                    }
                }
                if (didDrop)
                    s.getConnection().commit();
            } while (hadError && didDrop);
        }
    }

    /**
     * Does the work of assertDrainResults() as described
     * above.  If the received row count is non-negative,
     * this method also asserts that the number of rows
     * in the result set matches the received row count.
     * <p>
     * The ResultSet is closed by this method.
     *
     * @param rs           Result set to drain.
     * @param expectedRows If non-negative, indicates how
     *                     many rows we expected to see in the result set.
     * @return the number of rows seen.
     * @throws java.sql.SQLException
     */
    public static int assertDrainResults(ResultSet rs,
                                         int expectedRows) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();

        int rows = 0;
        while (rs.next()) {
            for (int col = 1; col <= rsmd.getColumnCount(); col++) {
                String s = rs.getString(col);
                Assert.assertEquals(s == null, rs.wasNull());
                if (rs.wasNull())
                    assertResultColumnNullable(rsmd, col);
            }
            rows++;
        }
        rs.close();

        if (expectedRows >= 0)
            Assert.assertEquals("Unexpected row count:", expectedRows, rows);

        return rows;
    }

    /**
     * Assert that a column is nullable in its ResultSetMetaData.
     * Used when a utility method checking the contents of a
     * ResultSet sees a NULL value. If the value is NULL then
     * the column's definition in ResultSetMetaData must allow NULLs
     * (or not disallow NULLS).
     *
     * @param rsmd Metadata of the ResultSet
     * @param col  Position of column just fetched that was NULL.
     * @throws java.sql.SQLException Error accessing meta data
     */
    private static void assertResultColumnNullable(ResultSetMetaData rsmd, int col)
            throws SQLException {
        Assert.assertFalse(rsmd.isNullable(col) == ResultSetMetaData.columnNoNulls);
    }

    /**
     * Escape a non-qualified name so that it is suitable
     * for use in a SQL query executed by JDBC.
     */
    public static String escape(String name) {
        StringBuffer buffer = new StringBuffer(name.length() + 2);
        buffer.append('"');
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            // escape double quote characters with an extra double quote
            if (c == '"') buffer.append('"');
            buffer.append(c);
        }
        buffer.append('"');
        return buffer.toString();
    }

    /**
     * Escape a schama-qualified name so that it is suitable
     * for use in a SQL query executed by JDBC.
     */
    public static String escape(String schema, String name) {
        return escape(schema) + "." + escape(name);
    }

}
