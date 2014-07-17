package org.umlg.sqlgraph.test;

import com.tinkerpop.gremlin.structure.Transaction;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.umlg.sqlgraph.sql.dialect.SqlGraphDialect;
import org.umlg.sqlgraph.structure.SqlGraphDataSource;
import org.umlg.sqlgraph.structure.SqlGraph;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;
import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Date: 2014/07/12
 * Time: 5:44 PM
 */
public abstract class BaseTest {

    protected SqlGraph sqlGraph;
    private static Configuration config;

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlgraph.properties");
        try {
            config = new PropertiesConfiguration(sqlProperties);
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void before() throws IOException {
        try {
            SqlGraphDataSource.INSTANCE.setupDataSource(config.getString("jdbc.driver"), config.getString("jdbc.url"));
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
        StringBuilder sql = new StringBuilder("DROP SCHEMA IF EXISTS PUBLIC CASCADE;");
        try (Connection conn = SqlGraphDataSource.INSTANCE.get(config.getString("jdbc.url")).getConnection()) {
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (!config.getString("jdbc.driver").equals(SqlGraphDialect.HSQLDB.getJdbcDriver())) {
            sql = new StringBuilder("CREATE SCHEMA IF NOT EXISTS PUBLIC;");
            // CREATE SCHEMA PUBLIC
            try (Connection conn = SqlGraphDataSource.INSTANCE.get(config.getString("jdbc.url")).getConnection()) {
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                    preparedStatement.executeUpdate();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        this.sqlGraph = SqlGraph.open(config);
    }

    @After
    public void after() throws Exception {
        this.sqlGraph.tx().onClose(Transaction.CLOSE_BEHAVIOR.COMMIT);
        this.sqlGraph.close();
    }

    protected void assertDb(String table, int numberOfRows) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = SqlGraphDataSource.INSTANCE.get(this.sqlGraph.getJdbcUrl()).getConnection();
            stmt = conn.createStatement();
            StringBuilder sql = new StringBuilder("SELECT * FROM \"");
            sql.append(table);
            sql.append("\";");
            ResultSet rs = stmt.executeQuery(sql.toString());
            int countRows = 0;

            while (rs.next()) {
                countRows++;
            }
            assertEquals(numberOfRows, countRows);
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
