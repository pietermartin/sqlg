package org.umlg.sqlgraph.h2database.test;

import com.tinkerpop.gremlin.structure.Transaction;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.umlg.sqlgraph.sql.impl.SqlGraphDataSource;
import org.umlg.sqlgraph.structure.SqlGraph;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Date: 2014/07/12
 * Time: 5:44 PM
 */
public abstract class BaseTest {

    protected static Path testDir = Paths.get("src/test/h2database");
    protected static final String JDBC_DRIVER = "org.h2.Driver";
    protected static final String DB_URL = "jdbc:h2:" + testDir.toAbsolutePath().toString() + "/test";
    protected SqlGraph sqlGraph;

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        Class.forName(JDBC_DRIVER);
    }

    @Before
    public void before() throws IOException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlgraph.properties");
        Configuration config;
        try {
            config = new PropertiesConfiguration(sqlProperties);
            SqlGraphDataSource.INSTANCE.setupDataSource(config.getString("jdbc.driver"), config.getString("jdbc.url"));
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
        StringBuilder sql = new StringBuilder("DROP ALL OBJECTS;");
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = SqlGraphDataSource.INSTANCE.get(config.getString("jdbc.url")).getConnection();
            preparedStatement = conn.prepareStatement(sql.toString());
            preparedStatement.executeUpdate();
            preparedStatement.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (preparedStatement != null)
                    preparedStatement.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException se2) {
            }
        }
        this.sqlGraph = new SqlGraph(config);
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
