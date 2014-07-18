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
import org.umlg.sqlgraph.structure.SqlUtil;

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
        SqlGraphDialect sqlGraphDialect = null;
        try {
            Class.forName(SqlGraphDialect.HSQLDB.getJdbcDriver());
            sqlGraphDialect = SqlGraphDialect.HSQLDB;
        } catch (ClassNotFoundException e) {
        }
        try {
            Class.forName(SqlGraphDialect.POSTGRES.getJdbcDriver());
            sqlGraphDialect = SqlGraphDialect.POSTGRES;
        } catch (ClassNotFoundException e) {
        }
        try {
            Class.forName(SqlGraphDialect.MARIADBDB.getJdbcDriver());
            sqlGraphDialect = SqlGraphDialect.MARIADBDB;
        } catch (ClassNotFoundException e) {
        }
        if (sqlGraphDialect == null) {
            throw new IllegalStateException("Postgres driver " + SqlGraphDialect.POSTGRES.getJdbcDriver() + " or Hsqldb driver " + SqlGraphDialect.HSQLDB.getJdbcDriver() + " must be on the classpath!");
        }
        try {
            SqlGraphDataSource.INSTANCE.setupDataSource(
                    sqlGraphDialect.getJdbcDriver(),
                    config.getString("jdbc.url"),
                    config.getString("jdbc.username"),
                    config.getString("jdbc.password"));
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
        try (Connection conn = SqlGraphDataSource.INSTANCE.get(config.getString("jdbc.url")).getConnection()) {
            DatabaseMetaData metadata = conn.getMetaData();
            String catalog = "sqlgraphdb";
            String schemaPattern = null;
            String tableNamePattern = "%";
            String[] types = {"TABLE"};
            ResultSet result = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
            while (result.next()) {
                StringBuilder sql = new StringBuilder("DROP TABLE ");
                sql.append(sqlGraphDialect.getSqlDialect().maybeWrapInQoutes(result.getString(3)));
                sql.append(" CASCADE;");
                try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                    preparedStatement.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
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
            StringBuilder sql = new StringBuilder("SELECT * FROM ");
            sql.append(this.sqlGraph.getSchemaManager().getSqlDialect().maybeWrapInQoutes(table));
            sql.append(";");
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
