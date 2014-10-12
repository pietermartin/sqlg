package org.umlg.sqlg.test;

import com.tinkerpop.gremlin.structure.Transaction;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.SqlgDataSource;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Date: 2014/07/12
 * Time: 5:44 PM
 */
public abstract class BaseTest {

    private Logger logger = LoggerFactory.getLogger(BaseTest.class.getName());
    protected SqlgGraph sqlgGraph;
    protected static Configuration configuration;

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);

            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void before() throws IOException {
        SqlDialect sqlDialect;
        try {
            Class<?> sqlDialectClass = findSqlGDialect();
            Constructor<?> constructor = sqlDialectClass.getConstructor(Configuration.class);
            sqlDialect = (SqlDialect) constructor.newInstance(configuration);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            SqlgDataSource.INSTANCE.setupDataSource(
                    sqlDialect.getJdbcDriver(),
                    configuration.getString("jdbc.url"),
                    configuration.getString("jdbc.username"),
                    configuration.getString("jdbc.password"));
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
        Connection conn;
        try {
            conn = SqlgDataSource.INSTANCE.get(configuration.getString("jdbc.url")).getConnection();
            DatabaseMetaData metadata = conn.getMetaData();
            if (sqlDialect.supportsCascade()) {
                String catalog = null;
                String schemaPattern = null;
                String tableNamePattern = "%";
                String[] types = {"TABLE"};
                ResultSet result = metadata.getTables(catalog, schemaPattern, tableNamePattern, types);
                while (result.next()) {
                    StringBuilder sql = new StringBuilder("DROP TABLE ");
                    sql.append(sqlDialect.maybeWrapInQoutes(result.getString(2)));
                    sql.append(".");
                    sql.append(sqlDialect.maybeWrapInQoutes(result.getString(3)));
                    sql.append(" CASCADE");
                    if (sqlDialect.needsSemicolon()) {
                        sql.append(";");
                    }
                    try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                        preparedStatement.executeUpdate();
                    }
                }
                catalog = null;
                schemaPattern = null;
                result = metadata.getSchemas(catalog, schemaPattern);
                while (result.next()) {
                    String schema = result.getString(1);
                    if (!sqlDialect.getDefaultSchemas().contains(schema)) {
                        StringBuilder sql = new StringBuilder("DROP SCHEMA ");
                        sql.append(sqlDialect.maybeWrapInQoutes(schema));
                        sql.append(" CASCADE");
                        if (sqlDialect.needsSemicolon()) {
                            sql.append(";");
                        }
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                            preparedStatement.executeUpdate();
                        }
                    }
                }
            } else if (!sqlDialect.supportSchemas()) {
                ResultSet result = metadata.getCatalogs();
                while (result.next()) {
                    StringBuilder sql = new StringBuilder("DROP DATABASE ");
                    String database = result.getString(1);
                    if (!sqlDialect.getDefaultSchemas().contains(database)) {
                        sql.append(sqlDialect.maybeWrapInQoutes(database));
                        if (sqlDialect.needsSemicolon()) {
                            sql.append(";");
                        }
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql.toString())) {
                            preparedStatement.executeUpdate();
                        }
                    }
                }
            } else {
                conn.setAutoCommit(false);
                JDBC.dropSchema(metadata, "APP");
                conn.commit();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        this.sqlgGraph = SqlgGraph.open(configuration);
    }

    @After
    public void after() throws Exception {
        this.sqlgGraph.tx().onClose(Transaction.CLOSE_BEHAVIOR.COMMIT);
        this.sqlgGraph.close();
    }

    protected void assertDb(String table, int numberOfRows) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = SqlgDataSource.INSTANCE.get(this.sqlgGraph.getJdbcUrl()).getConnection();
            stmt = conn.createStatement();
            StringBuilder sql = new StringBuilder("SELECT * FROM ");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(this.sqlgGraph.getSqlDialect().getPublicSchema()));
            sql.append(".");
            sql.append(this.sqlgGraph.getSqlDialect().maybeWrapInQoutes(table));
            if (this.sqlgGraph.getSqlDialect().needsSemicolon()) {
                sql.append(";");
            }
            if(logger.isDebugEnabled()) {
                logger.debug(sql.toString());
            }
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

    private Class<?> findSqlGDialect() {
        try {
            return Class.forName("org.umlg.sqlg.sql.dialect.PostgresDialect");
        } catch (ClassNotFoundException e) {
        }
        try {
            return Class.forName("org.umlg.sqlg.sql.dialect.MariaDbDialect");
        } catch (ClassNotFoundException e) {
        }
        try {
            return Class.forName("org.umlg.sqlg.sql.dialect.HsqldbDialect");
        } catch (ClassNotFoundException e) {
        }
        throw new IllegalStateException("No sqlg dialect found!");
    }
}
