package org.umlg.sqlg.test.datasource;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.*;
import org.umlg.sqlg.sql.dialect.SqlDialect;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.net.URL;
import java.sql.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/12/20
 */
public class TestDataSource {

    protected Configuration configuration;

    @BeforeClass
    public static void beforeClass() {
    }

    @SuppressWarnings("Duplicates")
    @Before
    public void before() throws ConfigurationException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        Configurations configs = new Configurations();
        configuration = configs.properties(sqlProperties);
        if (!this.configuration.containsKey("jdbc.url")) {
            throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
        }
        SqlgGraph sqlgGraph = SqlgGraph.open(configuration);
        SqlgUtil.dropDb(sqlgGraph);
        sqlgGraph.tx().commit();
        sqlgGraph.close();

        sqlgGraph = SqlgGraph.open(configuration);
        sqlgGraph.getSqlDialect().grantReadOnlyUserPrivilegesToSqlgSchemas(sqlgGraph);
        SqlDialect sqlDialect = sqlgGraph.getSqlDialect();
        Connection conn = sqlgGraph.tx().getConnection();
        SqlgUtil.dropDb(sqlDialect, conn);
        sqlgGraph.tx().commit();
        sqlgGraph.close();
    }

    @Test
    public void testQueryEmptyGraph() {
        for (int i = 0; i < 100; i++) {
            Assume.assumeTrue(this.configuration.getString("jdbc.url").contains("postgresql"));
            try {
//                Configuration readOnlyConfiguration = new PropertiesConfiguration("sqlg.readonly.properties");

                Configurations readOnlyConfiguration = new Configurations();
                configuration = readOnlyConfiguration.properties("sqlg.readonly.properties");
                try (SqlgGraph ignored = SqlgGraph.open(configuration)) {
                    Assert.fail("user is readOnly, should not be able to start up on an empty graph.");
                }
            } catch (RuntimeException e) {
                Assert.assertEquals("org.postgresql.util.PSQLException", e.getCause().getClass().getName());
            } catch (ConfigurationException e) {
                Assert.fail(e.getMessage());
            }
        }
        int count = countConnections();
        //7 is a tad arbitary, not really getting it.
        //C3P0 has 3 helper threads, looks like they hang around after closing the datasource. going with 7 for good measure.

        //Setting the dataSource = null in C3P0DataSource.close(), looks like the count is 0 now.
        Assert.assertTrue(String.format("Expected count < 12, found %d",  count), count < 12);
    }

    private int countConnections() {
        //check no leaked connections to sqlgraphdb
        try {
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection(
                    this.configuration.getString("jdbc.url"),
                    this.configuration.getString("jdbc.username"),
                    this.configuration.getString("jdbc.password")
            );
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select count(*) from pg_stat_activity where " +
                    "datname = 'sqlgraphdb' and " +
                    "application_name not like 'pgAdmin 4%' and " +
                    "application_name not like 'Citus Maintenance%' and " +
                    "application_name not like 'PostgreSQL JDBC Driver' and " +
                    "usename is not null");
            Assert.assertTrue(rs.next());
            //only count the jdbc connection for this command.
            int result = rs.getInt(1);
            connection.close();
            return result;
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            return -1;
        }
    }
}
