package org.umlg.sqlg.test.datasource;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
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
    private SqlgGraph sqlgGraph;

    @BeforeClass
    public static void beforeClass() {
    }

    @SuppressWarnings("Duplicates")
    @Before
    public void before() throws ConfigurationException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        this.configuration = new PropertiesConfiguration(sqlProperties);
        if (!this.configuration.containsKey("jdbc.url")) {
            throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
        }
        this.sqlgGraph = SqlgGraph.open(configuration);
        SqlgUtil.dropDb(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();

        this.sqlgGraph = SqlgGraph.open(configuration);
        this.sqlgGraph.getSqlDialect().grantReadOnlyUserPrivilegesToSqlgSchemas(this.sqlgGraph);
        SqlDialect sqlDialect = sqlgGraph.getSqlDialect();
        Connection conn = sqlgGraph.tx().getConnection();
        SqlgUtil.dropDb(sqlDialect, conn);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
    }

    @Test
    public void testQueryEmptyGraph() {
        for (int i = 0; i < 10; i++) {
            Assume.assumeTrue(this.configuration.getString("jdbc.url").contains("postgresql"));
            try {
                Configuration readOnlyConfiguration = new PropertiesConfiguration("sqlg.readonly.properties");
                try (SqlgGraph ignored = SqlgGraph.open(readOnlyConfiguration)) {
                    Assert.fail("user is readOnly, should not be able to start up on an empty graph.");
                }
            } catch (RuntimeException e) {
                Assert.assertEquals("org.postgresql.util.PSQLException", e.getCause().getClass().getName());
            } catch (ConfigurationException e) {
                Assert.fail(e.getMessage());
            }
        }
        //check no leaked connections to sqlgraphdb
        try {
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection(
                    this.configuration.getString("jdbc.url"),
                    this.configuration.getString("jdbc.username"),
                    this.configuration.getString("jdbc.password")
            );
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select count(*) from pg_stat_activity where datname = 'sqlgraphdb' and application_name not like 'pgAdmin 4%' and application_name not like 'Citus Maintenance%'");
            Assert.assertTrue(rs.next());
            //only count the jdbc connection for this command.
            Assert.assertEquals(1, rs.getInt(1));
            connection.close();
        } catch (ClassNotFoundException | SQLException e) {
            Assert.fail(e.getMessage());
        }
    }
}
