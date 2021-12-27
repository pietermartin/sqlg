package org.umlg.sqlg.test.datasource;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.ArgumentMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgDataSource;
import org.umlg.sqlg.structure.SqlgGraph;

import javax.naming.Context;
import javax.naming.spi.InitialContextFactory;
import javax.naming.spi.NamingManager;
import javax.sql.DataSource;
import java.net.URL;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Lukas Krejci
 */
public class TestJNDIInitialization {

    private static final Logger logger = LoggerFactory.getLogger(TestJNDIInitialization.class);

    private static SqlgGraph sqlgGraph;
    private static Configuration configuration;
    private static DataSource ds;

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            logger.info("Starting test: " + description.getClassName() + "." + description.getMethodName());
        }

        protected void finished(Description description) {
            logger.info("Finished test: " + description.getClassName() + "." + description.getMethodName());
        }
    };

    @BeforeClass
    public static void beforeClass() throws Exception {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        Configurations configs = new Configurations();
        configuration = configs.properties(sqlProperties);
        if (!configuration.containsKey("jdbc.url")) {
            throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
        }
        sqlgGraph = SqlgGraph.open(configuration);

        SqlgDataSource sqlgDataSource = sqlgGraph.getSqlgDataSource();
        ds = sqlgDataSource.getDatasource();

        //change the connection url to be a JNDI one
        configuration.setProperty("jdbc.url", "jndi:testConnection");

        //set up the initial context
        NamingManager.setInitialContextFactoryBuilder(environment -> {
            InitialContextFactory mockFactory = mock(InitialContextFactory.class);
            Context mockContext = mock(Context.class);
            when(mockFactory.getInitialContext(ArgumentMatchers.any())).thenReturn(mockContext);

            when(mockContext.lookup("testConnection")).thenReturn(ds);

            return mockFactory;
        });
    }

    @AfterClass
    public static void afterClass() {
        sqlgGraph.close();
    }

    @Test
    public void testLoadingDatasourceFromJndi() throws Exception {
        try (SqlgGraph g = SqlgGraph.open(configuration)) {
            Assert.assertNotNull(g.getSqlDialect());
            Assert.assertEquals(configuration.getString("jdbc.url"), g.getJdbcUrl());
            Assert.assertNotNull(g.getConnection());
        }
    }
}
