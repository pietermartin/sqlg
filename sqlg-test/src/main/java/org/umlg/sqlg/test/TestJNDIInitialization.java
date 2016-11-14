package org.umlg.sqlg.test;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import javax.naming.spi.NamingManager;
import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Lukas Krejci
 */
public class TestJNDIInitialization {

    private static Logger logger = LoggerFactory.getLogger(TestJNDIInitialization.class);

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
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException, NamingException, ConfigurationException {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        configuration = new PropertiesConfiguration(sqlProperties);
        if (!configuration.containsKey("jdbc.url")) {
            throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
        }

        String url = configuration.getString("jdbc.url");

        //obtain the connection that we will later supply from JNDI
        SqlgGraph g = SqlgGraph.open(configuration);
        ds = g.getSqlgDataSource().get(url);
//        g.getTopology().close();

        //change the connection url to be a JNDI one
        configuration.setProperty("jdbc.url", "jndi:testConnection");

        //set up the initial context
        NamingManager.setInitialContextFactoryBuilder(environment -> {
            InitialContextFactory mockFactory = mock(InitialContextFactory.class);
            Context mockContext = mock(Context.class);
            when(mockFactory.getInitialContext(any())).thenReturn(mockContext);

            when(mockContext.lookup("testConnection")).thenReturn(ds);

            return mockFactory;
        });
    }

    @Test
    public void testLoadingDatasourceFromJndi() throws Exception {
        SqlgGraph g = SqlgGraph.open(configuration);
        assertNotNull(g.getSqlDialect());
        assertNotNull(g.getSqlgDataSource().get(configuration.getString("jdbc.url")));
    }
}
