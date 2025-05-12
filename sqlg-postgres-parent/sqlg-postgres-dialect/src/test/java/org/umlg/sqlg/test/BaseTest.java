package org.umlg.sqlg.test;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.services.SqlgPGRoutingFactory;
import org.umlg.sqlg.services.SqlgPGVectorFactory;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.net.URL;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Date: 2014/07/12
 * Time: 5:44 PM
 */
public abstract class BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class.getName());
    protected SqlgGraph sqlgGraph;
    protected SqlgGraph sqlgGraph1;
    protected GraphTraversalSource gt;
    protected static Configuration configuration;
    private long start;
    protected static final int SLEEP_TIME = 1000;

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            BaseTest.this.start = System.currentTimeMillis();
            LOGGER.info("Starting test: " + description.getClassName() + "." + description.getMethodName());
        }

        protected void finished(Description description) {
            long millis = System.currentTimeMillis() - BaseTest.this.start;
            String time = String.format("%02d min, %02d sec, %02d mil",
                    TimeUnit.MILLISECONDS.toMinutes(millis),
                    TimeUnit.MILLISECONDS.toSeconds(millis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)),
                    TimeUnit.MILLISECONDS.toMillis(millis) - TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(millis))
            );
            LOGGER.info(String.format("Finished test: %s.%s Time taken: %s", description.getClassName(), description.getMethodName(), time));
        }
    };

    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            if (!configuration.containsKey("jdbc.url")) {
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
            }
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void before() throws Exception {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph = SqlgGraph.open(configuration);
        SqlgUtil.dropDb(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        grantReadOnlyUserPrivileges();
        assertNotNull(this.sqlgGraph);
        assertNotNull(this.sqlgGraph.getBuildVersion());
        this.gt = this.sqlgGraph.traversal();
        if (configuration.getBoolean("distributed", false)) {
            this.sqlgGraph1 = SqlgGraph.open(configuration);
            assertNotNull(this.sqlgGraph1);
            assertEquals(this.sqlgGraph.getBuildVersion(), this.sqlgGraph1.getBuildVersion());
        }
        stopWatch.stop();
        sqlgGraph.getServiceRegistry().registerService(new SqlgPGRoutingFactory(sqlgGraph));
        sqlgGraph.getServiceRegistry().registerService(new SqlgPGVectorFactory(sqlgGraph));
        LOGGER.info("Startup time for test = " + stopWatch);
    }

    protected void grantReadOnlyUserPrivileges() {
        this.sqlgGraph.getSqlDialect().grantReadOnlyUserPrivilegesToSqlgSchemas(this.sqlgGraph);
        this.sqlgGraph.tx().commit();
    }

    @After
    public void after() {
        try {
            this.sqlgGraph.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
            this.sqlgGraph.close();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        try {
            if (this.sqlgGraph1 != null) {
                this.sqlgGraph1.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
                this.sqlgGraph1.close();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

}
