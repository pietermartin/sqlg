package org.umlg.sqlg.test.datasource;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.util.SqlgUtil;

import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;

public class TestAdditionalPools {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestAdditionalPools.class);
    private long start;
    protected static Configuration configuration;
    protected static Configuration pool1Configuration;
    protected static Configuration pool2Configuration;
    protected static Configuration pool3Configuration;
    protected SqlgGraph sqlgGraph;

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            TestAdditionalPools.this.start = System.currentTimeMillis();
            LOGGER.info("Starting test: {}.{}", description.getClassName(), description.getMethodName());
        }

        protected void finished(Description description) {
            long millis = System.currentTimeMillis() - TestAdditionalPools.this.start;
            String time = String.format("%02d min, %02d sec, %02d mil",
                    TimeUnit.MILLISECONDS.toMinutes(millis),
                    TimeUnit.MILLISECONDS.toSeconds(millis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)),
                    TimeUnit.MILLISECONDS.toMillis(millis) - TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(millis))
            );
            LOGGER.info("Finished test: {}.{} Time taken: {}", description.getClassName(), description.getMethodName(), time);
        }
    };

    @BeforeClass
    public static void beforeClass() {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        URL sqlPool1Properties = Thread.currentThread().getContextClassLoader().getResource("sqlg.pool1.properties");
        URL sqlPool2Properties = Thread.currentThread().getContextClassLoader().getResource("sqlg.pool2.properties");
        //This is the 'readOnly' pool
        URL sqlPool3Properties = Thread.currentThread().getContextClassLoader().getResource("sqlg.pool3.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            if (!configuration.containsKey("jdbc.url")) {
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));
            }
            pool1Configuration = configs.properties(sqlPool1Properties);
            pool2Configuration = configs.properties(sqlPool2Properties);
            pool3Configuration = configs.properties(sqlPool3Properties);
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
        this.sqlgGraph = SqlgGraph.open(configuration, pool1Configuration, pool2Configuration, pool3Configuration);
        assertNotNull(this.sqlgGraph);
        assertNotNull(this.sqlgGraph.getBuildVersion());
        stopWatch.stop();
        LOGGER.info("Startup time for test = {}", stopWatch);
    }

    @After
    public void after() {
        try {
            this.sqlgGraph.tx().onClose(Transaction.CLOSE_BEHAVIOR.ROLLBACK);
            this.sqlgGraph.close();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Test
    public void testPool1() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        this.sqlgGraph.tx().commit();

        List<Vertex> vertexList = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(1, vertexList.size());
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().dataSource("pool1");
        Assert.assertEquals("pool1", this.sqlgGraph.tx().dataSource());
        vertexList = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(1, vertexList.size());
        Assert.assertEquals("pool1", this.sqlgGraph.tx().dataSource());
        this.sqlgGraph.tx().rollback();
        Assert.assertNull(this.sqlgGraph.tx().dataSource());
    }

    @Test
    public void testReadOnlyPool() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        this.sqlgGraph.tx().commit();

        List<Vertex> vertexList = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(1, vertexList.size());
        this.sqlgGraph.tx().rollback();

        this.sqlgGraph.tx().dataSource("readOnly");
        Assert.assertEquals("readOnly", this.sqlgGraph.tx().dataSource());
        vertexList = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(1, vertexList.size());
        this.sqlgGraph.tx().rollback();
        Assert.assertNull(this.sqlgGraph.tx().dataSource());

        this.sqlgGraph.tx().dataSource("readOnly");
        try {
            this.sqlgGraph.traversal().addV("A").property("name", "b").iterate();
            Assert.fail();
        } catch (Exception e) {
            //noop
        }
        Assert.assertEquals("readOnly", this.sqlgGraph.tx().dataSource());
        this.sqlgGraph.tx().rollback();
        Assert.assertNull(this.sqlgGraph.tx().dataSource());

        this.sqlgGraph.tx().dataSource("readOnly");
        vertexList = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(1, vertexList.size());

        Vertex v = vertexList.get(0);
        try {
            v.property("name", "aa");
            Assert.fail();
        } catch (Exception e) {
            //noop
        }
        this.sqlgGraph.tx().rollback();
        Assert.assertNull(this.sqlgGraph.tx().dataSource());

        v.property("name", "aa");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.tx().dataSource("readOnly");
        Assert.assertTrue(this.sqlgGraph.traversal().V().hasLabel("A").has("name", "aa").hasNext());
        Assert.assertEquals("readOnly", this.sqlgGraph.tx().dataSource());
        this.sqlgGraph.tx().commit();
        Assert.assertNull(this.sqlgGraph.tx().dataSource());
    }

}
