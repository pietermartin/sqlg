package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Date: 2014/11/06
 * Time: 5:48 AM
 */
public class TestMultiThreadedBatch extends BaseTest {

    private static final Logger logger = LoggerFactory.getLogger(TestMultiThreadedBatch.class.getName());

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        if (isPostgres()) {
            configuration.addProperty("distributed", true);
        }
    }

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testMultiThreadAddVertex() throws InterruptedException {
        sqlgGraph.tx().rollback();
        Set<Integer> tables = new ConcurrentSkipListSet<>();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        for (int j = 0; j < 50; j++) {
            executorService.submit(() -> {
                sqlgGraph.tx().rollback();
                sqlgGraph.tx().normalBatchModeOn();
                final Random random = new Random();
                int randomInt = random.nextInt();
                try {
                    for (int i = 0; i < 1000; i++) {
                        Vertex v1 = sqlgGraph.addVertex(T.label, "Person" + randomInt, "name", randomInt);
                        Vertex v2 = sqlgGraph.addVertex(T.label, "Person" + randomInt, "name", randomInt);
                        v1.addEdge(String.valueOf(randomInt), v2, "name", randomInt);
                    }
                    tables.add(randomInt);
                    sqlgGraph.tx().commit();
                    sqlgGraph.tx().normalBatchModeOn();
                } catch (Exception e) {
                	e.printStackTrace();
                    Assert.fail(e.getMessage());
                }
            });
        }
        executorService.shutdown();
        boolean terminated = executorService.awaitTermination(60000, TimeUnit.SECONDS);
        Assert.assertTrue(terminated);
        Assert.assertEquals(50, tables.size());
        testMultiThreadAddVertex_assert(this.sqlgGraph, tables);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testMultiThreadAddVertex_assert(this.sqlgGraph1, tables);
        }
    }

    private void testMultiThreadAddVertex_assert(SqlgGraph sqlgGraph, Set<Integer> tables) {
        for (Integer i : tables) {
            Assert.assertTrue(sqlgGraph.getTopology().getVertexLabel(sqlgGraph.getSqlDialect().getPublicSchema(), "Person" + i).isPresent());
            Assert.assertEquals(2000, sqlgGraph.traversal().V().has(T.label, "Person" + i).has("name", i).count().next().intValue());
            List<Vertex> persons = sqlgGraph.traversal().V().has(T.label, "Person" + i).toList();
            for (Vertex v : persons) {
                Assert.assertEquals(i, v.value("name"));
            }
        }
    }

    @Test
    public void testMultiThreadAddVertexSameLabel() throws InterruptedException {
        sqlgGraph.tx().rollback();
        Set<Integer> tables = new ConcurrentSkipListSet<>();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        for (int j = 0; j < 50; j++) {
            executorService.submit(() -> {
                sqlgGraph.tx().rollback();
                sqlgGraph.tx().normalBatchModeOn();
                final Random random = new Random();
                int randomInt = random.nextInt();
                while (tables.contains(randomInt)) {
                    randomInt = random.nextInt();
                }
                try {
                    for (int k = 0; k < 3; k++) {
                        boolean failed = false;
                        for (int i = 0; i < 1000; i++) {
                            try {
                                Vertex v1 = sqlgGraph.addVertex(T.label, "House", "name", randomInt);
                                Vertex v2 = sqlgGraph.addVertex(T.label, "Street", "name", randomInt);
                                v1.addEdge("Overlooks", v2, "name", randomInt);
                                tables.add(randomInt);
                            } catch (Exception e) {
                                failed = true;
                                tables.remove(randomInt);
                                sqlgGraph.tx().rollback();
                                if ((isPostgres() && e.getCause().getClass().getSimpleName().equals("PSQLException")) ||
                                        (isHsqldb() && e.getCause().getClass().getSimpleName().equals("SQLSyntaxErrorException")) ||
                                        (isMariaDb() && e.getCause().getClass().getSimpleName().equals("SQLSyntaxErrorException")) ||
                                        (isMysql() && e.getCause().getClass().getSimpleName().equals("SQLSyntaxErrorException")) ||
                                        (isH2() && e.getCause().getClass().getSimpleName().equals("JdbcSQLSyntaxErrorException"))) {

                                    //swallow
                                    logger.warn("Rollback transaction due to schema creation failure.", e);
                                    Thread.sleep(1000);
                                    break;
                                } else {
                                    logger.error(String.format("got exception %s", e.getCause().getClass().getSimpleName()), e);
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                        if (!failed) {
                            sqlgGraph.tx().commit();
                            break;
                        }
                    }
                    sqlgGraph.tx().normalBatchModeOn();
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail(e.getMessage());
                }
            });
        }
        executorService.shutdown();
        boolean terminated = executorService.awaitTermination(60000, TimeUnit.SECONDS);
        Assert.assertTrue(terminated);
        Assert.assertEquals(50, tables.size());
        testMultiThreadAddVertexSameLabel_assert(this.sqlgGraph, tables);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testMultiThreadAddVertexSameLabel_assert(this.sqlgGraph1, tables);
        }
    }

    private void testMultiThreadAddVertexSameLabel_assert(SqlgGraph sqlgGraph, Set<Integer> tables) {
        Assert.assertTrue(sqlgGraph.getTopology().getVertexLabel(sqlgGraph.getSqlDialect().getPublicSchema(), "House").isPresent());
        Assert.assertTrue(sqlgGraph.getTopology().getVertexLabel(sqlgGraph.getSqlDialect().getPublicSchema(), "Street").isPresent());

        for (Integer i : tables) {
            Assert.assertEquals(String.valueOf(i), 1000, sqlgGraph.traversal().V().has(T.label, "House").has("name", i).count().next().intValue());
            Assert.assertEquals(String.valueOf(i), 1000, sqlgGraph.traversal().V().has(T.label, "House").has("name", i).out("Overlooks").has("name", i).count().next().intValue());

        }
    }
}
