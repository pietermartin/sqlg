package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.*;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.beans.PropertyVetoException;
import java.io.IOException;
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

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException, IOException, PropertyVetoException {
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
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        for (int j = 0; j < 50; j++) {
            executorService.submit(() -> {
                sqlgGraph.tx().rollback();
                sqlgGraph.tx().normalBatchModeOn();
                final Random random = new Random();
                int randomInt = random.nextInt();
                try {
                    for (int i = 0; i < 1000; i++) {
                        Vertex v1 = sqlgGraph.addVertex(T.label, "Person" + String.valueOf(randomInt), "name", randomInt);
                        Vertex v2 = sqlgGraph.addVertex(T.label, "Person" + String.valueOf(randomInt), "name", randomInt);
                        v1.addEdge(String.valueOf(randomInt), v2, "name", randomInt);
                        tables.add(randomInt);
                    }
                    sqlgGraph.tx().commit();
                    sqlgGraph.tx().normalBatchModeOn();
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(60000, TimeUnit.SECONDS);
        Assert.assertEquals(50, tables.size());
        testNultiThreadAddVertex_assert(this.sqlgGraph, tables);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(SLEEP_TIME);
            testNultiThreadAddVertex_assert(this.sqlgGraph1, tables);
        }
    }

    private void testNultiThreadAddVertex_assert(SqlgGraph sqlgGraph, Set<Integer> tables) {
        for (Integer i : tables) {
            Assert.assertTrue(sqlgGraph.getTopology().getVertexLabel(sqlgGraph.getSqlDialect().getPublicSchema(), "Person" + String.valueOf(i)).isPresent());
            Assert.assertEquals(2000, sqlgGraph.traversal().V().has(T.label, "Person" + String.valueOf(i)).has("name", i).count().next().intValue());
            List<Vertex> persons = sqlgGraph.traversal().V().<Vertex>has(T.label, "Person" + String.valueOf(i)).toList();
            for (Vertex v : persons) {
                Assert.assertEquals(i, v.value("name"));
            }
        }
    }

}
