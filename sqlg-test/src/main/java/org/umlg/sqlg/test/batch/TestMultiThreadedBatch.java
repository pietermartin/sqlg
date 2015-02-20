package org.umlg.sqlg.test.batch;

import org.apache.tinkerpop.gremlin.process.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Date: 2014/11/06
 * Time: 5:48 AM
 */
public class TestMultiThreadedBatch extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

    @Test
    public void testMultiThreadAddVertex() throws InterruptedException {
        sqlgGraph.tx().rollback();
        AtomicInteger atomicInteger = new AtomicInteger(1);
        Set<Integer> tables = new ConcurrentSkipListSet<>();
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        for (int j = 0; j < 100; j++) {
            executorService.submit(() -> {
                sqlgGraph.tx().rollback();
                sqlgGraph.tx().batchModeOn();
                final Random random = new Random();
                int randomInt = random.nextInt();
                try {
                    for (int i = 0; i < 10000; i++) {
                        Vertex v1 = sqlgGraph.addVertex(T.label, "Person" + String.valueOf(randomInt), "name", randomInt);
                        Vertex v2 = sqlgGraph.addVertex(T.label, "Person" + String.valueOf(randomInt), "name", randomInt);
                        v1.addEdge(String.valueOf(randomInt), v2, "name", randomInt);
                        tables.add(randomInt);
                    }
                    sqlgGraph.tx().commit();
                    sqlgGraph.tx().batchModeOn();
                    System.out.println(atomicInteger.getAndIncrement());
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(60000, TimeUnit.SECONDS);
        Assert.assertEquals(100, tables.size());
        for (Integer i : tables) {
            Assert.assertTrue(this.sqlgGraph.getSchemaManager().tableExist(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person" + String.valueOf(i)));
            Assert.assertEquals(20000, this.sqlgGraph.V().has(T.label, "Person" + String.valueOf(i)).has("name", i).count().next().intValue());
            List<Vertex> persons = this.sqlgGraph.V().<Vertex>has(T.label, "Person" + String.valueOf(i)).toList();
            for (Vertex v : persons) {
                Assert.assertEquals(i, v.value("name"));
            }
        }
    }

}
