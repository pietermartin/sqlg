package org.umlg.sqlg.test.batch;

import com.tinkerpop.gremlin.process.T;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

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
                try {
                    final Random random = new Random();
                    int randomInt = random.nextInt();
                    for (int i = 0; i < 10; i++) {
                        sqlgGraph.addVertex(T.label, "Person" + String.valueOf(randomInt), "name", String.valueOf(randomInt));
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
            Assert.assertEquals(10, this.sqlgGraph.V().has(T.label, "Person" + String.valueOf(i)).has("name", String.valueOf(i)).count().next().intValue());
        }

    }
}
