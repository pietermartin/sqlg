package org.umlg.sqlg.test.multithread;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Date: 2014/09/24
 * Time: 10:46 AM
 */
public class TestMultiThread extends BaseTest {

    @Test
    public void testMultiThreadVertices() throws InterruptedException, ExecutionException {
        AtomicInteger atomicInteger = new AtomicInteger(1);
        Set<Integer> tables = new ConcurrentSkipListSet<>();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int j = 0; j < 100; j++) {
            executorService.submit(() -> {
                final Random random = new Random();
                int randomInt = random.nextInt();
                for (int i = 0; i < 10; i++) {
                    sqlG.addVertex(T.label, "Person" + String.valueOf(randomInt), "name", String.valueOf(randomInt));
                    tables.add(randomInt);
                }
                sqlG.tx().commit();
                System.out.println(atomicInteger.getAndIncrement());
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);
        Assert.assertEquals(100, tables.size());
        for (Integer i : tables) {
            Assert.assertTrue(this.sqlG.getSchemaManager().tableExist(this.sqlG.getSqlDialect().getPublicSchema(), "V_Person" + String.valueOf(i)));
            Assert.assertEquals(10, this.sqlG.V().has(T.label, "Person" + String.valueOf(i)).has("name", String.valueOf(i)).count().next().intValue());
        }
    }

    @Test
    public void testMultiThreadEdges() throws InterruptedException, ExecutionException {
        AtomicInteger atomicInteger = new AtomicInteger(1);
        Vertex v1 = sqlG.addVertex(T.label, "Person", "name", "0");
        sqlG.tx().commit();
        Set<Integer> tables = new ConcurrentSkipListSet<>();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int j = 0; j < 100; j++) {
            executorService.submit(() -> {
                final Random random = new Random();
                int randomInt = random.nextInt();
                for (int i = 0; i < 10; i++) {
                    Vertex v2 = sqlG.addVertex(T.label, "Person" + String.valueOf(randomInt), "name", String.valueOf(randomInt));
                    v1.addEdge("test" + String.valueOf(randomInt), v2);
                    tables.add(randomInt);
                }
                sqlG.tx().commit();
                System.out.println(atomicInteger.getAndIncrement());
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);
        Assert.assertEquals(100, tables.size());
        for (Integer i : tables) {
            Assert.assertTrue(this.sqlG.getSchemaManager().tableExist(this.sqlG.getSqlDialect().getPublicSchema(), "V_Person" + String.valueOf(i)));
            Assert.assertEquals(10, this.sqlG.V().has(T.label, "Person" + String.valueOf(i)).has("name", String.valueOf(i)).count().next().intValue());
            Assert.assertEquals(10, v1.out("test" + String.valueOf(i)).count().next().intValue());
        }
    }

}
