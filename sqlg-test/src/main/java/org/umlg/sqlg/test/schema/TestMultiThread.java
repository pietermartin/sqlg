package org.umlg.sqlg.test.schema;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Date: 2014/09/24
 * Time: 10:46 AM
 */
public class TestMultiThread extends BaseTest {

    private Logger logger = LoggerFactory.getLogger(TestMultiThread.class.getName());

    /**
     * This test is a duplicate of TransactionTest.shouldSupportTransactionIsolationCommitCheck but with the schema created upfront else it deadlocks.
     * @throws Exception
     */
    @Test
    public void shouldSupportTransactionIsolationCommitCheck() throws Exception {
        Vertex v1 = this.sqlgGraph.addVertex();
        this.sqlgGraph.tx().commit();
        v1.remove();
        this.sqlgGraph.tx().commit();
        // the purpose of this test is to simulate gremlin server access to a graph instance, where one thread modifies
        // the graph and a separate thread cannot affect the transaction of the first
        final CountDownLatch latchCommittedInOtherThread = new CountDownLatch(1);
        final CountDownLatch latchCommitInOtherThread = new CountDownLatch(1);

        final AtomicBoolean noVerticesInFirstThread = new AtomicBoolean(false);

        // this thread starts a transaction then waits while the second thread tries to commit it.
        final Thread threadTxStarter = new Thread("thread1") {
            @Override
            public void run() {
                TestMultiThread.this.sqlgGraph.addVertex();
                latchCommitInOtherThread.countDown();

                try {
                    latchCommittedInOtherThread.await();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }

                TestMultiThread.this.sqlgGraph.tx().rollback();

                // there should be no vertices here
                noVerticesInFirstThread.set(!TestMultiThread.this.sqlgGraph.vertices().hasNext());
            }
        };

        threadTxStarter.start();

        // this thread tries to commit the transaction started in the first thread above.
        final Thread threadTryCommitTx = new Thread("thread2") {
            @Override
            public void run() {
                try {
                    latchCommitInOtherThread.await();
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }

                // try to commit the other transaction
                TestMultiThread.this.sqlgGraph.tx().commit();

                latchCommittedInOtherThread.countDown();
            }
        };

        threadTryCommitTx.start();

        threadTxStarter.join();
        threadTryCommitTx.join();

        assertTrue(noVerticesInFirstThread.get());
        assertVertexEdgeCounts(0, 0);
    }

    @Test
    public void shouldExecuteWithCompetingThreads() throws InterruptedException {
        final Graph graph = this.sqlgGraph;
        int totalThreads = 250;
        final AtomicInteger vertices = new AtomicInteger(0);
        final AtomicInteger edges = new AtomicInteger(0);
        final AtomicInteger completedThreads = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(totalThreads);
        for (int i = 0; i < totalThreads; i++) {
            new Thread() {
                @Override
                public void run() {
                    try {
                        final Random random = new Random();
                        for (int i = 0; i < 100; i++) {
                            if (random.nextBoolean()) {
                                final Vertex a = graph.addVertex();
                                final Vertex b = graph.addVertex();
                                final Edge e = a.addEdge("friend", b);

                                vertices.getAndAdd(2);
                                a.property("test", this.getId());
                                b.property("blah", random.nextDouble());
                                e.property("bloop", random.nextInt());
                                edges.getAndAdd(1);
                                graph.tx().commit();
                            } else {
                                final Vertex a = graph.addVertex();
                                final Vertex b = graph.addVertex();
                                final Edge e = a.addEdge("friend", b);

                                a.property("test", this.getId());
                                b.property("blah", random.nextDouble());
                                e.property("bloop", random.nextInt());

                                if (random.nextBoolean()) {
                                    graph.tx().commit();
                                    vertices.getAndAdd(2);
                                    edges.getAndAdd(1);
                                } else {
                                    graph.tx().rollback();
                                }
                            }
                        }
                        countDownLatch.countDown();
                        completedThreads.getAndAdd(1);
                        logger.info("shouldExecuteWithCompetingThreads " + completedThreads.get());
                    } catch (Exception e) {
                        logger.error("failure", e);
                        fail(e.getMessage());
                    }
                }
            }.start();
        }
        countDownLatch.await();
        assertEquals(completedThreads.get(), totalThreads);
        System.out.println(vertices.get());
        assertVertexEdgeCounts(vertices.get(), edges.get());
    }

    private static Consumer<Graph> assertVertexEdgeCounts(final int expectedVertexCount, final int expectedEdgeCount) {
        return (g) -> {
            assertEquals(new Long(expectedVertexCount), g.traversal().V().count().next());
            assertEquals(new Long(expectedEdgeCount), g.traversal().E().count().next());
        };
    }

    @Test
    public void testMultiThreadVertices() throws InterruptedException, ExecutionException {
        Set<Integer> tables = new ConcurrentSkipListSet<>();
        ExecutorService executorService = newFixedThreadPool(10);
        for (int j = 0; j < 100; j++) {
            executorService.submit(() -> {
                final Random random = new Random();
                int randomInt = random.nextInt();
                for (int i = 0; i < 10; i++) {
                    sqlgGraph.addVertex(T.label, "Person" + String.valueOf(randomInt), "name", String.valueOf(randomInt));
                    tables.add(randomInt);
                }
                sqlgGraph.tx().commit();
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(6000, TimeUnit.SECONDS);
        for (Integer i : tables) {
            assertTrue(this.sqlgGraph.getSchemaManager().tableExist(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person" + String.valueOf(i)));
            assertEquals(10, this.sqlgGraph.traversal().V().has(T.label, "Person" + String.valueOf(i)).has("name", String.valueOf(i)).count().next().intValue());
        }
    }

    @Test
    public void testMultiThreadEdges() throws InterruptedException, ExecutionException {
        Vertex v1 = sqlgGraph.addVertex(T.label, "Person", "name", "0");
        sqlgGraph.tx().commit();
        Set<Integer> tables = new ConcurrentSkipListSet<>();
        ExecutorService executorService = newFixedThreadPool(10);
        for (int j = 0; j < 100; j++) {
            executorService.submit(() -> {
                final Random random = new Random();
                int randomInt = random.nextInt();
                for (int i = 0; i < 10; i++) {
                    Vertex v2 = sqlgGraph.addVertex(T.label, "Person" + String.valueOf(randomInt), "name", String.valueOf(randomInt));
                    v1.addEdge("test" + String.valueOf(randomInt), v2);
                    tables.add(randomInt);
                }
                sqlgGraph.tx().commit();
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);
        for (Integer i : tables) {
            assertTrue(this.sqlgGraph.getSchemaManager().tableExist(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person" + String.valueOf(i)));
            assertEquals(10, this.sqlgGraph.traversal().V().has(T.label, "Person" + String.valueOf(i)).has("name", String.valueOf(i)).count().next().intValue());
            assertEquals(10, vertexTraversal(v1).out("test" + String.valueOf(i)).count().next().intValue());
        }
    }

    @Test
    public void testMultiThreadCreateSchemas() throws InterruptedException, ExecutionException {
        Set<Integer> schemas = new HashSet<>();
        ExecutorService executorService = newFixedThreadPool(200);
        for (int i = 0; i < 10_000; i++) {
            Integer schema = new Random().nextInt(99);
            schemas.add(schema);
            Future<?> f = executorService.submit(() -> {
                this.sqlgGraph.getTopology().ensureSchemaExist("schema_" + schema);
                this.sqlgGraph.tx().commit();
            });
            f.get();
        }
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
        //+ 1 for the public schema and gui_schema i.e. globalUniqueIndex
        assertEquals(schemas.size() + 2, this.sqlgGraph.getTopology().getSchemas().size());
    }

}
