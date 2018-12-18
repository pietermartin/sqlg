package org.umlg.sqlg.test.schema;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.fail;

/**
 * Date: 2014/09/24
 * Time: 10:46 AM
 */
public class TestMultiThread extends BaseTest {

    private final Logger logger = LoggerFactory.getLogger(TestMultiThread.class.getName());

    /**
     * This test is a duplicate of TransactionTest.shouldSupportTransactionIsolationCommitCheck but with the schema created upfront else it deadlocks.
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

        Assert.assertTrue(noVerticesInFirstThread.get());
        assertVertexEdgeCounts(sqlgGraph, 0, 0);
    }

    @Test
    public void shouldExecuteWithCompetingThreads() throws InterruptedException {
        //Create the schema upfront so that graphs (Hsqldb, H2, Mysql...) that do not support transactional schema's can succeed.
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("vertex", new HashMap<String, PropertyType>() {{
            put("test", PropertyType.LONG);
            put("blah", PropertyType.DOUBLE);
        }});
        vertexLabel.ensureEdgeLabelExist("friend", vertexLabel, new HashMap<String, PropertyType>() {{
            put("bloop", PropertyType.INTEGER);
        }});
        this.sqlgGraph.tx().commit();
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
                        completedThreads.getAndAdd(1);
                        logger.info("shouldExecuteWithCompetingThreads " + completedThreads.get());

                    } catch (Exception e) {
                        logger.error("failure", e);
                        fail(e.getMessage());
                    } finally {
                        countDownLatch.countDown();
                    }
                }
            }.start();
        }
        countDownLatch.await(5, TimeUnit.MINUTES);
        Assert.assertEquals(completedThreads.get(), totalThreads);
        System.out.println(vertices.get());
        assertVertexEdgeCounts(graph, vertices.get(), edges.get());
    }

    private static void assertVertexEdgeCounts(final Graph graph, final int expectedVertexCount, final int expectedEdgeCount) {
        getAssertVertexEdgeCounts(expectedVertexCount, expectedEdgeCount).accept(graph);
    }

    private static Consumer<Graph> getAssertVertexEdgeCounts(final int expectedVertexCount, final int expectedEdgeCount) {
        return (g) -> {
            Assert.assertEquals(expectedVertexCount, IteratorUtils.count(g.vertices()));
            Assert.assertEquals(expectedEdgeCount, IteratorUtils.count(g.edges()));
        };
    }

    @Test
    public void testMultiThreadVertices() throws InterruptedException {
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
            Assert.assertTrue(this.sqlgGraph.getTopology().getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "Person" + String.valueOf(i)).isPresent());
            Assert.assertEquals(10, this.sqlgGraph.traversal().V().has(T.label, "Person" + String.valueOf(i)).has("name", String.valueOf(i)).count().next().intValue());
        }
    }

    @Test
    public void testMultiThreadEdges() throws InterruptedException {
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
            Assert.assertTrue(this.sqlgGraph.getTopology().getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "Person" + String.valueOf(i)).isPresent());
            Assert.assertEquals(10, this.sqlgGraph.traversal().V().has(T.label, "Person" + String.valueOf(i)).has("name", String.valueOf(i)).count().next().intValue());
            Assert.assertEquals(10, vertexTraversal(this.sqlgGraph, v1).out("test" + String.valueOf(i)).count().next().intValue());
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
        //+ 1 for the public schema
        Assert.assertEquals(schemas.size() + 1, this.sqlgGraph.getTopology().getSchemas().size());
    }

    /**
     * test when each graph is created in its own thread, in distributed mode
     */
    @Test
    public void testMultipleGraphs() throws Exception {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            Assume.assumeTrue(isPostgres());
            configuration.addProperty("distributed", true);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
        ExecutorService executorService = newFixedThreadPool(50);
        int loop = 400;
        for (int i = 0; i < loop; i++) {
            String n = "person" + i;
            executorService.submit(() -> {
                try {
                    try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
                        final Random random = new Random();
                        if (random.nextBoolean()) {
                            Vertex person = sqlgGraph1.addVertex(T.label, "Person_True", "name", n);
                            Vertex address = sqlgGraph1.addVertex(T.label, "Address_True", "name", n);
                            person.addEdge("address_True", address, "name", n);
                        } else {
                            Vertex person = sqlgGraph1.addVertex(T.label, "Person", "name", n);
                            Vertex address = sqlgGraph1.addVertex(T.label, "Address", "name", n);
                            person.addEdge("address", address, "name", n);
                        }
                        sqlgGraph1.tx().commit();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    fail(e.getMessage());
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(100, TimeUnit.SECONDS);
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(400, sqlgGraph1.traversal().V().hasLabel("Person_True").count().next() + sqlgGraph1.traversal().V().hasLabel("Person").count().next());
            Assert.assertEquals(400, sqlgGraph1.traversal().V().hasLabel("Address_True").count().next() + sqlgGraph1.traversal().V().hasLabel("Address").count().next());
            Assert.assertEquals(400, sqlgGraph1.traversal().E().hasLabel("address_True").count().next() + sqlgGraph1.traversal().E().hasLabel("address").count().next());
        }
    }

    /**
     * test when each graph is created in its own thread, in distributed mode
     * each thread created a different label
     *
     * @throws Exception
     */
    @Test
    public void testMultipleGraphsMultipleLabels() throws Exception {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            configuration = new PropertiesConfiguration(sqlProperties);
            Assume.assumeTrue(isPostgres());
            configuration.addProperty("distributed", true);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        ExecutorService executorService = newFixedThreadPool(200);
        int loop = 20;
        for (int i = 0; i < loop; i++) {
            String n = "person" + i;
            executorService.submit(() -> {
                try {
                    try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
                        sqlgGraph1.addVertex(T.label, "Person" + n, "name", n);
                        sqlgGraph1.tx().commit();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            for (int i = 0; i < loop; i++) {
                String n = "person" + i;
                Assert.assertEquals(1, sqlgGraph1.traversal().V().hasLabel("Person" + n).count().next().longValue());
            }
        }
    }
}
