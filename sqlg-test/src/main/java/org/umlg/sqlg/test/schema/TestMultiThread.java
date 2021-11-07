package org.umlg.sqlg.test.schema;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

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
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("vertex", new HashMap<>() {{
            put("test", PropertyType.LONG);
            put("blah", PropertyType.DOUBLE);
        }});
        vertexLabel.ensureEdgeLabelExist("friend", vertexLabel, new HashMap<>() {{
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
                        Assert.fail(e.getMessage());
                    } finally {
                        countDownLatch.countDown();
                    }
                }
            }.start();
        }
        boolean success = countDownLatch.await(5, TimeUnit.MINUTES);
        Assert.assertTrue(success);
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
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int j = 0; j < 20; j++) {
            int finalJ = j;
            executorService.submit(() -> {
                try {
                    final Random random = new Random();
                    random.nextInt();
                    for (int i = 0; i < 10; i++) {
                        sqlgGraph.addVertex(T.label, "Person" + finalJ, "name", String.valueOf(finalJ));
                    }
                    sqlgGraph.tx().commit();
                    tables.add(finalJ);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    Assert.fail(e.getMessage());
                    sqlgGraph.tx().rollback();
                }
            });
        }
        executorService.shutdown();
        if (executorService.awaitTermination(6000, TimeUnit.SECONDS)) {
            logger.info("normal termination");
        } else {
            Assert.fail("failed to terminate executor service normally");
        }
        for (Integer i : tables) {
            logger.info(String.format("looking for 'Person%d'", i));
            Assert.assertTrue(String.format("Person%d not found", i), this.sqlgGraph.getTopology().getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "Person" + i).isPresent());
            Assert.assertEquals(10, this.sqlgGraph.traversal().V().has(T.label, "Person" + i).has("name", String.valueOf(i)).count().next().intValue());
        }
    }

    @Test
    public void testMultiThreadEdges() throws InterruptedException {
        //For some reason Maria don't like this one on teamcity
        Assume.assumeFalse(isMariaDb());
        Vertex v1 = sqlgGraph.addVertex(T.label, "Person", "name", "0");
        sqlgGraph.tx().commit();
        Set<Integer> tables = new ConcurrentSkipListSet<>();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int j = 0; j < 100; j++) {
            executorService.submit(() -> {
                final Random random = new Random();
                int randomInt = random.nextInt();
                for (int i = 0; i < 10; i++) {
                    Vertex v2 = sqlgGraph.addVertex(T.label, "Person" + randomInt, "name", String.valueOf(randomInt));
                    v1.addEdge("test" + randomInt, v2);
                    tables.add(randomInt);
                }
                sqlgGraph.tx().commit();
            });
        }
        executorService.shutdown();
        boolean success = executorService.awaitTermination(60, TimeUnit.SECONDS);
        Assert.assertTrue(success);
        for (Integer i : tables) {
            Assert.assertTrue(this.sqlgGraph.getTopology().getVertexLabel(this.sqlgGraph.getSqlDialect().getPublicSchema(), "Person" + i).isPresent());
            Assert.assertEquals(10, this.sqlgGraph.traversal().V().has(T.label, "Person" + i).has("name", String.valueOf(i)).count().next().intValue());
            Assert.assertEquals(10, vertexTraversal(this.sqlgGraph, v1).out("test" + i).count().next().intValue());
        }
    }

    @Test
    public void testMultiThreadCreateSchemas() throws InterruptedException, ExecutionException {
        Set<Integer> schemas = new HashSet<>();
        ExecutorService executorService = Executors.newFixedThreadPool(200);
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
        boolean terminated = executorService.awaitTermination(5, TimeUnit.SECONDS);
        Assert.assertTrue(terminated);
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
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            Assume.assumeTrue(isPostgres());
            configuration.addProperty("distributed", true);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
        VertexLabel personTrue = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person_True", new LinkedHashMap<>() {{
            put("name", PropertyType.STRING);
        }});
        VertexLabel addressTrue = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Address_True", new LinkedHashMap<>() {{
            put("name", PropertyType.STRING);
        }});
        personTrue.ensureEdgeLabelExist("address_True", addressTrue, new LinkedHashMap<>() {{
            put("name", PropertyType.STRING);
        }});
        VertexLabel personLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Person", new LinkedHashMap<>() {{
            put("name", PropertyType.STRING);
        }});
        VertexLabel addressLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Address", new LinkedHashMap<>() {{
            put("name", PropertyType.STRING);
        }});
        personLabel.ensureEdgeLabelExist("address", addressLabel, new LinkedHashMap<>() {{
            put("name", PropertyType.STRING);
        }});
        this.sqlgGraph.tx().commit();
        ExecutorService executorService = Executors.newFixedThreadPool(50);
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
                    Assert.fail(e.getMessage());
                }
            });
        }
        executorService.shutdown();
        boolean terminated = executorService.awaitTermination(100, TimeUnit.SECONDS);
        Assert.assertTrue(terminated);
        try (SqlgGraph sqlgGraph1 = SqlgGraph.open(configuration)) {
            Assert.assertEquals(400, sqlgGraph1.traversal().V().hasLabel("Person_True").count().next() + sqlgGraph1.traversal().V().hasLabel("Person").count().next());
            Assert.assertEquals(400, sqlgGraph1.traversal().V().hasLabel("Address_True").count().next() + sqlgGraph1.traversal().V().hasLabel("Address").count().next());
            Assert.assertEquals(400, sqlgGraph1.traversal().E().hasLabel("address_True").count().next() + sqlgGraph1.traversal().E().hasLabel("address").count().next());
        }
    }

    /**
     * test when each graph is created in its own thread, in distributed mode
     * each thread created a different label
     */
    @Test
    public void testMultipleGraphsMultipleLabels() throws Exception {
        URL sqlProperties = Thread.currentThread().getContextClassLoader().getResource("sqlg.properties");
        try {
            Configurations configs = new Configurations();
            configuration = configs.properties(sqlProperties);
            Assume.assumeTrue(isPostgres());
            configuration.addProperty("distributed", true);
            configuration.addProperty("maxPoolSize", 3);
            if (!configuration.containsKey("jdbc.url"))
                throw new IllegalArgumentException(String.format("SqlGraph configuration requires that the %s be set", "jdbc.url"));

        } catch (ConfigurationException e) {
            Assert.fail(e.getMessage());
        }

        ExecutorService executorService = Executors.newFixedThreadPool(200);
        int loop = 200;
//        int loop = 2;
        for (int i = 0; i < loop; i++) {
            String n = "person" + i;
            executorService.submit(() -> {
                try {
                    try (SqlgGraph sqlgGraph2 = SqlgGraph.open(configuration)) {
                        sqlgGraph2.addVertex(T.label, "Person" + n, "name", n);
                        sqlgGraph2.tx().commit();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail(e.getMessage());
                }
            });
        }
        executorService.shutdown();
        boolean terminatedNormally = executorService.awaitTermination(1, TimeUnit.MINUTES);
        Preconditions.checkState(terminatedNormally);

        try (SqlgGraph sqlgGraph2 = SqlgGraph.open(configuration)) {
            for (int i = 0; i < loop; i++) {
                String n = "person" + i;
                Assert.assertEquals(1, sqlgGraph2.traversal().V().hasLabel("Person" + n).count().next().longValue());
            }
        }
    }

    @Test
    public void testLoadsOfSchemaChanges() throws InterruptedException, ExecutionException {
        Assume.assumeFalse(isH2());
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Future<Integer>> futureList = new ArrayList<>();
        int loop = 1000;
        for (int i = 0; i < loop; i++) {
            String n = "person" + i;
            String edge = "e" + i;
            int current = i;
            futureList.add(executorService.submit(() -> {
                try {
                    Vertex v1 = this.sqlgGraph.addVertex(T.label, n, "name", n);
                    Vertex v2 = this.sqlgGraph.addVertex(T.label, n, "name", n);
                    final Random random = new Random();
                    if (random.nextBoolean()) {
                        v1.property("another" + n, "asd");
                    }
                    if (random.nextBoolean()) {
                        Edge e = v1.addEdge(edge, v2);
                        if (random.nextBoolean()) {
                            e.property("yetanother" + n, "asd");
                        }
                    }
                    this.sqlgGraph.tx().commit();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    Assert.fail(e.getMessage());
                }
                return current;
            }));
        }
        executorService.shutdown();
        for (Future<Integer> future : futureList) {
            logger.info("Completed " + future.get());
        }
        boolean terminated = executorService.awaitTermination(1, TimeUnit.SECONDS);
        Preconditions.checkState(terminated, "executorService terminated via timeout");
        for (int i = 0; i < loop; i++) {
            String n = "person" + i;
            Assert.assertEquals(n + " failed", 2, this.sqlgGraph.traversal().V().hasLabel(n).count().next().longValue());
        }
    }

    @Test
    public void simulateReadWriteChange() throws ExecutionException, InterruptedException {
        //Sleep here, help with testing connections from previous test staying idle on postgres.
        Thread.sleep(3_000);
        List<String> labels = new ArrayList<>();
        List<String> properties = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            labels.add("label" + i);
            properties.add("property" + i);
        }
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        List<Future<Integer>> futureList = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            int count = i;
            futureList.add(executorService.submit(() -> {
                try {
                    for (int i1 = 0; i1 < 1000; i1++) {
                        sqlgGraph.addVertex(T.label, labels.get(i1), properties.get(i1), "asd");
                        sqlgGraph.tx().commit();
                    }
                    return count;
                } catch (Exception e) {
                    sqlgGraph.tx().rollback();
                    throw new RuntimeException(e);
                }
            }));
        }
        for (int i = 0; i < 3; i++) {
            int count = i;
            futureList.add(executorService.submit(() -> {
                try {
                    for (int i12 = 0; i12 < 1000; i12++) {
                        sqlgGraph.traversal().V().hasLabel(labels.get(i12)).iterate();
                        sqlgGraph.tx().rollback();
                    }
                    return count;
                } catch (Exception e) {
                    sqlgGraph.tx().rollback();
                    throw new RuntimeException(e);
                }
            }));
        }
        executorService.shutdown();
        for (Future<Integer> integerFuture : futureList) {
            logger.info("done " + integerFuture.get());
        }
        Assert.assertEquals(1000, this.sqlgGraph.getTopology().getPublicSchema().getVertexLabels().size());
    }

}
