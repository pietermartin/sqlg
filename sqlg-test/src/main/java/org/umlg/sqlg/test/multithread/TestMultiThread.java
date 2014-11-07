package org.umlg.sqlg.test.multithread;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgDataSource;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2014/09/24
 * Time: 10:46 AM
 */
public class TestMultiThread extends BaseTest {

    @Test
    public void shouldExecuteWithCompetingThreads() {
        final Graph graph = this.sqlgGraph;
        int totalThreads = 250;
        final AtomicInteger vertices = new AtomicInteger(0);
        final AtomicInteger edges = new AtomicInteger(0);
        final AtomicInteger completedThreads = new AtomicInteger(0);
        for (int i = 0; i < totalThreads; i++) {
            new Thread() {
                @Override
                public void run() {
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
                    completedThreads.getAndAdd(1);
                }
            }.start();
        }

        while (completedThreads.get() < totalThreads) {
        }

        assertEquals(completedThreads.get(), 250);
        assertVertexEdgeCounts(vertices.get(), edges.get());
    }

    public static Consumer<Graph> assertVertexEdgeCounts(final int expectedVertexCount, final int expectedEdgeCount) {
        return (g) -> {
            assertEquals(new Long(expectedVertexCount), g.V().count().next());
            assertEquals(new Long(expectedEdgeCount), g.E().count().next());
        };
    }


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
                    sqlgGraph.addVertex(T.label, "Person" + String.valueOf(randomInt), "name", String.valueOf(randomInt));
                    tables.add(randomInt);
                }
                sqlgGraph.tx().commit();
                System.out.println(atomicInteger.getAndIncrement());
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(6000, TimeUnit.SECONDS);
        Assert.assertEquals(100, tables.size());
        for (Integer i : tables) {
            Assert.assertTrue(this.sqlgGraph.getSchemaManager().tableExist(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person" + String.valueOf(i)));
            Assert.assertEquals(10, this.sqlgGraph.V().has(T.label, "Person" + String.valueOf(i)).has("name", String.valueOf(i)).count().next().intValue());
        }
    }

    @Test
    public void testMultiThreadEdges() throws InterruptedException, ExecutionException {
        AtomicInteger atomicInteger = new AtomicInteger(1);
        Vertex v1 = sqlgGraph.addVertex(T.label, "Person", "name", "0");
        sqlgGraph.tx().commit();
        Set<Integer> tables = new ConcurrentSkipListSet<>();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
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
                System.out.println(atomicInteger.getAndIncrement());
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);
        Assert.assertEquals(100, tables.size());
        for (Integer i : tables) {
            Assert.assertTrue(this.sqlgGraph.getSchemaManager().tableExist(this.sqlgGraph.getSqlDialect().getPublicSchema(), "V_Person" + String.valueOf(i)));
            Assert.assertEquals(10, this.sqlgGraph.V().has(T.label, "Person" + String.valueOf(i)).has("name", String.valueOf(i)).count().next().intValue());
            Assert.assertEquals(10, v1.out("test" + String.valueOf(i)).count().next().intValue());
        }
    }

}
