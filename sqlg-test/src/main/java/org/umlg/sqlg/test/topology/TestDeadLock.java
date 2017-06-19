package org.umlg.sqlg.test.topology;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/06/19
 */
public class TestDeadLock extends BaseTest {

    /**
     * this deadlocks!
     *
     * @throws Exception
     */
    @Test
    public void testDeadlock1() throws Exception {

        final Vertex v1 = sqlgGraph.addVertex(T.label, "t1", "name", "n1");
        Vertex v2 = sqlgGraph.addVertex(T.label, "t1", "name", "n2");
        Vertex v3 = sqlgGraph.addVertex(T.label, "t2", "name", "n3");
        v1.addEdge("e1", v2);

        sqlgGraph.tx().commit();
//        sqlgGraph.getTopology().setLockTimeout(5);
        Object o1 = new Object();
        Object o2 = new Object();

        AtomicInteger ok = new AtomicInteger(0);

        Thread t1 = new Thread(() -> {
            try {
                synchronized (o1) {
                    o1.wait();
                }
                GraphTraversal<Vertex, Vertex> gt = sqlgGraph.traversal().V().hasLabel("t1").out("e1");
                int cnt = 0;
                // this lock the E_e1 table and then request topology read lock
                while (gt.hasNext()) {
                    gt.next();
                    synchronized (o2) {
                        o2.notify();
                    }

                    if (cnt == 0) {
                        synchronized (o1) {
                            o1.wait(1000);
                        }
                    }
                    cnt++;
                }
                sqlgGraph.tx().commit();
                ok.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
                //fail(e.getMessage());
            }
        }, "thread-1");

        t1.start();

        Thread t2 = new Thread(() -> {
            try {
                synchronized (o2) {
                    o2.wait();
                }
                // this locks the topology and then tries to modify the E_e1 table
                v1.addEdge("e1", v3);
                synchronized (o1) {
                    o1.notify();
                }

                sqlgGraph.tx().commit();
                ok.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
                //fail(e.getMessage());
            }
        }, "thread-2");

        t2.start();
        Thread.sleep(1000);
        synchronized (o1) {
            o1.notifyAll();
        }

        t1.join();
        t2.join();
        Assert.assertEquals(2, ok.get());
    }

    @Test
    public void testDeadlock1DifferentGraphs() throws Exception {

        Vertex v1 = sqlgGraph.addVertex(T.label, "t1", "name", "n1");
        Vertex v2 = sqlgGraph.addVertex(T.label, "t1", "name", "n2");
        Vertex v3 = sqlgGraph.addVertex(T.label, "t2", "name", "n3");
        v1.addEdge("e1", v2);

        sqlgGraph.tx().commit();

        Object o1 = new Object();
        Object o2 = new Object();

        AtomicInteger ok = new AtomicInteger(0);

        Thread t1 = new Thread(() -> {
            try {
                synchronized (o1) {
                    o1.wait();
                }
                GraphTraversal<Vertex, Vertex> gt = sqlgGraph.traversal().V().hasLabel("t1").out("e1");
                int cnt = 0;
                // this lock the E_e1 table and then request topology read lock
                while (gt.hasNext()) {
                    gt.next();
                    synchronized (o2) {
                        o2.notify();
                    }

                    if (cnt == 0) {
                        synchronized (o1) {
                            o1.wait(1000);
                        }
                    }
                    cnt++;
                }
                sqlgGraph.tx().commit();
                ok.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
        }, "thread-1");

        t1.start();

        Thread t2 = new Thread(() -> {
            try {
                try (SqlgGraph sqlgGraph1 = SqlgGraph.open(getConfigurationClone())) {
                    Vertex v1b = sqlgGraph1.vertices(v1.id()).next();
                    Vertex v3b = sqlgGraph1.vertices(v3.id()).next();
                    synchronized (o2) {
                        o2.wait();
                    }
                    // this locks the topology and then tries to modify the E_e1 table
                    v1b.addEdge("e1", v3b);
                    synchronized (o1) {
                        o1.notify();
                    }

                    sqlgGraph1.tx().commit();
                    ok.incrementAndGet();
                }
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
        }, "thread-2");

        t2.start();
        Thread.sleep(1000);
        synchronized (o1) {
            o1.notifyAll();
        }

        t1.join();
        t2.join();
        Assert.assertEquals(2, ok.get());
    }
}
