package org.umlg.sqlg.test.topology;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/06/19
 */
@SuppressWarnings("Duplicates")
public class TestDeadLock extends BaseTest {

    @BeforeClass
    public static void beforeClass() {
        Assume.assumeFalse(isH2());
    }

    @Test
    public void testDeadLock4() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "nameA1", "haloA1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "nameB1", "haloB1");
        this.sqlgGraph.tx().commit();

        Thread t1 = new Thread(() -> {
            //Lock table A
            for (int i = 0; i < 3; i++) {
                try {
                    this.sqlgGraph.addVertex(T.label, "A", "nameA2", "haloA2");
                    b1.property("nameB1", "haloAgainB2");
                    this.sqlgGraph.tx().commit();
                    break;
                } catch (Exception e) {
                    this.sqlgGraph.tx().rollback();
                }
            }
        }, "First writer");

        Thread t2 = new Thread(() -> {
            //Lock table B
            for (int i = 0; i < 3; i++) {
                try {
                    this.sqlgGraph.addVertex(T.label, "B", "nameB2", "haloB2");
                    a1.property("nameA1", "haloAgainA1");
                    this.sqlgGraph.tx().commit();
                    break;
                } catch (Exception e) {
                    this.sqlgGraph.tx().rollback();
                }
            }
        }, "Second writer");

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().hasLabel("B").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("B").has("nameB1", "haloAgainB2").count().next(), 0);
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").has("nameA1", "haloAgainA1").count().next(), 0);
    }

    @Test
    public void testDeadLock3() throws InterruptedException {
        SqlgGraph g = this.sqlgGraph;
        Map<String, Object> m1 = new HashMap<>();
        m1.put("name", "name1");
        g.addVertex("s1.v1", m1);
        g.tx().commit();
        CountDownLatch t1Wrote = new CountDownLatch(1);
        CountDownLatch t2Wrote = new CountDownLatch(1);

        Thread t1 = new Thread(() -> {
            Map<String, Object> m11 = new HashMap<>();
            m11.put("name", "name2");
            g.addVertex("s1.v1", m11);

            t1Wrote.countDown();
            try {
                t2Wrote.await(10, TimeUnit.SECONDS);

                Map<String, Object> m2 = new HashMap<>();
                m2.put("name", "name3");
                m2.put("att1", "val1");
                g.addVertex("s1.v1", m2);


                g.tx().commit();
            } catch (InterruptedException ie) {
                Assert.fail(ie.getMessage());
            }
        }, "First writer");

        Thread t2 = new Thread(() -> {
            try {
                t1Wrote.await();

                Map<String, Object> m112 = new HashMap<>();
                m112.put("name", "name4");
                m112.put("att2", "val2");
                g.addVertex("s1.v1", m112);

                t2Wrote.countDown();

                g.tx().commit();
            } catch (InterruptedException ie) {
                Assert.fail(ie.getMessage());
            }
        }, "Second writer");

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        if (isPostgres()) {
            Assert.assertEquals(4L, g.traversal().V().hasLabel("s1.v1").count().next(), 0);
        } else if (isHsqldb()) {
            Assert.assertEquals(1L, g.traversal().V().hasLabel("s1.v1").count().next(), 0);
        } else if (isH2()) {
            Assert.assertEquals(3L, g.traversal().V().hasLabel("s1.v1").count().next(), 0);
        }
    }

    @Test
    public void testDeadLock2() throws InterruptedException {

        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();

        CountDownLatch latch = new CountDownLatch(1);
        Thread thread1 = new Thread(() -> {
            //#1 open a transaction.
            this.sqlgGraph.traversal().V().hasLabel("A").next();
            try {
                System.out.println("await");
                latch.await();
                //sleep for a bit to let Thread2 first take the topology lock
                Thread.sleep(1000);
                System.out.println("thread1 wakeup");
                //This will try to take a read lock that will dead lock
                this.sqlgGraph.traversal().V().hasLabel("A").next();
                System.out.println("thread1 complete");
                this.sqlgGraph.tx().commit();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, "thread1");
        Thread thread2 = new Thread(() -> {
            //#2 take the topology lock, adding a name field will lock the topology.
            //this will not be able to complete while Thread1's transaction is still in progress.
            //It locks in postgres.
            latch.countDown();
            this.sqlgGraph.addVertex(T.label, "A", "name", "a");
            this.sqlgGraph.tx().commit();
            System.out.println("thread2 fini");
        }, "thread2");
        thread1.start();
        Thread.sleep(1000);
        thread2.start();
        thread1.join();
        thread2.join();
    }

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
