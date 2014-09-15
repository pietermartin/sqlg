package org.umlg.sqlg.test.batch;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.lang.time.StopWatch;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Date: 2014/09/12
 * Time: 5:14 PM
 */
public class TestBatch extends BaseTest {

    @Test
    public void testVerticesBatchOn() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlG.tx().batchModeOn();
        for (int i = 0; i < 10000; i++) {
            Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person", "name", "marko" + i);
            Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person", "name", "marko" + i);
            v1.addEdge("Friend", v2);
        }
        this.sqlG.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        Assert.assertEquals(20000, this.sqlG.V().count().next(), 0);
        Assert.assertEquals(10000, this.sqlG.E().count().next(), 0);
    }

    @Test
    public void testBatchVertices() {
        this.sqlG.tx().batchModeOn();
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person", "name", "peter");
        Vertex v3 = this.sqlG.addVertex(Element.LABEL, "Person", "name", "john");
        v1.addEdge("Friend", v2, "weight", 1);
        v1.addEdge("Friend", v3, "weight", 2);
        this.sqlG.tx().commit();
        Assert.assertEquals(3, this.sqlG.V().count().next(), 0);
        Assert.assertEquals(2, v1.out("Friend").count().next(), 0);
        Assert.assertTrue(v1.out("Friend").toList().contains(v2));
        Assert.assertTrue(v1.out("Friend").toList().contains(v3));
        Assert.assertTrue(v2.in("Friend").toList().contains(v1));
        Assert.assertTrue(v3.in("Friend").toList().contains(v1));
    }

    @Test
    public void testBatchModeNeedsCleanTransactionPass() {
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.tx().rollback();
        this.sqlG.tx().batchModeOn();
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.addVertex(Element.LABEL, "Person");
        this.sqlG.tx().commit();
        Assert.assertEquals(4, this.sqlG.V().count().next(), 0);
    }


    /**
     * The copy command locks the copy to table.
     * This is great as there is no need to worry about the elements not having sequential ids generated.
     * This test that this is indeed the case.
     * Cars only go in after Persons
     *
     * @throws InterruptedException
     */
    @Test
    public void testEdgeCopyHappensInIsolation() throws InterruptedException {

        //This is needed else the schema manager lock on creating schemas
        Vertex firstVertex = sqlG.addVertex();
        sqlG.tx().commit();
        AtomicLong lastPerson = new AtomicLong();
        AtomicLong lastCar = new AtomicLong();

        CountDownLatch firstLatch = new CountDownLatch(1);
        final Thread thread1 = new Thread() {
            @Override
            public void run() {
                sqlG.tx().batchModeOn();
                for (int i = 0; i < 100000; i++) {
                    Vertex v1 = sqlG.addVertex(Element.LABEL, "Person");
                    Vertex v2 = sqlG.addVertex(Element.LABEL, "Person");
                    v1.addEdge("Friend", v2);
                }
                System.out.println("thread1 starting commit!");
                firstLatch.countDown();
                sqlG.tx().commit();
                List<Vertex> persons = sqlG.V().<Vertex>has(Element.LABEL, "Person").toList();
                lastPerson.set((Long) persons.get(persons.size() - 1).id());
                System.out.println("thread1 done!");

            }
        };
        thread1.start();
        final Thread thread2 = new Thread() {
            @Override
            public void run() {
                try {
                    System.out.println("waiting for first thread!");
                    firstLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Vertex v1 = sqlG.addVertex(Element.LABEL, "Car");
                Vertex v2 = sqlG.addVertex(Element.LABEL, "Car");
                v1.addEdge("Same", v2);
                sqlG.tx().commit();
                List<Vertex> cars = sqlG.V().<Vertex>has(Element.LABEL, "Car").toList();
                lastCar.set((Long) cars.get(cars.size() - 1).id());
                System.out.println("second thread done!");
            }
        };
        thread2.start();
        thread1.join();
        thread2.join();
        Assert.assertEquals(200000, lastPerson.get() - (Long)firstVertex.id());
        Assert.assertEquals(200002, lastCar.get() - (Long)firstVertex.id());
    }

    @Test
    public void testVertexProperties() {
        List<Short> shortList = new ArrayList<>();
        List<Integer> integerList = new ArrayList<>();
        List<Long> longList = new ArrayList<>();
        List<Double> doubleList = new ArrayList<>();
        this.sqlG.tx().batchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlG.addVertex(Element.LABEL, "Person",
                    "age2", (short)i,
                    "age3", i,
                    "age4", new Long(i),
                    "age6", new Double(i)
            );
            shortList.add((short)i);
            integerList.add(new Integer(i));
            longList.add(new Long(i));
            doubleList.add(new Double(i));
        }
        Assert.assertEquals(100, shortList.size());
        this.sqlG.tx().commit();
        List<Vertex> vertices = this.sqlG.V().toList();
        for (Vertex v : vertices) {
            shortList.remove((Short)v.value("age2"));
            integerList.remove((Integer)v.value("age3"));
            longList.remove((Long)v.value("age4"));
            doubleList.remove((Double)v.value("age6"));
        }
        Assert.assertTrue(shortList.isEmpty());
        Assert.assertTrue(integerList.isEmpty());
        Assert.assertTrue(longList.isEmpty());
        Assert.assertTrue(doubleList.isEmpty());
    }

    @Test
    public void testEdgeProperties() {
        List<Short> shortList = new ArrayList<>();
        List<Integer> integerList = new ArrayList<>();
        List<Long> longList = new ArrayList<>();
        List<Double> doubleList = new ArrayList<>();
        this.sqlG.tx().batchModeOn();
        for (int i = 0; i < 100; i++) {
            Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person", "name", "marko");
            Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person", "name", "john");
            Edge e1 = v1.addEdge("Friend", v2,
                    "age2", (short)i,
                    "age3", i,
                    "age4", new Long(i),
                    "age6", new Double(i)
            );
            shortList.add((short)i);
            integerList.add(new Integer(i));
            longList.add(new Long(i));
            doubleList.add(new Double(i));
        }
        Assert.assertEquals(100, shortList.size());
        this.sqlG.tx().commit();
        List<Edge> edges = this.sqlG.E().toList();
        for (Edge e : edges) {
            shortList.remove((Short)e.value("age2"));
            integerList.remove((Integer)e.value("age3"));
            longList.remove((Long)e.value("age4"));
            doubleList.remove((Double)e.value("age6"));
        }
        Assert.assertTrue(shortList.isEmpty());
        Assert.assertTrue(integerList.isEmpty());
        Assert.assertTrue(longList.isEmpty());
        Assert.assertTrue(doubleList.isEmpty());
    }

    @Test
    public void testUpdateInsertedVertexProperty() {
        this.sqlG.tx().batchModeOn();
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person", "name", "marko");
        v1.property("name", "john");
        this.sqlG.tx().commit();
        Assert.assertEquals("john", v1.value("name"));
        Assert.assertEquals("john", this.sqlG.V().next().value("name"));
    }

    @Test
    public void testUpdateInsertedEdgeProperty() {
        this.sqlG.tx().batchModeOn();
        Vertex v1 = this.sqlG.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex v2 = this.sqlG.addVertex(Element.LABEL, "Person", "name", "john");
        Edge edge = v1.addEdge("Friend", v2, "weight", 1);
        edge.property("weight", 2);
        this.sqlG.tx().commit();
        Assert.assertEquals(2, edge.<Integer>value("weight"), 0);
        Assert.assertEquals(2, this.sqlG.E().next().<Integer>value("weight"), 0);
    }

//    @Test
    //TODO need to deal with missing properties, set them to null
    public void testRemoveProperty() {
        this.sqlG.tx().batchModeOn();
        Vertex marko = this.sqlG.addVertex(Element.LABEL, "Person", "name", "marko");
        Vertex john = this.sqlG.addVertex(Element.LABEL, "Person", "name", "john");
        Edge friend = marko.addEdge("Friend", john, "weight", 1);
        Edge colleague = marko.addEdge("Colleague", john, "toRemove", "a");
        marko.property("name").remove();
        colleague.property("toRemove").remove();
        this.sqlG.tx().commit();

        marko = this.sqlG.v(marko.id());
        Assert.assertFalse(marko.property("name").isPresent());
    }

    @Test
    public void testInOutOnEdges() {
        this.sqlG.tx().batchModeOn();
        Vertex root = this.sqlG.addVertex(Element.LABEL, "ROOT");
        Vertex god = this.sqlG.addVertex(Element.LABEL, "God");
        root.addEdge("rootGod", god);
        this.sqlG.tx().commit();
    }

    @Test
    public void testGetEdges() {
        this.sqlG.tx().batchModeOn();
        Vertex root = this.sqlG.addVertex(Element.LABEL, "ROOT");
        Vertex god = this.sqlG.addVertex(Element.LABEL, "God");
        Edge sqlgEdge = root.addEdge("rootGod", god);
        Assert.assertEquals(sqlgEdge, root.outE("rootGod").next());
        this.sqlG.tx().commit();
    }

    @Test
    public void testGetVertices() {
        this.sqlG.tx().batchModeOn();
        Vertex root = this.sqlG.addVertex(Element.LABEL, "ROOT");
        Vertex god = this.sqlG.addVertex(Element.LABEL, "God");
        Vertex human = this.sqlG.addVertex(Element.LABEL, "Human");
        root.addEdge("rootGod", god);
        root.addEdge("rootHuman", human);
        god.addEdge("rootROOT", root);
        Assert.assertEquals(god, root.out("rootGod").next());
        Assert.assertEquals(human, root.out("rootHuman").next());
        this.sqlG.tx().commit();
    }

    @Test
    public void testPerformance() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlG.tx().batchModeOn();
        for (int i = 0; i < 10000; i++) {
            Vertex person = this.sqlG.addVertex(Element.LABEL, "Person", "name", "marko" + i);
            Vertex spaceTime = this.sqlG.addVertex(Element.LABEL, "SpaceTime", "name", "marko" + i);
            Vertex space = this.sqlG.addVertex(Element.LABEL, "Space", "name", "marko" + i);
            Vertex time = this.sqlG.addVertex(Element.LABEL, "Time", "name", "marko" + i);
            person.addEdge("spaceTime", spaceTime, "context", 1);
            spaceTime.addEdge("space", space, "dimension", 3);
            spaceTime.addEdge("time", time, "dimension", 1);
            if (i != 0 && i % 10000 == 0) {
                this.sqlG.tx().commit();
                this.sqlG.tx().batchModeOn();
            }
        }
        this.sqlG.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        Assert.assertEquals(10000, this.sqlG.V().has(Element.LABEL, "Person").count().next(), 0);
        Assert.assertEquals(10000, this.sqlG.V().has(Element.LABEL, "SpaceTime").count().next(), 0);
        Assert.assertEquals(10000, this.sqlG.V().has(Element.LABEL, "Space").count().next(), 0);
        Assert.assertEquals(10000, this.sqlG.V().has(Element.LABEL, "Time").count().next(), 0);
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    @Test
    public void testGetVerticesWithHas() {
        this.sqlG.tx().batchModeOn();
        Vertex root = this.sqlG.addVertex(Element.LABEL, "ROOT");
        Vertex jah = this.sqlG.addVertex(Element.LABEL, "God", "name", "Jah");
        Vertex jehova = this.sqlG.addVertex(Element.LABEL, "God", "name", "Jehova");
        root.addEdge("rootGod", jah);
        root.addEdge("rootGod", jehova);
        List<Vertex> vertices = root.out("rootGod").toList();
        Assert.assertTrue(vertices.contains(jah));
        Assert.assertTrue(vertices.contains(jehova));
        Assert.assertEquals(jah, root.out("rootGod").has("name", "Jah").next());
        Assert.assertEquals(jehova, root.out("rootGod").has("name", "Jehova").next());
        this.sqlG.tx().commit();
    }

    @Test
    public void testVertexLabelCache() {
        Vertex root = this.sqlG.addVertex(Element.LABEL, "ROOT");
        this.sqlG.tx().commit();
        this.sqlG.tx().batchModeOn();
        Vertex jah = this.sqlG.addVertex(Element.LABEL, "God", "name", "Jah");
        Vertex jehova = this.sqlG.addVertex(Element.LABEL, "God", "name", "Jehova");
        root.addEdge("rootGod", jah);
        root.addEdge("rootGod", jehova);
        this.sqlG.tx().commit();
        List<Vertex> vertices = root.out("rootGod").toList();
        Assert.assertTrue(vertices.contains(jah));
        Assert.assertTrue(vertices.contains(jehova));
        Assert.assertEquals(jah, root.out("rootGod").has("name", "Jah").next());
        Assert.assertEquals(jehova, root.out("rootGod").has("name", "Jehova").next());
    }


    //batch mode on is ignored if the graph does not support batch mode
//    @Test(expected = IllegalStateException.class)
//    public void testBatchModeNeedsCleanTransactionFail() {
//        this.sqlG.addVertex(Element.LABEL, "Person");
//        this.sqlG.tx().batchModeOn();
//        this.sqlG.addVertex(Element.LABEL, "Person");
//    }

}
