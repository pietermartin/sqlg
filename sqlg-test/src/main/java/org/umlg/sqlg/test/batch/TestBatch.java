package org.umlg.sqlg.test.batch;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Date: 2014/09/12
 * Time: 5:14 PM
 */
public class TestBatch extends BaseTest {

    @Before
    public void beforeTest() {
        Assume.assumeTrue(this.sqlgGraph.getSqlDialect().supportsBatchMode());
    }

//    @Test
    public void queryPerformance() {
        this.sqlgGraph.tx().batchModeOn();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (int i = 0; i < 600000; i++) {
            Map<String, Object> properties = new HashMap<>();
            properties.put("Adjacent Cell Inter-layer HO Hysteresis", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("BQ HO Margin", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("BQ HO Valid Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("BQ HO Watch Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Better Cell HO Valid Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Better Cell HO Watch Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Chain Neighbor Cell", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Chain Neighbour Cell Type", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Directed Retry Handover Level Range", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Directional of Neighboring Cell", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Edge HO AdjCell Valid Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Edge HO AdjCell Watch Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Edge HO Hysteresis", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Enhanced Outgoing Cell Handover Offset", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("HCS HO Valid Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("HCS HO Watch Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("IBCA Dyn Measure Neighbour Cell Flag", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("IBCA RxLev Offset", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Inter-cell HO Hysteresis", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Layer HO Valid Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Layer HO Watch Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Level Penalty Value on Neighboring Cell", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Load HO PBGT Threshold", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Min Access Level Offset", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("NCell Interf Type", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Neighbor 2G Cell Index", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Neighbor 2G Cell Name", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Neighboring Cell Penalty Switch", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Neighboring Cell Priority", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Neighboring Cell Type", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("PBGT HO Threshold", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("PBGT Valid Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("PBGT Watch Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Penalty Stop Level Threshold", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Penalty Timer Length", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Quick Handover Last Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Quick Handover Offset for Neighbor Cell", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Quick Handover Static Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Source Cell Index", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("Source Cell Name", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("TA HO Valid Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("TA HO Watch Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("UL BQ HO Last Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("UL BQ HO Static Time", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("cmSoftwareVersion", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("cm_parent_nodename", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("cm_uid", "MTN->South Africa->HUAWEI->GSM->REAL_WS->G2GNCELL->CNBSH3->5292B->5860C" + i);
            properties.put("createdOn", 111111111);
            properties.put("internal_cm_name", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("name", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("networkName", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("updatedOn", 10000000);
            properties.put("Is External Cell", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("BQ HO Neighbor Cell Select Absolute Thld Switch", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("BCCH TRX NoBCCH TS PC Neighbor Cell HO CMP Value", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            properties.put("2G Neighboring Cell Ranking Priority", "aaaaaaaaaaaaaaaaaaaaaaaaa");
            if (i % 100000 == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().batchModeOn();
            }
            this.sqlgGraph.addVertex("R_HG.G2GNCELL", properties);
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        List<Vertex> result = this.sqlgGraph.traversal().V().has(T.label, "R_HG.G2GNCELL").toList();
//        Map<String, SqlgVertex> cache = new HashMap<>();
//        this.sqlgGraph.traversal().V().has(T.label, "R_HG.G2GNCELL").forEachRemaining(
//                v -> cache.put(v.<String>value("cm_uid"), (SqlgVertex) v)
//        );
//        Assert.assertEquals(600000, cache.size());
        Assert.assertEquals(600000, result.size());
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }
    @Test
    public void testEscapingCharacters() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().batchModeOn();
        for (int i = 0; i < 10000; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "MO1", "name", "marko" + i, "test1", "\\", "test2", "\nhalo", "test3", "\rhalo", "test4", "\thalo");
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko" + i, "test1", "\\", "test2", "\nhalo", "test3", "\rhalo", "test4", "\thalo");
            v1.addEdge("Friend", v2, "name", "xxx");
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        Assert.assertEquals(20000, this.sqlgGraph.traversal().V().count().next(), 0);
        Assert.assertEquals(10000, this.sqlgGraph.traversal().E().count().next(), 0);
    }

    @Test
    public void testVerticesBatchOn() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().batchModeOn();
        for (int i = 0; i < 10000; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "MO1", "name", "marko" + i);
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko" + i);
            v1.addEdge("Friend", v2, "name", "xxx");
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        Assert.assertEquals(20000, this.sqlgGraph.traversal().V().count().next(), 0);
        Assert.assertEquals(10000, this.sqlgGraph.traversal().E().count().next(), 0);
    }

    @Test
    public void testBatchVertices() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "peter");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        v1.addEdge("Friend", v2, "weight", 1);
        v1.addEdge("Friend", v3, "weight", 2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(3, this.sqlgGraph.traversal().V().count().next(), 0);
        Assert.assertEquals(2, this.sqlgGraph.traversal().V(v1.id()).out("Friend").count().next(), 0);
        Assert.assertTrue(this.sqlgGraph.traversal().V(v1.id()).out("Friend").toList().contains(v2));
        Assert.assertTrue(this.sqlgGraph.traversal().V(v1.id()).out("Friend").toList().contains(v3));
        Assert.assertTrue(this.sqlgGraph.traversal().V(v2.id()).in("Friend").toList().contains(v1));
        Assert.assertTrue(this.sqlgGraph.traversal().V(v3.id()).in("Friend").toList().contains(v1));
    }

    @Test
    public void testBatchModeNeedsCleanTransactionPass() {
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().batchModeOn();
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(4, this.sqlgGraph.traversal().V().count().next(), 0);
    }

    //this test a 'contains' bug in the update of labels batch logic
    @Test
    public void testBatchUpdateOfLabels() throws Exception {
        this.sqlgGraph.tx().batchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "mike");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Car", "name", "bmw");
        Vertex v4 = this.sqlgGraph.addVertex(T.label, "Bike", "name", "ktm");
        v1.addEdge("bts_aaaaaa", v2);
        v1.addEdge("bts_btsalmtos", v4);
        v1.addEdge("bts_btsalm", v3);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.close();
        this.sqlgGraph = SqlgGraph.open(configuration);
        v1 = this.sqlgGraph.v(v1.id());
        Assert.assertEquals(1, this.sqlgGraph.traversal().V(v1.id()).out("bts_btsalm").count().next().intValue());
        Assert.assertEquals(1, this.sqlgGraph.traversal().V(v1.id()).out("bts_btsalmtos").count().next().intValue());
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
        Vertex firstVertex = sqlgGraph.addVertex();
        sqlgGraph.tx().commit();
        AtomicLong lastPerson = new AtomicLong();
        AtomicLong lastCar = new AtomicLong();


        CountDownLatch firstLatch = new CountDownLatch(1);
        final Thread thread1 = new Thread() {
            @Override
            public void run() {
                try {
                    sqlgGraph.tx().batchModeOn();
                    for (int i = 0; i < 100000; i++) {
                        Vertex v1 = sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
                        Vertex v2 = sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
                        v1.addEdge("Friend", v2);
                    }
                    System.out.println("thread1 starting commit!");
                    firstLatch.countDown();
                    sqlgGraph.tx().commit();
                    List<Vertex> persons = sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").toList();
                    System.out.println("person size = " + persons.size());
                    System.out.println(persons.get(persons.size() - 1).id());
                    lastPerson.set(((RecordId) persons.get(persons.size() - 1).id()).getId());
                    System.out.println("thread1 done!");
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail(e.getMessage());
                } finally {
                    sqlgGraph.tx().rollback();
                }

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
                    e.printStackTrace();
                    Assert.fail(e.getMessage());
                }
                Vertex v1 = sqlgGraph.addVertex(T.label, "Car", "dummy", "a");
                Vertex v2 = sqlgGraph.addVertex(T.label, "Car", "dummy", "a");
                v1.addEdge("Same", v2);
                sqlgGraph.tx().commit();
                List<Vertex> cars = sqlgGraph.traversal().V().<Vertex>has(T.label, "Car").toList();
                lastCar.set(((RecordId) cars.get(cars.size() - 1).id()).getId());
                System.out.println("second thread done!");
            }
        };
        thread2.start();
        thread1.join();
        thread2.join();
        Assert.assertEquals(200000, lastPerson.get());
        Assert.assertEquals(2, lastCar.get());
    }

    @Test
    public void testVertexProperties() {
        List<Short> shortList = new ArrayList<>();
        List<Integer> integerList = new ArrayList<>();
        List<Long> longList = new ArrayList<>();
        List<Double> doubleList = new ArrayList<>();
        this.sqlgGraph.tx().batchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "Person",
                    "age2", (short) i,
                    "age3", i,
                    "age4", new Long(i),
                    "age6", new Double(i)
            );
            shortList.add((short) i);
            integerList.add(new Integer(i));
            longList.add(new Long(i));
            doubleList.add(new Double(i));
        }
        Assert.assertEquals(100, shortList.size());
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().toList();
        for (Vertex v : vertices) {
            shortList.remove((Short) v.value("age2"));
            integerList.remove((Integer) v.value("age3"));
            longList.remove((Long) v.value("age4"));
            doubleList.remove((Double) v.value("age6"));
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
        this.sqlgGraph.tx().batchModeOn();
        for (int i = 0; i < 100; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
            Edge e1 = v1.addEdge("Friend", v2,
                    "age2", (short) i,
                    "age3", i,
                    "age4", new Long(i),
                    "age6", new Double(i)
            );
            shortList.add((short) i);
            integerList.add(new Integer(i));
            longList.add(new Long(i));
            doubleList.add(new Double(i));
        }
        Assert.assertEquals(100, shortList.size());
        this.sqlgGraph.tx().commit();
        List<Edge> edges = this.sqlgGraph.traversal().E().toList();
        for (Edge e : edges) {
            shortList.remove((Short) e.value("age2"));
            integerList.remove((Integer) e.value("age3"));
            longList.remove((Long) e.value("age4"));
            doubleList.remove((Double) e.value("age6"));
        }
        Assert.assertTrue(shortList.isEmpty());
        Assert.assertTrue(integerList.isEmpty());
        Assert.assertTrue(longList.isEmpty());
        Assert.assertTrue(doubleList.isEmpty());
    }

    @Test
    public void testUpdateInsertedVertexProperty() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        v1.property("name", "john");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("john", v1.value("name"));
        Assert.assertEquals("john", this.sqlgGraph.traversal().V().next().value("name"));
        v1 = this.sqlgGraph.v(v1.id());
        Assert.assertEquals("john", v1.value("name"));
        Assert.assertEquals("john", this.sqlgGraph.traversal().V().next().value("name"));
    }

    @Test
    public void testAddPropertyToInsertedVertexProperty() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        v1.property("name", "john");
        v1.property("surname", "aaaa");
        this.sqlgGraph.tx().commit();
        v1 = this.sqlgGraph.v(v1.id());
        Assert.assertEquals("john", v1.value("name"));
        Assert.assertEquals("aaaa", v1.value("surname"));
        Assert.assertEquals("john", this.sqlgGraph.traversal().V().next().value("name"));
    }

    @Test
    public void testUpdateInsertedEdgeProperty() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Edge edge = v1.addEdge("Friend", v2, "weight", 1);
        edge.property("weight", 2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, edge.<Integer>value("weight"), 0);
        Assert.assertEquals(2, this.sqlgGraph.traversal().E().next().<Integer>value("weight"), 0);
    }

    @Test
    //TODO need to deal with missing properties, set them to null
    public void testRemoveProperty() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlgGraph.addVertex(T.label, "Person", "name", "john");
        Edge friend = marko.addEdge("Friend", john, "weight", 1);
        Edge colleague = marko.addEdge("Colleague", john, "toRemove", "a");
        marko.property("name").remove();
        colleague.property("toRemove").remove();
        this.sqlgGraph.tx().commit();

        marko = this.sqlgGraph.v(marko.id());
        Assert.assertFalse(marko.property("name").isPresent());
    }

    @Test
    public void testInOutOnEdges() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT", "dummy", "a");
        Vertex god = this.sqlgGraph.addVertex(T.label, "God", "dummy", "a");
        root.addEdge("rootGod", god);
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testGetEdges() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT", "dummy", "a");
        Vertex god = this.sqlgGraph.addVertex(T.label, "God", "dummy", "a");
        Edge sqlgEdge = root.addEdge("rootGod", god);

        Assert.assertEquals(sqlgEdge, vertexTraversal(root).outE("rootGod").next());
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testGetVertices() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT", "dummy", "a");
        Vertex god = this.sqlgGraph.addVertex(T.label, "God", "dummy", "a");
        Vertex human = this.sqlgGraph.addVertex(T.label, "Human", "dummy", "a");
        root.addEdge("rootGod", god);
        root.addEdge("rootHuman", human);
        god.addEdge("rootROOT", root);
        Assert.assertEquals(god, vertexTraversal(root).out("rootGod").next());
        Assert.assertEquals(human, vertexTraversal(root).out("rootHuman").next());
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testPerformance() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().batchModeOn();
        for (int i = 0; i < 10000; i++) {
            Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko" + i);
            Vertex spaceTime = this.sqlgGraph.addVertex(T.label, "SpaceTime", "name", "marko" + i);
            Vertex space = this.sqlgGraph.addVertex(T.label, "Space", "name", "marko" + i);
            Vertex time = this.sqlgGraph.addVertex(T.label, "Time", "name", "marko" + i);
            person.addEdge("spaceTime", spaceTime, "context", 1);
            spaceTime.addEdge("space", space, "dimension", 3);
            spaceTime.addEdge("time", time, "dimension", 1);
            if (i != 0 && i % 10000 == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().batchModeOn();
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        Assert.assertEquals(10000, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
        Assert.assertEquals(10000, this.sqlgGraph.traversal().V().has(T.label, "SpaceTime").count().next(), 0);
        Assert.assertEquals(10000, this.sqlgGraph.traversal().V().has(T.label, "Space").count().next(), 0);
        Assert.assertEquals(10000, this.sqlgGraph.traversal().V().has(T.label, "Time").count().next(), 0);
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    @Test
    public void testGetVerticesWithHas() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT", "dummy", "a");
        Vertex jah = this.sqlgGraph.addVertex(T.label, "God", "name", "Jah");
        Vertex jehova = this.sqlgGraph.addVertex(T.label, "God", "name", "Jehova");
        root.addEdge("rootGod", jah);
        root.addEdge("rootGod", jehova);
        List<Vertex> vertices = vertexTraversal(root).out("rootGod").toList();
        Assert.assertTrue(vertices.contains(jah));
        Assert.assertTrue(vertices.contains(jehova));
        Assert.assertEquals(jah, vertexTraversal(root).out("rootGod").has("name", "Jah").next());
        Assert.assertEquals(jehova, vertexTraversal(root).out("rootGod").has("name", "Jehova").next());
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testVertexLabelCache() {
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        Vertex jah = this.sqlgGraph.addVertex(T.label, "God", "name", "Jah");
        Vertex jehova = this.sqlgGraph.addVertex(T.label, "God", "name", "Jehova");
        root.addEdge("rootGod", jah);
        root.addEdge("rootGod", jehova);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = vertexTraversal(root).out("rootGod").toList();
        Assert.assertTrue(vertices.contains(jah));
        Assert.assertTrue(vertices.contains(jehova));
        Assert.assertEquals(jah, vertexTraversal(root).out("rootGod").has("name", "Jah").next());
        Assert.assertEquals(jehova, vertexTraversal(root).out("rootGod").has("name", "Jehova").next());
    }

    @Test
    public void testVertexMultipleEdgesLabels() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex car = this.sqlgGraph.addVertex(T.label, "Car", "name", "b");
        Vertex bike = this.sqlgGraph.addVertex(T.label, "Bike", "name", "c");
        person.addEdge("car", car);
        person.addEdge("bike", bike);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(Long.valueOf(2), vertexTraversal(person).out().count().next());
    }

    @Test
    public void testAddEdgeAccrossSchema() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex person = this.sqlgGraph.addVertex(T.label, "Schema1.Person", "name", "a");
        Vertex car = this.sqlgGraph.addVertex(T.label, "Schema2.Car", "name", "b");
        Vertex bike = this.sqlgGraph.addVertex(T.label, "Schema2.Bike", "name", "c");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        person.addEdge("car", car);
        person.addEdge("bike", bike);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(Long.valueOf(2), vertexTraversal(person).out().count().next());
    }

    @Test
    public void testBatchCommit() {
        Assume.assumeTrue(this.sqlgGraph.features().supportsBatchMode());
        this.sqlgGraph.tx().batchModeOn();
        for (int i = 0; i < 111; i++) {
            this.sqlgGraph.addVertex(T.label, "Person1", "name", i);
            this.sqlgGraph.addVertex(T.label, "Person2", "name", i);
        }
        Map<SchemaTable, Pair<Long, Long>> result = this.sqlgGraph.tx().batchCommit();
        Assert.assertEquals(1l, result.get(SchemaTable.of("public", "Person1")).getLeft(), 0);
        Assert.assertEquals(111l, result.get(SchemaTable.of("public", "Person1")).getRight(), 0);
        Assert.assertEquals(1l, result.get(SchemaTable.of("public", "Person2")).getLeft(), 0);
        Assert.assertEquals(111l, result.get(SchemaTable.of("public", "Person2")).getRight(), 0);

        this.sqlgGraph.tx().batchModeOn();
        for (int i = 0; i < 111; i++) {
            this.sqlgGraph.addVertex(T.label, "Person1", "name", i);
            this.sqlgGraph.addVertex(T.label, "Person2", "name", i);
        }
        result = this.sqlgGraph.tx().batchCommit();
        Assert.assertEquals(112l, result.get(SchemaTable.of("public", "Person1")).getLeft(), 0);
        Assert.assertEquals(222l, result.get(SchemaTable.of("public", "Person1")).getRight(), 0);
        Assert.assertEquals(112l, result.get(SchemaTable.of("public", "Person2")).getLeft(), 0);
        Assert.assertEquals(222l, result.get(SchemaTable.of("public", "Person2")).getRight(), 0);

    }
    @Test
    public void testCacheAndUpdateVERTICESLabels() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "person");
        List<Vertex> cache = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            cache.add(this.sqlgGraph.addVertex(T.label, "Person", "name", "person" + i));
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        for (Vertex person2 : cache) {
            person1.addEdge("Friend", person2);
        }
        this.sqlgGraph.tx().commit();
        person1 = this.sqlgGraph.v(person1.id());
        Assert.assertTrue(vertexTraversal(person1).out("Friend").hasNext());
        Assert.assertEquals(Long.valueOf(10000), vertexTraversal(person1).out("Friend").count().next());
        List<Vertex> friends = vertexTraversal(person1).out("Friend").toList();
        List<String> names = friends.stream().map(v -> v.<String>value("name")).collect(Collectors.toList());
        Assert.assertEquals(10000, names.size(), 0);
        for (int i = 0; i < 10000; i++) {
            Assert.assertTrue(names.contains("person" + i));
        }
    }

    @Test
    public void testBatchInsertDifferentKeys() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "surname", "b");
        this.sqlgGraph.tx().commit();

        List<Vertex> persons = this.sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").<Vertex>has("name", "a").toList();
        Assert.assertEquals(1, persons.size());
        Assert.assertFalse(persons.get(0).property("surname").isPresent());

        persons = this.sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").<Vertex>has("surname", "b").toList();
        Assert.assertEquals(1, persons.size());
        Assert.assertFalse(persons.get(0).property("name").isPresent());

        persons = this.sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").has("surname", "b").<Vertex>has("name", "a").toList();
        Assert.assertEquals(0, persons.size());
    }

    @Test
    public void testVerticesOutLabelsForPersistentVertices() {
        Vertex realWorkspace = this.sqlgGraph.addVertex(T.label, "RealWorkspace", "name", "realWorkspace1");
        Vertex softwareVersion = this.sqlgGraph.addVertex(T.label, "SoftwareVersion", "name", "R15");
        Vertex vendorTechnology = this.sqlgGraph.addVertex(T.label, "VendorTechnology", "name", "Huawei_Gsm");
        vendorTechnology.addEdge("vendorTechnology_softwareVersion", softwareVersion);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals("Huawei_Gsm", vertexTraversal(softwareVersion).in("vendorTechnology_softwareVersion").next().value("name"));

        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().batchModeOn();
        Vertex rwe1 = this.sqlgGraph.addVertex(T.label, "RWE", "name", "cell1");
        rwe1.addEdge("workspaceElement_softwareVersion", softwareVersion);
        this.sqlgGraph.tx().commit();

        softwareVersion = this.sqlgGraph.v(softwareVersion.id());
        Assert.assertEquals("Huawei_Gsm", vertexTraversal(softwareVersion).in("vendorTechnology_softwareVersion").next().value("name"));
    }

    @Test
    public void testVerticesInLabelsForPersistentVertices() {
        Vertex realWorkspace = this.sqlgGraph.addVertex(T.label, "RealWorkspace", "name", "realWorkspace1");
        Vertex softwareVersion = this.sqlgGraph.addVertex(T.label, "SoftwareVersion", "name", "R15");
        Vertex vendorTechnology = this.sqlgGraph.addVertex(T.label, "VendorTechnology", "name", "Huawei_Gsm");
        softwareVersion.addEdge("softwareVersion_vendorTechnology", vendorTechnology);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals("Huawei_Gsm", vertexTraversal(softwareVersion).out("softwareVersion_vendorTechnology").next().value("name"));

        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().batchModeOn();
        Vertex rwe1 = this.sqlgGraph.addVertex(T.label, "RWE", "name", "cell1");
        rwe1.addEdge("workspaceElement_softwareVersion", softwareVersion);
        this.sqlgGraph.tx().commit();

        softwareVersion = this.sqlgGraph.v(softwareVersion.id());
        Assert.assertEquals("Huawei_Gsm", vertexTraversal(softwareVersion).out("softwareVersion_vendorTechnology").next().value("name"));
    }

    @Test
    public void testBatchUpdatePersistentVertices() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "surname", "b");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("a", this.sqlgGraph.v(v1.id()).value("name"));
        Assert.assertEquals("b", this.sqlgGraph.v(v2.id()).value("surname"));

        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().batchModeOn();
        v1.property("name", "aa");
        v2.property("surname", "bb");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals("aa", this.sqlgGraph.v(v1.id()).value("name"));
        Assert.assertEquals("bb", this.sqlgGraph.v(v2.id()).value("surname"));
    }

    @Test
    public void testBatchUpdatePersistentVerticesAllTypes() {

        Assume.assumeTrue(this.sqlgGraph.features().vertex().properties().supportsFloatValues());

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "surname", "b");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("a", this.sqlgGraph.v(v1.id()).value("name"));
        Assert.assertEquals("b", this.sqlgGraph.v(v2.id()).value("surname"));

        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().batchModeOn();
        v1.property("name", "aa");
        v1.property("boolean", true);
        v1.property("short", (short) 1);
        v1.property("integer", 1);
        v1.property("long", 1L);
        v1.property("float", 1F);
        v1.property("double", 1D);

        v2.property("surname", "bb");
        v2.property("boolean", false);
        v2.property("short", (short) 2);
        v2.property("integer", 2);
        v2.property("long", 2L);
        v2.property("float", 2F);
        v2.property("double", 2D);
        this.sqlgGraph.tx().commit();

        Assert.assertEquals("aa", this.sqlgGraph.v(v1.id()).value("name"));
        Assert.assertEquals(true, this.sqlgGraph.v(v1.id()).value("boolean"));
        Assert.assertEquals((short) 1, this.sqlgGraph.v(v1.id()).<Short>value("short").shortValue());
        Assert.assertEquals(1, this.sqlgGraph.v(v1.id()).<Integer>value("integer").intValue());
        Assert.assertEquals(1L, this.sqlgGraph.v(v1.id()).<Long>value("long").longValue(), 0);
        Assert.assertEquals(1F, this.sqlgGraph.v(v1.id()).<Float>value("float").floatValue(), 0);
        Assert.assertEquals(1D, this.sqlgGraph.v(v1.id()).<Double>value("double").doubleValue(), 0);

        Assert.assertEquals("bb", this.sqlgGraph.v(v2.id()).value("surname"));
        Assert.assertEquals(false, this.sqlgGraph.v(v2.id()).value("boolean"));
        Assert.assertEquals((short) 2, this.sqlgGraph.v(v2.id()).<Short>value("short").shortValue());
        Assert.assertEquals(2, this.sqlgGraph.v(v2.id()).<Integer>value("integer").intValue());
        Assert.assertEquals(2L, this.sqlgGraph.v(v2.id()).<Long>value("long").longValue(), 0);
        Assert.assertEquals(2F, this.sqlgGraph.v(v2.id()).<Float>value("float").floatValue(), 0);
        Assert.assertEquals(2D, this.sqlgGraph.v(v2.id()).<Double>value("double").doubleValue(), 0);
    }

    @Test
    public void testBatchUpdatePersistentVerticesPerformance1() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().batchModeOn();
        int count = 1;
        for (int i = 0; i < 100000; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
            if (count++ % 10000 == 0) {
                this.sqlgGraph.tx().commit();
            }

        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().toList();
        count = 1;
        for (Vertex v : vertices) {
            v.property("name", "b");
            if (count++ % 10000 == 0) {
                this.sqlgGraph.tx().commit();
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    @Test
    public void testInsertUpdateQuotedStrings() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().batchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "'a'");
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        this.sqlgGraph.tx().batchModeOn();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().toList();
        for (Vertex v : vertices) {
            v.property("name", "'b'");
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    @Test
    public void testBatchRemoveVertex() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        v1.remove();
        v2.remove();
        v3.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().count().next().intValue());
    }

    @Test
    public void testBatchRemoveEdges() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        Edge edge1 = v1.addEdge("test", v2);
        Edge edge2 = v1.addEdge("test", v3);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        edge1.remove();
        edge2.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(3, this.sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().count().next().intValue());
    }

    @Test
    public void testBatchRemoveVerticesAndEdges() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        Edge edge1 = v1.addEdge("test", v2);
        Edge edge2 = v1.addEdge("test", v3);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        edge1.remove();
        edge2.remove();
        v1.remove();
        v2.remove();
        v3.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().count().next().intValue());
    }

    @Test
    public void testBatchRemoveVerticesEdgesMustBeGone() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person");
        Edge edge1 = v1.addEdge("test", v2);
        Edge edge2 = v1.addEdge("test", v3);
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        v1.remove();
        v2.remove();
        v3.remove();
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().count().next().intValue());
    }

    @Test
    public void testDeletePerformance() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlgGraph.tx().batchModeOn();
        //32767
        int j = 1;
        //create 280 foreign keys
        for (int i = 0; i < 2810; i++) {
            Vertex v1 = this.sqlgGraph.addVertex(T.label, "public.WorkspaceElement", "name", "workspaceElement" + i);
            if (j == 281) {
                j = 1;
            }
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "huawei.NetworkElement", "name", "networkElement" + i + "_" + j);
            v2.addEdge("WorkspaceElement_NetworkElement" + j, v1);
            j++;
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        List<Vertex> vertexes = this.sqlgGraph.traversal().V().has(T.label, "WorkspaceElement").toList();
        for (Vertex sqlgVertex : vertexes) {
            sqlgVertex.remove();
        }
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().count().next().intValue());
        vertexes = this.sqlgGraph.traversal().V().<SqlgVertex>has(T.label, "huawei.NetworkElement").toList();
        for (Vertex sqlgVertex : vertexes) {
            sqlgVertex.remove();
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().count().next().intValue());
    }

    @Test
    public void testDropForeignKeys() {
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        Vertex workspaceElementBsc = this.sqlgGraph.addVertex(T.label, "WorkspaceElement", "name", "bsc1");
        Vertex networkElementBsc = this.sqlgGraph.addVertex(T.label, "bsc", "name", "bsc1");
        Vertex workspaceElementCell1 = this.sqlgGraph.addVertex(T.label, "WorkspaceElement", "name", "cell1");
        Vertex networkElementCell1 = this.sqlgGraph.addVertex(T.label, "cell", "name", "cell1");
        Vertex workspaceElementCell2 = this.sqlgGraph.addVertex(T.label, "WorkspaceElement", "name", "cell2");
        Vertex networkElementCell2 = this.sqlgGraph.addVertex(T.label, "cell", "name", "cell2");
        Vertex workspaceElementBsctmr1 = this.sqlgGraph.addVertex(T.label, "WorkspaceElement", "name", "bsctmr1");
        Vertex networkElementBsctmr1 = this.sqlgGraph.addVertex(T.label, "bsctmr", "name", "bsctms1");
        //add edges to workspaceelement
        networkElementBsc.addEdge("bsc_workspaceElement", workspaceElementBsc);
        networkElementCell1.addEdge("cell_workspaceElement", workspaceElementCell1);
        networkElementCell2.addEdge("cell_workspaceElement", workspaceElementCell2);
        networkElementBsctmr1.addEdge("bsctmr_workspaceElement", workspaceElementBsctmr1);

        //add edges to between elements
        networkElementBsc.addEdge("bsc_cell", networkElementCell1);
        networkElementBsc.addEdge("bsc_cell", networkElementCell2);
        networkElementBsc.addEdge("bsc_bsctmr", networkElementBsctmr1);

        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();

        List<Vertex> vertexes = this.sqlgGraph.traversal().V().<SqlgVertex>has(T.label, "WorkspaceElement").toList();
        for (Vertex sqlgVertex : vertexes) {
            sqlgVertex.remove();
        }
        vertexes = this.sqlgGraph.traversal().V().<SqlgVertex>has(T.label, "bsc").toList();
        for (Vertex sqlgVertex : vertexes) {
            sqlgVertex.remove();
        }
        vertexes = this.sqlgGraph.traversal().V().<SqlgVertex>has(T.label, "cell").toList();
        for (Vertex sqlgVertex : vertexes) {
            sqlgVertex.remove();
        }
        vertexes = this.sqlgGraph.traversal().V().<SqlgVertex>has(T.label, "bsctmr").toList();
        for (Vertex sqlgVertex : vertexes) {
            sqlgVertex.remove();
        }
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testBatchDeleteVertexNewlyAdded() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "test1.Person", "name", "john");
        for (int i = 0; i < 100; i++) {
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "test2.Car", "model", "vw");
            v1.addEdge("car", v2, "bought", 1);
        }
        List<Vertex> cars = vertexTraversal(v1).out("car").toList();
        for (int i = 0; i < 50; i++) {
            cars.get(i).remove();
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(51, this.sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(50, this.sqlgGraph.traversal().E().count().next().intValue());
    }

    @Test
    public void testBatchDeleteEdgeNewlyAdded() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "test1.Person", "name", "john");
        for (int i = 0; i < 100; i++) {
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "test2.Car", "model", "vw");
            v1.addEdge("car", v2, "bought", 1);
        }
        List<Edge> cars = vertexTraversal(v1).outE("car").toList();
        for (int i = 0; i < 50; i++) {
            cars.get(i).remove();
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(101, this.sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(50, this.sqlgGraph.traversal().E().count().next().intValue());
    }

    @Test
    public void testNullEdge() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        Assert.assertEquals(0, vertexTraversal(v1).out("cars").count().next().intValue());
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Car", "dummy", "a");
        v1.addEdge("cars", v2);
        Assert.assertEquals(1, vertexTraversal(v1).out("cars").count().next().intValue());
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, vertexTraversal(v1).out("cars").count().next().intValue());
    }

    @Test
    public void testBatchModeStuffsUpProperties() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "b");
        Assert.assertEquals("a", v1.value("name"));
        Assert.assertEquals("b", v2.value("name"));
    }

    @Test
    public void testBatchUpdateDifferentPropertiesDifferentRows() {

        Vertex sqlgVertex1 = this.sqlgGraph.addVertex(T.label, "Person", "property1", "a1", "property2", "b1", "property3", "c1");
        Vertex sqlgVertex2 = this.sqlgGraph.addVertex(T.label, "Person", "property1", "a2", "property2", "b2", "property3", "c2");
        Vertex sqlgVertex3 = this.sqlgGraph.addVertex(T.label, "Person", "property1", "a3", "property2", "b3", "property3", "c3");
        this.sqlgGraph.tx().commit();

        sqlgVertex1 = this.sqlgGraph.v(sqlgVertex1.id());
        Assert.assertEquals("a1", sqlgVertex1.value("property1"));
        Assert.assertEquals("b1", sqlgVertex1.value("property2"));
        Assert.assertEquals("c1", sqlgVertex1.value("property3"));

        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().batchModeOn();
        sqlgVertex1 = this.sqlgGraph.v(sqlgVertex1.id());
        sqlgVertex1.property("property1", "a11");
        sqlgVertex2.property("property2", "b22");
        sqlgVertex3.property("property3", "b33");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals("a11", sqlgVertex1.value("property1"));
        Assert.assertEquals("b1", sqlgVertex1.value("property2"));
        Assert.assertEquals("c1", sqlgVertex1.value("property3"));

        sqlgVertex1 = this.sqlgGraph.v(sqlgVertex1.id());
        Assert.assertEquals("a11", sqlgVertex1.value("property1"));
        Assert.assertEquals("b1", sqlgVertex1.value("property2"));
        Assert.assertEquals("c1", sqlgVertex1.value("property3"));

    }

    @Test
    public void testBatchUpdateNewVertex() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "property1", "a");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v2.property("property2", "bb");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals("a", v1.value("property1"));
        Assert.assertFalse(v1.property("property2").isPresent());
        Assert.assertFalse(v2.property("property1").isPresent());
        Assert.assertEquals("bb", v2.value("property2"));

    }

    @Test
    public void testBatchRemoveManyEdgesTestPostgresLimit() {
        this.sqlgGraph.tx().batchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
        for (int i = 0; i < 1000000; i++) {
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "dummy", "a");
            Edge edge1 = v1.addEdge("test", v2);
        }
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1000001, this.sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(1000000, this.sqlgGraph.traversal().E().count().next().intValue());
        this.sqlgGraph.tx().rollback();
        this.sqlgGraph.tx().batchModeOn();
        vertexTraversal(v1).outE("test").forEachRemaining(Edge::remove);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1000001, this.sqlgGraph.traversal().V().count().next().intValue());
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().count().next().intValue());
    }

////    @Test
//    public void testPerformance1() {
//        this.sqlgGraph.tx().batchModeOn();
//        for (int i = 0; i < 1000000; i++) {
//            Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a" + i);
//            Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "b" + i);
//            person1.addEdge("friend", person2, "context", 1);
//            if (i != 0 && i % 100000 == 0) {
//                this.sqlgGraph.tx().commit();
//                this.sqlgGraph.tx().batchModeOn();
//            }
//        }
//        this.sqlgGraph.tx().commit();
//    }
//
    @Test
    public void testNoProperties() {
        this.sqlgGraph.tx().batchModeOn();
        for (int i = 0; i < 100; i++) {
            Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person");
            Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person");
            person1.addEdge("friend", person2);
            if (i != 0 && i % 100 == 0) {
                this.sqlgGraph.tx().commit();
                this.sqlgGraph.tx().batchModeOn();
            }
        }
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testBatchEdgeLoadProperty() {
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD");
        Edge edgeToRoot = root.addEdge("edgeToRoot", god);
        edgeToRoot.property("className", "thisthatandanother");
        this.sqlgGraph.tx().commit();
    }

    @Test
    public void testBatchArrayString() {
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new String[]{"name1", "name2"});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Iterator<Object> array = god.values("array");
        Assert.assertTrue(array.hasNext());
        Object o = array.next();
        Assert.assertTrue(o instanceof String[]);
        List<String> list = Arrays.asList((String[]) o);
        Assert.assertEquals(2, list.size());
        Assert.assertTrue(list.contains("name1"));
        Assert.assertTrue(list.contains("name2"));
    }

    @Test
    public void testBatchArrayInteger() {
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new int[]{1, 2});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Iterator<Object> array = god.values("array");
        Assert.assertTrue(array.hasNext());
        Object o = array.next();
        Assert.assertTrue(o instanceof int[]);
        List<Integer> list = Arrays.asList(ArrayUtils.toObject((int[]) o));
        Assert.assertEquals(2, list.size());
        Assert.assertTrue(list.contains(1));
        Assert.assertTrue(list.contains(2));
    }

    @Test
    public void testBatchArrayDouble() {
        Vertex root = this.sqlgGraph.addVertex(T.label, "ROOT");
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.tx().batchModeOn();
        Vertex god = this.sqlgGraph.addVertex(T.label, "GOD", "array", new double[]{1, 2});
        this.sqlgGraph.tx().commit();
        god = this.sqlgGraph.traversal().V(god.id()).next();
        Iterator<Object> array = god.values("array");
        Assert.assertTrue(array.hasNext());
        Object o = array.next();
        Assert.assertTrue(o instanceof double[]);
        List<Double> list = Arrays.asList(ArrayUtils.toObject((double[]) o));
        Assert.assertEquals(2, list.size());
        Assert.assertTrue(list.contains(1d));
        Assert.assertTrue(list.contains(2d));
    }

}
