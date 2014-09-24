package org.umlg.sqlg.test.batch;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SchemaTable;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
            Vertex v1 = this.sqlG.addVertex(T.label, "Person", "name", "marko" + i);
            Vertex v2 = this.sqlG.addVertex(T.label, "Person", "name", "marko" + i);
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
        Vertex v1 = this.sqlG.addVertex(T.label, "Person", "name", "marko");
        Vertex v2 = this.sqlG.addVertex(T.label, "Person", "name", "peter");
        Vertex v3 = this.sqlG.addVertex(T.label, "Person", "name", "john");
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
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.tx().rollback();
        this.sqlG.tx().batchModeOn();
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.addVertex(T.label, "Person");
        this.sqlG.addVertex(T.label, "Person");
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
                    Vertex v1 = sqlG.addVertex(T.label, "Person");
                    Vertex v2 = sqlG.addVertex(T.label, "Person");
                    v1.addEdge("Friend", v2);
                }
                System.out.println("thread1 starting commit!");
                firstLatch.countDown();
                sqlG.tx().commit();
                List<Vertex> persons = sqlG.V().<Vertex>has(T.label, "Person").toList();
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
                Vertex v1 = sqlG.addVertex(T.label, "Car");
                Vertex v2 = sqlG.addVertex(T.label, "Car");
                v1.addEdge("Same", v2);
                sqlG.tx().commit();
                List<Vertex> cars = sqlG.V().<Vertex>has(T.label, "Car").toList();
                lastCar.set((Long) cars.get(cars.size() - 1).id());
                System.out.println("second thread done!");
            }
        };
        thread2.start();
        thread1.join();
        thread2.join();
        Assert.assertEquals(200000, lastPerson.get() - (Long) firstVertex.id());
        Assert.assertEquals(200002, lastCar.get() - (Long) firstVertex.id());
    }

    @Test
    public void testVertexProperties() {
        List<Short> shortList = new ArrayList<>();
        List<Integer> integerList = new ArrayList<>();
        List<Long> longList = new ArrayList<>();
        List<Double> doubleList = new ArrayList<>();
        this.sqlG.tx().batchModeOn();
        for (int i = 0; i < 100; i++) {
            this.sqlG.addVertex(T.label, "Person",
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
        this.sqlG.tx().commit();
        List<Vertex> vertices = this.sqlG.V().toList();
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
        this.sqlG.tx().batchModeOn();
        for (int i = 0; i < 100; i++) {
            Vertex v1 = this.sqlG.addVertex(T.label, "Person", "name", "marko");
            Vertex v2 = this.sqlG.addVertex(T.label, "Person", "name", "john");
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
        this.sqlG.tx().commit();
        List<Edge> edges = this.sqlG.E().toList();
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
        this.sqlG.tx().batchModeOn();
        Vertex v1 = this.sqlG.addVertex(T.label, "Person", "name", "marko");
        v1.property("name", "john");
        this.sqlG.tx().commit();
        Assert.assertEquals("john", v1.value("name"));
        Assert.assertEquals("john", this.sqlG.V().next().value("name"));
        v1 = this.sqlG.v(v1.id());
        Assert.assertEquals("john", v1.value("name"));
        Assert.assertEquals("john", this.sqlG.V().next().value("name"));
    }

    @Test
    public void testAddPropertyToInsertedVertexProperty() {
        this.sqlG.tx().batchModeOn();
        Vertex v1 = this.sqlG.addVertex(T.label, "Person", "name", "marko");
        v1.property("name", "john");
        v1.property("surname", "aaaa");
        this.sqlG.tx().commit();
        v1 = this.sqlG.v(v1.id());
        Assert.assertEquals("john", v1.value("name"));
        Assert.assertEquals("aaaa", v1.value("surname"));
        Assert.assertEquals("john", this.sqlG.V().next().value("name"));
    }

    @Test
    public void testUpdateInsertedEdgeProperty() {
        this.sqlG.tx().batchModeOn();
        Vertex v1 = this.sqlG.addVertex(T.label, "Person", "name", "marko");
        Vertex v2 = this.sqlG.addVertex(T.label, "Person", "name", "john");
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
        Vertex marko = this.sqlG.addVertex(T.label, "Person", "name", "marko");
        Vertex john = this.sqlG.addVertex(T.label, "Person", "name", "john");
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
        Vertex root = this.sqlG.addVertex(T.label, "ROOT");
        Vertex god = this.sqlG.addVertex(T.label, "God");
        root.addEdge("rootGod", god);
        this.sqlG.tx().commit();
    }

    @Test
    public void testGetEdges() {
        this.sqlG.tx().batchModeOn();
        Vertex root = this.sqlG.addVertex(T.label, "ROOT");
        Vertex god = this.sqlG.addVertex(T.label, "God");
        Edge sqlgEdge = root.addEdge("rootGod", god);
        Assert.assertEquals(sqlgEdge, root.outE("rootGod").next());
        this.sqlG.tx().commit();
    }

    @Test
    public void testGetVertices() {
        this.sqlG.tx().batchModeOn();
        Vertex root = this.sqlG.addVertex(T.label, "ROOT");
        Vertex god = this.sqlG.addVertex(T.label, "God");
        Vertex human = this.sqlG.addVertex(T.label, "Human");
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
            Vertex person = this.sqlG.addVertex(T.label, "Person", "name", "marko" + i);
            Vertex spaceTime = this.sqlG.addVertex(T.label, "SpaceTime", "name", "marko" + i);
            Vertex space = this.sqlG.addVertex(T.label, "Space", "name", "marko" + i);
            Vertex time = this.sqlG.addVertex(T.label, "Time", "name", "marko" + i);
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
        Assert.assertEquals(10000, this.sqlG.V().has(T.label, "Person").count().next(), 0);
        Assert.assertEquals(10000, this.sqlG.V().has(T.label, "SpaceTime").count().next(), 0);
        Assert.assertEquals(10000, this.sqlG.V().has(T.label, "Space").count().next(), 0);
        Assert.assertEquals(10000, this.sqlG.V().has(T.label, "Time").count().next(), 0);
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    @Test
    public void testGetVerticesWithHas() {
        this.sqlG.tx().batchModeOn();
        Vertex root = this.sqlG.addVertex(T.label, "ROOT");
        Vertex jah = this.sqlG.addVertex(T.label, "God", "name", "Jah");
        Vertex jehova = this.sqlG.addVertex(T.label, "God", "name", "Jehova");
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
        Vertex root = this.sqlG.addVertex(T.label, "ROOT");
        this.sqlG.tx().commit();
        this.sqlG.tx().batchModeOn();
        Vertex jah = this.sqlG.addVertex(T.label, "God", "name", "Jah");
        Vertex jehova = this.sqlG.addVertex(T.label, "God", "name", "Jehova");
        root.addEdge("rootGod", jah);
        root.addEdge("rootGod", jehova);
        this.sqlG.tx().commit();
        List<Vertex> vertices = root.out("rootGod").toList();
        Assert.assertTrue(vertices.contains(jah));
        Assert.assertTrue(vertices.contains(jehova));
        Assert.assertEquals(jah, root.out("rootGod").has("name", "Jah").next());
        Assert.assertEquals(jehova, root.out("rootGod").has("name", "Jehova").next());
    }

    @Test
    public void testVertexMultipleEdgesLabels() {
        this.sqlG.tx().batchModeOn();
        Vertex person = this.sqlG.addVertex(T.label, "Person", "name", "a");
        Vertex car = this.sqlG.addVertex(T.label, "Car", "name", "b");
        Vertex bike = this.sqlG.addVertex(T.label, "Bike", "name", "c");
        person.addEdge("car", car);
        person.addEdge("bike", bike);
        this.sqlG.tx().commit();
        Assert.assertEquals(2, person.out().count().next(), 0);
    }

    @Test
    public void testAddEdgeAccrossSchema() {
        this.sqlG.tx().batchModeOn();
        Vertex person = this.sqlG.addVertex(T.label, "Schema1.Person", "name", "a");
        Vertex car = this.sqlG.addVertex(T.label, "Schema2.Car", "name", "b");
        Vertex bike = this.sqlG.addVertex(T.label, "Schema2.Bike", "name", "c");
        this.sqlG.tx().commit();
        this.sqlG.tx().batchModeOn();
        person.addEdge("car", car);
        person.addEdge("bike", bike);
        this.sqlG.tx().commit();
        Assert.assertEquals(2, person.out().count().next(), 0);
    }

    @Test
    public void testBatchCommit() {
        this.sqlG.tx().batchModeOn();
        for (int i = 0; i < 111; i++) {
            this.sqlG.addVertex(T.label, "Person1", "name", i);
            this.sqlG.addVertex(T.label, "Person2", "name", i);
        }
        Map<SchemaTable, Pair<Long, Long>> result = this.sqlG.tx().batchCommit();
        Assert.assertEquals(1l, result.get(SchemaTable.of("public", "Person1")).getLeft(), 0);
        Assert.assertEquals(111l, result.get(SchemaTable.of("public", "Person1")).getRight(), 0);
        Assert.assertEquals(112l, result.get(SchemaTable.of("public", "Person2")).getLeft(), 0);
        Assert.assertEquals(222l, result.get(SchemaTable.of("public", "Person2")).getRight(), 0);

        this.sqlG.tx().batchModeOn();
        for (int i = 0; i < 111; i++) {
            this.sqlG.addVertex(T.label, "Person1", "name", i);
            this.sqlG.addVertex(T.label, "Person2", "name", i);
        }
        result = this.sqlG.tx().batchCommit();
        Assert.assertEquals(223l, result.get(SchemaTable.of("public", "Person1")).getLeft(), 0);
        Assert.assertEquals(333l, result.get(SchemaTable.of("public", "Person1")).getRight(), 0);
        Assert.assertEquals(334l, result.get(SchemaTable.of("public", "Person2")).getLeft(), 0);
        Assert.assertEquals(444l, result.get(SchemaTable.of("public", "Person2")).getRight(), 0);

    }

    @Test
    public void testCacheAndUpdateVERTICESLabels() {
        this.sqlG.tx().batchModeOn();
        Vertex person1 = this.sqlG.addVertex(T.label, "Person", "name", "person");
        List<Vertex> cache = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            cache.add(this.sqlG.addVertex(T.label, "Person", "name", "person" + i));
        }
        this.sqlG.tx().commit();
        this.sqlG.tx().batchModeOn();
        for (Vertex person2 : cache) {
            person1.addEdge("Friend", person2);
        }
        this.sqlG.tx().commit();
        person1 = this.sqlG.v(person1.id());
        Assert.assertTrue(person1.out("Friend").hasNext());
        Assert.assertEquals(10000, person1.out("Friend").count().next(), 0);
        List<Vertex> friends = person1.out("Friend").toList();
        List<String> names = friends.stream().map(v->v.<String>value("name")).collect(Collectors.toList());
        Assert.assertEquals(10000, names.size(), 0);
        for (int i = 0; i < 10000; i++) {
            Assert.assertTrue(names.contains("person" + i));
        }
    }

    @Test
    public void testBatchInsertDifferentKeys() {
        this.sqlG.tx().batchModeOn();
        Vertex v1 = this.sqlG.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlG.addVertex(T.label, "Person", "surname", "b");
        this.sqlG.tx().commit();

        List<Vertex> persons = this.sqlG.V().<Vertex>has(T.label, "Person").<Vertex>has("name", "a").toList();
        Assert.assertEquals(1, persons.size());
        Assert.assertFalse(persons.get(0).property("surname").isPresent());

        persons = this.sqlG.V().<Vertex>has(T.label, "Person").<Vertex>has("surname", "b").toList();
        Assert.assertEquals(1, persons.size());
        Assert.assertFalse(persons.get(0).property("name").isPresent());

        persons = this.sqlG.V().<Vertex>has(T.label, "Person").has("surname", "b").<Vertex>has("name", "a").toList();
        Assert.assertEquals(0, persons.size());
    }

    @Test
    public void testVerticesOutLabelsForPersistentVertices() {
        Vertex realWorkspace = this.sqlG.addVertex(T.label, "RealWorkspace", "name", "realWorkspace1");
        Vertex softwareVersion = this.sqlG.addVertex(T.label, "SoftwareVersion", "name", "R15");
        Vertex vendorTechnology = this.sqlG.addVertex(T.label, "VendorTechnology", "name", "Huawei_Gsm");
        vendorTechnology.addEdge("vendorTechnology_softwareVersion", softwareVersion);
        this.sqlG.tx().commit();

        Assert.assertEquals("Huawei_Gsm", softwareVersion.in("vendorTechnology_softwareVersion").next().value("name"));

        this.sqlG.tx().rollback();
        this.sqlG.tx().batchModeOn();
        Vertex rwe1 = this.sqlG.addVertex(T.label, "RWE", "name", "cell1");
        rwe1.addEdge("workspaceElement_softwareVersion", softwareVersion);
        this.sqlG.tx().commit();

        softwareVersion = this.sqlG.v(softwareVersion.id());
        Assert.assertEquals("Huawei_Gsm", softwareVersion.in("vendorTechnology_softwareVersion").next().value("name"));
    }

    @Test
    public void testVerticesInLabelsForPersistentVertices() {
        Vertex realWorkspace = this.sqlG.addVertex(T.label, "RealWorkspace", "name", "realWorkspace1");
        Vertex softwareVersion = this.sqlG.addVertex(T.label, "SoftwareVersion", "name", "R15");
        Vertex vendorTechnology = this.sqlG.addVertex(T.label, "VendorTechnology", "name", "Huawei_Gsm");
        softwareVersion.addEdge("softwareVersion_vendorTechnology", vendorTechnology);
        this.sqlG.tx().commit();

        Assert.assertEquals("Huawei_Gsm", softwareVersion.out("softwareVersion_vendorTechnology").next().value("name"));

        this.sqlG.tx().rollback();
        this.sqlG.tx().batchModeOn();
        Vertex rwe1 = this.sqlG.addVertex(T.label, "RWE", "name", "cell1");
        rwe1.addEdge("workspaceElement_softwareVersion", softwareVersion);
        this.sqlG.tx().commit();

        softwareVersion = this.sqlG.v(softwareVersion.id());
        Assert.assertEquals("Huawei_Gsm", softwareVersion.out("softwareVersion_vendorTechnology").next().value("name"));
    }

    @Test
    public void testBatchUpdatePersistentVertices() {
        Vertex v1 = this.sqlG.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlG.addVertex(T.label, "Person", "surname", "b");
        this.sqlG.tx().commit();
        Assert.assertEquals("a", this.sqlG.v(v1.id()).value("name"));
        Assert.assertEquals("b", this.sqlG.v(v2.id()).value("surname"));

        this.sqlG.tx().rollback();
        this.sqlG.tx().batchModeOn();
        v1.property("name", "aa");
        v2.property("surname", "bb");
        this.sqlG.tx().commit();

        Assert.assertEquals("aa", this.sqlG.v(v1.id()).value("name"));
        Assert.assertEquals("bb", this.sqlG.v(v2.id()).value("surname"));
    }

    @Test
    public void testBatchUpdatePersistentVerticesAllTypes() {
        Vertex v1 = this.sqlG.addVertex(T.label, "Person", "name", "a");
        Vertex v2 = this.sqlG.addVertex(T.label, "Person", "surname", "b");
        this.sqlG.tx().commit();
        Assert.assertEquals("a", this.sqlG.v(v1.id()).value("name"));
        Assert.assertEquals("b", this.sqlG.v(v2.id()).value("surname"));

        this.sqlG.tx().rollback();
        this.sqlG.tx().batchModeOn();
        v1.property("name", "aa");
        v1.property("boolean", true);
        v1.property("short", (short)1);
        v1.property("integer", 1);
        v1.property("long", 1L);
        v1.property("float", 1F);
        v1.property("double", 1D);

        v2.property("surname", "bb");
        v2.property("boolean", false);
        v2.property("short", (short)2);
        v2.property("integer", 2);
        v2.property("long", 2L);
        v2.property("float", 2F);
        v2.property("double", 2D);
        this.sqlG.tx().commit();

        Assert.assertEquals("aa", this.sqlG.v(v1.id()).value("name"));
        Assert.assertEquals(true, this.sqlG.v(v1.id()).value("boolean"));
        Assert.assertEquals((short)1, this.sqlG.v(v1.id()).<Short>value("short").shortValue());
        Assert.assertEquals(1, this.sqlG.v(v1.id()).<Integer>value("integer").intValue());
        Assert.assertEquals(1L, this.sqlG.v(v1.id()).<Long>value("long").longValue(), 0);
        Assert.assertEquals(1F, this.sqlG.v(v1.id()).<Float>value("float").floatValue(), 0);
        Assert.assertEquals(1D, this.sqlG.v(v1.id()).<Double>value("double").doubleValue(), 0);

        Assert.assertEquals("bb", this.sqlG.v(v2.id()).value("surname"));
        Assert.assertEquals(false, this.sqlG.v(v2.id()).value("boolean"));
        Assert.assertEquals((short)2, this.sqlG.v(v2.id()).<Short>value("short").shortValue());
        Assert.assertEquals(2, this.sqlG.v(v2.id()).<Integer>value("integer").intValue());
        Assert.assertEquals(2L, this.sqlG.v(v2.id()).<Long>value("long").longValue(), 0);
        Assert.assertEquals(2F, this.sqlG.v(v2.id()).<Float>value("float").floatValue(), 0);
        Assert.assertEquals(2D, this.sqlG.v(v2.id()).<Double>value("double").doubleValue(), 0);
    }

    @Test
    public void testBatchUpdatePersistentVerticesPerformance1() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        int count = 1;
        for (int i = 0; i < 100000; i++) {
            this.sqlG.addVertex(T.label, "Person", "name", "a");
            if (count++ % 10000 == 0) {
                this.sqlG.tx().commit();
            }

        }
        this.sqlG.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();

        List<Vertex> vertices = this.sqlG.V().toList();
        count = 1;
        for (Vertex v : vertices) {
            v.property("name", "b");
            if (count++ % 10000 == 0) {
                this.sqlG.tx().commit();
            }
        }
        this.sqlG.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    @Test
    public void testBatchUpdatePersistentVerticesPerformance2() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        this.sqlG.tx().batchModeOn();
        for (int i = 0; i < 100000; i++) {
            this.sqlG.addVertex(T.label, "Person", "name", "a");
        }
        this.sqlG.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();
        this.sqlG.tx().batchModeOn();
        List<Vertex> vertices = this.sqlG.V().toList();
        for (Vertex v : vertices) {
            v.property("name", "b");
        }
        this.sqlG.tx().commit();
        stopWatch.stop();
        System.out.println(stopWatch.toString());
    }

    //batch mode on is ignored if the graph does not support batch mode
//    @Test(expected = IllegalStateException.class)
//    public void testBatchModeNeedsCleanTransactionFail() {
//        this.sqlG.addVertex(T.label, "Person");
//        this.sqlG.tx().batchModeOn();
//        this.sqlG.addVertex(T.label, "Person");
//    }

}
