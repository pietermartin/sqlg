package org.umlg.sqlg.test.doctests;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

/**
 * Date: 2014/12/14
 * Time: 3:29 PM
 */
public class TestForDocs extends BaseTest {

//    //Create a 10000 objects, each with 2 properties
//    @Test
//    public void testAddPersons() {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        for (int i = 0; i < 10000; i++) {
//            this.sqlgGraph.addVertex(T.label, "Person", "prop1", "property1", "prop2", "property2");
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println("Time to insert: " + stopWatch.toString());
//        stopWatch.reset();
//        stopWatch.start();
//        Assert.assertEquals(Long.valueOf(10000), this.sqlgGraph.traversal().V().has(T.label, "Person").count().next());
//        stopWatch.stop();
//        System.out.println("Time to read: " + stopWatch.toString());
//    }
//
//    //Create a 10001 Persons, each with 2 properties and one friend
//    @Test
//    public void testAddPersonAndFriends() {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        Vertex previous = this.sqlgGraph.addVertex(T.label, "Person", "name", "first");
//
//        for (int i = 0; i < 10000; i++) {
//            Vertex current = this.sqlgGraph.addVertex(T.label, "Person", "name", "current" + i);
//            previous.addEdge("friend", current);
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println("Time to insert: " + stopWatch.toString());
//        stopWatch.reset();
//        stopWatch.start();
//        List<Vertex> persons = this.sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").toList();
//        Map<Vertex, List<Vertex>> friendMap = new HashMap<>();
//        persons.forEach(
//                p -> friendMap.put(p, p.in("friend").toList())
//        );
//        Assert.assertEquals(10001, friendMap.size());
//        stopWatch.stop();
//        System.out.println("Time to read all vertices: " + stopWatch.toString());
//    }
//
//    @Test
//    public void testPostgresBatchMode() {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        for (int i = 1; i < 1000001; i++) {
//            Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "John" + i);
//            Vertex dog = this.sqlgGraph.addVertex(T.label, "Dog", "name", "snowy" + i);
//            person.addEdge("pet", dog);
//            if (i % 100000 == 0) {
//                this.sqlgGraph.tx().commit();
//                this.sqlgGraph.tx().normalBatchModeOn();
//            }
//        }
//        this.sqlgGraph.tx().commit();
//        stopWatch.stop();
//        System.out.println("Time to insert: " + stopWatch.toString());
//        stopWatch.reset();
//        stopWatch.start();
//
//        Assert.assertEquals(1000000, this.sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").count().next().intValue());
//        Assert.assertEquals(1000000, this.sqlgGraph.traversal().V().<Vertex>has(T.label, "Dog").count().next().intValue());
//
//        stopWatch.stop();
//        System.out.println("Time to read all vertices: " + stopWatch.toString());
//    }

    @Test
    public void testHsqldbLargeLoad() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (int i = 1; i < 1000001; i++) {
            Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "John" + i);
            Vertex dog = this.sqlgGraph.addVertex(T.label, "Dog", "name", "snowy" + i);
            person.addEdge("pet", dog);
            if (i % 100000 == 0) {
                this.sqlgGraph.tx().commit();
            }
        }
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        System.out.println("Time to insert: " + stopWatch.toString());
        stopWatch.reset();
        stopWatch.start();

        Assert.assertEquals(1000000, this.sqlgGraph.traversal().V().<Vertex>has(T.label, "Person").count().next().intValue());
        Assert.assertEquals(1000000, this.sqlgGraph.traversal().V().<Vertex>has(T.label, "Dog").count().next().intValue());

        stopWatch.stop();
        System.out.println("Time to read all vertices: " + stopWatch.toString());
    }


}
