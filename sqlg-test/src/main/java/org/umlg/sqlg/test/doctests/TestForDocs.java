package org.umlg.sqlg.test.doctests;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2014/12/14
 * Time: 3:29 PM
 */
public class TestForDocs extends BaseTest {

//    @Test
    public void showGraphStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c2);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V()
                .hasLabel("A")
                .out()
                .out();
        System.out.println(traversal);
        traversal.hasNext();
        System.out.println(traversal);
        List<Vertex> c = traversal.toList();
        assertEquals(2, c.size());
    }

//    @Test
    public void showHasStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c2);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V()
                .hasLabel("A")
                .out().has("name", "b1")
                .out();
        System.out.println(traversal);
        traversal.hasNext();
        System.out.println(traversal);
        List<Vertex> c = traversal.toList();
        assertEquals(1, c.size());
    }

//    @Test
    public void showOrStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
        Vertex c5 = this.sqlgGraph.addVertex(T.label, "C", "name", "c5");
        Vertex c6 = this.sqlgGraph.addVertex(T.label, "C", "name", "c6");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c2);
        b2.addEdge("bc", c3);
        b2.addEdge("bc", c4);
        b2.addEdge("bc", c5);
        b2.addEdge("bc", c6);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V()
                .hasLabel("A")
                .out()
                .out()
                .or(
                        __.has("name", "c1"),
                        __.has("name", "c3"),
                        __.has("name", "c6")
                );

        System.out.println(traversal);
        traversal.hasNext();
        System.out.println(traversal);
        List<Vertex> c = traversal.toList();
        assertEquals(3, c.size());
    }

    @Test
    public void showAndStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1", "surname", "x", "address", "y");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2", "surname", "x", "address", "y");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3", "surname", "x", "address", "y");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4", "surname", "x", "address", "y");
        Vertex c5 = this.sqlgGraph.addVertex(T.label, "C", "name", "c5", "surname", "x", "address", "y");
        Vertex c6 = this.sqlgGraph.addVertex(T.label, "C", "name", "c6", "surname", "x", "address", "y");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c2);
        b2.addEdge("bc", c3);
        b2.addEdge("bc", c4);
        b2.addEdge("bc", c5);
        b2.addEdge("bc", c6);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V()
                .hasLabel("A")
                .out()
                .out()
                .and(
                        __.has("name", "c1"),
                        __.has("surname", "x"),
                        __.has("address", "y")
                );

        System.out.println(traversal);
        traversal.hasNext();
        System.out.println(traversal);
        List<Vertex> c = traversal.toList();
        assertEquals(1, c.size());
    }


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
//        Assert.assertEquals(Long.valueOf(10000), this.sqlgGraph.traversal().V().existVertexLabel(T.label, "Person").count().next());
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
//        List<Vertex> persons = this.sqlgGraph.traversal().V().<Vertex>existVertexLabel(T.label, "Person").toList();
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
//        Assert.assertEquals(1000000, this.sqlgGraph.traversal().V().<Vertex>existVertexLabel(T.label, "Person").count().next().intValue());
//        Assert.assertEquals(1000000, this.sqlgGraph.traversal().V().<Vertex>existVertexLabel(T.label, "Dog").count().next().intValue());
//
//        stopWatch.stop();
//        System.out.println("Time to read all vertices: " + stopWatch.toString());
//    }
//
//    @Test
//    public void testHsqldbLargeLoad() {
//        StopWatch stopWatch = new StopWatch();
//        stopWatch.start();
//        for (int i = 1; i < 1000001; i++) {
//            Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "name", "John" + i);
//            Vertex dog = this.sqlgGraph.addVertex(T.label, "Dog", "name", "snowy" + i);
//            person.addEdge("pet", dog);
//            if (i % 100000 == 0) {
//                this.sqlgGraph.tx().commit();
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


}
