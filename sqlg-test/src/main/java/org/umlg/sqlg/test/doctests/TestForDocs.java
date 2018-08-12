package org.umlg.sqlg.test.doctests;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2014/12/14
 * Time: 3:29 PM
 */
public class TestForDocs extends BaseTest {

    @Test
    public void testStrategy2VertexStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        a3.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, String> t = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .limit(2)
                .out()
                .values("name");
        printTraversalForm(t);
        List<String> result = t.toList();
        for (String name : result) {
            System.out.println(name);
        }
    }

    //    @Test
    public void showContainsPredicate() {
        List<Integer> numbers = new ArrayList<>(10000);
        for (int i = 0; i < 10000; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "number", i);
            numbers.add(i);
        }
        this.sqlgGraph.tx().commit();

        List<Vertex> persons = this.sqlgGraph.traversal().V()
                .hasLabel("A")
                .has("number", P.within(numbers))
                .toList();

        assertEquals(10000, persons.size());
    }

    //    @Test
    public void showComparePredicates() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b2.addEdge("bc", c3);
        b2.addEdge("bc", c4);
        this.sqlgGraph.tx().commit();

        List<String> result = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .out().has("name", P.eq("b1"))
                .out().has("name", P.eq("c2"))
                .<String>values("name")
                .toList();
        for (String name : result) {
            System.out.println(name);
        }
    }

    //    @Test
    public void testsDropStepWithHas() {
        this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.traversal().V().hasLabel("A").has("name", P.within("a1", "a2")).drop().iterate();
        this.sqlgGraph.tx().commit();

        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
    }

    //    @Test
    public void testsDropStepTrivial() {
        this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.traversal().V().hasLabel("A").drop().iterate();
        this.sqlgGraph.tx().commit();

        assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
    }


    //    @Test
    public void testDropStepWithEdges() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.traversal().V().hasLabel("A").out().drop().iterate();

        assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("B").count().next(), 0);
    }

    //    @Test
    public void testLimitOnVertexLabels() {
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "person" + i);
        }
        this.sqlgGraph.tx().commit();
        List<String> names = this.sqlgGraph.traversal()
                .V().hasLabel("Person")
                .order().by("name")
                .limit(3)
                .<String>values("name")
                .toList();
        for (String name : names) {
            System.out.println(name);
        }
    }

    //    @Test
    public void testRangeOnVertexLabels() {
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "Person", "name", "person" + i);
        }
        this.sqlgGraph.tx().commit();
        List<String> names = this.sqlgGraph.traversal()
                .V().hasLabel("Person")
                .order().by("name")
                .range(1, 4)
                .<String>values("name")
                .toList();
        for (String name : names) {
            System.out.println(name);
        }
    }

    //    @Test
    public void testOrderBy() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "a");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "b");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "c");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "a");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "b");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "c");
        this.sqlgGraph.tx().commit();

        List<Vertex> result = this.sqlgGraph.traversal().V().hasLabel("A")
                .order().by("name", Order.incr).by("surname", Order.decr)
                .toList();
        for (Vertex v : result) {
            System.out.println(v.value("name") + " " + v.value("surname"));
        }
    }

    //    @Test
    public void showChooseStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .choose(__.out(), __.out())
                .path().by("name")
                .toList();
        for (Path path : paths) {
            System.out.println(path);
        }
    }

    //    @Test
    public void showOptionalStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .optional(
                        __.out().optional(
                                __.out()
                        )
                )
                .path().by("name")
                .toList();
        for (Path path : paths) {
            System.out.println(path);
        }
    }

    //    @Test
    public void showRepeatStepEmitLast() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A")
                .repeat(
                        __.out()
                )
                .emit()
                .times(2)
                .path().by("name")
                .toList();
        for (Path path : paths) {
            System.out.println(path);
        }
    }

    //    @Test
    public void showRepeatStepEmitFirst() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A")
                .emit()
                .times(2)
                .repeat(
                        __.out()
                )
                .path().by("name")
                .toList();
        for (Path path : paths) {
            System.out.println(path);
        }
    }

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

    //    @Test
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
