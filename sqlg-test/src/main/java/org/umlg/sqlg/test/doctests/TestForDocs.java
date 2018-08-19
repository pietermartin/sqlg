package org.umlg.sqlg.test.doctests;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

/**
 * Date: 2014/12/14
 * Time: 3:29 PM
 */
@SuppressWarnings("Duplicates")
public class TestForDocs extends BaseTest {

    @Test
    public void testMatch() {
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

        Traversal<Vertex, Map<String, Object>> traversal = this.sqlgGraph.traversal()
                .V()
                .match(
                        __.as("a").in("ab").as("x"),
                        __.as("a").out("bc").as("y")
                );
        printTraversalForm(traversal);

        List<Map<String, Object>> result = traversal.toList();
        for (Map<String, Object> map : result) {
            System.out.println(map.toString());
        }
    }

//    @Test
//    public void testStrategy2WhereStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        a2.addEdge("ab", b1);
//        this.sqlgGraph.tx().commit();
//
//        Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
//                .V().hasLabel("A")
//                .where(
//                        __.out()
//                ).values("name");
//        printTraversalForm(traversal);
//
//        List<String> names = traversal.toList();
//        for (String name : names) {
//            System.out.println(name);
//        }
//    }
//
//    @Test
//    public void testStrategy2NotStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        a1.addEdge("ab", b1);
//        this.sqlgGraph.tx().commit();
//
//        Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
//                .V().hasLabel("A")
//                .not(
//                        __.out()
//                ).values("name");
//        printTraversalForm(traversal);
//
//        List<String> names = traversal.toList();
//        for (String name : names) {
//            System.out.println(name);
//        }
//    }
//
//    @Test
//    public void testStrategy2OrStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        a1.addEdge("ab", b1);
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        a2.addEdge("abb", b2);
//        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a3.addEdge("abbb", b3);
//        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4");
//        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
//        a4.addEdge("abbbb", b4);
//
//
//        Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
//                .V().hasLabel("A")
//                .or(
//                        __.out("ab"),
//                        __.out("abb"),
//                        __.out("abbb")
//                ).values("name");
//        printTraversalForm(traversal);
//
//        List<String> names = traversal.toList();
//        for (String name : names) {
//            System.out.println(name);
//        }
//    }
//
//    @Test
//    public void testStrategy2AndStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        a1.addEdge("ab", b1);
//        a1.addEdge("abb", b1);
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        a2.addEdge("abb", b2);
//        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a3.addEdge("abbb", b3);
//
//        Traversal<Vertex, String> traversal = this.sqlgGraph.traversal().V().hasLabel("A").and(
//                __.out("ab"),
//                __.out("abb")
//        ).values("name");
//        printTraversalForm(traversal);
//
//        List<String> names = traversal.toList();
//        for (String name : names) {
//            System.out.println(name);
//        }
//    }
//
//    @Test
//    public void testStrategy2LocalStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        Vertex c11 = this.sqlgGraph.addVertex(T.label, "C", "name", "c11");
//        Vertex c12 = this.sqlgGraph.addVertex(T.label, "C", "name", "c12");
//        Vertex c13 = this.sqlgGraph.addVertex(T.label, "C", "name", "c13");
//        Vertex c21 = this.sqlgGraph.addVertex(T.label, "C", "name", "c21");
//        Vertex c22 = this.sqlgGraph.addVertex(T.label, "C", "name", "c22");
//        Vertex c23 = this.sqlgGraph.addVertex(T.label, "C", "name", "c23");
//        Vertex c31 = this.sqlgGraph.addVertex(T.label, "C", "name", "c31");
//        Vertex c32 = this.sqlgGraph.addVertex(T.label, "C", "name", "c32");
//        Vertex c33 = this.sqlgGraph.addVertex(T.label, "C", "name", "c33");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        b1.addEdge("bc", c11);
//        b1.addEdge("bc", c12);
//        b1.addEdge("bc", c13);
//        b2.addEdge("bc", c21);
//        b2.addEdge("bc", c22);
//        b2.addEdge("bc", c23);
//        b3.addEdge("bc", c31);
//        b3.addEdge("bc", c32);
//        b3.addEdge("bc", c33);
//        this.sqlgGraph.tx().commit();
//
//        Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
//                .V(a1)
//                .local(
//                        __.out().limit(1).out()
//                ).values("name");
//        printTraversalForm(traversal);
//
//        List<String> names = traversal.toList();
//        for (String name : names) {
//            System.out.println(name);
//        }
//    }
//
//    @Test
//    public void testStrategy2ChooseStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "a3");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "a4");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        this.sqlgGraph.tx().commit();
//
//        Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
//                .V()
//                .hasLabel("A")
//                .choose(
//                        v -> v.label().equals("A"),
//                        __.out(),
//                        __.in()
//                ).values("name");
//        printTraversalForm(traversal);
//
//        List<String> names = traversal.toList();
//        for (String name : names) {
//            System.out.println(name);
//        }
//    }
//
//    @Test
//    public void testStrategy2OptionalStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        b1.addEdge("bc", c1);
//
//
//        this.sqlgGraph.tx().commit();
//
//        Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
//                .V().hasLabel("A")
//                .optional(
//                    __.repeat(
//                            __.out()
//                    ).times(2)
//                )
//                .values("name");
//        printTraversalForm(traversal);
//        List<String> names = traversal.toList();
//        for (String name : names) {
//            System.out.println(name);
//        }
//    }
//
//    @Test
//    public void testStrategy2RepeatStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
//        Vertex b5 = this.sqlgGraph.addVertex(T.label, "B", "name", "b5");
//        Vertex b6 = this.sqlgGraph.addVertex(T.label, "B", "name", "b6");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
//        Vertex x = this.sqlgGraph.addVertex(T.label, "X", "name", "hallo");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        a2.addEdge("ab", b4);
//        a2.addEdge("ab", b5);
//        a2.addEdge("ab", b6);
//
//        b1.addEdge("bx", x);
//
//        b4.addEdge("bc", c1);
//        b4.addEdge("bc", c2);
//        b4.addEdge("bc", c3);
//
//        c1.addEdge("cx", x);
//
//        this.sqlgGraph.tx().commit();
//
//        Traversal<Vertex, String> t = this.sqlgGraph.traversal()
//                .V().hasLabel("A")
//                .repeat(__.out())
//                .until(__.out().has("name", "hallo"))
//                .values("name");
//        printTraversalForm(t);
//
//        List<String> names = t.toList();
//        for (String name: names) {
//            System.out.println(name);
//        }
//    }
//
//    @Test
//    public void testStrategy2VertexStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("ab", b1);
//        a2.addEdge("ab", b2);
//        a3.addEdge("ab", b3);
//        this.sqlgGraph.tx().commit();
//
//        Traversal<Vertex, String> t = this.sqlgGraph.traversal()
//                .V().hasLabel("A")
//                .limit(2)
//                .out()
//                .values("name");
//        printTraversalForm(t);
//        List<String> result = t.toList();
//        for (String name : result) {
//            System.out.println(name);
//        }
//    }
//
//    @Test
//    public void showContainsPredicate() {
//        List<Integer> numbers = new ArrayList<>(10000);
//        for (int i = 0; i < 10000; i++) {
//            this.sqlgGraph.addVertex(T.label, "A", "number", i);
//            numbers.add(i);
//        }
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> persons = this.sqlgGraph.traversal().V()
//                .hasLabel("A")
//                .has("number", P.within(numbers))
//                .toList();
//
//        assertEquals(10000, persons.size());
//    }
//
//    @Test
//    public void showComparePredicates() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c2);
//        b2.addEdge("bc", c3);
//        b2.addEdge("bc", c4);
//        this.sqlgGraph.tx().commit();
//
//        List<String> result = this.sqlgGraph.traversal()
//                .V().hasLabel("A")
//                .out().has("name", P.eq("b1"))
//                .out().has("name", P.eq("c2"))
//                .<String>values("name")
//                .toList();
//        for (String name : result) {
//            System.out.println(name);
//        }
//    }
//
//    @Test
//    public void testsDropStepWithHas() {
//        this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
//        this.sqlgGraph.tx().commit();
//
//        Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V()
//                .hasLabel("A")
//                .has("name", P.within("a1", "a2"))
//                .drop();
//        printTraversalForm(traversal);
//
//        traversal.iterate();
//        this.sqlgGraph.tx().commit();
//
//        assertEquals(1, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
//    }
//
//    @Test
//    public void testsDropStepTrivial() {
//        this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
//        this.sqlgGraph.tx().commit();
//
//        Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("A").drop();
//        printTraversalForm(traversal);
//
//        traversal.iterate();
//        this.sqlgGraph.tx().commit();
//
//        assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("A").count().next(), 0);
//    }
//
//    @Test
//    public void testDropStepWithEdges() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        this.sqlgGraph.tx().commit();
//
//        Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("A").out().drop();
//        printTraversalForm(traversal);
//
//        traversal.iterate();
//        this.sqlgGraph.tx().commit();
//
//        assertEquals(0, this.sqlgGraph.traversal().V().hasLabel("B").count().next(), 0);
//    }
//
//    @Test
//    public void testLimitOnVertexLabels() {
//        for (int i = 0; i < 100; i++) {
//            this.sqlgGraph.addVertex(T.label, "Person", "name", "person" + i);
//        }
//        this.sqlgGraph.tx().commit();
//        Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
//                .V().hasLabel("Person")
//                .order().by("name")
//                .limit(3)
//                .values("name");
//        printTraversalForm(traversal);
//
//        List<String> names = traversal.toList();
//        for (String name : names) {
//            System.out.println(name);
//        }
//    }
//
//    @Test
//    public void testRangeOnVertexLabels() {
//        for (int i = 0; i < 100; i++) {
//            this.sqlgGraph.addVertex(T.label, "Person", "name", "person" + i);
//        }
//        this.sqlgGraph.tx().commit();
//        Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
//                .V().hasLabel("Person")
//                .order().by("name")
//                .range(1, 4)
//                .values("name");
//        printTraversalForm(traversal);
//
//        List<String> names = traversal.toList();
//        for (String name : names) {
//            System.out.println(name);
//        }
//    }
//
//    @Test
//    public void testOrderBy() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "a");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "b");
//        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a", "surname", "c");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "a");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "b");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "A", "name", "b", "surname", "c");
//        this.sqlgGraph.tx().commit();
//
//        Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("A")
//                .order().by("name", Order.incr).by("surname", Order.decr);
//        printTraversalForm(traversal);
//
//        List<Vertex> vertices = traversal.toList();
//        for (Vertex v : vertices) {
//            System.out.println(v.value("name") + " " + v.value("surname"));
//        }
//    }
//
//    @Test
//    public void showChooseStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        this.sqlgGraph.tx().commit();
//
//        Traversal<Vertex, Path> traversal = this.sqlgGraph.traversal()
//                .V().hasLabel("A")
//                .choose(__.out(), __.out())
//                .path().by("name");
//
//        printTraversalForm(traversal);
//
//        List<Path> paths = traversal.toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//    }
//
//    @Test
//    public void showOptionalStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c2);
//        b1.addEdge("bc", c3);
//        this.sqlgGraph.tx().commit();
//
//        Traversal<Vertex, Path> traversal = this.sqlgGraph.traversal()
//                .V().hasLabel("A")
//                .optional(
//                        __.out().optional(
//                                __.out()
//                        )
//                )
//                .path().by("name");
//        printTraversalForm(traversal);
//        List<Path> paths = traversal.toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//    }
//
//    @Test
//    public void showRepeatStepEmitLast() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c2);
//        b1.addEdge("bc", c3);
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A")
//                .repeat(
//                        __.out()
//                )
//                .emit()
//                .times(2)
//                .path().by("name")
//                .toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//    }
//
//    @Test
//    public void showRepeatStepEmitFirst() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        b1.addEdge("bc", c1);
//        b1.addEdge("bc", c2);
//        b1.addEdge("bc", c3);
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A")
//                .emit()
//                .times(2)
//                .repeat(
//                        __.out()
//                )
//                .path().by("name")
//                .toList();
//        for (Path path : paths) {
//            System.out.println(path);
//        }
//    }
//
//    @Test
//    public void showGraphStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        b1.addEdge("bc", c1);
//        b2.addEdge("bc", c2);
//        this.sqlgGraph.tx().commit();
//
//        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V()
//                .hasLabel("A")
//                .out()
//                .out();
//        System.out.println(traversal);
//        traversal.hasNext();
//        System.out.println(traversal);
//        List<Vertex> c = traversal.toList();
//        assertEquals(2, c.size());
//    }
//
//    @Test
//    public void showHasStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        b1.addEdge("bc", c1);
//        b2.addEdge("bc", c2);
//        this.sqlgGraph.tx().commit();
//
//        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V()
//                .hasLabel("A")
//                .out().has("name", "b1")
//                .out();
//        System.out.println(traversal);
//        traversal.hasNext();
//        System.out.println(traversal);
//        List<Vertex> c = traversal.toList();
//        assertEquals(1, c.size());
//    }
//
//    @Test
//    public void showOrStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
//        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
//        Vertex c5 = this.sqlgGraph.addVertex(T.label, "C", "name", "c5");
//        Vertex c6 = this.sqlgGraph.addVertex(T.label, "C", "name", "c6");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        b1.addEdge("bc", c1);
//        b2.addEdge("bc", c2);
//        b2.addEdge("bc", c3);
//        b2.addEdge("bc", c4);
//        b2.addEdge("bc", c5);
//        b2.addEdge("bc", c6);
//        this.sqlgGraph.tx().commit();
//
//        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V()
//                .hasLabel("A")
//                .out()
//                .out()
//                .or(
//                        __.has("name", "c1"),
//                        __.has("name", "c3"),
//                        __.has("name", "c6")
//                );
//
//        System.out.println(traversal);
//        traversal.hasNext();
//        System.out.println(traversal);
//        List<Vertex> c = traversal.toList();
//        assertEquals(3, c.size());
//    }
//
//    @Test
//    public void showAndStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1", "surname", "x", "address", "y");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2", "surname", "x", "address", "y");
//        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3", "surname", "x", "address", "y");
//        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4", "surname", "x", "address", "y");
//        Vertex c5 = this.sqlgGraph.addVertex(T.label, "C", "name", "c5", "surname", "x", "address", "y");
//        Vertex c6 = this.sqlgGraph.addVertex(T.label, "C", "name", "c6", "surname", "x", "address", "y");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        b1.addEdge("bc", c1);
//        b2.addEdge("bc", c2);
//        b2.addEdge("bc", c3);
//        b2.addEdge("bc", c4);
//        b2.addEdge("bc", c5);
//        b2.addEdge("bc", c6);
//        this.sqlgGraph.tx().commit();
//
//        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V()
//                .hasLabel("A")
//                .out()
//                .out()
//                .and(
//                        __.has("name", "c1"),
//                        __.has("surname", "x"),
//                        __.has("address", "y")
//                );
//
//        System.out.println(traversal);
//        traversal.hasNext();
//        System.out.println(traversal);
//        List<Vertex> c = traversal.toList();
//        assertEquals(1, c.size());
//    }


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
