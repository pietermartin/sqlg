package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2016/04/14
 * Time: 6:36 PM
 */
public class TestGremlinOptional extends BaseTest {

//    @Test
//    public void testUnoptimizableChooseStep() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        this.sqlgGraph.tx().commit();
//
//        List<Vertex> vertices = this.sqlgGraph.traversal().V(a1).choose(v -> v.label().equals("A"), out(), in()).toList();
//        assertEquals(2, vertices.size());
//    }
//
//    @Test
//    public void testOptional() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        this.sqlgGraph.tx().commit();
//        List<Path> paths = this.sqlgGraph.traversal().V(a1).optional(out()).path().toList();
//        assertEquals(2, paths.size());
//        List<Predicate<Path>> pathsToAssert = Arrays.asList(
//                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
//                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)
//        );
//        for (Predicate<Path> pathPredicate : pathsToAssert) {
//            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
//            assertTrue(path.isPresent());
//            assertTrue(paths.remove(path.get()));
//        }
//        assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testOptionalNested() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        b1.addEdge("bc", c1);
//        this.sqlgGraph.tx().commit();
//        List<Path> paths = this.sqlgGraph.traversal().V(a1).optional(out().optional(out())).path().toList();
//        assertEquals(2, paths.size());
//        List<Predicate<Path>> pathsToAssert = Arrays.asList(
//                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1),
//                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)
//        );
//        for (Predicate<Path> pathPredicate : pathsToAssert) {
//            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
//            assertTrue(path.isPresent());
//            assertTrue(paths.remove(path.get()));
//        }
//        assertTrue(paths.isEmpty());
//
//        paths = this.sqlgGraph.traversal().V().hasLabel("A").optional(out().optional(out())).path().toList();
//        assertEquals(3, paths.size());
//        pathsToAssert = Arrays.asList(
//                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1),
//                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2),
//                p -> p.size() == 1 && p.get(0).equals(a2)
//        );
//        for (Predicate<Path> pathPredicate : pathsToAssert) {
//            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
//            assertTrue(path.isPresent());
//            assertTrue(paths.remove(path.get()));
//        }
//        assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testOptionalMultipleEdgeLabels() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//
//        Vertex bb1 = this.sqlgGraph.addVertex(T.label, "BB");
//        Vertex bb2 = this.sqlgGraph.addVertex(T.label, "BB");
//        a1.addEdge("abb", bb1);
//        a1.addEdge("abb", bb2);
//
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V(a1).optional(out("ab", "abb")).path().toList();
//        assertEquals(4, paths.size());
//        List<Predicate<Path>> pathsToAssert = Arrays.asList(
//                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
//                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2),
//                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(bb1),
//                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(bb2)
//        );
//        for (Predicate<Path> pathPredicate : pathsToAssert) {
//            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
//            assertTrue(path.isPresent());
//            assertTrue(paths.remove(path.get()));
//        }
//        assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testOptionalNestedMultipleEdgeLabels() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        b1.addEdge("bc", c1);
//
//        Vertex bb1 = this.sqlgGraph.addVertex(T.label, "BB");
//        Vertex bb2 = this.sqlgGraph.addVertex(T.label, "BB");
//        Vertex cc1 = this.sqlgGraph.addVertex(T.label, "CC");
//        a1.addEdge("abb", bb1);
//        a1.addEdge("abb", bb2);
//        bb1.addEdge("bbcc", cc1);
//
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V(a1).optional(out("ab", "abb").optional(out("bc", "bbcc"))).path().toList();
//        assertEquals(4, paths.size());
//        List<Predicate<Path>> pathsToAssert = Arrays.asList(
//                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1),
//                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2),
//                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(bb1) && p.get(2).equals(cc1),
//                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(bb2)
//        );
//        for (Predicate<Path> pathPredicate : pathsToAssert) {
//            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
//            assertTrue(path.isPresent());
//            assertTrue(paths.remove(path.get()));
//        }
//        assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testOptionalOnNonExistingEdgeLabel() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        a1.addEdge("ab", b1);
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V(a1).optional(out("ab", "bb")).path().toList();
//        assertEquals(1, paths.size());
//        List<Predicate<Path>> pathsToAssert = Arrays.asList(
//                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)
//        );
//        for (Predicate<Path> pathPredicate : pathsToAssert) {
//            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
//            assertTrue(path.isPresent());
//            assertTrue(paths.remove(path.get()));
//        }
//        assertTrue(paths.isEmpty());
//
//        paths = this.sqlgGraph.traversal().V(a1).optional(out("bb")).path().toList();
//        assertEquals(1, paths.size());
//        pathsToAssert = Arrays.asList(
//                p -> p.size() == 1 && p.get(0).equals(a1)
//        );
//        for (Predicate<Path> pathPredicate : pathsToAssert) {
//            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
//            assertTrue(path.isPresent());
//            assertTrue(paths.remove(path.get()));
//        }
//        assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testMultipleNestedOptional() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C");
//        Vertex cc1 = this.sqlgGraph.addVertex(T.label, "CC");
//        Vertex cc2 = this.sqlgGraph.addVertex(T.label, "CC");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        b1.addEdge("bc", c1);
//        b2.addEdge("bcc", cc2);
//        c1.addEdge("ccc", cc1);
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V(a1).optional(out("ab").optional(out("bc"))).path().toList();
//        assertEquals(2, paths.size());
//        List<Predicate<Path>> pathsToAssert = Arrays.asList(
//                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1),
//                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)
//        );
//        for (Predicate<Path> pathPredicate : pathsToAssert) {
//            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
//            assertTrue(path.isPresent());
//            assertTrue(paths.remove(path.get()));
//        }
//        assertTrue(paths.isEmpty());
//
//        paths = this.sqlgGraph.traversal().V(a1).optional(out("ab").optional(out("bc"))).out().path().toList();
//        assertEquals(2, paths.size());
//        pathsToAssert = Arrays.asList(
//                p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(cc1),
//                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b2) && p.get(2).equals(cc2)
//        );
//        for (Predicate<Path> pathPredicate : pathsToAssert) {
//            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
//            assertTrue(path.isPresent());
//            assertTrue(paths.remove(path.get()));
//        }
//        assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void g_VX2X_optionalXoutXknowsXX() throws IOException {
//        Graph g = this.sqlgGraph;
//        final GraphReader reader = GryoReader.build()
//                .mapper(g.io(GryoIo.build()).mapper().create())
//                .create();
//        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
//            reader.readGraph(stream, g);
//        }
//        this.sqlgGraph.tx().commit();
//        assertModernGraph(g, true, false);
//
//        Object vadas = convertToVertexId(g, "vadas");
//        Vertex vadasVertex = g.traversal().V(vadas).next();
//        List<Path> paths = g.traversal().V(vadasVertex).optional(out("knows")).path().toList();
//        assertEquals(1, paths.size());
//
//        List<Predicate<Path>> pathsToAssert = Arrays.asList(
//                p -> p.size() == 1 && p.get(0).equals(vadasVertex)
//        );
//        for (Predicate<Path> pathPredicate : pathsToAssert) {
//            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
//            assertTrue(path.isPresent());
//            assertTrue(paths.remove(path.get()));
//        }
//        assertTrue(paths.isEmpty());
//
//        List<Vertex> vertices = g.traversal().V(vadasVertex).optional(out("knows")).toList();
//        assertEquals(1, vertices.size());
//        assertEquals(vadasVertex, vertices.get(0));
//
//        paths = g.traversal().V().optional(out().optional(out())).path().toList();
//        for (Path path : paths) {
//            System.out.println(path.toString());
//        }
//        assertEquals(10, paths.size());
//        pathsToAssert = Arrays.asList(
//                p -> p.size() == 2 && p.get(0).equals(convertToVertex(g, "marko")) && p.get(1).equals(convertToVertex(g, "lop")),
//                p -> p.size() == 2 && p.get(0).equals(convertToVertex(g, "marko")) && p.get(1).equals(convertToVertex(g, "vadas")),
//                p -> p.size() == 3 && p.get(0).equals(convertToVertex(g, "marko")) && p.get(1).equals(convertToVertex(g, "josh")) && p.get(2).equals(convertToVertex(g, "lop")),
//                p -> p.size() == 3 && p.get(0).equals(convertToVertex(g, "marko")) && p.get(1).equals(convertToVertex(g, "josh")) && p.get(2).equals(convertToVertex(g, "ripple")),
//                p -> p.size() == 1 && p.get(0).equals(convertToVertex(g, "vadas")),
//                p -> p.size() == 1 && p.get(0).equals(convertToVertex(g, "lop")),
//                p -> p.size() == 2 && p.get(0).equals(convertToVertex(g, "josh")) && p.get(1).equals(convertToVertex(g, "lop")),
//                p -> p.size() == 2 && p.get(0).equals(convertToVertex(g, "josh")) && p.get(1).equals(convertToVertex(g, "ripple")),
//                p -> p.size() == 1 && p.get(0).equals(convertToVertex(g, "ripple")),
//                p -> p.size() == 2 && p.get(0).equals(convertToVertex(g, "peter")) && p.get(1).equals(convertToVertex(g, "lop"))
//        );
//        for (Predicate<Path> pathPredicate : pathsToAssert) {
//            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
//            assertTrue(path.isPresent());
//            assertTrue(paths.remove(path.get()));
//        }
//        assertTrue(paths.isEmpty());
//    }
//
//    @Test
//    public void testCurrentTreeLabelToSelf1() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
//        a1.addEdge("aa", a2);
//        a1.addEdge("ab", b1);
//        this.sqlgGraph.tx().commit();
//
//        List<Path> paths = this.sqlgGraph.traversal().V().optional(out().optional(out())).path().toList();
//        for (Path path : paths) {
//            System.out.println(path.toString());
//        }
//        assertEquals(4, paths.size());
//
//        List<Predicate<Path>> pathsToAssert = Arrays.asList(
//                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
//                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2),
//                p -> p.size() == 1 && p.get(0).equals(a2),
//                p -> p.size() == 1 && p.get(0).equals(b1)
//        );
//        for (Predicate<Path> pathPredicate : pathsToAssert) {
//            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
//            assertTrue(path.isPresent());
//            assertTrue(paths.remove(path.get()));
//        }
//        assertTrue(paths.isEmpty());
//    }

    @Test
    public void testCurrentTreeLabelToSelf2() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("aa", a1);
        a1.addEdge("aa", a1);
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V(a1).emit().times(2).repeat(out()).path().toList();
//        List<Path> paths = this.sqlgGraph.traversal().V(a1).optional(out().optional(out())).path().toList();
        for (Path path : paths) {
            System.out.println(path.toString());
        }
        assertEquals(13, paths.size());

        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(a1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(a1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(a1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(a1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(b1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(b1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(b1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a1) && p.get(2).equals(b1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a1),
                p -> p.size() == 1 && p.get(0).equals(a1)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }
}
