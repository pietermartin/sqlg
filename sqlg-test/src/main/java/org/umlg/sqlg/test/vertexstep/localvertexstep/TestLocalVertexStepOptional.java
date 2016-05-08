package org.umlg.sqlg.test.vertexstep.localvertexstep;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.optional;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2016/05/04
 * Time: 8:01 PM
 */
public class TestLocalVertexStepOptional extends BaseTest {

    @Test
    public void testLocalVertexStepOptimized() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a11 = this.sqlgGraph.addVertex(T.label, "A", "name", "a11");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.gt.V(a11).local(optional(out()).values("name")).path().toList();
        assertEquals(1, paths.size());
        for (Path path : paths) {
            System.out.println(path);
        }
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 2 && p.get(0).equals(a11) && p.get(1).equals("a11")
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());

        paths = this.gt.V(a1).local(out().out().values("name")).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        assertEquals(1, paths.size());
        pathsToAssert = Arrays.asList(
                p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals("c1")
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testLocalOptional2() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a2.addEdge("ab", b2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V().local(optional(out()).optional(out())).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        assertEquals(6, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1),
                p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1),
                p -> p.size() == 1 && p.get(0).equals(c1),
                p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b2),
                p -> p.size() == 1 && p.get(0).equals(b2),
                p -> p.size() == 1 && p.get(0).equals(a3)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }
}
