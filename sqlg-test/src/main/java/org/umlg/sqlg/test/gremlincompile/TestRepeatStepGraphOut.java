package org.umlg.sqlg.test.gremlincompile;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.*;

/**
 * Date: 2015/10/21
 * Time: 8:18 PM
 */
public class TestRepeatStepGraphOut extends BaseTest {

    @Test
    //This is not optimized
    public void testUntilRepeat() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        Traversal t = this.sqlgGraph.traversal().V().repeat(__.out()).until(__.hasLabel("C"));
        printTraversalForm(t);
        List<Vertex> vertices = t.toList();
        assertEquals(6, vertices.size());
        assertTrue(vertices.remove(c1));
        assertTrue(vertices.remove(c1));
        assertTrue(vertices.remove(c2));
        assertTrue(vertices.remove(c2));
        assertTrue(vertices.remove(c3));
        assertTrue(vertices.remove(c3));
        assertTrue(vertices.isEmpty());

        vertices = this.sqlgGraph.traversal().V().until(__.hasLabel("C")).repeat(__.out()).toList();
        assertEquals(9, vertices.size());
        assertTrue(vertices.remove(c1));
        assertTrue(vertices.remove(c1));
        assertTrue(vertices.remove(c1));
        assertTrue(vertices.remove(c2));
        assertTrue(vertices.remove(c2));
        assertTrue(vertices.remove(c2));
        assertTrue(vertices.remove(c3));
        assertTrue(vertices.remove(c3));
        assertTrue(vertices.remove(c3));
        assertTrue(vertices.isEmpty());
    }

    @Test
    //This is not optimized
    public void testRepeatWithUnoptimizableInternalSteps() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        List<Map<String, Vertex>> t = this.sqlgGraph.traversal().V().repeat(__.groupCount("m").by("name").out()).times(2).<Map<String, Vertex>>cap("m").toList();
        assertEquals(1, t.size());
        assertTrue(t.get(0).containsKey("a1"));
        assertTrue(t.get(0).containsKey("b1"));
        assertTrue(t.get(0).containsKey("b1"));
        assertTrue(t.get(0).containsKey("b1"));
        assertTrue(t.get(0).containsKey("c1"));
        assertTrue(t.get(0).containsKey("c1"));
        assertTrue(t.get(0).containsKey("c1"));

        assertEquals(1L, t.get(0).get("a1"));
        assertEquals(2L, t.get(0).get("b1"));
        assertEquals(2L, t.get(0).get("b2"));
        assertEquals(2L, t.get(0).get("b3"));
        assertEquals(2L, t.get(0).get("c1"));
        assertEquals(2L, t.get(0).get("c2"));
        assertEquals(2L, t.get(0).get("c3"));
    }

    @Test
    //This is not optimized
    public void testRepeatWithUnoptimizableInternalStepsAndPropertyWithPeriod() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name.A", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name.A", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name.A", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name.A", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name.A", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name.A", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name.A", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        List<Map<String, Vertex>> t = this.sqlgGraph.traversal().V().repeat(__.groupCount("m").by("name.A").out()).times(2).<Map<String, Vertex>>cap("m").toList();
        assertEquals(1, t.size());
        assertTrue(t.get(0).containsKey("a1"));
        assertTrue(t.get(0).containsKey("b1"));
        assertTrue(t.get(0).containsKey("b1"));
        assertTrue(t.get(0).containsKey("b1"));
        assertTrue(t.get(0).containsKey("c1"));
        assertTrue(t.get(0).containsKey("c1"));
        assertTrue(t.get(0).containsKey("c1"));

        assertEquals(1L, t.get(0).get("a1"));
        assertEquals(2L, t.get(0).get("b1"));
        assertEquals(2L, t.get(0).get("b2"));
        assertEquals(2L, t.get(0).get("b3"));
        assertEquals(2L, t.get(0).get("c1"));
        assertEquals(2L, t.get(0).get("c2"));
        assertEquals(2L, t.get(0).get("c3"));
    }

    @Test
    //This is not optimized because there is no until not times
    public void testRepeatNoLimit() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().repeat(__.out("ab").out("bc")).toList();
        assertTrue(vertices.isEmpty());
    }

    @Test
    public void testRepeatSimpleTimesEmitBefore() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V(a1).times(1).emit().repeat(__.out()).path().toList();
        assertEquals(2, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 1 && p.get(0).equals(a1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testRepeatSimpleTimeEmitAfter() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V(a1).repeat(__.out()).times(1).emit().path().toList();
        assertEquals(1, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testRepeat() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").times(0).repeat(__.out("ab").out("bc")).toList();
        assertEquals(1, vertices.size());
        assertTrue(vertices.contains(a1));

        vertices = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out("ab", "bc")).times(1).toList();
        assertEquals(3, vertices.size());
        assertTrue(vertices.contains(b1));
        assertTrue(vertices.contains(b2));
        assertTrue(vertices.contains(b3));

        vertices = this.sqlgGraph.traversal().V().hasLabel("A").times(1).repeat(__.out("ab", "bc")).toList();
        assertEquals(3, vertices.size());
        assertTrue(vertices.contains(b1));
        assertTrue(vertices.contains(b2));
        assertTrue(vertices.contains(b3));

        vertices = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out("ab", "bc")).times(2).toList();
        assertEquals(3, vertices.size());
        assertTrue(vertices.contains(c1));
        assertTrue(vertices.contains(c2));
        assertTrue(vertices.contains(c3));

        vertices = this.sqlgGraph.traversal().V().hasLabel("A").times(2).repeat(__.out("ab", "bc")).toList();
        assertEquals(3, vertices.size());
        assertTrue(vertices.contains(c1));
        assertTrue(vertices.contains(c2));
        assertTrue(vertices.contains(c3));
    }

    @Test
    public void testSmallerRepeatWithEmitFirst() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "bc")).times(1).path().toList();
        assertEquals(3, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 1 && p.get(0).equals(a1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());

        paths = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "bc")).times(1).path().toList();
        assertEquals(3, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testRepeatWithEmitFirst() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
        c1.addEdge("cd", d1);
        c1.addEdge("cd", d2);
        c1.addEdge("cd", d3);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "bc", "cd")).times(3).toList();
        assertEquals(14, vertices.size());
        assertTrue(vertices.remove(a1));
        assertTrue(vertices.remove(b1));
        assertTrue(vertices.remove(b2));
        assertTrue(vertices.remove(b3));
        assertTrue(vertices.remove(c1));
        assertTrue(vertices.remove(c1));
        assertTrue(vertices.remove(c2));
        assertTrue(vertices.remove(c3));
        assertTrue(vertices.remove(d1));
        assertTrue(vertices.remove(d2));
        assertTrue(vertices.remove(d3));
        assertTrue(vertices.remove(d1));
        assertTrue(vertices.remove(d2));
        assertTrue(vertices.remove(d3));
        assertTrue(vertices.isEmpty());
    }

    @Test
    public void testRepeatWithEmitFirstWithPeriod() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name.AA", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name.AA", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name.AA", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name.AA", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name.AA", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name.AA", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name.AA", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name.AA", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name.AA", "d2");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name.AA", "d3");
        c1.addEdge("cd", d1);
        c1.addEdge("cd", d2);
        c1.addEdge("cd", d3);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "bc", "cd")).times(3).toList();
        assertEquals(14, vertices.size());
        assertTrue(vertices.remove(a1));
        assertTrue(vertices.remove(b1));
        assertTrue(vertices.remove(b2));
        assertTrue(vertices.remove(b3));
        assertTrue(vertices.remove(c1));
        assertTrue(vertices.remove(c1));
        assertTrue(vertices.remove(c2));
        assertTrue(vertices.remove(c3));
        assertTrue(vertices.remove(d1));
        assertTrue(vertices.remove(d2));
        assertTrue(vertices.remove(d3));
        assertTrue(vertices.remove(d1));
        assertTrue(vertices.remove(d2));
        assertTrue(vertices.remove(d3));
        assertTrue(vertices.isEmpty());
    }

    @Test
    public void testRepeatWithEmitFirstPathWithPeriod() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name.AA", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
        c1.addEdge("cd", d1);
        c1.addEdge("cd", d2);
        c1.addEdge("cd", d3);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "bc", "cd")).times(3).path().toList();
        assertEquals(14, paths.size());
        for (Path path : paths) {
            System.out.println(path);
        }
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1) && ((Vertex)p.get(0)).value("name.AA").equals("a1")));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testRepeatWithEmitFirstPath() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
        c1.addEdge("cd", d1);
        c1.addEdge("cd", d2);
        c1.addEdge("cd", d3);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "bc", "cd")).times(3).path().toList();
        assertEquals(14, paths.size());
        for (Path path : paths) {
            System.out.println(path);
        }
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testRepeatWithEmitTimesFirst() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
        c1.addEdge("cd", d1);
        c1.addEdge("cd", d2);
        c1.addEdge("cd", d3);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").emit().times(3).repeat(__.out("ab", "bc", "cd")).path().toList();
        for (Path path : paths) {
            System.out.println(path.toString());
        }
        assertEquals(14, paths.size());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)).findAny().get());

        assertTrue(paths.isEmpty());
    }

    @Test
    public void testRepeatWithEmitTimesLast() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
        c1.addEdge("cd", d1);
        c1.addEdge("cd", d2);
        c1.addEdge("cd", d3);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out("ab", "bc", "cd")).emit().times(3).path().toList();
        for (Path path : paths) {
            System.out.println(path.toString());
        }
        assertEquals(13, paths.size());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b2)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b3)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c3)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)).findAny().get());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d3)).findAny().get());

        assertTrue(paths.isEmpty());
    }

    @Test
    public void testRepeatWithEmitLastShouldNotLeftJoinFirstDegree() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
        c1.addEdge("cd", d1);
        c1.addEdge("cd", d2);
        c1.addEdge("cd", d3);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out("ab", "bc", "cd")).emit().times(3).toList();
        for (Vertex vertex : vertices) {
            System.out.println(vertex.value("name").toString());
        }
        assertEquals(13, vertices.size());
        assertTrue(vertices.remove(b1));
        assertTrue(vertices.remove(b2));
        assertTrue(vertices.remove(b3));
        assertTrue(vertices.remove(c1));
        assertTrue(vertices.remove(c1));
        assertTrue(vertices.remove(c2));
        assertTrue(vertices.remove(c3));
        assertTrue(vertices.remove(d1));
        assertTrue(vertices.remove(d2));
        assertTrue(vertices.remove(d3));
        assertTrue(vertices.remove(d1));
        assertTrue(vertices.remove(d2));
        assertTrue(vertices.remove(d3));
        assertTrue(vertices.isEmpty());
    }

    @Test
    public void testRepeatWithEmitLastWithTimesFirst() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2");
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3");
        c1.addEdge("cd", d1);
        c1.addEdge("cd", d2);
        c1.addEdge("cd", d3);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").times(3).repeat(__.out("ab", "bc", "cd")).emit().toList();
        for (Vertex vertex : vertices) {
            System.out.println(vertex.value("name").toString());
        }
        assertEquals(19, vertices.size());
        assertTrue(vertices.remove(b1));
        assertTrue(vertices.remove(b2));
        assertTrue(vertices.remove(b3));
        assertTrue(vertices.remove(c1));
        assertTrue(vertices.remove(c1));
        assertTrue(vertices.remove(c2));
        assertTrue(vertices.remove(c3));
        assertTrue(vertices.remove(d1));
        assertTrue(vertices.remove(d2));
        assertTrue(vertices.remove(d3));
        assertTrue(vertices.remove(d1));
        assertTrue(vertices.remove(d2));
        assertTrue(vertices.remove(d3));
        assertTrue(vertices.remove(d1));
        assertTrue(vertices.remove(d2));
        assertTrue(vertices.remove(d3));
        assertTrue(vertices.remove(d1));
        assertTrue(vertices.remove(d2));
        assertTrue(vertices.remove(d3));
        assertTrue(vertices.isEmpty());
    }

    @Test
    public void g_V_repeatXoutX_timesX2X() throws IOException {
        final List<Traversal<Vertex, Vertex>> traversals = new ArrayList<>();
        Graph graph = this.sqlgGraph;
        graph.io(GryoIo.build()).readGraph("../sqlg-test/src/main/resources/tinkerpop-modern.kryo");
        assertModernGraph(graph, true, false);

        Traversal t = graph.traversal().V().repeat(__.out()).times(2);
        traversals.add(t);
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            int counter = 0;
            while (traversal.hasNext()) {
                counter++;
                Vertex vertex = traversal.next();
                assertTrue(vertex.value("name").equals("lop") || vertex.value("name").equals("ripple"));
            }
            assertEquals(2, counter);
            assertFalse(traversal.hasNext());
        });
    }

    @Test
    public void g_V_repeatXoutX_timesX2X_path_byXitX_byXnameX_byXlangX() throws IOException {
        Graph graph = this.sqlgGraph;
        graph.io(GryoIo.build()).readGraph("../sqlg-test/src/main/resources/tinkerpop-modern.kryo");
        assertModernGraph(graph, true, false);
        final Traversal<Vertex, Path> traversal = graph.traversal().V().repeat(__.out()).times(2).path().by().by("name").by("lang");
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Path path = traversal.next();
            assertEquals(3, path.size());
            assertEquals("marko", ((Vertex) path.get(0)).<String>value("name"));
            assertEquals("josh", path.<String>get(1));
            assertEquals("java", path.<String>get(2));
        }
        assertEquals(2, counter);
    }

    @Test
    public void g_V_repeatXoutX_timesX2X_emit_path() throws IOException {
        Graph graph = this.sqlgGraph;
        graph.io(GryoIo.build()).readGraph("../sqlg-test/src/main/resources/tinkerpop-modern.kryo");
        assertModernGraph(graph, true, false);
        GraphTraversalSource g = graph.traversal();
        final List<Traversal<Vertex, Path>> traversals = new ArrayList<>();
        Traversal t = g.V().repeat(out()).emit().times(2).path();
        traversals.add(t);
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Map<Integer, Long> pathLengths = new HashMap<>();
            int counter = 0;
            while (traversal.hasNext()) {
                counter++;
                MapHelper.incr(pathLengths, traversal.next().size(), 1L);
            }
            assertEquals(2, pathLengths.size());
            assertEquals(8, counter);
            assertEquals(new Long(6), pathLengths.get(2));
            assertEquals(new Long(2), pathLengths.get(3));
        });
    }

    @Test
    public void testTimesBeforeEmitBefore() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        b1.addEdge("ba", a1);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V(a1).emit().times(2).repeat(out()).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 1 && p.get(0).equals(a1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a1)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testTimesAfterEmitBefore() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        b1.addEdge("ba", a1);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V(a1).emit().repeat(out()).times(2).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 1 && p.get(0).equals(a1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a1)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testTimesBeforeEmitBeforeToSelf() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");
        a1.addEdge("aa", a2);
        a1.addEdge("ab", b1);
        a2.addEdge("aa", a3);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V(a1).emit().times(2).repeat(out()).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 1 && p.get(0).equals(a1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(a3)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testTimesAfterEmitBeforeToSelf() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");
        a1.addEdge("ab", a2);
        a2.addEdge("ab", a3);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V(a1).emit().repeat(out()).times(2).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 1 && p.get(0).equals(a1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(a3)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }

    @Test
    public void g_V_emit_timesX2X_repeatXoutX_path() throws IOException {
        Graph graph = this.sqlgGraph;
        graph.io(GryoIo.build()).readGraph("../sqlg-test/src/main/resources/tinkerpop-modern.kryo");
        GraphTraversalSource g = graph.traversal();
        final List<Traversal<Vertex, Path>> traversals = Arrays.asList(
                g.V().emit().times(2).repeat(out()).path(),
                g.V().emit().repeat(out()).times(2).path()
        );
        traversals.forEach(traversal -> {
            int path1 = 0;
            int path2 = 0;
            int path3 = 0;
            while (traversal.hasNext()) {
                final Path path = traversal.next();
                System.out.println(path);
                if (path.size() == 1) {
                    path1++;
                } else if (path.size() == 2) {
                    path2++;
                } else if (path.size() == 3) {
                    path3++;
                } else {
                    fail("Only path lengths of 1, 2, or 3 should be seen");
                }
            }
            assertEquals(6, path1);
            assertEquals(6, path2);
            assertEquals(2, path3);
        });
    }

    @Test
    public void testGremlinLeftJoin() {
        Vertex marko = this.sqlgGraph.addVertex(T.label, "Person", "name", "marko");
        Vertex josh = this.sqlgGraph.addVertex(T.label, "Person", "name", "josh");
        Vertex lop = this.sqlgGraph.addVertex(T.label, "Software", "name", "lop");
        marko.addEdge("knows", josh);
        marko.addEdge("created", lop);
        josh.addEdge("created", lop);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V().repeat(out()).times(2).emit().path().toList();
        assertEquals(4, paths.size());

        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(marko) && p.get(1).equals(lop)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(marko) && p.get(1).equals(lop)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(josh) && p.get(1).equals(lop)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(josh) && p.get(1).equals(lop)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(marko) && p.get(1).equals(josh)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(marko) && p.get(1).equals(josh)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(marko) && p.get(1).equals(josh) && p.get(2).equals(lop)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(marko) && p.get(1).equals(josh) && p.get(2).equals(lop)).findAny().get());
        assertTrue(paths.isEmpty());

        List<Vertex> vertices = this.sqlgGraph.traversal().V().repeat(out()).times(2).emit().toList();
        assertEquals(4, vertices.size());
        assertTrue(vertices.remove(josh));
        assertTrue(vertices.remove(lop));
        assertTrue(vertices.remove(lop));
        assertTrue(vertices.remove(lop));
        assertTrue(vertices.isEmpty());
    }

    @Test
    public void testDuplicatePathToSelf() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");

        a1.addEdge("knows", a2);
        a2.addEdge("knows", a3);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().repeat(out("knows")).times(2).emit().toList();
        assertEquals(3, vertices.size());
        assertTrue(vertices.remove(a2));
        assertTrue(vertices.remove(a3));
        assertTrue(vertices.remove(a3));
        assertEquals(0, vertices.size());

        List<Path> paths = this.sqlgGraph.traversal().V().repeat(out("knows")).emit().times(2).path().toList();
        assertEquals(3, paths.size());
        for (Path path : paths) {
            System.out.println(path);
        }
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(a3)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(a3)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(a3)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(a3)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testDuplicatePathToSelfEmitFirst() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");

        a1.addEdge("knows", a2);
        a2.addEdge("knows", a3);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().emit().repeat(out("knows")).times(2).toList();
        assertEquals(6, vertices.size());
        assertTrue(vertices.remove(a1));
        assertTrue(vertices.remove(a2));
        assertTrue(vertices.remove(a3));
        assertTrue(vertices.remove(a2));
        assertTrue(vertices.remove(a3));
        assertTrue(vertices.remove(a3));
        assertEquals(0, vertices.size());

        List<Path> paths = this.sqlgGraph.traversal().V().emit().repeat(out("knows")).times(2).path().toList();
        assertEquals(6, paths.size());
        for (Path path : paths) {
            System.out.println(path);
        }
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(a3)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(a3)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(a3)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(a3)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a3)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a3)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testOnLeftJoinOnLeaveNode() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(out("ab")).times(1).toList();
        assertEquals(4, vertices.size());
    }

    @Test
    public void testOnDuplicatePaths() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        b1.addEdge("ba", a2);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(out("ab", "ba")).times(2).toList();
        assertEquals(4, vertices.size());
    }

    @Test
    public void testOnDuplicatePathsFromVertexTimesAfter() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        b1.addEdge("ba", a2);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V().emit().repeat(out("ab", "ba")).times(2).path().toList();
        assertEquals(6, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)));
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)).findAny().get());
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testOnDuplicatePathsFromVertexTimes1After() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        b1.addEdge("ba", a2);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V().emit().repeat(out("ab", "ba")).times(1).path().toList();
        assertEquals(5, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testOnDuplicatePathsFromVertexTimes2After() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        b1.addEdge("ba", a2);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V().emit().repeat(out("ab", "ba")).times(2).path().toList();
        assertEquals(6, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testOnDuplicatePathsFromVertexTimes1EmitAfter() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        b1.addEdge("ba", a2);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V().repeat(out("ab", "ba")).emit().times(1).path().toList();
        assertEquals(2, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testOnDuplicatePathsFromVertexTimes2EmitAfter() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        b1.addEdge("ba", a2);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V().repeat(out("ab", "ba")).emit().times(2).path().toList();
        assertEquals(3, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(a2)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(a2)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testRepeatWithTimesBefore() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V().emit().times(2).repeat(out()).path().toList();
        assertEquals(3, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testPathToSelfTreeValidatedTakingTheRootIntoAccount() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("aa", a2);
        a2.addEdge("ba", b1);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").repeat(out()).emit().times(2).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        assertEquals(3, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b1)));
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)));
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b1)).findAny().get());
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)).findAny().get());
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(b1)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testEmitAfterTimesAfterAndBefore() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        c1.addEdge("cd", d1);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").repeat(out()).emit().times(3).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        assertEquals(3, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
        assertTrue(paths.isEmpty());

        paths = this.sqlgGraph.traversal().V().hasLabel("A").repeat(out()).emit().times(4).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        paths.remove(paths.stream().filter(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)).findAny().get());
        assertTrue(paths.isEmpty());

        paths = this.sqlgGraph.traversal().V().hasLabel("A").times(2).repeat(out()).emit().path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        assertEquals(3, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testTimesBeforeAfterFirstNoEmit() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        c1.addEdge("cd", d1);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").times(3).repeat(out()).path().toList();
        assertEquals(1, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1) && p.get(3).equals(d1)));
        paths = this.sqlgGraph.traversal().V().hasLabel("A").repeat(out()).times(4).path().toList();
        assertEquals(0, paths.size());
    }

    @Test
    public void testEmitTimesBeforeAfter() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("B").emit().times(2).repeat(out()).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        assertEquals(2, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
        assertTrue(paths.isEmpty());

        paths = this.sqlgGraph.traversal().V().emit().times(2).repeat(out()).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        assertEquals(6, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(c1)).findAny().get());
        assertTrue(paths.isEmpty());
        System.out.println("-----------------");

        paths = this.sqlgGraph.traversal().V().emit().times(3).repeat(out()).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        assertEquals(6, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(c1)).findAny().get());
        assertTrue(paths.isEmpty());

        System.out.println("-----------------");
        paths = this.sqlgGraph.traversal().V().emit().repeat(out()).times(3).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        assertEquals(6, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(c1)).findAny().get());
        assertTrue(paths.isEmpty());
        System.out.println("-----------------");
        paths = this.sqlgGraph.traversal().V().emit().repeat(out()).times(2).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        assertEquals(6, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(b1) && p.get(1).equals(c1)).findAny().get());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(c1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(c1)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testEmitTimes2MultiplePathsSimple() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("aa", a2);
        a2.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V(a1).emit().times(2).repeat(out()).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        assertEquals(3, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 1 && p.get(0).equals(a1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(b1)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testEmitTimes2MultiplePaths() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("aa", a2);
        a2.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V().emit().times(2).repeat(out()).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
        assertEquals(6, paths.size());
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a1)));
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(a2)));
        assertTrue(paths.stream().anyMatch(p -> p.size() == 1 && p.get(0).equals(b1)));
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)));
        assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b1)));
        assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(b1)));
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a1)).findAny().get());
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(a2)).findAny().get());
        paths.remove(paths.stream().filter(p -> p.size() == 1 && p.get(0).equals(b1)).findAny().get());
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(a2)).findAny().get());
        paths.remove(paths.stream().filter(p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b1)).findAny().get());
        paths.remove(paths.stream().filter(p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(a2) && p.get(2).equals(b1)).findAny().get());
        assertTrue(paths.isEmpty());
    }

    @Test
    public void testRepeatStepPerformance() throws InterruptedException {
        Vertex group = this.sqlgGraph.addVertex(T.label, "Group", "name", "MTN");
        Vertex network = this.sqlgGraph.addVertex(T.label, "Network", "name", "SouthAfrica");
        Vertex ericssonGsm = this.sqlgGraph.addVertex(T.label, "NetworkSoftwareVersion", "name", "EricssonGsm");
        Vertex ericssonUmts = this.sqlgGraph.addVertex(T.label, "NetworkSoftwareVersion", "name", "EricssonUmts");
        Vertex ericssonLte = this.sqlgGraph.addVertex(T.label, "NetworkSoftwareVersion", "name", "EricssonLte");
        Vertex huaweiGsm = this.sqlgGraph.addVertex(T.label, "NetworkSoftwareVersion", "name", "HuaweiGsm");
        Vertex huaweiUmts = this.sqlgGraph.addVertex(T.label, "NetworkSoftwareVersion", "name", "HuaweiUmts");
        Vertex huaweiLte = this.sqlgGraph.addVertex(T.label, "NetworkSoftwareVersion", "name", "HuaweiLte");

        group.addEdge("group_network", network);
        network.addEdge("network_networkSoftwareVersion", ericssonGsm);
        network.addEdge("network_networkSoftwareVersion", ericssonUmts);
        network.addEdge("network_networkSoftwareVersion", ericssonLte);
        network.addEdge("network_networkSoftwareVersion", huaweiGsm);
        network.addEdge("network_networkSoftwareVersion", huaweiUmts);
        network.addEdge("network_networkSoftwareVersion", huaweiLte);


        Vertex ericssonGsmNetworkNodeGroup= this.sqlgGraph.addVertex(T.label, "NetworkNodeGroup", "name", "EricssonGsmNetworkNodeGroup");
        Vertex ericssonUmtsNetworkNodeGroup= this.sqlgGraph.addVertex(T.label, "NetworkNodeGroup", "name", "EricssonUmtsNetworkNodeGroup");
        Vertex ericssonLteNetworkNodeGroup= this.sqlgGraph.addVertex(T.label, "NetworkNodeGroup", "name", "EricssonLteNetworkNodeGroup");
        Vertex huaweiGsmNetworkNodeGroup= this.sqlgGraph.addVertex(T.label, "NetworkNodeGroup", "name", "HuaweiGsmNetworkNodeGroup");
        Vertex huaweiUmtsNetworkNodeGroup= this.sqlgGraph.addVertex(T.label, "NetworkNodeGroup", "name", "HuaweiUmtsNetworkNodeGroup");
        Vertex huaweiLteNetworkNodeGroup= this.sqlgGraph.addVertex(T.label, "NetworkNodeGroup", "name", "HuaweiLteNetworkNodeGroup");

        ericssonGsm.addEdge("networkSoftwareVersion_networkNodeGroup", ericssonGsmNetworkNodeGroup);
        ericssonUmts.addEdge("networkSoftwareVersion_networkNodeGroup", ericssonUmtsNetworkNodeGroup);
        ericssonLte.addEdge("networkSoftwareVersion_networkNodeGroup", ericssonLteNetworkNodeGroup);
        huaweiGsm.addEdge("networkSoftwareVersion_networkNodeGroup", huaweiGsmNetworkNodeGroup);
        huaweiUmts.addEdge("networkSoftwareVersion_networkNodeGroup", huaweiUmtsNetworkNodeGroup);
        huaweiLte.addEdge("networkSoftwareVersion_networkNodeGroup", huaweiLteNetworkNodeGroup);

        for (int i = 0; i < 1000; i++) {
            Vertex ericssonGsmNode = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "ericssonGsm" + i);
            ericssonGsmNetworkNodeGroup.addEdge("networkNodeGroup_networkNode", ericssonGsmNode);
            Vertex ericssonUmtsNode = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "ericssonUmts" + i);
            ericssonUmtsNetworkNodeGroup.addEdge("networkNodeGroup_networkNode", ericssonUmtsNode);
            Vertex ericssonLteNode = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "ericssonLte" + i);
            ericssonLteNetworkNodeGroup.addEdge("networkNodeGroup_networkNode", ericssonLteNode);
            Vertex huaweiGsmNode = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "huaweiGsm" + i);
            huaweiGsmNetworkNodeGroup.addEdge("networkNodeGroup_networkNode", huaweiGsmNode);
            Vertex huaweiUmtsNode = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "huaweiUmts" + i);
            huaweiUmtsNetworkNodeGroup.addEdge("networkNodeGroup_networkNode", huaweiUmtsNode);
            Vertex huaweiLteNode = this.sqlgGraph.addVertex(T.label, "NetworkNode", "name", "huaweiLte" + i);
            huaweiLteNetworkNodeGroup.addEdge("networkNodeGroup_networkNode", huaweiLteNode);
        }

        this.sqlgGraph.tx().commit();

//        Thread.sleep(20000);
        System.out.println("go");
        for (int i = 0; i < 1; i++) {
            Tree tree = this.sqlgGraph.traversal().V()
                    .hasLabel("Group")
                    .emit().repeat(out("group_network", "network_networkSoftwareVersion", "networkSoftwareVersion_networkNodeGroup", "networkNodeGroup_networkNode"))
                    .times(4)
                    .tree()
                    .next();
        }
        StopWatch stopWatch1 = new StopWatch();
        stopWatch1.start();

        Tree tree = this.sqlgGraph.traversal().V()
                .hasLabel("Group")
                .emit().repeat(out("group_network", "network_networkSoftwareVersion", "networkSoftwareVersion_networkNodeGroup", "networkNodeGroup_networkNode"))
                .times(4)
                .tree()
                .next();
        stopWatch1.stop();
        System.out.println(stopWatch1.toString());
    }
}
