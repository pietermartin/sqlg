package org.umlg.sqlg.test.localvertexstep;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2016/05/08
 * Time: 10:28 AM
 */
public class TestLocalRepeatStep extends BaseTest {

    @Test
    public void testLocalRepeatStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V(a1).local(__.<Vertex>emit().times(2).repeat(__.out())).path().toList();
        assertEquals(3, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 1 && p.get(0).equals(a1),
                p -> p.size() == 2 && p.get(0).equals(a1) && p.get(1).equals(b1),
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }

}
