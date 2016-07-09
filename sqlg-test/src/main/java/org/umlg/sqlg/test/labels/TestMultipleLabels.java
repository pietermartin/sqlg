package org.umlg.sqlg.test.labels;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/06/05
 * Time: 2:18 PM
 */
public class TestMultipleLabels extends BaseTest {

    @Test
    public void testMultipleLabels() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices =  this.sqlgGraph.traversal().V(a1.id()).as("a").out("ab").as("a").out("bc").as("a").toList();
        assertEquals(1, vertices.size());
    }

    @Test
    public void testSameElementHasMultipleLabels() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        b1.addEdge("bc", c1);
        b2.addEdge("bc", c1);
        b3.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal()
                .V(a1.id()).as("a", "b")
                .out("ab").as("a", "b")
                .out("bc")
                .path()
                .toList();
        for (Path path : paths) {
            System.out.println(path);
        }

        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V(a1.id())
                .out("ab")
                .out("bc")
                .toList();
        for (Vertex v: vertices) {
            System.out.println(v);
        }

    }
}
