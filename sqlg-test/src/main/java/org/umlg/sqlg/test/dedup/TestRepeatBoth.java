package org.umlg.sqlg.test.dedup;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;

/**
 * Date: 2016/10/18
 * Time: 7:49 AM
 */
public class TestRepeatBoth extends BaseTest {

//    @Test
    public void testDedupFailure() {
        loadModern();
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V().as("a")
                .repeat(both()).times(3).emit().as("b")
                .toList();
    }

    @Test
    public void testRepeatEmitLabel() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);

        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V(a1.id()).as("a")
                .repeat(both()).times(2).emit()
                .toList();
    }
}
