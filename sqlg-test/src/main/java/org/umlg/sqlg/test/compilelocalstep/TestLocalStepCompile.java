package org.umlg.sqlg.test.compilelocalstep;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

/**
 * Date: 2016/05/07
 * Time: 1:46 PM
 */
public class TestLocalStepCompile extends BaseTest {

    @Test
    public void test() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a11 = this.sqlgGraph.addVertex(T.label, "A", "name", "a11");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();

        List<Path> paths = gt.V(a1).local(out().out()).path().toList();
        for (Path path : paths) {
            System.out.println(path);
        }
    }
}
