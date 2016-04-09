package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

/**
 * Date: 2015/11/21
 * Time: 2:24 PM
 */
public class TestGremlinCompileVertexStep extends BaseTest {

    @Test
    public void testVertexStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        List<Map<String, Object>> t = this.sqlgGraph.traversal().V(a1).as("a").local(__.out().as("b")).select("a", "b").toList();
        for (Map<String, Object> stringObjectMap : t) {
            System.out.println(stringObjectMap);
        }
    }
}
