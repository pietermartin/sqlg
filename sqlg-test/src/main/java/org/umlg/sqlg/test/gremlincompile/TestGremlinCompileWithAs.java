package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.SqlgVertex;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

/**
 * Date: 2015/01/19
 * Time: 6:22 AM
 */
public class TestGremlinCompileWithAs extends BaseTest {

    @Test
    public void testHasLabelOutWithAs() {
        SqlgVertex a1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        SqlgVertex b1 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        SqlgVertex b2 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        SqlgVertex b3 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        SqlgVertex b4 = (SqlgVertex) this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
        a1.addEdge("outB", b1, "seqID", 0);
        a1.addEdge("outB", b2, "seqID", 1);
        a1.addEdge("outB", b3, "seqID", 2);
        a1.addEdge("outB", b4, "seqID", 3);
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Map<String, Element>> traversal = this.sqlgGraph.traversal().V(a1)
                .outE("outB")
                .as("edge1")
                .inV()
                .as("vertex")
                .select();
        List<Map<String, Element>> result = traversal.toList();
        Assert.assertEquals(4, result.size());
    }

}
