package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
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
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
        Edge e1 = a1.addEdge("outB", b1, "edgeName", "edge1");
        Edge e2 = a1.addEdge("outB", b2, "edgeName", "edge2");
        Edge e3 = a1.addEdge("outB", b3, "edgeName", "edge3");
        Edge e4 = a1.addEdge("outB", b4, "edgeName", "edge4");
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Map<String, Element>> traversal = this.sqlgGraph.traversal().V(a1)
                .outE("outB")
                .as("e")
                .inV()
                .as("B")
                .select("e", "B");
        List<Map<String, Element>> result = traversal.toList();
        Assert.assertEquals(4, result.size());

        Assert.assertEquals(e1, result.get(0).get("e"));
        Assert.assertEquals("edge1", result.get(0).get("e").value("edgeName"));
        Assert.assertEquals(b1, result.get(0).get("B"));
        Assert.assertEquals("b1", result.get(0).get("B").value("name"));

        Assert.assertEquals(e2, result.get(1).get("e"));
        Assert.assertEquals("edge2", result.get(1).get("e").value("edgeName"));
        Assert.assertEquals(b2, result.get(1).get("B"));
        Assert.assertEquals("b2", result.get(1).get("B").value("name"));

        Assert.assertEquals(e3, result.get(2).get("e"));
        Assert.assertEquals("edge3", result.get(2).get("e").value("edgeName"));
        Assert.assertEquals(b3, result.get(2).get("B"));
        Assert.assertEquals("b3", result.get(2).get("B").value("name"));

        Assert.assertEquals(e4, result.get(3).get("e"));
        Assert.assertEquals("edge4", result.get(3).get("e").value("edgeName"));
        Assert.assertEquals(b4, result.get(3).get("B"));
        Assert.assertEquals("b4", result.get(3).get("B").value("name"));
    }

    @Test
    public void testHasLabelOutWithAsNotFromStart() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Edge e1 = a1.addEdge("outB", b1, "edgeName", "edge1");
        Edge e2 = a1.addEdge("outB", b2, "edgeName", "edge2");
        Edge e3 = a1.addEdge("outB", b3, "edgeName", "edge3");
        Edge e4 = a1.addEdge("outB", b4, "edgeName", "edge4");
        Edge e5 = b1.addEdge("outC", c1, "edgeName", "edge5");
        Edge e6 = b2.addEdge("outC", c2, "edgeName", "edge6");

        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V(a1)
                .out("outB")
                .out("outC")
                .as("x")
                .select("x");
        List<Vertex> result = traversal.toList();
        Assert.assertEquals(2, result.size());

        Assert.assertEquals(c1, result.get(0));
        Assert.assertEquals(c2, result.get(1));

    }
}
