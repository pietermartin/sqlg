package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

/**
 * Date: 2015/01/19
 * Time: 6:22 AM
 */
public class TestGremlinCompileWithAs extends BaseTest {

    @Test
    public void testSchemaTableTreeNextSchemaTableTreeIsEdgeVertex() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Edge e1 = a1.addEdge("outA", b1);
        Edge e2 = b1.addEdge("outB", c1);
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Map<String, Vertex>> gt = this.sqlgGraph.traversal().V(a1).out().as("b").out().as("c").select("b", "c");
        List<Map<String, Vertex>> list = gt.toList();
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(b1, list.get(0).get("b"));
        Assert.assertEquals(c1, list.get(0).get("c"));

    }

//    @Test
//    public void testHasLabelOutWithAs() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
//        Edge e1 = a1.addEdge("outB", b1, "edgeName", "edge1");
//        Edge e2 = a1.addEdge("outB", b2, "edgeName", "edge2");
//        Edge e3 = a1.addEdge("outB", b3, "edgeName", "edge3");
//        Edge e4 = a1.addEdge("outB", b4, "edgeName", "edge4");
//        this.sqlgGraph.tx().commit();
//        GraphTraversal<Vertex, Map<String, Element>> traversal = this.sqlgGraph.traversal().V(a1)
//                .outE("outB")
//                .as("e")
//                .inV()
//                .as("B")
//                .select("e", "B");
//        List<Map<String, Element>> result = traversal.toList();
//        Collections.sort(result, (o1, o2) -> o1.get("e").<String>value("edgeName").compareTo(o2.get("e").<String>value("edgeName")));
//        Assert.assertEquals(4, result.size());
//
//        Edge queryE0 = (Edge) result.get(0).get("e");
//        Vertex queryB0 = (Vertex) result.get(0).get("B");
//        Assert.assertEquals(e1, queryE0);
//        Assert.assertEquals("b1", queryE0.inVertex().value("name"));
//        Assert.assertEquals("a1", queryE0.outVertex().value("name"));
//        Assert.assertEquals("edge1", queryE0.value("edgeName"));
//        Assert.assertEquals(b1, queryB0);
//        Assert.assertEquals("b1", queryB0.value("name"));
//
//        Element queryE2 = result.get(1).get("e");
//        Element queryB2 = result.get(1).get("B");
//        Assert.assertEquals(e2, queryE2);
//        Assert.assertEquals("edge2", queryE2.value("edgeName"));
//        Assert.assertEquals(b2, queryB2);
//        Assert.assertEquals("b2", queryB2.value("name"));
//
//        Element queryE3 = result.get(2).get("e");
//        Element queryB3 = result.get(2).get("B");
//        Assert.assertEquals(e3, queryE3);
//        Assert.assertEquals("edge3", queryE3.value("edgeName"));
//        Assert.assertEquals(b3, queryB3);
//        Assert.assertEquals("b3", queryB3.value("name"));
//
//        Element queryE4 = result.get(3).get("e");
//        Element queryB4 = result.get(3).get("B");
//        Assert.assertEquals(e4, queryE4);
//        Assert.assertEquals("edge4", queryE4.value("edgeName"));
//        Assert.assertEquals(b4, queryB4);
//        Assert.assertEquals("b4", queryB4.value("name"));
//
//        final List<Edge> a1Edges = this.sqlgGraph.traversal().V(a1.id()).bothE().toList();
//        Assert.assertEquals(4, a1Edges.size());
//        List<String> names = new ArrayList<>(Arrays.asList("b1", "b2", "b3", "b4"));
//        for (Edge a1Edge : a1Edges) {
//            names.remove(a1Edge.inVertex().<String>value("name"));
//            Assert.assertEquals("a1", a1Edge.outVertex().<String>value("name"));
//        }
//        Assert.assertTrue(names.isEmpty());
//
//    }
//
//    @Test
//    public void testHasLabelOutWithAsNotFromStart() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
//        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
//        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
//        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
//        Edge e1 = a1.addEdge("outB", b1, "edgeName", "edge1");
//        Edge e2 = a1.addEdge("outB", b2, "edgeName", "edge2");
//        Edge e3 = a1.addEdge("outB", b3, "edgeName", "edge3");
//        Edge e4 = a1.addEdge("outB", b4, "edgeName", "edge4");
//        Edge e5 = b1.addEdge("outC", c1, "edgeName", "edge5");
//        Edge e6 = b2.addEdge("outC", c2, "edgeName", "edge6");
//
//        this.sqlgGraph.tx().commit();
//        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V(a1)
//                .out("outB")
//                .out("outC")
//                .as("x")
//                .select("x");
//        List<Vertex> result = traversal.toList();
//        Collections.sort(result, (o1, o2) -> o1.<String>value("name").compareTo(o2.<String>value("name")));
//        Assert.assertEquals(2, result.size());
//
//        Assert.assertEquals(c1, result.get(0));
//        Assert.assertEquals(c2, result.get(1));
//
//    }
//
//    @Test
//    public void testAsWithDuplicatePaths() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a1");
//        Vertex a2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "a2");
//        Edge e1 = a1.addEdge("friend", a2, "weight", 5);
//        this.sqlgGraph.tx().commit();
//
//        GraphTraversal<Vertex,Map<String,Element>> gt = this.sqlgGraph.traversal()
//                .V(a1)
//                .outE().as("e")
//                .inV()
//                .in().as("v")
//                .select("e","v");
//
//        List<Map<String,Element>> result = gt.toList();
//        Assert.assertEquals(1, result.size());
//        Assert.assertEquals(e1, result.get(0).get("e"));
//        Assert.assertEquals(a1, result.get(0).get("v"));
//    }
}
