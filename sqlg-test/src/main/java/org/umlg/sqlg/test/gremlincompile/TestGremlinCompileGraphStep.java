package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by pieter on 2015/07/19.
 */
public class TestGremlinCompileGraphStep extends BaseTest {

    @Test
    public void testCompileGraphStep() {

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");

        Vertex b11 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b12 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b13 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b21 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b22 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b23 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b31 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b32 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b33 = this.sqlgGraph.addVertex(T.label, "B");

        a1.addEdge("ab", b11);
        a1.addEdge("ab", b12);
        a1.addEdge("ab", b13);
        a2.addEdge("ab", b21);
        a2.addEdge("ab", b22);
        a2.addEdge("ab", b23);
        a3.addEdge("ab", b31);
        a3.addEdge("ab", b32);
        a3.addEdge("ab", b33);

        this.sqlgGraph.tx().commit();
        GraphTraversalSource g = this.sqlgGraph.traversal();
        List<Vertex> vertexes = g.V().hasLabel("A").out("ab").toList();
        Assert.assertEquals(9, vertexes.size());
        Assert.assertEquals(9, vertexes.size());
        Assert.assertTrue(vertexes.containsAll(Arrays.asList(b11, b12, b13, b21, b22, b23, b31, b32, b33)));

        vertexes = g.V().has(T.label, "A").out("ab").toList();
        Assert.assertEquals(9, vertexes.size());
        Assert.assertEquals(9, vertexes.size());
        Assert.assertTrue(vertexes.containsAll(Arrays.asList(b11, b12, b13, b21, b22, b23, b31, b32, b33)));

        vertexes = g.V().has(T.label, "A").toList();
        Assert.assertEquals(3, vertexes.size());

        GraphTraversal<Vertex, Map<String, Vertex>> gt = g.V().hasLabel("A").as("a").out("ab").as("b").select("a", "b");
        List<Map<String, Vertex>> list = gt.toList();
        Assert.assertEquals(9, list.size());
        Assert.assertEquals(a1, list.get(0).get("a"));
        Assert.assertEquals(b11, list.get(0).get("b"));
        Assert.assertEquals(a1, list.get(1).get("a"));
        Assert.assertEquals(b12, list.get(1).get("b"));
        Assert.assertEquals(a3, list.get(8).get("a"));
        Assert.assertEquals(b33, list.get(8).get("b"));
    }

    @Test
    public void testVWithHas() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "b");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "c");
        this.sqlgGraph.tx().commit();
        GraphTraversalSource g = this.sqlgGraph.traversal();
        List<Vertex> vertexes = g.V().has(T.label, "A").has("name", "a").toList();
        Assert.assertEquals(1, vertexes.size());
    }

    @Test
    public void testE() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Edge e1 = a1.addEdge("ab", b1);
        Edge e2 = a1.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();
        GraphTraversalSource gts = this.sqlgGraph.traversal();
        List<Edge> edges = gts.E().hasLabel("ab").toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertTrue(edges.containsAll(Arrays.asList(e1, e2)));
    }

    @Test
    public void TestEWithLabels() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c11 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c12 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c21 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c22 = this.sqlgGraph.addVertex(T.label, "C");
        Edge e1 = a1.addEdge("ab", b1);
        Edge e2 = a1.addEdge("ab", b2);
        Edge bc1 = b1.addEdge("bc", c11);
        Edge bc2 = b1.addEdge("bc", c12);
        Edge bc3 = b2.addEdge("bc", c21);
        Edge bc4 = b2.addEdge("bc", c22);
        this.sqlgGraph.tx().commit();
        GraphTraversalSource gts = this.sqlgGraph.traversal();
        List<Map<String, Object>> result = gts.E().hasLabel("ab").as("ab").inV().as("b").out("bc").as("c").select("ab", "b", "c").toList();
        Assert.assertEquals(4, result.size());
        Map<String, Object> c1 = result.get(0);
        Assert.assertTrue(c1.containsKey("ab") && c1.containsKey("b") && c1.containsKey("c"));
        Map<String, Object> c2 = result.get(1);
        Assert.assertTrue(c2.containsKey("ab") && c2.containsKey("b") && c2.containsKey("c"));
        Map<String, Object> c3 = result.get(2);
        Assert.assertTrue(c3.containsKey("ab") && c1.containsKey("b") && c3.containsKey("c"));
        Map<String, Object> c4 = result.get(3);
        Assert.assertTrue(c4.containsKey("ab") && c1.containsKey("b") && c4.containsKey("c"));

        Assert.assertTrue(c1.get("c") instanceof Vertex);
        Vertex c = (Vertex) c1.get("c");
        Assert.assertEquals("C", c.label());
        Vertex b = (Vertex) c1.get("b");
        Assert.assertEquals("B", b.label());
        Edge e = (Edge) c1.get("ab");
        Assert.assertEquals("ab", e.label());
    }
}
