package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.List;

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
        GraphTraversalSource gt = this.sqlgGraph.traversal();
//        Assert.assertTrue(gt.V(a1).out("ab").toList().containsAll(Arrays.asList(b11, b12, b13)));
        List<Vertex> vertexes = gt.V().hasLabel("A").out("ab").toList();
        Assert.assertEquals(9, vertexes.size());
        for (Vertex vertex : vertexes) {
            System.out.println(vertex);
        }
        Assert.assertEquals(9, vertexes.size());
        Assert.assertTrue(vertexes.containsAll(Arrays.asList(b11, b12, b13, b21, b22, b23, b31, b32, b33)));
    }
}
