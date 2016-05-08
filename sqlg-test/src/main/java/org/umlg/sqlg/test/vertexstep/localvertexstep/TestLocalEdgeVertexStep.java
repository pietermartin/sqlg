package org.umlg.sqlg.test.vertexstep.localvertexstep;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/05/08
 * Time: 4:36 PM
 */
public class TestLocalEdgeVertexStep extends BaseTest {

    @Test
    public void testLocalEdgeVertexStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Edge e1 = a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices =  this.sqlgGraph.traversal().V(a1).local(__.outE("ab").inV()).toList();
        assertEquals(1, vertices.size());
        assertEquals(b1, vertices.get(0));

    }
}
