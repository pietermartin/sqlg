package org.umlg.sqlg.test.union;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/05/30
 * Time: 9:01 PM
 */
public class TestUnion extends BaseTest {

    @Test
    public void testUnionAsPerUMLG() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex bb1 = this.sqlgGraph.addVertex(T.label, "BB");
        Vertex bb2 = this.sqlgGraph.addVertex(T.label, "BB");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        bb1.addEdge("ab", a1);
        bb2.addEdge("ab", a1);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V(a1).in("ab").toList();
        assertEquals(2, vertices.size());
        vertices = this.sqlgGraph.traversal().V(a1).union(optional(out("ab")), optional(in("ab"))).toList();
        assertEquals(4, vertices.size());
        vertices = this.sqlgGraph.traversal().V(a1).union(out("ab"), in("ab")).toList();
        assertEquals(4, vertices.size());
        vertices = this.sqlgGraph.traversal().V(a1).union(out("ab"), in("ab")).toList();
        assertEquals(4, vertices.size());
    }

}
