package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.junit.Assert.assertEquals;

/**
 * Date: 2016/05/31
 * Time: 7:32 PM
 */
public class TestOptionalWithOrder extends BaseTest {

    @Test
    public void testOptionalWithOrder() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "order", 3);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "order", 2);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "order", 1);
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();
//        List<Vertex> vertices =  this.sqlgGraph.traversal().V(a1.id()).as("a").optional(outE().as("e").otherV().as("v")).order().by(select("v").by("order"), Order.incr).toList();
        List<Vertex> vertices =  this.sqlgGraph.traversal().V(a1.id()).as("a").optional(outE().as("e").otherV().as("v")).order().by("order").toList();
        assertEquals(3, vertices.size());
        for (Vertex vertex : vertices) {
            System.out.println(vertex.<Integer>value("order"));
        }
    }
}
