package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

/**
 * Created by pieter on 2015/08/30.
 */
public class TestVertexStepOrderBy extends BaseTest {

//    @Test
//    public void testVertexStepOrderBy() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "a");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "c");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        this.sqlgGraph.tx().commit();
//        List<Map<String, Vertex>> result = this.sqlgGraph.traversal()
//                .V(a1).as("a")
//                .out().as("b")
//                .<Vertex>select("a", "b")
//                .order()
//                .by(__.select("b").by("name"), Order.incr)
//                .toList();
//
//        Assert.assertEquals(3, result.size());
//        Assert.assertEquals(b1, result.get(0).get("b"));
//        Assert.assertEquals(b2, result.get(1).get("b"));
//        Assert.assertEquals(b3, result.get(2).get("b"));
//
//        result = this.sqlgGraph.traversal()
//                .V(a1).as("a")
//                .out().as("b")
//                .<Vertex>select("a", "b")
//                .order()
//                .by(__.select("b").by("name"), Order.decr)
//                .toList();
//
//        Assert.assertEquals(3, result.size());
//        Assert.assertEquals(b1, result.get(2).get("b"));
//        Assert.assertEquals(b2, result.get(1).get("b"));
//        Assert.assertEquals(b3, result.get(0).get("b"));
//    }
//
//    @Test
//    public void testOrderby() {
//        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
//        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "a");
//        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
//        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "c");
//        a1.addEdge("ab", b1);
//        a1.addEdge("ab", b2);
//        a1.addEdge("ab", b3);
//        this.sqlgGraph.tx().commit();
//        List<Vertex> result = this.sqlgGraph.traversal()
//                .V(a1)
//                .out()
//                .order()
//                .by("name", Order.incr)
//                .toList();
//        Assert.assertEquals(3, result.size());
//        Assert.assertEquals(b1, result.get(0));
//        Assert.assertEquals(b2, result.get(1));
//        Assert.assertEquals(b3, result.get(2));
//
//        result = this.sqlgGraph.traversal()
//                .V(a1)
//                .out()
//                .order()
//                .by("name", Order.decr)
//                .toList();
//        Assert.assertEquals(3, result.size());
//        Assert.assertEquals(b1, result.get(2));
//        Assert.assertEquals(b2, result.get(1));
//        Assert.assertEquals(b3, result.get(0));
//    }

    @Test
    public void testOrderbyDuplicatePath() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "aa");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "a");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "c");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "ab");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "ac");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        b1.addEdge("ba", a2);
        b1.addEdge("ba", a3);
        this.sqlgGraph.tx().commit();
        List<Vertex> result = this.sqlgGraph.traversal()
                .V(a1)
                .out()
                .out()
                .order()
                .by("name", Order.decr)
                .toList();
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(a2, result.get(0));
        Assert.assertEquals(a3, result.get(1));

    }

}
