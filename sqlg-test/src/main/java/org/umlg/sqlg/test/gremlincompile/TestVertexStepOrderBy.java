package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
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

    @Test
    public void testVertexStepOrderBy() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "a");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "c");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();
        List<Map<String, Vertex>> result = this.sqlgGraph.traversal()
                .V(a1).as("a")
                .out().as("b")
                .<Vertex>select("a", "b")
                .order()
                .by(__.select("b").by("name"), Order.incr)
                .toList();

        Assert.assertEquals(3, result.size());
        Assert.assertEquals(b1, result.get(0).get("b"));
        Assert.assertEquals(b2, result.get(1).get("b"));
        Assert.assertEquals(b3, result.get(2).get("b"));

        result = this.sqlgGraph.traversal()
                .V(a1).as("a")
                .out().as("b")
                .<Vertex>select("a", "b")
                .order()
                .by(__.select("b").by("name"), Order.decr)
                .toList();

        Assert.assertEquals(3, result.size());
        Assert.assertEquals(b1, result.get(2).get("b"));
        Assert.assertEquals(b2, result.get(1).get("b"));
        Assert.assertEquals(b3, result.get(0).get("b"));
    }

    @Test
    public void testOrderby() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "a");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "c");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();
        List<Vertex> result = this.sqlgGraph.traversal()
                .V(a1)
                .out()
                .order()
                .by("name", Order.incr)
                .toList();
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(b1, result.get(0));
        Assert.assertEquals(b2, result.get(1));
        Assert.assertEquals(b3, result.get(2));

        result = this.sqlgGraph.traversal()
                .V(a1)
                .out()
                .order()
                .by("name", Order.decr)
                .toList();
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(b1, result.get(2));
        Assert.assertEquals(b2, result.get(1));
        Assert.assertEquals(b3, result.get(0));
    }

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
        Assert.assertEquals(a3, result.get(0));
        Assert.assertEquals(a2, result.get(1));
    }

    @Test
    public void testOrderbyDuplicatePathLabaled() {
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
                .out().as("x")
                .<Vertex>select("x")
                .order()
                .by(__.select("x").by("name"), Order.decr)
                .toList();
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(a3, result.get(0));
        Assert.assertEquals(a2, result.get(1));
    }

    @Test
    public void testOrderbyDuplicatePathOrderInMemory() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "aa");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "a");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "c");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "ab");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "ac");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "ca");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        b1.addEdge("ba", a2);
        b1.addEdge("ba", a3);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        List<Vertex> result = this.sqlgGraph.traversal()
                .V(a1)
                .out()
                .out()
                .order()
                .by("name", Order.incr)
                .toList();
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(c1, result.get(2));
        Assert.assertEquals(a3, result.get(1));
        Assert.assertEquals(a2, result.get(0));
    }

    @Test
    public void testOrderOnEdge() {
        Vertex god = this.sqlgGraph.addVertex(T.label, "God");
        Vertex fantasy1 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan1");
        Vertex fantasy2 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan2");
        Vertex fantasy3 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan3");
        Vertex fantasy4 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan4");
        god.addEdge("godDream", fantasy1, "sequence", 1);
        god.addEdge("godDream", fantasy2, "sequence", 2);
        god.addEdge("godDream", fantasy3, "sequence", 3);
        god.addEdge("godDream", fantasy4, "sequence", 4);
        this.sqlgGraph.tx().commit();
        List<Vertex> result = this.sqlgGraph.traversal().V(god)
                .outE("godDream").as("e")
                .inV().as("v")
                .select("e", "v")
                .order().by(__.select("e").by("sequence"), Order.decr)
                .map(m -> (Vertex)m.get().get("v"))
                .toList();
        Assert.assertEquals(fantasy4, result.get(0));
        Assert.assertEquals(fantasy3, result.get(1));
        Assert.assertEquals(fantasy2, result.get(2));
        Assert.assertEquals(fantasy1, result.get(3));
    }

//    @Test
//    public void testOrderOnEdgeClearThreadVarOnFailures() {
//        Vertex god = this.sqlgGraph.addVertex(T.label, "God");
//        Vertex fantasy1 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan1");
//        Vertex fantasy2 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan2");
//        Vertex fantasy3 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan3");
//        Vertex fantasy4 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan4");
//        god.addEdge("godDream", fantasy1);
//        god.addEdge("godDream", fantasy2);
//        god.addEdge("godDream", fantasy3);
//        god.addEdge("godDream", fantasy4);
//        this.sqlgGraph.tx().commit();
//        List<Vertex> result = this.sqlgGraph.traversal().V(god)
//                .outE("godDream").as("e")
//                .inV().as("v")
//                .select("e", "v")
//                .order().by(__.select("e").by("sequence"), Order.decr)
//                .map(m -> (Vertex)m.get().get("v"))
//                .toList();
//        Assert.assertTrue(SchemaTableTree.threadLocalAliasColumnNameMap.get().isEmpty());
//        Assert.assertTrue(SchemaTableTree.threadLocalColumnNameAliasMap.get().isEmpty());
//    }

    @Test
    public void testSelectVertexAndEdgeOrderByEdge() {
        Vertex god = this.sqlgGraph.addVertex(T.label, "God");
        Vertex fantasy1 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan1");
        Vertex fantasy2 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan2");
        Vertex fantasy3 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan3");
        Vertex fantasy4 = this.sqlgGraph.addVertex(T.label, "Fantasy", "name", "fan4");
        Edge e1 = god.addEdge("godDream", fantasy1, "sequence", 1);
        Edge e2 = god.addEdge("godDream", fantasy2, "sequence", 2);
        Edge e3 = god.addEdge("godDream", fantasy3, "sequence", 3);
        Edge e4 = god.addEdge("godDream", fantasy4, "sequence", 4);
        this.sqlgGraph.tx().commit();
        List<Map<String, Object>> result = this.sqlgGraph.traversal().V(god)
                .outE("godDream").as("e")
                .inV().as("v")
                .select("e", "v")
                .order().by(__.select("e").by("sequence"), Order.decr)
                .toList();

        Assert.assertEquals(4, result.size());
        Assert.assertEquals(fantasy4, result.get(0).get("v"));
        Assert.assertEquals(fantasy3, result.get(1).get("v"));
        Assert.assertEquals(fantasy2, result.get(2).get("v"));
        Assert.assertEquals(fantasy1, result.get(3).get("v"));

        Assert.assertEquals(e4, result.get(0).get("e"));
        Assert.assertEquals(e3, result.get(1).get("e"));
        Assert.assertEquals(e2, result.get(2).get("e"));
        Assert.assertEquals(e1, result.get(3).get("e"));
    }

    @Test
    public void testOrderByToSelf() {

        Vertex root = this.sqlgGraph.addVertex(T.label, "Root");
        Vertex folder1 = this.sqlgGraph.addVertex(T.label, "Folder");
        Vertex folder2 = this.sqlgGraph.addVertex(T.label, "Folder");
        Edge e1 = root.addEdge("rootFolder", folder1);
        Edge e2 = folder1.addEdge("subFolder", folder2, "sequence", 1);
        this.sqlgGraph.tx().commit();
        List<Vertex> result = this.sqlgGraph.traversal().V(folder1)
                .outE("subFolder").as("e")
                .inV().as("v")
                .select("e", "v")
                .order().by(__.select("e").by("sequence"), Order.incr)
                .map(m -> (Vertex)m.get().get("v")).toList();

        Assert.assertEquals(1, result.size());
    }

}
