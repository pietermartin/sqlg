package org.umlg.sqlg.test.match;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

/**
 * Date: 2017/04/18
 * Time: 10:22 PM
 */
public class TestMatch extends BaseTest {

    @Test
    public void testMatch() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Map<String, Object>> traversal = (DefaultGraphTraversal<Vertex, Map<String, Object>>) this.sqlgGraph.traversal()
                .V()
                .match(
                        __.as("a").in("ab").as("x"),
                        __.as("a").out("bc").as("y")
                );
        List<Map<String, Object>> result = traversal.toList();
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.get(0).containsKey("a"));
        Assert.assertTrue(result.get(0).containsKey("x"));
        Assert.assertTrue(result.get(0).containsKey("y"));
        Assert.assertEquals(b1, result.get(0).get("a"));
        Assert.assertEquals(a1, result.get(0).get("x"));
        Assert.assertEquals(c1, result.get(0).get("y"));
    }

    @Test
    public void testMatch2() {
        loadModern();
        Vertex marko = convertToVertex(this.sqlgGraph, "marko");
        DefaultGraphTraversal<Vertex, Map<String, Object>> traversal = (DefaultGraphTraversal<Vertex, Map<String, Object>>)this.sqlgGraph.traversal()
                .V(marko)
                .match(
                        __.as("a").out("knows").as("b"),
                        __.as("a").out("created").as("c")
                );
//        List<Map<String, Object>> result = traversal.toList();
        checkResults(makeMapList(3,
                "a", convertToVertex(this.sqlgGraph, "marko"), "b", convertToVertex(this.sqlgGraph, "vadas"), "c", convertToVertex(this.sqlgGraph, "lop"),
                "a", convertToVertex(this.sqlgGraph, "marko"), "b", convertToVertex(this.sqlgGraph, "josh"), "c", convertToVertex(this.sqlgGraph, "lop")),
                traversal
        );
        
    }
}
