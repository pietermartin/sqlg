package org.umlg.sqlg.test.match;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.test.BaseTest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 2017/04/18
 * Time: 10:22 PM
 */
public class TestMatch extends BaseTest {

    @Test
    public void testDuplicateQueryJoin() {
        Schema aSchema = this.sqlgGraph.getTopology().getPublicSchema();
        aSchema.ensureVertexLabelExist("person",
                new HashMap<>() {{
                    put("personid", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(Collections.singletonList("personid"))
        );
        this.sqlgGraph.tx().commit();

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "person", "personid", "1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "person", "personid", "2");
        v1.addEdge("knows", v2);
        this.sqlgGraph.tx().commit();

// Evalute the match step using ScriptEngine
        String query = "g.V().match(__.as('a').out('knows').as('b')).select('a','b')";
        GraphTraversal<Vertex, Map<String, Vertex>> traversal = this.sqlgGraph.traversal().V()
                .match(
                        __.as("a").out("knows").as("b")
                ).select("a", "b");
        printTraversalForm(traversal);
        List<Map<String, Vertex>> result = traversal.toList();
        System.out.println(result);
    }

    @Test
    public void testDuplicateQueryJoinMultipleKeys() {
        Schema aSchema = this.sqlgGraph.getTopology().getPublicSchema();
        aSchema.ensureVertexLabelExist("person",
                new HashMap<>() {{
                    put("personid1", PropertyType.varChar(100));
                    put("personid2", PropertyType.varChar(100));
                }},
                ListOrderedSet.listOrderedSet(List.of("personid1", "personid2"))
        );
        this.sqlgGraph.tx().commit();

        Vertex v1 = this.sqlgGraph.addVertex(T.label, "person", "personid1", "1", "personid2", "1");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "person", "personid1", "2", "personid2", "2");
        v1.addEdge("knows", v2);
        this.sqlgGraph.tx().commit();

// Evalute the match step using ScriptEngine
        String query = "g.V().match(__.as('a').out('knows').as('b')).select('a','b')";
        GraphTraversal<Vertex, Map<String, Vertex>> traversal = this.sqlgGraph.traversal().V()
                .match(
                        __.as("a").out("knows").as("b")
                ).select("a", "b");
        printTraversalForm(traversal);
        List<Map<String, Vertex>> result = traversal.toList();
        System.out.println(result);
    }

    @Test
    public void g_V_matchXa__a_out_b__notXa_created_bXX() {
        loadModern();
        final Traversal<Vertex, Map<String, Object>> traversal = this.sqlgGraph.traversal().V().match(
                __.as("a").out().as("b"),
                __.not(__.as("a").out("created").as("b")));
        printTraversalForm(traversal);
        checkResults(makeMapList(2,
                "a", convertToVertex(this.sqlgGraph, "marko"),
                "b", convertToVertex(this.sqlgGraph, "josh"),
                "a", convertToVertex(this.sqlgGraph, "marko"),
                "b", convertToVertex(this.sqlgGraph, "vadas")), traversal);
    }

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
