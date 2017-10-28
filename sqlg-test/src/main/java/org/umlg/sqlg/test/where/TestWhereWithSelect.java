package org.umlg.sqlg.test.where;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/10/22
 */
public class TestWhereWithSelect extends BaseTest {

    @Test
    public void g_V_matchXa__a_out_b__notXa_created_bXX() {
        loadModern();
        final Traversal<Vertex, Map<String, Object>> traversal = this.sqlgGraph.traversal().V().match(
                __.as("a").out().as("b"),
                __.not(__.as("a").out("created").as("b")));
        printTraversalForm(traversal);
        checkResults(makeMapList(2,
                "a", convertToVertex(this.sqlgGraph, "marko"), "b", convertToVertex(this.sqlgGraph, "josh"),
                "a", convertToVertex(this.sqlgGraph, "marko"), "b", convertToVertex(this.sqlgGraph, "vadas")), traversal);
    }

    @Test
    public void coworkerSummaryOLTP() {
        loadModern();
        final Traversal<Vertex, Map<String, Map<String, Map<String, Object>>>> traversal =  this.sqlgGraph.traversal()
                .V().hasLabel("person").filter(__.outE("created")).as("p1")
                .V().hasLabel("person").where(P.neq("p1")).filter(__.outE("created")).as("p2")
                .map(__.out("created").where(__.in("created").as("p1")).values("name").fold())
                .<String, Map<String, Map<String, Object>>>group().by(__.select("p1").by("name")).
                        by(__.group().by(__.select("p2").by("name")).
                                by(__.project("numCoCreated", "coCreated").by(__.count(Scope.local)).by()));
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        checkCoworkerSummary(traversal.next());
        Assert.assertFalse(traversal.hasNext());
    }

    private static void checkCoworkerSummary(final Map<String, Map<String, Map<String, Object>>> summary) {
        Assert.assertNotNull(summary);
        Assert.assertEquals(3, summary.size());
        Assert.assertTrue(summary.containsKey("marko"));
        Assert.assertTrue(summary.containsKey("josh"));
        Assert.assertTrue(summary.containsKey("peter"));
        for (final Map.Entry<String, Map<String, Map<String, Object>>> entry : summary.entrySet()) {
            Assert.assertEquals(2, entry.getValue().size());
            switch (entry.getKey()) {
                case "marko":
                    Assert.assertTrue(entry.getValue().containsKey("josh") && entry.getValue().containsKey("peter"));
                    break;
                case "josh":
                    Assert.assertTrue(entry.getValue().containsKey("peter") && entry.getValue().containsKey("marko"));
                    break;
                case "peter":
                    Assert.assertTrue(entry.getValue().containsKey("marko") && entry.getValue().containsKey("josh"));
                    break;
            }
            for (final Map<String, Object> m : entry.getValue().values()) {
                Assert.assertTrue(m.containsKey("numCoCreated"));
                Assert.assertTrue(m.containsKey("coCreated"));
                Assert.assertTrue(m.get("numCoCreated") instanceof Number);
                Assert.assertTrue(m.get("coCreated") instanceof Collection);
                Assert.assertEquals(1, ((Number) m.get("numCoCreated")).intValue());
                Assert.assertEquals(1, ((Collection) m.get("coCreated")).size());
                Assert.assertEquals("lop", ((Collection) m.get("coCreated")).iterator().next());
            }
        }
    }

    @Test
    public void g_V_asXaX_out_asXbX_whereXin_count_isXeqX3XX_or_whereXoutXcreatedX_and_hasXlabel_personXXX_selectXa_bX() {
        loadModern();
        final Traversal<Vertex, Map<String, Object>> traversal = this.sqlgGraph.traversal()
                .V().as("a").out().as("b")
                .where(
                        __.as("b").in().count().is(P.eq(3))
                                .or()
                                .where(
                                        __.as("b").out("created").and().as("b").has(T.label, "person"))
                )
                .select("a", "b");
        printTraversalForm(traversal);
//        while (traversal.hasNext()) {
//            System.out.println(traversal.next());
//        }
        checkResults(makeMapList(2,
                "a", convertToVertex(this.sqlgGraph, "marko"), "b", convertToVertex(this.sqlgGraph, "josh"),
                "a", convertToVertex(this.sqlgGraph, "marko"), "b", convertToVertex(this.sqlgGraph, "lop"),
                "a", convertToVertex(this.sqlgGraph, "peter"), "b", convertToVertex(this.sqlgGraph, "lop"),
                "a", convertToVertex(this.sqlgGraph, "josh"), "b", convertToVertex(this.sqlgGraph, "lop")), traversal);
    }

    @Test
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX() {
        loadModern();

        final Traversal<Vertex, Map<String, Object>> traversal = this.sqlgGraph.traversal()
                .V().has("age").as("a")
                .out().in().has("age").as("b")
                .select("a", "b")
                .where(__.as("a").out("knows").as("b"));

        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Object> map = traversal.next();
            Assert.assertEquals(2, map.size());
            Assert.assertTrue(map.containsKey("a"));
            Assert.assertTrue(map.containsKey("b"));
            Assert.assertEquals(convertToVertexId("marko"), ((Vertex) map.get("a")).id());
            Assert.assertEquals(convertToVertexId("josh"), ((Vertex) map.get("b")).id());
        }
        Assert.assertEquals(1, counter);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testWhere() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a2.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Vertex> traversal = sqlgGraph.traversal()
                .V().hasLabel("A").as("a")
                .where(
                        __.as("a")
                                .out()
                )
                .select("a");
        List<Vertex> result = traversal.toList();
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.contains(a1) && result.contains(a2));
    }

    @Test
    public void testWhere2Labels() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a2.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Map<String, Vertex>> traversal = this.sqlgGraph.traversal()
                .V().hasLabel("A").as("a").out().as("b")
                .where(
                        __.as("a").out().as("b")
                )
                .select("a", "b");
        List<Map<String, Vertex>> result = traversal.toList();
        Assert.assertEquals(4, result.size());
        for (Map<String, Vertex> stringVertexMap : result) {
            Assert.assertEquals(2, stringVertexMap.size());
            Assert.assertTrue(stringVertexMap.containsKey("a"));
            Assert.assertTrue(stringVertexMap.containsKey("b"));
        }
        boolean found1 = false;
        boolean found2 = false;
        boolean found3 = false;
        boolean found4 = false;
        for (Map<String, Vertex> stringVertexMap : result) {
            if (!found1) {
                found1 = stringVertexMap.get("a").equals(a1) && stringVertexMap.get("b").equals(b1);
            }
            if (!found2) {
                found2 = stringVertexMap.get("a").equals(a1) && stringVertexMap.get("b").equals(b2);
            }
            if (!found3) {
                found3 = stringVertexMap.get("a").equals(a2) && stringVertexMap.get("b").equals(b1);
            }
            if (!found4) {
                found4 = stringVertexMap.get("a").equals(a2) && stringVertexMap.get("b").equals(b2);
            }
        }
        Assert.assertTrue(found1);
        Assert.assertTrue(found2);
        Assert.assertTrue(found3);
        Assert.assertTrue(found4);
        System.out.println(result);
    }

}
