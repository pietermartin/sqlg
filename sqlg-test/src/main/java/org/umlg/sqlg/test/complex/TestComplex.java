package org.umlg.sqlg.test.complex;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

import static org.junit.Assert.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/06/14
 */
@SuppressWarnings({"DuplicatedCode", "unchecked", "rawtypes"})
public class TestComplex extends BaseTest {

    @Test
    public void testProject() {
        Map<String, Object> aValues = new HashMap<>();
        aValues.put("name", "root");
        Vertex vA = sqlgGraph.addVertex("A", aValues);
        Map<String, Object> iValues = new HashMap<>();
        iValues.put("name", "item1");
        Vertex vI = sqlgGraph.addVertex("I", iValues);
        vA.addEdge("likes", vI, "howMuch", 5, "who", "Joe");
        this.sqlgGraph.tx().commit();
        Object id0 = vI.id();
        GraphTraversal<Vertex, Map<String, Object>> gt = sqlgGraph.traversal().V()
                .hasLabel("A")
                .has("name", "root")
                .outE("likes")
                .project("stars", "user", "item")
                .by("howMuch")
                .by("who")
                .by(__.inV().id())
                .select("user", "stars", "item");
        Assert.assertTrue(gt.hasNext());
        Map<String, Object> m = gt.next();
        Assert.assertEquals(5, m.get("stars"));
        Assert.assertEquals("Joe", m.get("user"));
        Assert.assertEquals(id0, m.get("item"));
    }

    @Test
    public void playlistPaths() {
        loadGratefulDead();
        final Traversal<Vertex, Map<String, List<String>>> traversal = getPlaylistPaths();
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        Map<String, List<String>> map = traversal.next();
        Assert.assertTrue(map.get("artists").contains("Bob_Dylan"));
        boolean hasJohnnyCash = false;
        while (traversal.hasNext()) {
            map = traversal.next();
            if (map.get("artists").contains("Johnny_Cash"))
                hasJohnnyCash = true;
        }
        Assert.assertTrue(hasJohnnyCash);
        Assert.assertTrue(map.get("artists").contains("Grateful_Dead"));
    }

    @SuppressWarnings("unchecked")
    private Traversal<Vertex, Map<String, List<String>>> getPlaylistPaths() {
        return this.sqlgGraph.traversal().V().has("name", "Bob_Dylan").in("sungBy").as("a").
                repeat(__.out().order().by(Order.shuffle).simplePath().from("a")).
                until(__.out("writtenBy").has("name", "Johnny_Cash")).limit(1).as("b").
                repeat(__.out().order().by(Order.shuffle).as("c").simplePath().from("b").to("c")).
                until(__.out("sungBy").has("name", "Grateful_Dead")).limit(1).
                path().from("a").unfold().
                <List<String>>project("song", "artists").
                by("name").
                by(__.coalesce(__.out("sungBy", "writtenBy").dedup().values("name"), __.constant("Unknown")).fold());
    }

    @Test
    public void coworkerSummary() {
        loadModern();
        final Traversal<Vertex, Map<String, Map<String, Map<String, Object>>>> traversal = this.sqlgGraph.traversal()
                .V().hasLabel("person")
                .filter(__.outE("created")).aggregate("p").as("p1").values("name").as("p1n")
                .select("p").unfold().where(P.neq("p1")).as("p2").values("name").as("p2n").select("p2")
                .out("created").choose(__.in("created").where(P.eq("p1")), __.values("name"), __.constant(Collections.emptyList()))
                .<String, Map<String, Map<String, Object>>>group().by(__.select("p1n")).
                        by(__.group().by(__.select("p2n")).
                                by(__.unfold().fold().project("numCoCreated", "coCreated").by(__.count(Scope.local)).by()));
        this.printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        checkCoworkerSummary(traversal.next());
        Assert.assertFalse(traversal.hasNext());
    }

    private static void checkCoworkerSummary(final Map<String, Map<String, Map<String, Object>>> summary) {
        assertNotNull(summary);
        assertEquals(3, summary.size());
        assertTrue(summary.containsKey("marko"));
        assertTrue(summary.containsKey("josh"));
        assertTrue(summary.containsKey("peter"));
        for (final Map.Entry<String, Map<String, Map<String, Object>>> entry : summary.entrySet()) {
            assertEquals(2, entry.getValue().size());
            switch (entry.getKey()) {
                case "marko":
                    assertTrue(entry.getValue().containsKey("josh") && entry.getValue().containsKey("peter"));
                    break;
                case "josh":
                    assertTrue(entry.getValue().containsKey("peter") && entry.getValue().containsKey("marko"));
                    break;
                case "peter":
                    assertTrue(entry.getValue().containsKey("marko") && entry.getValue().containsKey("josh"));
                    break;
            }
            for (final Map<String, Object> m : entry.getValue().values()) {
                assertTrue(m.containsKey("numCoCreated"));
                assertTrue(m.containsKey("coCreated"));
                assertTrue(m.get("numCoCreated") instanceof Number);
                assertTrue(m.get("coCreated") instanceof Collection);
                assertEquals(1, ((Number) m.get("numCoCreated")).intValue());
                assertEquals(1, ((Collection) m.get("coCreated")).size());
                assertEquals("lop", ((Collection) m.get("coCreated")).iterator().next());
            }
        }
    }
}
