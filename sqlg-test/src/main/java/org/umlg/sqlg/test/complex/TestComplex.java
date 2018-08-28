package org.umlg.sqlg.test.complex;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/06/14
 */
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
        Assert.assertEquals(new Integer(5), m.get("stars"));
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

}
