package org.umlg.sqlg.test.complex;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 *         Date: 2017/06/14
 */
public class TestComplex extends BaseTest {

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

    public Traversal<Vertex, Map<String, List<String>>> getPlaylistPaths() {
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
