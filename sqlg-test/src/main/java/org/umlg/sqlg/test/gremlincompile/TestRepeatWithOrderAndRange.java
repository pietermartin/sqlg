package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;
import java.util.function.Predicate;

/**
 * Date: 2017/04/07
 * Time: 10:31 AM
 */
public class TestRepeatWithOrderAndRange extends BaseTest {

    @Test
    public void testVertexStepOrderBy() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex aa1 = this.sqlgGraph.addVertex(T.label, "AA", "name", "aa1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1", "order", 2);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2", "order", 4);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1", "order", 1);
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2", "order", 3);
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        aa1.addEdge("ac", c1);
        aa1.addEdge("ac", c2);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V()
                .has(T.label, P.within("A", "AA"))
                .dedup()
                .out().order().by("order", Order.desc);
        printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        List<String> names = new ArrayList<>();
        for (Vertex vertex : vertices) {
            names.add(vertex.value("name"));
        }
        Assert.assertEquals(List.of("b2", "c2", "b1", "c1"), names);
    }

    @Test
    public void testRepeatMultipleEdgesWithOrder2() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1", "order", 1);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2", "order", 3);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3", "order", 5);
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1", "order", 2);
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2", "order", 4);
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3", "order", 6);
        a1.addEdge("ac", c1);
        a1.addEdge("ac", c2);
        a1.addEdge("ac", c3);
        Vertex d1 = this.sqlgGraph.addVertex(T.label, "D", "name", "d1", "order", 7);
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D", "name", "d2", "order", 9);
        Vertex d3 = this.sqlgGraph.addVertex(T.label, "D", "name", "d3", "order", 11);
        b1.addEdge("bd", d1);
        b1.addEdge("bd", d2);
        b1.addEdge("bd", d3);
        Vertex e1 = this.sqlgGraph.addVertex(T.label, "E", "name", "e1", "order", 8);
        Vertex e2 = this.sqlgGraph.addVertex(T.label, "E", "name", "e2", "order", 10);
        Vertex e3 = this.sqlgGraph.addVertex(T.label, "E", "name", "e3", "order", 12);
        c1.addEdge("cd", e1);
        c1.addEdge("cd", e2);
        c1.addEdge("cd", e3);
        this.sqlgGraph.tx().commit();

        final Predicate<Traverser<Vertex>> predicate = t -> t.loops() > 1;
        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .repeat(
                        __.out().order().by("order", Order.desc)
                ).until(predicate);
        printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(6, vertices.size());
        List<String> names = new ArrayList<>();
        for (Vertex vertex : vertices) {
            names.add(vertex.value("name"));
        }
        System.out.println(names);
        Assert.assertEquals(e3, vertices.get(0));
        Assert.assertEquals(d3, vertices.get(1));
        Assert.assertEquals(e2, vertices.get(2));
        Assert.assertEquals(d2, vertices.get(3));
        Assert.assertEquals(e1, vertices.get(4));
        Assert.assertEquals(d1, vertices.get(5));
    }

    @Test
    public void testRepeatMultipleEdgesWithOrder() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1", "order", 1);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2", "order", 3);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3", "order", 5);
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1", "order", 2);
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2", "order", 4);
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3", "order", 6);
        a1.addEdge("ac", c1);
        a1.addEdge("ac", c2);
        a1.addEdge("ac", c3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .repeat(
                        __.out().order().by("order", Order.desc)
                ).until(__.in());
        printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(6, vertices.size());
        Assert.assertEquals(c3, vertices.get(0));
        Assert.assertEquals(b3, vertices.get(1));
        Assert.assertEquals(c2, vertices.get(2));
        Assert.assertEquals(b2, vertices.get(3));
        Assert.assertEquals(c1, vertices.get(4));
        Assert.assertEquals(b1, vertices.get(5));
    }

    @Test
    public void testRepeatWithOrder() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1", "order", 1);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2", "order", 2);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3", "order", 3);
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1", "order", 4);
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2", "order", 5);
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3", "order", 6);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .repeat(
                        __.out().order().by("order", Order.desc)
                ).times(2);
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(3, vertices.size());
        Assert.assertEquals(c3, vertices.get(0));
        Assert.assertEquals(c2, vertices.get(1));
        Assert.assertEquals(c1, vertices.get(2));
    }

    @Test
    public void test_g_V_playlist_paths() {
        loadGratefulDead();
        List<String> bobSinging = this.sqlgGraph.traversal().V().has("name", "Bob_Dylan").
                in("sungBy").order().by("name").as("a").<String>values("name").toList();
        Assert.assertEquals(22, bobSinging.size());
//        Assert.assertEquals(Arrays.asList("BALLAD OF FRANKIE LEE AND JUDAS PRIEST", "CHIMES OF FREEDOM", "DEAD MAN DEAD MAN", "DONT THINK TWICE ITS ALL RIGHT",
//                        "GOTTA SERVE SOMEBODY", "HEART OF MINE", "HIGHWAY 61 REVISITED", "I WANT YOU", "ILL BE YOUR BABY TONIGHT", "JOEY",
//                        "JOHN BROWN", "MAN OF PEACE", "MR TAMBOURINE MAN", "RAINY DAY WOMEN", "SHELTER FROM THE STORM", "SIMPLE TWIST OF FATE",
//                        "SLOW TRAIN", "TANGLED UP IN BLUE", "TIMES THEY ARE A CHANGING THE", "TOMORROW IS A LONG TIME", "WATCHING THE RIVER FLOW",
//                        "WICKED MESSENGER"),
//                bobSinging);
        List<Path> paths = this.sqlgGraph.traversal().V().has("name", "Bob_Dylan")
                .in("sungBy").order().by("name")
                .repeat(__.out("writtenBy", "sungBy", "followedBy").order().by(__.path().by("name").map(pathTraverser -> pathTraverser.get().toString()), String::compareTo))
                .until(__.out("writtenBy").has("name", "Johnny_Cash"))
                .limit(10)
                .path()
                .toList();
        List<String> resultX = new ArrayList<>();
        for (Path path : paths) {
            List<String> names = new ArrayList<>();
            for (Object object : path.objects()) {
                Vertex v = (Vertex) object;
                names.add(v.value("name"));
            }
            resultX.add(names.toString());
        }

        Assert.assertEquals(
                List.of(
                        "[Bob_Dylan, CHIMES OF FREEDOM, QUEEN JANE, ALTHEA, BIG RIVER]",
                        "[Bob_Dylan, CHIMES OF FREEDOM, QUEEN JANE, BIG RAILROAD BLUES, BIG RIVER]",
                        "[Bob_Dylan, CHIMES OF FREEDOM, QUEEN JANE, BIRD SONG, BIG RIVER]",
                        "[Bob_Dylan, CHIMES OF FREEDOM, QUEEN JANE, BROKEN ARROW, BIG RIVER]",
                        "[Bob_Dylan, CHIMES OF FREEDOM, QUEEN JANE, BROWN EYED WOMEN, BIG RIVER]",
                        "[Bob_Dylan, CHIMES OF FREEDOM, QUEEN JANE, CANDYMAN, BIG RIVER]",
                        "[Bob_Dylan, CHIMES OF FREEDOM, QUEEN JANE, CASSIDY, BIG RIVER]",
                        "[Bob_Dylan, CHIMES OF FREEDOM, QUEEN JANE, COLD RAIN AND SNOW, BIG RIVER]",
                        "[Bob_Dylan, CHIMES OF FREEDOM, QUEEN JANE, DEAL, BIG RIVER]",
                        "[Bob_Dylan, CHIMES OF FREEDOM, QUEEN JANE, DIRE WOLF, BIG RIVER]"
                ),
                resultX
        );
        List<Map<String, Object>> result = this.sqlgGraph.traversal().V().has("name", "Bob_Dylan").
                in("sungBy").order().by("name").as("a").
                repeat(__.out().order().by(__.path().by("name").map(pathTraverser -> pathTraverser.get().toString()), String::compareTo).simplePath().from("a")).
                until(__.out("writtenBy").has("name", "Johnny_Cash")).limit(1).as("b").
                repeat(__.out().order().by(__.path().by("name").map(pathTraverser -> pathTraverser.get().toString()), String::compareTo).as("c").simplePath().from("b").to("c")).
                until(__.out("sungBy").has("name", "Grateful_Dead")).limit(1).
                path().from("a").unfold().
                project("song", "artists").
                by("name").
                by(__.coalesce(__.out("sungBy", "writtenBy").dedup().values("name").order(), __.constant("Unknown")).fold()).toList();

        List<Map<String, Object>> expected = Arrays.asList(
                new HashMap<>() {{
                    put("song", "CHIMES OF FREEDOM");
                    put("artists", List.of("Bob_Dylan"));
                }},
                new HashMap<>() {{
                    put("song", "QUEEN JANE");
                    put("artists", List.of("Unknown"));
                }},
                new HashMap<>() {{
                    put("song", "ALTHEA");
                    put("artists", List.of("Garcia", "Hunter"));
                }},
                new HashMap<>() {{
                    put("song", "BIG RIVER");
                    put("artists", List.of("Johnny_Cash", "Weir"));
                }},
                new HashMap<>() {{
                    put("song", "BERTHA");
                    put("artists", List.of("Garcia", "Hunter"));
                }},
                new HashMap<>() {{
                    put("song", "DRUMS");
                    put("artists", List.of("Grateful_Dead"));
                }}
        );
        Assert.assertEquals(expected, result);
////        Then the result should be unordered
////      | result |
////      | m[{"song": "CHIMES OF FREEDOM", "artists": ["Bob_Dylan"]}] |
////      | m[{"song": "QUEEN JANE", "artists": ["Unknown"]}] |
////      | m[{"song": "ALTHEA", "artists": ["Garcia","Hunter"]}] |
////      | m[{"song": "BIG RIVER", "artists": ["Johnny_Cash","Weir"]}] |
////      | m[{"song": "HES GONE", "artists": ["Garcia","Hunter"]}] |
////      | m[{"song": "CAUTION", "artists": ["Grateful_Dead"]}] |
//        System.out.println(result);
    }

}
