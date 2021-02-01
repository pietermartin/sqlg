package org.umlg.sqlg.test.where;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/09/29
 */
public class TestTraversalFilterStepBarrier extends BaseTest {
    
    @Before
    public void before() throws Exception {
        super.before();
        if (isHsqldb()) {
            Connection connection = this.sqlgGraph.tx().getConnection();
            Statement statement = connection.createStatement();
            statement.execute("SET DATABASE SQL AVG SCALE 2");
            this.sqlgGraph.tx().commit();
        }
    }

//    @Test
//    public void testSqlgTraversalFilterStepPerformance() {
//        this.sqlgGraph.tx().normalBatchModeOn();
//        int count = 10000;
//        for (int i = 0; i < count; i++) {
//            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
//            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
//            a1.addEdge("ab", b1);
//        }
//        this.sqlgGraph.tx().commit();
//
//        StopWatch stopWatch = new StopWatch();
//        for (int i = 0; i < 1000; i++) {
//            stopWatch.start();
//            GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal()
//                    .V().hasLabel("A")
//                    .where(__.out().hasLabel("B"));
//            List<Vertex> vertices = traversal.toList();
//            Assert.assertEquals(count, vertices.size());
//            stopWatch.stop();
//            System.out.println(stopWatch.toString());
//            stopWatch.reset();
//        }
//    }

    @Test
    public void testOutEWithAttributes() {
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p1");
        for (int j = 0; j < 10_000; j++) {
            Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p2");
            Vertex v3 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p3");
            Vertex v4 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p4");
            Vertex v5 = this.sqlgGraph.addVertex(T.label, "Person", "name", "p5");
            v1.addEdge("aaa", v2, "real", true);
            v1.addEdge("aaa", v3, "real", false);
            v1.addEdge("aaa", v4, "real", true, "other", "one");
            v1.addEdge("aaa", v5, "real", false);
        }
        this.sqlgGraph.tx().commit();
//        int count = 10_000;
        int count = 1;
        for (int i = 0; i < count; i++) {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            GraphTraversal<Vertex, Edge> gt = this.sqlgGraph.traversal()
                    .V(v1)
                    .outE()
                    .where(
                            __.inV().has("name", P.within("p4", "p2"))
                    );
            Assert.assertEquals(20_000, gt.count().next().intValue());
            stopWatch.stop();
            System.out.println(stopWatch.toString());
            stopWatch.reset();
        }
    }

    @Test
    public void testWhereVertexStepTraversalStep1() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        a2.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("A").where(__.out());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testWhereVertexStepTraversalStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        a2.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("A").where(__.out().has("name", "b3"));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_eqXbXX() {
        loadModern();
        final Traversal<Vertex, Map<String, Object>> traversal = this.sqlgGraph.traversal()
                .V().has("age").as("a")
                .out().in().has("age").as("b")
                .select("a", "b")
                .where("a", P.eq("b"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, Object> map = traversal.next();
            Assert.assertEquals(2, map.size());
            Assert.assertTrue(map.containsKey("a"));
            Assert.assertTrue(map.containsKey("b"));
            Assert.assertEquals(map.get("a"), map.get("b"));
        }
        Assert.assertEquals(6, counter);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_VX1X_asXaX_out_hasXageX_whereXgtXaXX_byXageX_name() {
        loadModern();
        Object marko = convertToVertexId(this.sqlgGraph, "marko");
        final Traversal<Vertex, String> traversal = this.sqlgGraph.traversal()
                .V(marko).as("a")
                .out().has("age")
                .where(P.gt("a")).by("age")
                .values("name");
        printTraversalForm(traversal);
        Assert.assertEquals("josh", traversal.next());
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void g_V_matchXa_created_lop_b__b_0created_29_c__c_whereXrepeatXoutX_timesX2XXX() {
        loadModern();
        final Traversal<Vertex, Map<String, String>> traversal = this.sqlgGraph.traversal().V().match(
                __.as("a").out("created").has("name", "lop").as("b"),
                __.as("b").in("created").has("age", 29).as("c"),
                __.as("c").where(__.repeat(__.out()).times(2)));
        printTraversalForm(traversal);
        checkResults(makeMapList(3,
                "a", convertToVertex(this.sqlgGraph, "marko"), "b", convertToVertex(this.sqlgGraph, "lop"), "c", convertToVertex(this.sqlgGraph, "marko"),
                "a", convertToVertex(this.sqlgGraph, "josh"), "b", convertToVertex(this.sqlgGraph, "lop"), "c", convertToVertex(this.sqlgGraph, "marko"),
                "a", convertToVertex(this.sqlgGraph, "peter"), "b", convertToVertex(this.sqlgGraph, "lop"), "c", convertToVertex(this.sqlgGraph, "marko")), traversal);
    }

    @Test
    public void g_V_matchXa_hasXsong_name_sunshineX__a_mapX0followedBy_weight_meanX_b__a_0followedBy_c__c_filterXweight_whereXgteXbXXX_outV_dX_selectXdX_byXnameX() {
        loadGratefulDead();
        final Traversal<Vertex, String> traversal = this.sqlgGraph.traversal().V().match(
                __.as("a").has("song", "name", "HERE COMES SUNSHINE"),
                __.as("a").map(__.inE("followedBy").values("weight").mean()).as("b"),
                __.as("a").inE("followedBy").as("c"),
                __.as("c").filter(__.values("weight").where(P.gte("b"))).outV().as("d")).
                <String>select("d").by("name");
        printTraversalForm(traversal);
        checkResults(Arrays.asList("THE MUSIC NEVER STOPPED", "PROMISED LAND", "PLAYING IN THE BAND",
                "CASEY JONES", "BIG RIVER", "EL PASO", "LIBERTY", "LOOKS LIKE RAIN"), traversal);
    }
}
