package org.umlg.sqlg.test.select;

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSelect extends BaseTest {

    @Test
    public void g_V_asXa_nX_selectXa_nX_byXageX_byXnameX() {
        loadModern();
        Map<String, Integer> _result = new HashMap<>(Map.of("marko", 29, "vadas", 27, "josh", 32, "peter", 35));
        List<Map<String, Object>> results = sqlgGraph.traversal().V().as("a", "n").select("a", "n").by("age").by("name").toList();
        for (Map<String, Object> result : results) {
            Assert.assertTrue(result.get("a") instanceof Integer);
            Assert.assertTrue(result.get("n") instanceof String);
            Integer a = (Integer) result.get("a");
            String n = result.get("n").toString();
            Assert.assertEquals(_result.remove(n), a);
        }
        Assert.assertTrue(_result.isEmpty());
        //    g.V().as("a","n").select("a","n").by("age").by("name")
//        When iterated to list
//        Then the result should be unordered
//      | result |
//      | m[{"a":"d[29].i","n":"marko"}] |
//      | m[{"a":"d[27].i","n":"vadas"}] |
//      | m[{"a":"d[32].i","n":"josh"}] |
//      | m[{"a":"d[35].i","n":"peter"}] |
    }

    @Test
    public void testSelectWithMultipleBy() {
        Assume.assumeTrue(isPostgres());
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "aa");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "ab");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "ac");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "bc", "surname", "whatthe1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "bd", "surname", "whatthe2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "ca");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "cb");

        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);

        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Map<String, String>> traversal = this.sqlgGraph.traversal().V().hasLabel("A").as("a")
                .out("ab").as("b")
                .<String>select("a", "b").by("name").by("surname");

        String sql = getSQL(traversal);
        Assert.assertEquals("""
                SELECT
                \t"public"."V_A"."ID" AS "alias1",
                \t"public"."V_A"."name" AS "alias2",
                \t"public"."V_B"."ID" AS "alias3",
                \t"public"."V_B"."surname" AS "alias4"
                FROM
                \t"public"."V_A" INNER JOIN
                \t"public"."E_ab" ON "public"."V_A"."ID" = "public"."E_ab"."public.A__O" INNER JOIN
                \t"public"."V_B" ON "public"."E_ab"."public.B__I" = "public"."V_B"."ID\"""", sql);
        List<Map<String, String>> results = traversal.toList();
        Map<String, String> _results = new HashMap<>(Map.of("aa", "whatthe1", "ab", "whatthe2"));
        for (Map<String, String> result : results) {
            String name = result.get("a");
            String surname = result.get("b");
            Assert.assertEquals(_results.remove(name), surname);
        }
        Assert.assertTrue(_results.isEmpty());

        List<Map<String, Map<String, Object>>> _resultsAgain = this.sqlgGraph.traversal().V().hasLabel("A").as("a")
                .out("ab").as("b")
                .<Map<String, Object>>select("a", "b")
                .by(__.elementMap("name"))
                .by(__.elementMap("surname"))
                .toList();

        _results = new HashMap<>(Map.of("aa", "whatthe1", "ab", "whatthe2"));
        for (Map<String, Map<String, Object>> result : _resultsAgain) {
            Map<String, Object> a = result.get("a");
            Map<String, Object> b = result.get("b");

            Assert.assertTrue(a.get("name") instanceof String);
            Assert.assertTrue(b.get("surname") instanceof String);
            String name = (String)a.get("name");
            String surname = (String)b.get("surname");
            Assert.assertEquals(_results.remove(name), surname);
        }
        Assert.assertTrue(_results.isEmpty());
    }

    @Test
    public void testSelectOptionalPropertiesOnTraversal() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "prop1", "aaaa", "prop3", "c3");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "prop2", "bbbb", "prop3", "c33");
        a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();

        List<Map<String, Map<String, Object>>> result = this.sqlgGraph.traversal().V().hasLabel("A").as("a")
                .out("ab").as("b")
                .<Map<String, Object>>select("a", "b")
                .by(__.elementMap("prop1"))
                .by(__.elementMap("prop2"))
                .toList();
        Assert.assertEquals(1, result.size());
        Map<String, Map<String, Object>> map = result.get(0);
        Assert.assertEquals("aaaa", map.get("a").get("prop1"));
        Assert.assertEquals("bbbb", map.get("b").get("prop2"));
        Assert.assertNull(map.get("a").get("prop2"));
        Assert.assertNull(map.get("b").get("prop1"));
    }

    /*
  Scenario: g_V_out_in_selectXall_a_a_aX_byXunfold_name_foldX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("A").property("name", "a1").as("a1").
        addV("A").property("name", "a2").as("a2").
        addV("A").property("name", "a3").as("a3").
        addV("B").property("name", "b1").as("b1").
        addV("B").property("name", "b2").as("b2").
        addV("B").property("name", "b3").as("b3").
        addE("ab").from("a1").to("b1").
        addE("ab").from("a2").to("b2").
        addE("ab").from("a3").to("b3")
      """
    And the traversal of
      """
      g.V().as("a").out().as("a").in().as("a").
        select(Pop.all, "a", "a", "a").
          by(unfold().values('name').fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":["a1","b1","a1"]}] |
      | m[{"a":["a2","b2","a2"]}] |
      | m[{"a":["a3","b3","a3"]}] |
     */
//    @Test
    public void testSelect() {
        this.sqlgGraph.traversal()
                .addV("A").property("name", "a1").as("a1")
                .addV("A").property("name", "a2").as("a2")
                .addV("A").property("name", "a3").as("a3")
                .addV("B").property("name", "b1").as("b1")
                .addV("B").property("name", "b2").as("b2")
                .addV("B").property("name", "b3").as("b3")
                .addE("ab").from("a1").to("b1")
                .addE("ab").from("a2").to("b2")
                .addE("ab").from("a3").to("b3")
                .iterate();
        List<Map<String, Object>> result = this.sqlgGraph.traversal().V().as("a")
                .out().as("a")
                .in().as("a")
                .select(Pop.all, "a", "a", "a")
                .by(__.unfold().values("name").fold())
                .toList();

        System.out.println(result);
        Assert.fail("implement this test");

    }
}
