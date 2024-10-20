package org.umlg.sqlg.test.select;

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

public class TestSelect extends BaseTest {

    @Test
    public void testSelectOptionalPropertiesOnTraversal() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "prop1", "aaaa");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B", "prop2", "bbbb");
        a.addEdge("ab", b);
        this.sqlgGraph.tx().commit();

        List<Map<String, Map<String, Object>>> result = this.sqlgGraph.traversal().V().hasLabel("A").as("a")
                .out("ab").as("b")
                .<Map<String, Object>>select("a", "b")
                .<Map<String, Object>>by(__.elementMap("prop1"))
                .<Map<String, Object>>by(__.elementMap("prop2"))
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
