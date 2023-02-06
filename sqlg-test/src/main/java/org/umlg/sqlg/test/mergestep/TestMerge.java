package org.umlg.sqlg.test.mergestep;

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestMerge extends BaseTest {

    @Test
    public void testMergeWithDefaultLabel() {
        this.sqlgGraph.traversal().mergeV(new HashMap<>() {{
            put("name", "Brandy");
        }}).iterate();
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testMergeWithLabel() {
        this.sqlgGraph.traversal().mergeV(new HashMap<>() {{
            put(T.label, "Person");
            put("name", "Brandy");
        }}).iterate();
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").has("name", "Brandy").toList();
        Assert.assertEquals(1, vertices.size());
    }

    /**
     * Scenario: g_V_mergeEXlabel_selfX_optionXonMatch_nullX
     * Given the empty graph
     * And the graph initializer of
     * """
     * g.addV("person").property("name", "marko").property("age", 29).
     * addE("self")
     * """
     * And using the parameter xx1 defined as "m[{\"t[label]\": \"self\"}]"
     * And the traversal of
     * """
     * g.V().mergeE(xx1).option(Merge.onMatch,null)
     * """
     * When iterated to list
     * Then the result should have a count of 1
     * And the graph should return 1 for count of "g.E()"
     * And the graph should return 0 for count of "g.E().properties()"
     * And the graph should return 1 for count of "g.V()"
     */
    @Test
    public void g_V_mergeEXlabel_selfX_optionXonMatch_nullX() {
        this.sqlgGraph.traversal().addV("person").property("name", "marko").property("age", 29).addE("self").iterate();
        List<Edge> edges = this.sqlgGraph.traversal().V()
                .mergeE(new HashMap<>() {{
                    put(T.label, "self");
                }})
                .option(Merge.onMatch, (Map) null)
                .toList();
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, edges.size());
        List<? extends Property<?>> properties = this.sqlgGraph.traversal().E().properties().toList();
        Assert.assertEquals(0, properties.size());
        List<Vertex> vertices = this.sqlgGraph.traversal().V().toList();
        Assert.assertEquals(1, vertices.size());
    }

}
