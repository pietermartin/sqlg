package org.umlg.sqlg.test.complex;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/08/25
 */
public class TestMultipleSelect extends BaseTest {

    private Object id0;

    @Before
    public void createData() {
        Map<String, Object> aValues = new HashMap<>();
        aValues.put("name", "root");
        Vertex vA = sqlgGraph.addVertex("A", aValues);
        Map<String, Object> iValues = new HashMap<>();
        iValues.put("name", "item1");
        Vertex vI = sqlgGraph.addVertex("I", iValues);
        vA.addEdge("likes", vI, "howMuch", 5, "who", "Joe");
        this.sqlgGraph.tx().commit();
        this.id0 = vI.id();
    }

    @Test
    public void testMultipleSelect() {
        GraphTraversal<Vertex, Map<String, Object>> gt = sqlgGraph.traversal().V()
                .hasLabel("A")
                .has("name", "root")
                .outE("likes")
                .as("e")
                .values("howMuch").as("stars")
                .select("e")
                .values("who").as("user")
                .select("e")
                .inV()
                .id().as("item")
                .select("user", "stars", "item");
        printTraversalForm(gt);
        assertTrue(gt.hasNext());
        Map<String, Object> m = gt.next();
        assertEquals(new Integer(5), m.get("stars"));
        assertEquals("Joe", m.get("user"));
        assertEquals(id0, m.get("item"));
    }

    @Test
    public void testProject() {
        GraphTraversal<Vertex, Map<String, Object>> gt = sqlgGraph.traversal().V()
                .hasLabel("A")
                .has("name", "root")
                .outE("likes")
                .project("stars", "user", "item")
                .by("howMuch")
                .by("who")
                .by(__.inV().id())
                .select("user", "stars", "item");
        assertTrue(gt.hasNext());
        Map<String, Object> m = gt.next();
        assertEquals(new Integer(5), m.get("stars"));
        assertEquals("Joe", m.get("user"));
        assertEquals(id0, m.get("item"));
    }
}
