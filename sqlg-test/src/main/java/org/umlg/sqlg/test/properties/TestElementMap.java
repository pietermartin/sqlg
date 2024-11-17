package org.umlg.sqlg.test.properties;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 */
public class TestElementMap extends BaseTest {

    @Test
    public void testElementMap() {
        this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        this.sqlgGraph.tx().commit();
        final Traversal<Vertex, Map<Object, Object>> traversal =  this.sqlgGraph.traversal().V().hasLabel("A")
                .elementMap(T.id.getAccessor(), "name");
        printTraversalForm(traversal);
        TestPropertyValues.checkRestrictedProperties(SqlgGraphStep.class, traversal, 0, T.id.getAccessor(), "name");
        List<Map<Object, Object>> results = traversal.toList();
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void testElementMapOnEdge() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1", "age", 1);
        v1.addEdge("ab", v2, "name", "edge1", "prop1", "x", "prop2", "y");
        this.sqlgGraph.tx().commit();
        final Traversal<Vertex, Map<Object, Object>> traversal =  this.sqlgGraph.traversal().V().hasLabel("A")
                .outE("ab")
                .elementMap(T.id.getAccessor(), "name");
        printTraversalForm(traversal);
        TestPropertyValues.checkRestrictedProperties(SqlgGraphStep.class, traversal, 0, T.id.getAccessor(), "name");
        List<Map<Object, Object>> results = traversal.toList();
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void testElementMapSelectBy() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "prop1", "aaaa", "propx", "xxxx");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "prop2", "bbbb", "propy", "yyyy");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "propy", "yy");
        a.addEdge("ab", b1);
        a.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();

        final Traversal<Vertex, Map<String, Object>> traversal = this.sqlgGraph.traversal().V().hasLabel("A").as("a")
                .out("ab").as("b")
                .order()
                .by(__.select("a").by("prop1"), Order.asc)
                .select("a", "b")
                .by(__.elementMap("prop1"))
                .by(__.elementMap("prop2"));
        printTraversalForm(traversal);
        List<Map<String, Object>> result = traversal.toList();

        TestPropertyValues.checkRestrictedProperties(SqlgGraphStep.class, traversal, 0, "prop1");
        TestPropertyValues.checkRestrictedProperties(SqlgGraphStep.class, traversal, 1, "prop2");

        Assert.assertEquals(2, result.size());
        Map<String, Object> one = result.get(0);
        LinkedHashMap<Object, Object> aMap = (LinkedHashMap<Object, Object>) one.get("a");
        Assert.assertEquals(a.id(), aMap.get(T.id));
        Assert.assertEquals("A", aMap.get(T.label));
        Assert.assertEquals("aaaa", aMap.get("prop1"));
        Assert.assertNull(aMap.get("prop2"));

        LinkedHashMap<Object, Object> bMap = (LinkedHashMap<Object, Object>) one.get("b");
        Assert.assertEquals(b1.id(), bMap.get(T.id));
        Assert.assertEquals("B", bMap.get(T.label));
        Assert.assertNull(bMap.get("prop1"));
        Assert.assertEquals("bbbb", bMap.get("prop2"));

        Map<String, Object> two = result.get(1);
        aMap = (LinkedHashMap<Object, Object>) two.get("a");
        Assert.assertEquals(a.id(), aMap.get(T.id));
        Assert.assertEquals("A", aMap.get(T.label));
        Assert.assertEquals("aaaa", aMap.get("prop1"));
        Assert.assertNull(aMap.get("prop2"));

        bMap = (LinkedHashMap<Object, Object>) two.get("b");
        Assert.assertEquals(b2.id(), bMap.get(T.id));
        Assert.assertEquals("B", bMap.get(T.label));
        Assert.assertNull(bMap.get("prop1"));
        Assert.assertNull(bMap.get("prop2"));
    }

}
