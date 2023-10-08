package org.umlg.sqlg.test.properties;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.test.BaseTest;

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
}
