package org.umlg.sqlg.test.graph;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;

/**
 * Date: 2015/11/18
 * Time: 8:47 AM
 */
public class TestGraphStepWithIds extends BaseTest {

    @Test
    public void testGraphWithIds() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V(a1.id()).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(a1, vertices.get(0));

        vertices = this.sqlgGraph.traversal().V(a1).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(a1, vertices.get(0));
    }

    @Test
    public void testGraphWithIdsGroupingOfIdsAccordingToLabel() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Edge e1 = b3.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V(new Object[]{a1, a2, b1, b2, b3}).toList();
        Assert.assertEquals(5, vertices.size());

        vertices = this.sqlgGraph.traversal().V(new Object[]{a1, a2, b1, b2, b3}).outE().outV().toList();
        Assert.assertEquals(1, vertices.size());

        List<Object> values =this.sqlgGraph.traversal().E(e1.id()).inV().values("name").toList();
        Assert.assertEquals(1, values.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfIdsAreMixed() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Edge e1 = b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V(new Object[]{a1,  b1.id()}).toList();
    }

    @Test
    public void testIdAndHasLabel() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal().V(a1).hasLabel("A").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(a1));

    }
}
