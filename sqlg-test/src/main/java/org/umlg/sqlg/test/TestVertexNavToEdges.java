package org.umlg.sqlg.test;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.umlg.sqlg.structure.SchemaManager;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Date: 2014/07/13
 * Time: 9:55 AM
 */
public class TestVertexNavToEdges extends BaseTest {

    @Test
    public void testFromVertexGetEdges() {
        Vertex v1 = sqlgGraph.addVertex();
        Vertex v2 = sqlgGraph.addVertex();
        Edge e = v1.addEdge("label1", v2, "name", "marko");
        sqlgGraph.tx().commit();
        assertDb(SchemaManager.EDGE_PREFIX + "label1", 1);
        assertDb(SchemaManager.VERTEX_PREFIX  +  "vertex", 2);

        Iterator<Edge> edges = v1.edges(Direction.BOTH, "label1");
        List<Edge> toList= IteratorUtils.toList(edges);
        assertEquals(1, toList.size());
        Edge edge = toList.get(0);
        assertEquals(e, edge);
        String name = edge.<String>property("name").value();
        assertEquals("marko", name);

        assertFalse(vertexTraversal(v1).inE("label1").hasNext());
        edge = vertexTraversal(v1).bothE("label1").next();
        assertEquals(e, edge);

        name = edge.<String>property("name").value();
        assertEquals("marko", name);
    }

    @Test
    public void testOutE() {
        Vertex v1 = sqlgGraph.addVertex();
        Vertex v2 = sqlgGraph.addVertex();
        Vertex v3 = sqlgGraph.addVertex();
        Vertex v4 = sqlgGraph.addVertex();
        Edge e1 = v1.addEdge("label1", v2);
        Edge e2 = v1.addEdge("label1", v3);
        Edge e3 = v1.addEdge("label1", v4);
        sqlgGraph.tx().commit();
        assertEquals(3L, vertexTraversal(v1).outE("label1").count().next(), 0);
    }

    @Test
    public void testOutEAllLabels() {
        Vertex v1 = sqlgGraph.addVertex();
        Vertex v2 = sqlgGraph.addVertex();
        Vertex v3 = sqlgGraph.addVertex();
        Vertex v4 = sqlgGraph.addVertex();
        Edge e1 = v1.addEdge("label1", v2);
        Edge e2 = v1.addEdge("label2", v3);
        Edge e3 = v1.addEdge("label3", v4);
        sqlgGraph.tx().commit();
        assertEquals(3L, vertexTraversal(v1).outE().count().next(), 0);
    }

    @Test
    public void testInOut() {
        Vertex v1 = sqlgGraph.addVertex();
        Vertex v2 = sqlgGraph.addVertex();
        Vertex v3 = sqlgGraph.addVertex();
        Vertex v4 = sqlgGraph.addVertex();
        Vertex v5 = sqlgGraph.addVertex();
        Edge e1 = v1.addEdge("label1", v2);
        Edge e2 = v2.addEdge("label2", v3);
        Edge e3 = v3.addEdge("label3", v4);
        sqlgGraph.tx().commit();

        assertEquals(1, vertexTraversal(v2).inE().count().next(), 1);
        assertEquals(e1, vertexTraversal(v2).inE().next());
        assertEquals(1L, edgeTraversal(e1).inV().count().next(), 0);
        assertEquals(v2, edgeTraversal(e1).inV().next());
        assertEquals(0L, edgeTraversal(e1).outV().inE().count().next(), 0);
        assertEquals(1L, edgeTraversal(e2).inV().count().next(), 0);
        assertEquals(v3, edgeTraversal(e2).inV().next());
    }

}
