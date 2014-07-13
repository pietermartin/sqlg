package org.umlg.sqlgraph.h2database.test;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Date: 2014/07/13
 * Time: 9:55 AM
 */
public class TestVertexNavToEdges extends BaseTest {

    @Test
    public void testFromVertexGetEdges() {
        Vertex v1 = sqlGraph.addVertex();
        Vertex v2 = sqlGraph.addVertex();
        Edge e = v1.addEdge("label1", v2, "name", "marko");
        sqlGraph.tx().commit();
        assertDb("label1", 1);
        assertDb("vertex", 2);

        Edge edge = v1.outE("label1").next();
        assertEquals(e, edge);
        assertFalse(v1.inE("label1").hasNext());

        edge = v1.bothE("label1").next();
        assertEquals(e, edge);

        String name = edge.<String>property("name").value();
        assertEquals("marko", name);
    }

    @Test
    public void testOutE() {
        Vertex v1 = sqlGraph.addVertex();
        Vertex v2 = sqlGraph.addVertex();
        Vertex v3 = sqlGraph.addVertex();
        Vertex v4 = sqlGraph.addVertex();
        Edge e1 = v1.addEdge("label1", v2);
        Edge e2 = v1.addEdge("label1", v3);
        Edge e3 = v1.addEdge("label1", v4);
        sqlGraph.tx().commit();
        assertEquals(3L, v1.outE("label1").count().next(), 0);
    }

}
