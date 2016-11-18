package org.umlg.sqlg.test.edges;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Date: 2016/11/17
 * Time: 10:15 PM
 */
public class TestDetachedEdge extends BaseTest {


    @Test
    public void shouldConstructDetachedEdge() {
        loadModern();
        Object edgeId = convertToEdgeId("marko", "knows", "vadas");
        this.sqlgGraph.traversal().E(edgeId).next().property("year", 2002);
        Edge next = this.sqlgGraph.traversal().E(edgeId).next();

        assertTrue(this.sqlgGraph.traversal().E(edgeId).next().property("year").isPresent());

        final DetachedEdge detachedEdge = DetachedFactory.detach(next, true);
        assertEquals(convertToEdgeId("marko", "knows", "vadas"), detachedEdge.id());
        assertEquals("knows", detachedEdge.label());
        assertEquals(DetachedVertex.class, detachedEdge.vertices(Direction.OUT).next().getClass());
        assertEquals(convertToVertexId("marko"), detachedEdge.vertices(Direction.OUT).next().id());
        assertEquals("person", detachedEdge.vertices(Direction.IN).next().label());
        assertEquals(DetachedVertex.class, detachedEdge.vertices(Direction.IN).next().getClass());
        assertEquals(convertToVertexId("vadas"), detachedEdge.vertices(Direction.IN).next().id());
        assertEquals("person", detachedEdge.vertices(Direction.IN).next().label());

        assertEquals(2, IteratorUtils.count(detachedEdge.properties()));
        assertEquals(1, IteratorUtils.count(detachedEdge.properties("year")));
        assertEquals(0.5d, detachedEdge.properties("weight").next().value());
    }

    public Object convertToEdgeId(final String outVertexName, String edgeLabel, final String inVertexName) {
        return convertToEdgeId(this.sqlgGraph, outVertexName, edgeLabel, inVertexName);
    }

    public Object convertToEdgeId(final Graph graph, final String outVertexName, String edgeLabel, final String inVertexName) {
        return graph.traversal().V().has("name", outVertexName).outE(edgeLabel).as("e").inV().has("name", inVertexName).<Edge>select("e").next().id();
    }

}
