package org.umlg.sqlg.test.edges;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by pieter on 2015/07/05.
 */
public class TestLoadEdge extends BaseTest {

    @Test
    public void testLoadEdge() {
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person");
        Edge friend = person1.addEdge("friend", person2, "name", "edge1");
        this.sqlgGraph.tx().commit();
        Property p = this.sqlgGraph.traversal().E(friend).next().property("name", "edge2");
        Assert.assertTrue(p.isPresent());
    }

    @Test
    public void testEdgePropertyWithPeriod() {
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person");
        Edge friend = person1.addEdge("friend", person2, "name.A", "edge1");
        this.sqlgGraph.tx().commit();
        Property p = this.sqlgGraph.traversal().E(friend).next().property("name.A");
        Assert.assertTrue(p.isPresent());
        Assert.assertEquals("edge1", p.value());
    }

    @Test
    public void shouldConstructDetachedEdge() throws IOException {
        Graph g = this.sqlgGraph;
        final GraphReader reader = GryoReader.build()
                .mapper(g.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
            reader.readGraph(stream, g);
        }
        assertModernGraph(g, true, false);
        Edge e = g.traversal().E(convertToEdgeId("marko", "knows", "vadas")).next();
        e.property("year", 2002);
        g.tx().commit();
        e = g.traversal().E(convertToEdgeId("marko", "knows", "vadas")).next();
        final DetachedEdge detachedEdge = DetachedFactory.detach(e, true);
        Assert.assertEquals(convertToEdgeId("marko", "knows", "vadas"), detachedEdge.id());

        Assert.assertEquals("knows", detachedEdge.label());
        Assert.assertEquals(DetachedVertex.class, detachedEdge.vertices(Direction.OUT).next().getClass());
        Assert.assertEquals(convertToVertexId("marko"), detachedEdge.vertices(Direction.OUT).next().id());
        Assert.assertEquals("person", detachedEdge.vertices(Direction.IN).next().label());
        Assert.assertEquals(DetachedVertex.class, detachedEdge.vertices(Direction.IN).next().getClass());
        Assert.assertEquals(convertToVertexId("vadas"), detachedEdge.vertices(Direction.IN).next().id());
        Assert.assertEquals("person", detachedEdge.vertices(Direction.IN).next().label());

        Assert.assertEquals(2, IteratorUtils.count(detachedEdge.properties()));
        Assert.assertEquals(1, IteratorUtils.count(detachedEdge.properties("year")));
        Assert.assertEquals(0.5d, detachedEdge.properties("weight").next().value());
    }

    public Object convertToEdgeId(final String outVertexName, String edgeLabel, final String inVertexName) {
        return this.sqlgGraph.traversal().V().has("name", outVertexName).outE(edgeLabel).as("e").inV().has("name", inVertexName).<Edge>select("e").next().id();
    }
}
