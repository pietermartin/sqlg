package org.umlg.sqlg.test;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Date: 2014/07/13
 * Time: 6:36 PM
 */
public class TestHas extends BaseTest {

    @Test
    public void g_V_hasId() throws IOException {
        Graph graph = this.sqlgGraph;
        final GraphReader reader = GryoReader.build()
                .mapper(graph.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
            reader.readGraph(stream, graph);
        }
        assertModernGraph(graph, true, false);
        GraphTraversalSource g = graph.traversal();

        Object id = convertToVertexId("marko");

        List<Vertex> traversala2 =  g.V().has(T.id, id).toList();
        Assert.assertEquals(1, traversala2.size());
        Assert.assertEquals(convertToVertex(graph, "marko"), traversala2.get(0));

        traversala2 =  g.V().hasId(id).toList();
        Assert.assertEquals(1, traversala2.size());
        Assert.assertEquals(convertToVertex(graph, "marko"), traversala2.get(0));
    }

    @Test
    public void testHasId() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasId(a1.id()).toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(a1, vertices.get(0));
    }

    @Test
    public void testHas() {
        Vertex v1 = this.sqlgGraph.addVertex("name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex("name", "peter");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void testQueryTableNotYetExists() {
        this.sqlgGraph.addVertex(T.label, "Animal");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
    }

    @Test
    public void testQueryPropertyNotYetExists() {
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Person").has("name", "john").count().next(), 0);
    }

    @Test
    public void testHasOnEdge() {
        Vertex v1 = this.sqlgGraph.addVertex("name", "marko");
        Vertex v2 = this.sqlgGraph.addVertex("name", "peter");
        v1.addEdge("friend", v2, "weight", "5");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(1, this.sqlgGraph.traversal().E().has("weight", "5").count().next(), 0);
    }

    @Test
    public void testEdgeQueryTableNotYetExists() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Animal");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Animal");
        v1.addEdge("friend", v2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().E().has(T.label, "friendXXX").count().next(), 0);
    }

    @Test
    public void testEdgeQueryPropertyNotYetExists() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "friend").has("weight", "5").count().next(), 0);
    }

    @Test
    public void testHasOnTableThatDoesNotExist() {
        Vertex v1 = this.sqlgGraph.addVertex(T.label, "Person");
        Vertex v2 = this.sqlgGraph.addVertex(T.label, "Person");
        v1.addEdge("friend", v2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "friend").has("weight", "5").count().next(), 0);
        Assert.assertFalse(this.sqlgGraph.traversal().V().has(T.label, "xxx").hasNext());
        Assert.assertFalse(this.sqlgGraph.traversal().V().has(T.label, "public.xxx").hasNext());
    }

}
