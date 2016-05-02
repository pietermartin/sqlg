package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by pieter on 2015/08/30.
 */
public class TestPathStep extends BaseTest {

    @Test
    public void g_V_hasXlabel_personX_asXaX_localXoutXcreatedX_asXbXX_selectXa_bX_byXnameX_by() throws IOException {
        Graph graph = this.sqlgGraph;
        final GraphReader reader = GryoReader.build()
                .mapper(graph.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
            reader.readGraph(stream, graph);
        }
        assertModernGraph(graph, true, false);
        GraphTraversalSource g = graph.traversal();

        final Traversal<Vertex, Map<String, Object>> traversal =  g.V().has(T.label, "person").as("a").local(__.out("created").as("b")).select("a", "b").by("name").by(T.id);
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Map<String, Object> map = traversal.next();
            counter++;
            Assert.assertEquals(2, map.size());
            if (map.get("a").equals("marko")) {
                Assert.assertEquals(convertToVertexId("lop"), map.get("b"));
            } else if (map.get("a").equals("josh")) {
                Assert.assertTrue(convertToVertexId("lop").equals(map.get("b")) || convertToVertexId("ripple").equals(map.get("b")));
            } else if (map.get("a").equals("peter")) {
                Assert.assertEquals(convertToVertexId("lop"), map.get("b"));
            } else {
                Assert.fail("The following map should not have been returned: " + map);
            }
        }
        Assert.assertEquals(4, counter);
    }

    @Test
    public void testGraphPathWithBy() throws IOException {
        Graph graph = this.sqlgGraph;
        final GraphReader reader = GryoReader.build()
                .mapper(graph.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
            reader.readGraph(stream, graph);
        }
        assertModernGraph(graph, true, false);
        GraphTraversalSource g = graph.traversal();
        List<Path> paths = g.V().out().path().toList();
        Assert.assertEquals(6, paths.size());

        final Traversal<Vertex, Path> traversal = g.V().out().path().by("age").by("name");
        printTraversalForm(traversal);
        int counter = 0;
        final Set<String> names = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Path path = traversal.next();
            System.out.println(path);
            names.add(path.get(1));
        }
        Assert.assertEquals(6, counter);
        Assert.assertEquals(4, names.size());
    }

    @Test
    public void testVertexPathWithBy() throws IOException {
        Graph graph = this.sqlgGraph;
        final GraphReader reader = GryoReader.build()
                .mapper(graph.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
            reader.readGraph(stream, graph);
        }
        assertModernGraph(graph, true, false);
        GraphTraversalSource g = graph.traversal();
        final Traversal<Vertex, Path> traversal =  g.V(convertToVertexId("marko")).out().path().by("age").by("name");
        printTraversalForm(traversal);
        int counter = 0;
        final Set<String> names = new HashSet<>();
        while (traversal.hasNext()) {
            counter++;
            final Path path = traversal.next();
            System.out.println(path);
            Assert.assertEquals(Integer.valueOf(29), path.<Integer>get(0));
            Assert.assertTrue(path.get(1).equals("josh") || path.get(1).equals("vadas") || path.get(1).equals("lop"));
            names.add(path.get(1));
        }
        Assert.assertEquals(3, counter);
        Assert.assertEquals(3, names.size());
    }

    @Test
    public void testEdge() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        this.sqlgGraph.tx().commit();
        final List<Edge> v1Edges = this.sqlgGraph.traversal().V(a1.id()).bothE().toList();
        Assert.assertEquals(2, v1Edges.size());
        Assert.assertTrue(v1Edges.get(0) instanceof Edge);
        Assert.assertTrue(v1Edges.get(1) instanceof Edge);
    }

    @Test
    public void testVertexPathBackToSelf() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Path> gt = this.sqlgGraph.traversal().V(a1).out().in().path();
        Path p = gt.next();
        Assert.assertEquals(3, p.size());
        Assert.assertEquals(a1, p.get(0));
        Assert.assertEquals(b1, p.get(1));
        Assert.assertEquals(a1, p.get(2));
    }

    @Test
    public void testSimplePath() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        this.sqlgGraph.tx().commit();
        List<Path> paths = this.sqlgGraph.traversal().V().as("a").has("name", "a1").as("b").has("age", 1).as("c").path().toList();
        Assert.assertEquals(1, paths.size());
        Assert.assertEquals(1, paths.get(0).size());
    }

    @Test
    public void g_V_asXaX_hasXname_markoX_asXbX_hasXage_29X_asXcX_path() throws IOException {
        Graph graph = this.sqlgGraph;
        final GraphReader reader = GryoReader.build()
                .mapper(graph.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
            reader.readGraph(stream, graph);
        }
        assertModernGraph(graph, true, false);
        GraphTraversalSource g = graph.traversal();
        final Traversal<Vertex, Path> traversal =  g.V().as("a").has("name", "marko").as("b").has("age", 29).as("c").path();
        printTraversalForm(traversal);
        final Path path = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(1, path.size());
        Assert.assertTrue(path.hasLabel("a"));
        Assert.assertTrue(path.hasLabel("b"));
        Assert.assertTrue(path.hasLabel("c"));
        Assert.assertEquals(1, path.labels().size());
        Assert.assertEquals(3, path.labels().get(0).size());
    }

    @Test
    public void testNameWithMultipleSameLabel() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        a3.addEdge("ab", b3);
        this.sqlgGraph.tx().commit();

        List<Map<String, Object>> result = this.sqlgGraph.traversal()
                .V().as("a")
                .out().as("a")
                .in().as("a")
                .select("a", "a", "a")
                .toList();
        Assert.assertEquals(3, result.size());
        Object o1 = result.get(0).get("a");
        Assert.assertTrue(o1 instanceof List);
        List<Vertex> ass = (List) o1;
        Assert.assertEquals(a1, ass.get(0));
        Assert.assertEquals("a1", ass.get(0).value("name"));
        Assert.assertEquals(b1, ass.get(1));
        Assert.assertEquals("b1", ass.get(1).value("name"));
        Assert.assertEquals(a1, ass.get(2));
        Assert.assertEquals("a1", ass.get(2).value("name"));

    }

    @Test
    public void testPathStep() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");

        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);

        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);

        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").out().out().path().toList();
        Assert.assertEquals(3, paths.size());
        Assert.assertEquals(a1, paths.get(0).get(0));
        Assert.assertEquals(a1, paths.get(1).get(0));
        Assert.assertEquals(a1, paths.get(2).get(0));

        Assert.assertEquals(b1, paths.get(0).get(1));
        Assert.assertEquals(b1, paths.get(1).get(1));
        Assert.assertEquals(b1, paths.get(2).get(1));

        Set<Vertex> cs = new HashSet<>();
        cs.add(c1);
        cs.add(c2);
        cs.add(c3);
        Assert.assertTrue(cs.remove(paths.get(0).get(2)));
        Assert.assertTrue(cs.remove(paths.get(1).get(2)));
        Assert.assertTrue(cs.remove(paths.get(2).get(2)));
        Assert.assertTrue(cs.isEmpty());
    }

}
