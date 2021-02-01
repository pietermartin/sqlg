package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.strategy.BaseStrategy;
import org.umlg.sqlg.test.BaseTest;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by pieter on 2015/08/30.
 */
public class TestPathStep extends BaseTest {

    @Test
    public void testBug382() {
        GraphTraversalSource g = this.sqlgGraph.traversal();
        g.addV("Test").property("name", "John").next();
        this.sqlgGraph.tx().commit();
        List<Path> paths = g.V().hasLabel("Test")
                .or(__.has("name", "John")).as("t")
                .path().from("t")
                .toList();
        Assert.assertEquals(1, paths.size());
        System.out.println(paths.get(0));
    }

    @Test
    public void testPathFrom() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V().hasLabel("A").out("ab").as("b").out("bc").path().toList();
        System.out.println(paths);

        paths = this.sqlgGraph.traversal().V().hasLabel("A").out("ab").as("b").out("bc").path().from("b").toList();
        System.out.println(paths);

        GraphTraversal<Vertex, Path> t = this.sqlgGraph.traversal()
                .V().hasLabel("A").as("a")
                .out("ab").select("a").as("start")
                .path();
        printTraversalForm(t);
        paths = t.toList();
        System.out.println(paths);
        Assert.assertEquals(1, paths.size());
        Path path = paths.get(0);
        Assert.assertEquals(3, path.size());
        List<Set<String>> labels = path.labels();
        Assert.assertEquals(3, labels.size());
        Assert.assertEquals(1, labels.get(0).size());
        Assert.assertTrue(labels.get(0).contains("a"));
        Assert.assertEquals(1, labels.get(1).size());
        Assert.assertTrue(labels.get(1).contains(BaseStrategy.SQLG_PATH_FAKE_LABEL));
        Assert.assertEquals(1, labels.get(2).size());
        Assert.assertTrue(labels.get(2).contains("start"));

        t = this.sqlgGraph.traversal()
                .V().hasLabel("A").as("a")
                .out("ab").select("a").limit(1).as("start")
                .path();
        printTraversalForm(t);
        paths = t.toList();
        System.out.println(paths);
        Assert.assertEquals(1, paths.size());
        path = paths.get(0);
        Assert.assertEquals(3, path.size());
        labels = path.labels();
        Assert.assertEquals(3, labels.size());
        Assert.assertEquals(1, labels.get(0).size());
        Assert.assertTrue(labels.get(0).contains("a"));
        Assert.assertEquals(1, labels.get(1).size());

        //This breaks intermittently on hsqldb
//        Assert.assertTrue(labels.get(1).contains("sqlgPathFakeLabel"));
//        Assert.assertEquals(1, labels.get(2).size());
//        Assert.assertTrue(labels.get(2).contains("start"));
    }

    @Test
    public void g_V_hasXlabel_personX_asXaX_localXoutXcreatedX_asXbXX_selectXa_bX_byXnameX_by() {
        Graph graph = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(graph, true, false);
        GraphTraversalSource g = graph.traversal();

        DefaultGraphTraversal<Vertex, Map<String, Object>> traversal = (DefaultGraphTraversal<Vertex, Map<String, Object>>) g
                .V().has(T.label, "person").as("a").local(__.out("created").as("b")).select("a", "b").by("name").by(T.id);
        Assert.assertEquals(4, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertEquals(4, traversal.getSteps().size());
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
    public void testGraphPathWithBy() {
        Graph graph = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(graph, true, false);
        DefaultGraphTraversal<Vertex, Path> traversal1 = (DefaultGraphTraversal<Vertex, Path>) graph.traversal().V().out().path();
        Assert.assertEquals(3, traversal1.getSteps().size());
        List<Path> paths = traversal1.toList();
        Assert.assertEquals(2, traversal1.getSteps().size());
        Assert.assertEquals(6, paths.size());

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) graph.traversal().V().out().path().by("age").by("name");
        Assert.assertEquals(3, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.getSteps().size());
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
    public void testVertexPathWithBy() {
        Graph graph = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(graph, true, false);
        GraphTraversalSource g = graph.traversal();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) g
                .V(convertToVertexId("marko")).out().path().by("age").by("name");
        Assert.assertEquals(3, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.getSteps().size());
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
        DefaultGraphTraversal<Vertex, Edge> traversal = (DefaultGraphTraversal<Vertex, Edge>) this.sqlgGraph.traversal().V(a1.id()).bothE();
        Assert.assertEquals(2, traversal.getSteps().size());
        final List<Edge> v1Edges = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(2, v1Edges.size());
        Assert.assertNotNull(v1Edges.get(0));
        Assert.assertNotNull(v1Edges.get(1));
    }

    @Test
    public void testVertexPathBackToSelf() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Path> gt = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a1).out().in().path();
        Assert.assertEquals(4, gt.getSteps().size());
        Path p = gt.next();
        Assert.assertEquals(2, gt.getSteps().size());
        Assert.assertEquals(3, p.size());
        Assert.assertEquals(a1, p.get(0));
        Assert.assertEquals(b1, p.get(1));
        Assert.assertEquals(a1, p.get(2));
    }

    @Test
    public void testSimplePath() {
        this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V().as("a").has("name", "a1").as("b").has("age", 1).as("c").path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertEquals(1, paths.size());
        Assert.assertEquals(1, paths.get(0).size());
    }

    @Test
    public void g_V_asXaX_hasXname_markoX_asXbX_hasXage_29X_asXcX_path() {
        Graph graph = this.sqlgGraph;
        loadModern(this.sqlgGraph);
        assertModernGraph(graph, true, false);
        GraphTraversalSource g = graph.traversal();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) g
                .V().as("a").has("name", "marko").as("b").has("age", 29).as("c").path();
        Assert.assertEquals(3, traversal.getSteps().size());
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertEquals(3, traversal.getSteps().size());
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

        DefaultGraphTraversal<Vertex, Map<String, Object>> traversal = (DefaultGraphTraversal<Vertex, Map<String, Object>>) this.sqlgGraph.traversal()
                .V().as("a")
                .out().as("a")
                .in().as("a")
                .select(Pop.all, "a", "a", "a");
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Map<String, Object>> result = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(3, result.size());
        Object o1 = result.get(0).get("a");
        Assert.assertTrue(o1 instanceof List);
        @SuppressWarnings("unchecked") List<Vertex> ass = (List<Vertex>) o1;
        Assert.assertEquals(a1, ass.get(0));
        Assert.assertEquals("a1", ass.get(0).value("name"));
        Assert.assertEquals(b1, ass.get(1));
        Assert.assertEquals("b1", ass.get(1).value("name"));
        Assert.assertEquals(a1, ass.get(2));
        Assert.assertEquals("a1", ass.get(2).value("name"));

    }

    @SuppressWarnings("SuspiciousMethodCalls")
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

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V().hasLabel("A").out().out().path();
        Assert.assertEquals(5, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
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
