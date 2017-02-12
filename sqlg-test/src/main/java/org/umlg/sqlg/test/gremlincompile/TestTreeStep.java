package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Date: 2015/10/21
 * Time: 7:39 PM
 */
public class TestTreeStep extends BaseTest {

    @Test
    public void testTree() {
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

        DefaultGraphTraversal<Vertex, Tree> traversal = (DefaultGraphTraversal<Vertex, Tree>) this.sqlgGraph.traversal().V().hasLabel("A").out().out().tree();
        Assert.assertEquals(5, traversal.getSteps().size());
        Tree tree = traversal.next();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(1, tree.size());
        Assert.assertTrue(tree.containsKey(a1));
        Tree tree2  = (Tree) tree.get(a1);
        Assert.assertEquals(1, tree2.size());
        Assert.assertTrue(tree2.containsKey(b1));
        Tree tree3  = (Tree) tree2.get(b1);
        Assert.assertEquals(3, tree3.size());
        Assert.assertTrue(tree3.containsKey(c1));
        Assert.assertTrue(tree3.containsKey(c1));
        Assert.assertTrue(tree3.containsKey(c1));

        //left join todo
//        GraphTraversal gt = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "bc")).times(2).tree();
//        System.out.println(gt.toString());
//        System.out.println(gt.next());
    }

    @Test
    public void testTreeWithBy() {
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

        DefaultGraphTraversal<Vertex, Tree> traversal = (DefaultGraphTraversal<Vertex, Tree>) this.sqlgGraph.traversal()
                .V().hasLabel("A").out().out().tree().by("name");
        Assert.assertEquals(5, traversal.getSteps().size());
        Tree tree = traversal.next();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(1, tree.size());
        Assert.assertTrue(tree.containsKey("a1"));
        Tree tree2  = (Tree) tree.get("a1");
        Assert.assertEquals(1, tree2.size());
        Assert.assertTrue(tree2.containsKey("b1"));
        Tree tree3  = (Tree) tree2.get("b1");
        Assert.assertEquals(3, tree3.size());
        Assert.assertTrue(tree3.containsKey("c1"));
        Assert.assertTrue(tree3.containsKey("c1"));
        Assert.assertTrue(tree3.containsKey("c1"));
    }

    @Test
    public void testTreeWithSideEffect() {
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

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").out().out().tree("a");
        Assert.assertEquals(5, traversal.getSteps().size());
        Vertex v = traversal.next();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(Matchers.isOneOf(c1, c2, c3).matches(v));
    }

    @Test
    public void g_VX1X_out_out_tree_byXnameX() throws IOException {
        Graph graph = this.sqlgGraph;
        final GraphReader reader = GryoReader.build()
                .mapper(graph.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
            reader.readGraph(stream, graph);
        }
        assertModernGraph(graph, true, false);
        GraphTraversalSource g = this.sqlgGraph.traversal();
        final List<DefaultGraphTraversal<Vertex, Tree>> traversals = Arrays.asList(
                (DefaultGraphTraversal)g.V(convertToVertexId("marko")).out().out().tree().by("name"),
                (DefaultGraphTraversal)g.V(convertToVertexId("marko")).out().out().tree("a").by("name").both().both().cap("a")
        );
        traversals.forEach(traversal -> {
            printTraversalForm(traversal);
            final Tree tree = traversal.next();
            Assert.assertFalse(traversal.hasNext());
            Assert.assertEquals(1, tree.size());
            Assert.assertTrue(tree.containsKey("marko"));
            Assert.assertEquals(1, ((Map) tree.get("marko")).size());
            Assert.assertTrue(((Map) tree.get("marko")).containsKey("josh"));
            Assert.assertTrue(((Map) ((Map) tree.get("marko")).get("josh")).containsKey("lop"));
            Assert.assertTrue(((Map) ((Map) tree.get("marko")).get("josh")).containsKey("ripple"));
        });
    }
}
