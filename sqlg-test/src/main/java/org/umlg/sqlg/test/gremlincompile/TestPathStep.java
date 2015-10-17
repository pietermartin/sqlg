package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
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

/**
 * Created by pieter on 2015/08/30.
 */
public class TestPathStep extends BaseTest {

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

//    @Test
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

        GraphTraversal<Vertex, Path> path = this.sqlgGraph.traversal().V().hasLabel("A").out().out().path();
        while (path.hasNext()) {
            System.out.println(path.next());
        }

//        Tree tree = this.sqlgGraph.traversal().V().hasLabel("A").out().out().tree().next();
//        System.out.println(tree);

        //left join todo
//        GraphTraversal gt = this.sqlgGraph.traversal().V().hasLabel("A").emit().repeat(__.out("ab", "bc")).times(2).tree();
//        System.out.println(gt.toString());
//        System.out.println(gt.next());

    }
}
