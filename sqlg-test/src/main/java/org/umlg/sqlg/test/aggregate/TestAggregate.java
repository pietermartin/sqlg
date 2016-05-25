package org.umlg.sqlg.test.aggregate;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2015/10/17
 * Time: 12:45 PM
 */
public class TestAggregate extends BaseTest {

    @Test
    public void testAggregate() throws IOException {
        Graph graph = this.sqlgGraph;
        final GraphReader reader = GryoReader.build()
                .mapper(graph.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/tinkerpop-modern.kryo")) {
            reader.readGraph(stream, graph);
        }
        assertModernGraph(graph, true, false);


        GraphTraversalSource g = graph.traversal();
        Traversal<Vertex, Path> traversal = g.V().out().aggregate("a").path();
        printTraversalForm(traversal);
        int count = 0;
        final Map<String, Long> firstStepCounts = new HashMap<>();
        final Map<String, Long> secondStepCounts = new HashMap<>();
        while (traversal.hasNext()) {
            count++;
            final Path path = traversal.next();
            final String first = path.get(0).toString();
            final String second = path.get(1).toString();
            Assert.assertThat(first, CoreMatchers.not(second));
            MapHelper.incr(firstStepCounts, first, 1L);
            MapHelper.incr(secondStepCounts, second, 1L);
        }
        Assert.assertEquals(6, count);
        Assert.assertEquals(3, firstStepCounts.size());
        Assert.assertEquals(4, secondStepCounts.size());
        Assert.assertTrue(firstStepCounts.values().contains(3L));
        Assert.assertTrue(firstStepCounts.values().contains(2L));
        Assert.assertTrue(firstStepCounts.values().contains(1L));
        Assert.assertTrue(secondStepCounts.values().contains(3L));
        Assert.assertTrue(secondStepCounts.values().contains(1L));
    }
}
