package org.umlg.sqlg.test.recursive;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeDefinition;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;
import java.util.List;

public class TestRecursiveRepeatAll extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestRecursiveRepeatAll.class);

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        Assume.assumeTrue(isPostgres());
    }

    @Test
    public void testWithManyVertexAndEdgeLabels() {
        loadModern();

        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        }});
        friendVertexLabel.ensureEdgeLabelExist(
                "of",
                friendVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1)
                )
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();

        StopWatch stopWatch = StopWatch.createStarted();
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "name", "a");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "name", "b");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "name", "c");
        Vertex d = sqlgGraph.addVertex(T.label, "Friend", "name", "d");
        Vertex e = sqlgGraph.addVertex(T.label, "Friend", "name", "e");
        Vertex f = sqlgGraph.addVertex(T.label, "Friend", "name", "f");
        a.addEdge("of", b);
        b.addEdge("of", c);
        c.addEdge("of", d);
        c.addEdge("of", e);
        a.addEdge("of", f);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) sqlgGraph.traversal().V().hasLabel("Friend")
                .has("name", P.within("a"))
                .repeat(__.both("of").simplePath())
                .until(__.not(__.both("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        stopWatch.stop();
        LOGGER.info("repeat query time: {}", stopWatch);
        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c) && p.get(3).equals(d)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c) && p.get(3).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(f)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) sqlgGraph.traversal().V().hasLabel("Friend")
                .has("name", P.within("b"))
                .repeat(__.both("of").simplePath())
                .until(__.not(__.both("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(b) && p.get(1).equals(c) && p.get(2).equals(d)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(b) && p.get(1).equals(c) && p.get(2).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(b) && p.get(1).equals(a) && p.get(2).equals(f)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) sqlgGraph.traversal().V().hasLabel("Friend")
                .has("name", P.within("d"))
                .repeat(__.both("of").simplePath())
                .until(__.not(__.both("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(d) && p.get(1).equals(c) && p.get(2).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(d) && p.get(1).equals(c) && p.get(2).equals(b) && p.get(3).equals(a) && p.get(4).equals(f)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) sqlgGraph.traversal().V().hasLabel("Friend")
                .has("name", P.within("d", "c"))
                .repeat(__.both("of").simplePath())
                .until(__.not(__.both("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        Assert.assertEquals(5, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(d) && p.get(1).equals(c) && p.get(2).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(d) && p.get(1).equals(c) && p.get(2).equals(b) && p.get(3).equals(a) && p.get(4).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(c) && p.get(1).equals(d)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(c) && p.get(1).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(c) && p.get(1).equals(b) && p.get(2).equals(a) && p.get(3).equals(f)));
    }
}
