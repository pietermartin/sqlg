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

public class TestRecursiveRepeatWithoutNotStep extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestRecursiveRepeatWithoutNotStep.class);

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        Assume.assumeTrue(isPostgres());
    }

    @Test
    public void testFriendOfFriendOutWithUtil() {
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
        Vertex g = sqlgGraph.addVertex(T.label, "Friend", "name", "g");
        Vertex h = sqlgGraph.addVertex(T.label, "Friend", "name", "h");
        Vertex i = sqlgGraph.addVertex(T.label, "Friend", "name", "i");
        a.addEdge("of", b);
        a.addEdge("of", c);
        a.addEdge("of", d);
        d.addEdge("of", e);
        d.addEdge("of", f);
        d.addEdge("of", g);
        e.addEdge("of", h);
        e.addEdge("of", i);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.out("of").simplePath()
                ).until(
                        __.has("name", P.eq("e"))
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(e)));
    }

    @Test
    public void testFriendOfFriendInWithUtil() {
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
        Vertex g = sqlgGraph.addVertex(T.label, "Friend", "name", "g");
        Vertex h = sqlgGraph.addVertex(T.label, "Friend", "name", "h");
        Vertex i = sqlgGraph.addVertex(T.label, "Friend", "name", "i");

        h.addEdge("of", e);
        i.addEdge("of", e);
        e.addEdge("of", d);
        f.addEdge("of", d);
        g.addEdge("of", d);
        d.addEdge("of", a);
        c.addEdge("of", a);
        b.addEdge("of", a);


        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.in("of").simplePath()
                ).until(
                        __.has("name", P.eq("e"))
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(e)));
    }

    @Test
    public void testFriendOfFriendBothWithUtil() {
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
        Vertex g = sqlgGraph.addVertex(T.label, "Friend", "name", "g");
        Vertex h = sqlgGraph.addVertex(T.label, "Friend", "name", "h");
        Vertex i = sqlgGraph.addVertex(T.label, "Friend", "name", "i");
        a.addEdge("of", b);
        a.addEdge("of", c);
        a.addEdge("of", d);
        d.addEdge("of", e);
        d.addEdge("of", f);
        d.addEdge("of", g);
        e.addEdge("of", h);
        e.addEdge("of", i);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(e)
                .repeat(
                        __.both("of").simplePath()
                ).until(
                        __.has("name", P.within("c", "f"))
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(e) && p.get(1).equals(d) && p.get(2).equals(a) && p.get(3).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(e) && p.get(1).equals(d) && p.get(2).equals(f)));
    }

    @Test
    public void testFriendOfFriendOutWithLoopLimit() {
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
        Vertex g = sqlgGraph.addVertex(T.label, "Friend", "name", "g");
        Vertex h = sqlgGraph.addVertex(T.label, "Friend", "name", "h");
        Vertex i = sqlgGraph.addVertex(T.label, "Friend", "name", "i");
        a.addEdge("of", b);
        a.addEdge("of", c);
        a.addEdge("of", d);
        d.addEdge("of", e);
        d.addEdge("of", f);
        d.addEdge("of", g);
        e.addEdge("of", h);
        e.addEdge("of", i);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.out("of").simplePath()
                ).until(
                        __.loops().is(P.lt(3))
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(d)));
    }

    @Test
    public void testFriendOfFriendInWithLoopLimit() {
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
        Vertex g = sqlgGraph.addVertex(T.label, "Friend", "name", "g");
        Vertex h = sqlgGraph.addVertex(T.label, "Friend", "name", "h");
        Vertex i = sqlgGraph.addVertex(T.label, "Friend", "name", "i");

        h.addEdge("of", e);
        i.addEdge("of", e);
        e.addEdge("of", d);
        f.addEdge("of", d);
        g.addEdge("of", d);
        d.addEdge("of", a);
        c.addEdge("of", a);
        b.addEdge("of", a);

        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.in("of").simplePath()
                ).until(
                        __.loops().is(P.lt(3))
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(d)));
    }

    @Test
    public void testFriendOfFriendBothWithLoopLimit() {
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
        Vertex g = sqlgGraph.addVertex(T.label, "Friend", "name", "g");
        Vertex h = sqlgGraph.addVertex(T.label, "Friend", "name", "h");
        Vertex i = sqlgGraph.addVertex(T.label, "Friend", "name", "i");

        h.addEdge("of", e);
        i.addEdge("of", e);
        e.addEdge("of", d);
        f.addEdge("of", d);
        g.addEdge("of", d);
        d.addEdge("of", a);
        c.addEdge("of", a);
        b.addEdge("of", a);

        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(e)
                .repeat(
                        __.both("of").simplePath()
                ).until(
                        __.loops().is(P.lt(3))
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(e) && p.get(1).equals(h)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(e) && p.get(1).equals(i)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(e) && p.get(1).equals(d)));
    }

    @Test
    public void testFriendOfFriendOutWithLoopLimitAndConnectiveStep() {
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
        Vertex g = sqlgGraph.addVertex(T.label, "Friend", "name", "g");
        Vertex h = sqlgGraph.addVertex(T.label, "Friend", "name", "h");
        Vertex i = sqlgGraph.addVertex(T.label, "Friend", "name", "i");
        a.addEdge("of", b);
        a.addEdge("of", c);
        a.addEdge("of", d);
        d.addEdge("of", e);
        d.addEdge("of", f);
        d.addEdge("of", g);
        e.addEdge("of", h);
        e.addEdge("of", i);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.out("of").simplePath()
                ).until(
                        __.and(
                                __.loops().is(P.lt(3)),
                                __.has("name", P.within("c", "d"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(d)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.out("of").simplePath()
                ).until(
                        __.or(
                                __.loops().is(P.eq(2)),
                                __.has("name", P.eq("b"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(4, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(g)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.out("of").simplePath()
                ).until(
                        __.or(
                                __.not(__.out("of").simplePath()),
                                __.loops().is(P.eq(2)),
                                __.has("name", P.eq("b"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(5, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(g)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.out("of").simplePath()
                ).until(
                        __.or(
                                __.not(__.out("of").simplePath()),
                                __.and(
                                        __.loops().is(P.eq(2)),
                                        __.has("name", P.eq("e"))

                                )
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(5, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(g)));

        //unoptimized
        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.out("of").simplePath()
                ).until(
                        __.and(
                                __.not(__.out("of").simplePath()),
                                __.has("name", P.eq("f"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());

        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(f)));
    }

    @Test
    public void testFriendOfFriendInWithLoopLimitAndConnectiveStep() {
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
        Vertex g = sqlgGraph.addVertex(T.label, "Friend", "name", "g");
        Vertex h = sqlgGraph.addVertex(T.label, "Friend", "name", "h");
        Vertex i = sqlgGraph.addVertex(T.label, "Friend", "name", "i");
        h.addEdge("of", e);
        i.addEdge("of", e);
        e.addEdge("of", d);
        f.addEdge("of", d);
        g.addEdge("of", d);
        d.addEdge("of", a);
        c.addEdge("of", a);
        b.addEdge("of", a);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.in("of").simplePath()
                ).until(
                        __.and(
                                __.loops().is(P.lt(3)),
                                __.has("name", P.within("c", "d"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(d)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.in("of").simplePath()
                ).until(
                        __.or(
                                __.loops().is(P.eq(2)),
                                __.has("name", P.eq("b"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(4, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(g)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.in("of").simplePath()
                ).until(
                        __.or(
                                __.not(__.in("of").simplePath()),
                                __.loops().is(P.eq(2)),
                                __.has("name", P.eq("b"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(5, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(g)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.in("of").simplePath()
                ).until(
                        __.or(
                                __.not(__.in("of").simplePath()),
                                __.and(
                                        __.loops().is(P.eq(2)),
                                        __.has("name", P.eq("e"))

                                )
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(5, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(g)));

        //unoptimized
        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.in("of").simplePath()
                ).until(
                        __.and(
                                __.not(__.in("of").simplePath()),
                                __.has("name", P.eq("f"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());

        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(f)));
    }

    @Test
    public void testFriendOfFriendBothWithLoopLimitAndConnectiveStep() {
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
        Vertex g = sqlgGraph.addVertex(T.label, "Friend", "name", "g");
        Vertex h = sqlgGraph.addVertex(T.label, "Friend", "name", "h");
        Vertex i = sqlgGraph.addVertex(T.label, "Friend", "name", "i");
        a.addEdge("of", b);
        a.addEdge("of", c);
        a.addEdge("of", d);
        d.addEdge("of", e);
        d.addEdge("of", f);
        d.addEdge("of", g);
        e.addEdge("of", h);
        e.addEdge("of", i);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(d)
                .repeat(
                        __.both("of").simplePath()
                ).until(
                        __.and(
                                __.loops().is(P.lt(3)),
                                __.has("name", P.within("c", "h"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(d) && p.get(1).equals(a) && p.get(2).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(d) && p.get(1).equals(e) && p.get(2).equals(h)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(e)
                .repeat(
                        __.both("of").simplePath()
                ).until(
                        __.or(
                                __.loops().is(P.eq(2)),
                                __.has("name", P.eq("h"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(4, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(e) && p.get(1).equals(h)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(e) && p.get(1).equals(d) && p.get(2).equals(a)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(e) && p.get(1).equals(d) && p.get(2).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(e) && p.get(1).equals(d) && p.get(2).equals(g)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(e)
                .repeat(
                        __.both("of").simplePath()
                ).until(
                        __.or(
                                __.not(__.out("of").simplePath()),
                                __.loops().is(P.eq(2)),
                                __.has("name", P.eq("h"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(5, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(e) && p.get(1).equals(h)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(e) && p.get(1).equals(i)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(e) && p.get(1).equals(d) && p.get(2).equals(a)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(e) && p.get(1).equals(d) && p.get(2).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(e) && p.get(1).equals(d) && p.get(2).equals(g)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(e)
                .repeat(
                        __.both("of").simplePath()
                ).until(
                        __.or(
                                __.not(__.out("of").simplePath()),
                                __.and(
                                        __.loops().is(P.eq(2)),
                                        __.has("name", P.eq("a"))

                                )
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(5, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(e) && p.get(1).equals(d) && p.get(2).equals(a)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(e) && p.get(1).equals(h)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(e) && p.get(1).equals(i)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(e) && p.get(1).equals(d) && p.get(2).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(e) && p.get(1).equals(d) && p.get(2).equals(g)));

        //unoptimized
        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(e)
                .repeat(
                        __.both("of").simplePath()
                ).until(
                        __.and(
                                __.not(__.out("of").simplePath()),
                                __.has("name", P.eq("c"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());

        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(e) && p.get(1).equals(d) && p.get(2).equals(a) && p.get(3).equals(c)));
    }

}
