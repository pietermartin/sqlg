package org.umlg.sqlg.test.recursive;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.RecordId;
import org.umlg.sqlg.structure.topology.EdgeDefinition;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.sql.*;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

public class TestRecursiveRepeatWithNotStep extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestRecursiveRepeatWithNotStep.class);

    @Test
    public void testFriendOfFriendOutEWithAndStep() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
            put("prop1", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
            put("prop2", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
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
        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "name", "a", "prop1", "1", "prop2", "1");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "name", "b", "prop1", "1", "prop2", "2");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "name", "c", "prop1", "1", "prop2", "3");
        Vertex d = sqlgGraph.addVertex(T.label, "Friend", "name", "d", "prop1", "2", "prop2", "4");
        Vertex e = sqlgGraph.addVertex(T.label, "Friend", "name", "e", "prop1", "2", "prop2", "5");
        Vertex f = sqlgGraph.addVertex(T.label, "Friend", "name", "f", "prop1", "2", "prop2", "6");
        Edge e1 = a.addEdge("of", b);
        Edge e2 = b.addEdge("of", c);
        Edge e3 = a.addEdge("of", d);
        Edge e4 = d.addEdge("of", e);
        Edge e5 = e.addEdge("of", f);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(__.outE("of").as("e").inV().as("v").simplePath())
                .until(
                        __.or(
                                __.not(__.outE("of").simplePath()),
                                __.and(
                                        __.select("v").has("prop1", "1"),
                                        __.select("v").has("prop2", "3")
                                )
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        stopWatch.stop();
        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(a) && p.get(1).equals(e1) && p.get(2).equals(b) && p.get(3).equals(e2) && p.get(4).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 7 && p.get(0).equals(a) && p.get(1).equals(e3) && p.get(2).equals(d) && p.get(3).equals(e4) && p.get(4).equals(e) && p.get(5).equals(e5) && p.get(6).equals(f)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(__.outE("of").as("e").inV().as("v").simplePath())
                .until(
                        __.or(
                                __.not(__.outE("of").simplePath()),
                                __.or(
                                        __.select("v").has("prop1", "1"),
                                        __.select("v").has("prop2", "3")
                                )
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(e1) && p.get(2).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 7 && p.get(0).equals(a) && p.get(1).equals(e3) && p.get(2).equals(d)));
        LOGGER.info("repeat query time: {}", stopWatch);

    }

    @Test
    public void testFriendOfFriendOutWithAndStep() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
            put("prop1", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
            put("prop2", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
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
        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "name", "a", "prop1", "1", "prop2", "1");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "name", "b", "prop1", "1", "prop2", "2");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "name", "c", "prop1", "1", "prop2", "3");
        Vertex d = sqlgGraph.addVertex(T.label, "Friend", "name", "d", "prop1", "2", "prop2", "4");
        Vertex e = sqlgGraph.addVertex(T.label, "Friend", "name", "e", "prop1", "2", "prop2", "5");
        Vertex f = sqlgGraph.addVertex(T.label, "Friend", "name", "f", "prop1", "2", "prop2", "6");
        a.addEdge("of", b);
        b.addEdge("of", c);
        c.addEdge("of", d);
        d.addEdge("of", e);
        e.addEdge("of", f);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(__.out("of").simplePath())
                .until(
                        __.or(
                                __.not(__.out("of").simplePath()),
                                __.and(
                                        __.has("prop1", "1"),
                                        __.has("prop2", "3")
                                )
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        stopWatch.stop();
        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c)));
        LOGGER.info("repeat query time: {}", stopWatch);

    }

    @Test
    public void testFriendOfFriendOutWithOrStep() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
            put("prop1", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
            put("prop2", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
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
        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "name", "a", "prop1", "1", "prop2", "1");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "name", "b", "prop1", "1", "prop2", "2");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "name", "c", "prop1", "1", "prop2", "3");
        Vertex d = sqlgGraph.addVertex(T.label, "Friend", "name", "d", "prop1", "2", "prop2", "4");
        Vertex e = sqlgGraph.addVertex(T.label, "Friend", "name", "e", "prop1", "2", "prop2", "5");
        Vertex f = sqlgGraph.addVertex(T.label, "Friend", "name", "f", "prop1", "2", "prop2", "6");
        a.addEdge("of", b);
        b.addEdge("of", c);
        a.addEdge("of", d);
        d.addEdge("of", e);
        e.addEdge("of", f);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(__.out("of").simplePath())
                .until(
                        __.or(
                                __.not(__.out("of").simplePath()),
                                __.or(
                                        __.has("prop2", "2"),
                                        __.has("prop2", "5")
                                )
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        stopWatch.stop();
        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(d) && p.get(2).equals(e)));
        LOGGER.info("repeat query time: {}", stopWatch);

    }

    @Test
    public void testFriendOfFriendBothIncludeEdgeWithUtil() {
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
        Edge e1 = a.addEdge("of", b);
        Edge e2 = b.addEdge("of", c);
        Edge e3 = c.addEdge("of", d);
        Edge e4 = d.addEdge("of", e);
        Edge e5 = e.addEdge("of", f);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(c)
                .repeat(
                        __.bothE("of").as("e").otherV().as("v").simplePath()
                ).until(
                        __.or(
                                __.not(__.bothE("of").simplePath()),
                                __.select("v").has("name", P.within("b", "e"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(c) && p.get(1).equals(e2) && p.get(2).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(c) && p.get(1).equals(e3) && p.get(2).equals(d) && p.get(3).equals(e4) && p.get(4).equals(e)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(c)
                .repeat(
                        __.bothE("of").otherV().simplePath()
                ).until(
                        __.or(
                                __.has("name", P.within("b", "e")),
                                __.not(__.bothE("of").simplePath())
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(c) && p.get(1).equals(e2) && p.get(2).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(c) && p.get(1).equals(e3) && p.get(2).equals(d) && p.get(3).equals(e4) && p.get(4).equals(e)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(c)
                .repeat(
                        __.bothE("of").as("e").otherV().as("v").simplePath()
                ).until(
                        __.or(
                                __.not(__.bothE("of").simplePath()),
                                __.has("name", P.within("b", "e"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(c) && p.get(1).equals(e2) && p.get(2).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(c) && p.get(1).equals(e3) && p.get(2).equals(d) && p.get(3).equals(e4) && p.get(4).equals(e)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(c)
                .repeat(
                        __.bothE("of").as("e").otherV().as("v").simplePath()
                ).until(
                        __.or(
                                __.not(__.bothE("of").simplePath()),
                                __.select("v").has("name", P.within("b", "e"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(c) && p.get(1).equals(e2) && p.get(2).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(c) && p.get(1).equals(e3) && p.get(2).equals(d) && p.get(3).equals(e4) && p.get(4).equals(e)));
    }

    @Test
    public void testFriendOfFriendBothWithUtilAndAndOr() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
            put("prop1", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
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
        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "name", "a", "prop1", "1");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "name", "b", "prop1", "2");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "name", "c", "prop1", "2");
        Vertex d = sqlgGraph.addVertex(T.label, "Friend", "name", "d", "prop1", "2");
        Vertex e = sqlgGraph.addVertex(T.label, "Friend", "name", "e", "prop1", "3");
        Vertex f = sqlgGraph.addVertex(T.label, "Friend", "name", "f", "prop1", "3");
        Vertex g = sqlgGraph.addVertex(T.label, "Friend", "name", "g", "prop1", "4");
        Vertex h = sqlgGraph.addVertex(T.label, "Friend", "name", "h", "prop1", "4");
        Vertex i = sqlgGraph.addVertex(T.label, "Friend", "name", "i", "prop1", "5");
        Vertex j = sqlgGraph.addVertex(T.label, "Friend", "name", "j", "prop1", "5");
        a.addEdge("of", b);
        a.addEdge("of", c);
        a.addEdge("of", d);
        d.addEdge("of", e);
        d.addEdge("of", f);
        e.addEdge("of", g);
        f.addEdge("of", h);
        b.addEdge("of", i);
        c.addEdge("of", j);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(d)
                .repeat(
                        __.both("of").simplePath()
                ).until(
                        __.or(
                                __.not(__.both("of").simplePath()),
                                __.and(
                                        __.has("prop1", P.eq("2"))
                                )
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(4, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(d) && p.get(1).equals(a) && p.get(2).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(d) && p.get(1).equals(a) && p.get(2).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(d) && p.get(1).equals(e) && p.get(2).equals(g)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(d) && p.get(1).equals(f) && p.get(2).equals(h)));

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
        a.addEdge("of", b);
        b.addEdge("of", c);
        c.addEdge("of", d);
        d.addEdge("of", e);
        e.addEdge("of", f);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(e)
                .repeat(
                        __.both("of").simplePath()
                ).until(
                        __.or(
                                __.not(__.both("of").simplePath()),
                                __.has("name", P.within("b", "c"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(e) && p.get(1).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(e) && p.get(1).equals(d) && p.get(2).equals(c)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(e)
                .repeat(
                        __.both("of").simplePath()
                ).until(
                        __.or(
                                __.not(__.both("of").simplePath()),
                                __.has("name", P.within("b", "c"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(e) && p.get(1).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(e) && p.get(1).equals(d) && p.get(2).equals(c)));
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
        Vertex bb = sqlgGraph.addVertex(T.label, "Friend", "name", "bb");
        a.addEdge("of", b);
        b.addEdge("of", c);
        c.addEdge("of", d);
        d.addEdge("of", e);
        e.addEdge("of", f);
        bb.addEdge("of", f);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(e)
                .repeat(
                        __.in("of").simplePath()
                ).until(
                        __.or(
                                __.not(__.in("of").simplePath()),
                                __.has("name", P.within("b", "c"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Path p1 = paths.get(0);
        Assert.assertTrue(p1.size() == 3 && p1.get(0).equals(e) && p1.get(1).equals(d) && p1.get(2).equals(c));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(f)
                .repeat(
                        __.in("of").simplePath()
                ).until(
                        __.or(
                                __.not(__.in("of").simplePath()),
                                __.has("name", P.within("b", "c"))
                        )

                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(f) && p.get(1).equals(e) && p.get(2).equals(d) && p.get(3).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(f) && p.get(1).equals(bb)));
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
        Vertex bb = sqlgGraph.addVertex(T.label, "Friend", "name", "bb");
        a.addEdge("of", b);
        a.addEdge("of", bb);
        b.addEdge("of", c);
        c.addEdge("of", d);
        d.addEdge("of", e);
        e.addEdge("of", f);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(b)
                .repeat(
                        __.out("of").simplePath()
                ).until(
                        __.or(
                                __.not(__.out("of").simplePath()),
                                __.has("name", P.within("e", "f"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Path p1 = paths.get(0);
        Assert.assertTrue(p1.size() == 4 && p1.get(0).equals(b) && p1.get(1).equals(c) && p1.get(2).equals(d) && p1.get(3).equals(e));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(
                        __.out("of").simplePath()
                ).until(
                        __.or(
                                __.not(__.out("of").simplePath()),
                                __.has("name", P.neq("c"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(b)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(bb)));
    }

    @Test
    public void testFriendOfFriendOutIncludeEdgeWithUtil() {
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
        Edge e1 = a.addEdge("of", b);
        Edge e2 = b.addEdge("of", c);
        Edge e3 = c.addEdge("of", d);
        Edge e4 = d.addEdge("of", e);
        Edge e5 = e.addEdge("of", f);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(b)
                .repeat(
                        __.outE("of").as("e").inV().as("v").simplePath()
                ).until(
                        __.or(
                                __.not(__.outE("of").simplePath()),
                                __.select("v").has("name", P.within("e", "f"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Path p1 = paths.get(0);
        Assert.assertTrue(p1.size() == 7 && p1.get(0).equals(b) && p1.get(1).equals(e2) && p1.get(2).equals(c) &&
                p1.get(3).equals(e3) && p1.get(4).equals(d) && p1.get(5).equals(e4) && p1.get(6).equals(e));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(b)
                .repeat(
                        __.outE("of").inV().simplePath()
                ).until(
                        __.or(
                                __.not(__.outE("of").simplePath()),
                                __.has("name", P.within("e", "f"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        p1 = paths.get(0);
        Assert.assertTrue(p1.size() == 7 && p1.get(0).equals(b) && p1.get(1).equals(e2) && p1.get(2).equals(c) &&
                p1.get(3).equals(e3) && p1.get(4).equals(d) && p1.get(5).equals(e4) && p1.get(6).equals(e));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(b)
                .repeat(
                        __.outE("of").as("e").inV().as("v").simplePath()
                ).until(
                        __.or(
                                __.not(__.outE("of").simplePath()),
                                __.has("name", P.within("e", "f"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        p1 = paths.get(0);
        Assert.assertTrue(p1.size() == 7 && p1.get(0).equals(b) && p1.get(1).equals(e2) && p1.get(2).equals(c) &&
                p1.get(3).equals(e3) && p1.get(4).equals(d) && p1.get(5).equals(e4) && p1.get(6).equals(e));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(b)
                .repeat(
                        __.outE("of").as("e").inV().as("v").simplePath()
                ).until(
                        __.or(
                                __.not(__.outE("of").simplePath()),
                                __.select("v").has("name", P.within("e", "f"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        p1 = paths.get(0);
        Assert.assertTrue(p1.size() == 7 && p1.get(0).equals(b) && p1.get(1).equals(e2) && p1.get(2).equals(c) &&
                p1.get(3).equals(e3) && p1.get(4).equals(d) && p1.get(5).equals(e4) && p1.get(6).equals(e));
    }

    @Test
    public void testFriendOfFriendInIncludeEdgeWithUtil() {
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
        Edge e1 = a.addEdge("of", b);
        Edge e2 = b.addEdge("of", c);
        Edge e3 = c.addEdge("of", d);
        Edge e4 = d.addEdge("of", e);
        Edge e5 = e.addEdge("of", f);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(e)
                .repeat(
                        __.inE("of").as("e").outV().as("v").simplePath()
                ).until(
                        __.or(
                                __.not(__.inE("of").simplePath()),
                                __.select("v").has("name", P.within("b", "c"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Path p1 = paths.get(0);
        Assert.assertTrue(p1.size() == 5 && p1.get(0).equals(e) && p1.get(1).equals(e4) && p1.get(2).equals(d) &&
                p1.get(3).equals(e3) && p1.get(4).equals(c));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(e)
                .repeat(
                        __.inE("of").outV().simplePath()
                ).until(
                        __.or(
                                __.not(__.inE("of").simplePath()),
                                __.has("name", P.within("b", "c"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        p1 = paths.get(0);
        Assert.assertTrue(p1.size() == 5 && p1.get(0).equals(e) && p1.get(1).equals(e4) && p1.get(2).equals(d) &&
                p1.get(3).equals(e3) && p1.get(4).equals(c));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(e)
                .repeat(
                        __.inE("of").as("e").outV().as("v").simplePath()
                ).until(
                        __.or(
                                __.not(__.inE("of").simplePath()),
                                __.has("name", P.within("b", "c"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        p1 = paths.get(0);
        Assert.assertTrue(p1.size() == 5 && p1.get(0).equals(e) && p1.get(1).equals(e4) && p1.get(2).equals(d) &&
                p1.get(3).equals(e3) && p1.get(4).equals(c));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(e)
                .repeat(
                        __.inE("of").as("e").outV().as("v").simplePath()
                ).until(
                        __.or(
                                __.not(__.inE("of").simplePath()),
                                __.select("v").has("name", P.within("b", "c"))
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        p1 = paths.get(0);
        Assert.assertTrue(p1.size() == 5 && p1.get(0).equals(e) && p1.get(1).equals(e4) && p1.get(2).equals(d) &&
                p1.get(3).equals(e3) && p1.get(4).equals(c));
    }

    @Test
    public void testFriendOfFriendOutIncludeEdgeWithUtilValueOnEdge() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        }});
        friendVertexLabel.ensureEdgeLabelExist(
                "of",
                friendVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1)
                ), new LinkedHashMap<>() {{
                    put("ename", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
                }}
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
        Edge e1 = a.addEdge("of", b, "ename", "e1");
        Edge e2 = b.addEdge("of", c, "ename", "e2");
        Edge e3 = c.addEdge("of", d, "ename", "e3");
        Edge e4 = d.addEdge("of", e, "ename", "e4");
        Edge e5 = e.addEdge("of", f, "ename", "e5");
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(b)
                .repeat(
                        __.outE("of").as("e").inV().as("v").simplePath()
                ).until(
                        __.or(
                                __.not(__.outE("of").simplePath()),
                                __.select("e").has("ename", "e3"),
                                __.select("v").has("name", "b")
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Path p1 = paths.get(0);
        Assert.assertTrue(p1.size() == 5 && p1.get(0).equals(b) && p1.get(1).equals(e2) && p1.get(2).equals(c) && p1.get(3).equals(e3) && p1.get(4).equals(d));
    }

    @Test
    public void testInRepeatWithEdgeInPath0() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("field1", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
            put("field2", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        }});
        friendVertexLabel.ensureEdgeLabelExist(
                "of",
                friendVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1)
                ),
                new LinkedHashMap<>() {{
                    put("field1", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
                    put("field2", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
                }}
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();

        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "field1", "a", "field2", "aa");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "field1", "b", "field2", "bb");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "field1", "c", "field2", "cc");

        Edge e1 = a.addEdge("of", b, "field1", "of1", "field2", "of11");
        Edge e2 = b.addEdge("of", c, "field1", "of2", "field2", "of22");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V().hasId(c.id())
                .repeat(__.inE("of").outV().simplePath())
                .until(__.not(__.inE("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Path> inPaths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());

        Assert.assertEquals(1, inPaths.size());
        Path p1 = inPaths.get(0);
        Assert.assertEquals(5, p1.size());
        Assert.assertTrue(p1.get(0).equals(c) && p1.get(1).equals(e2) && p1.get(2).equals(b) && p1.get(3).equals(e1) && p1.get(4).equals(a));
        Vertex _c = p1.get(0);
        Edge _e2 = p1.get(1);
        Assert.assertEquals("c", _c.value("field1"));
        Assert.assertEquals("cc", _c.value("field2"));
        Assert.assertEquals("of2", _e2.value("field1"));
        Assert.assertEquals("of22", _e2.value("field2"));
        Vertex _b = p1.get(2);
        Edge _e1 = p1.get(3);
        Assert.assertEquals("b", _b.value("field1"));
        Assert.assertEquals("bb", _b.value("field2"));
        Assert.assertEquals("of1", _e1.value("field1"));
        Assert.assertEquals("of11", _e1.value("field2"));
        Vertex _a = p1.get(4);
        Assert.assertEquals("a", _a.value("field1"));
        Assert.assertEquals("aa", _a.value("field2"));
    }

    @Test
    public void testOutRepeatWithEdgeInPath0() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("field1", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
            put("field2", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        }});
        friendVertexLabel.ensureEdgeLabelExist(
                "of",
                friendVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1)
                ),
                new LinkedHashMap<>() {{
                    put("field1", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
                    put("field2", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
                }}
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();

        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "field1", "a", "field2", "aa");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "field1", "b", "field2", "bb");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "field1", "c", "field2", "cc");

        Edge e1 = a.addEdge("of", b, "field1", "of1", "field2", "of11");
        Edge e2 = b.addEdge("of", c, "field1", "of2", "field2", "of22");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(__.outE("of").inV().simplePath())
                .until(__.not(__.outE("of").simplePath()))
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> outPaths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, outPaths.size());
        Path p1 = outPaths.get(0);
        Assert.assertEquals(5, p1.size());
        Assert.assertTrue(p1.get(0).equals(a) && p1.get(1).equals(e1) && p1.get(2).equals(b) && p1.get(3).equals(e2) && p1.get(4).equals(c));
        Vertex _a = p1.get(0);
        Edge _e1 = p1.get(1);
        Assert.assertEquals("a", _a.value("field1"));
        Assert.assertEquals("aa", _a.value("field2"));
        Assert.assertEquals("of1", _e1.value("field1"));
        Assert.assertEquals("of11", _e1.value("field2"));
        Vertex _b = p1.get(2);
        Edge _e2 = p1.get(3);
        Assert.assertEquals("b", _b.value("field1"));
        Assert.assertEquals("bb", _b.value("field2"));
        Assert.assertEquals("of2", _e2.value("field1"));
        Assert.assertEquals("of22", _e2.value("field2"));
        Vertex _c = p1.get(4);
        Assert.assertEquals("c", _c.value("field1"));
        Assert.assertEquals("cc", _c.value("field2"));
    }

    @Test
    public void testOutRepeatWithEdgeInPath1() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("vertexName", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        }});
        friendVertexLabel.ensureEdgeLabelExist(
                "of",
                friendVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1)
                ),
                new LinkedHashMap<>() {{
                    put("edgeName", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
                }}
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();

        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "vertexName", "a");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "vertexName", "b");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "vertexName", "c");

        Edge e1 = a.addEdge("of", b, "edgeName", "of1");
        Edge e2 = b.addEdge("of", c, "edgeName", "of2");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V().hasId(a.id())
                .repeat(__.outE("of").inV().simplePath())
                .until(__.not(__.outE("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Path p1 = paths.get(0);
        Assert.assertEquals(5, p1.size());
    }

    @Test
    public void testFriendOfFriendBOTH() {
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

        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "name", "a");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "name", "b");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "name", "c");
        Vertex d = sqlgGraph.addVertex(T.label, "Friend", "name", "d");

        a.addEdge("of", b);
        b.addEdge("of", c);
        c.addEdge("of", b);
        this.sqlgGraph.tx().commit();

        StopWatch stopWatch = StopWatch.createStarted();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V().hasLabel("Friend").has("name", "a")
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

        Assert.assertEquals(2, paths.size());
        Path p1 = paths.get(0);
        Assert.assertTrue(p1.get(0).equals(a) && p1.get(1).equals(b) && p1.get(2).equals(c));
        Path p2 = paths.get(1);
        Assert.assertEquals(p1, p2);
        ListOrderedSet<RepeatRow> result = executeSqlForDirectionBOTH(((RecordId) a.id()).sequenceId());
        LOGGER.info("sql query time: {}", stopWatch);
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 2L, 3L})));
        Assert.assertArrayEquals(result.get(0).path, result.get(1).path);

        Vertex v = p1.get(0);
        System.out.println(v.toString());
    }

    @Test
    public void testFriendOfFriendSimple() {
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
        this.sqlgGraph.tx().normalBatchModeOn();
        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "name", "a");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "name", "b");
        a.addEdge("of", b);
        this.sqlgGraph.tx().commit();

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
        Assert.assertEquals(1, paths.size());
    }

    @Test
    public void testBothRepeatWithEdgeInPath1() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("vertexName", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        }});
        friendVertexLabel.ensureEdgeLabelExist(
                "of",
                friendVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1)
                ),
                new LinkedHashMap<>() {{
                    put("edgeName", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
                }}
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();

        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "vertexName", "a");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "vertexName", "b");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "vertexName", "c");

        Edge e1 = a.addEdge("of", b, "edgeName", "of1");
        Edge e2 = b.addEdge("of", c, "edgeName", "of2");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V().hasId(a.id())
                .repeat(__.bothE("of").otherV().simplePath())
                .until(
                        __.not(__.bothE("of").simplePath())
                )
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Path p1 = paths.get(0);
        Assert.assertEquals(5, p1.size());
        Assert.assertTrue(p1.get(0).equals(a) && p1.get(1).equals(e1) && p1.get(2).equals(b) && p1.get(3).equals(e2) && p1.get(4).equals(c));
    }

    @Test
    public void testFriendOfFriendBothHas() {
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

    @Test
    public void testFriendOfFriendBothHasIncludeEdge() {
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
        Edge e1 = a.addEdge("of", b);
        Edge e2 = b.addEdge("of", c);
        Edge e3 = c.addEdge("of", d);
        Edge e4 = c.addEdge("of", e);
        Edge e5 = a.addEdge("of", f);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) sqlgGraph.traversal().V().hasLabel("Friend")
                .has("name", P.within("a"))
                .repeat(__.bothE("of").otherV().simplePath())
                .until(__.not(__.bothE("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        stopWatch.stop();
        LOGGER.info("repeat query time: {}", stopWatch);
        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 7 && p.get(0).equals(a) && p.get(1).equals(e1) && p.get(2).equals(b) && p.get(3).equals(e2) && p.get(4).equals(c) && p.get(5).equals(e3) && p.get(6).equals(d)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 7 && p.get(0).equals(a) && p.get(1).equals(e1) && p.get(2).equals(b) && p.get(3).equals(e2) && p.get(4).equals(c) && p.get(5).equals(e4) && p.get(6).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(e5) && p.get(2).equals(f)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) sqlgGraph.traversal().V().hasLabel("Friend")
                .has("name", P.within("b"))
                .repeat(__.bothE("of").otherV().simplePath()).until(__.not(__.bothE("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(b) && p.get(1).equals(e2) && p.get(2).equals(c) && p.get(3).equals(e3) && p.get(4).equals(d)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(b) && p.get(1).equals(e2) && p.get(2).equals(c) && p.get(3).equals(e4) && p.get(4).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(b) && p.get(1).equals(e1) && p.get(2).equals(a) && p.get(3).equals(e5) && p.get(4).equals(f)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) sqlgGraph.traversal().V().hasLabel("Friend")
                .has("name", P.within("d"))
                .repeat(__.bothE("of").otherV().simplePath()).until(__.not(__.bothE("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 9 && p.get(0).equals(d) && p.get(1).equals(e3) && p.get(2).equals(c) && p.get(3).equals(e2) && p.get(4).equals(b) && p.get(5).equals(e1) && p.get(6).equals(a) && p.get(7).equals(e5) && p.get(8).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(d) && p.get(1).equals(e3) && p.get(2).equals(c) && p.get(3).equals(e4) && p.get(4).equals(e)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) sqlgGraph.traversal().V().hasLabel("Friend")
                .has("name", P.within("d", "c"))
                .repeat(__.bothE("of").otherV().simplePath()).until(__.not(__.bothE("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        Assert.assertEquals(5, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 9 && p.get(0).equals(d) && p.get(1).equals(e3) && p.get(2).equals(c) && p.get(3).equals(e2) && p.get(4).equals(b) && p.get(5).equals(e1) && p.get(6).equals(a) && p.get(7).equals(e5) && p.get(8).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(d) && p.get(1).equals(e3) && p.get(2).equals(c) && p.get(3).equals(e4) && p.get(4).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(c) && p.get(1).equals(e3) && p.get(2).equals(d)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(c) && p.get(1).equals(e4) && p.get(2).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 7 && p.get(0).equals(c) && p.get(1).equals(e2) && p.get(2).equals(b) && p.get(3).equals(e1) && p.get(4).equals(a) && p.get(5).equals(e5) && p.get(6).equals(f)));
    }

    @Test
    public void testFriendOfFriendInHas() {
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
                .has("name", P.within("e", "f"))
                .repeat(__.in("of").simplePath())
                .until(__.not(__.in("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        stopWatch.stop();
        LOGGER.info("repeat query time: {}", stopWatch);
        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(e) && p.get(1).equals(c) && p.get(2).equals(b) && p.get(3).equals(a)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(f) && p.get(1).equals(a)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) sqlgGraph.traversal().V().hasLabel("Friend")
                .has("name", P.within("e", "f", "d"))
                .repeat(__.in("of").simplePath())
                .until(__.not(__.in("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(e) && p.get(1).equals(c) && p.get(2).equals(b) && p.get(3).equals(a)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(d) && p.get(1).equals(c) && p.get(2).equals(b) && p.get(3).equals(a)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(f) && p.get(1).equals(a)));
    }

    @Test
    public void testFriendOfFriendInHasWithEdgeInPath() {
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
        Edge e1 = a.addEdge("of", b);
        Edge e2 = b.addEdge("of", c);
        Edge e3 = c.addEdge("of", d);
        Edge e4 = c.addEdge("of", e);
        Edge e5 = a.addEdge("of", f);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) sqlgGraph.traversal().V().hasLabel("Friend")
                .has("name", P.within("e", "f"))
                .repeat(__.inE("of").outV().simplePath())
                .until(__.not(__.inE("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        stopWatch.stop();
        LOGGER.info("repeat query time: {}", stopWatch);
        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 7 && p.get(0).equals(e) && p.get(1).equals(e4) && p.get(2).equals(c) && p.get(3).equals(e2) && p.get(4).equals(b) && p.get(5).equals(e1) && p.get(6).equals(a)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(f) && p.get(1).equals(e5) && p.get(2).equals(a)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) sqlgGraph.traversal().V().hasLabel("Friend")
                .has("name", P.within("e", "f", "d"))
                .repeat(__.inE("of").outV().simplePath())
                .until(__.not(__.inE("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 7 && p.get(0).equals(e) && p.get(1).equals(e4) && p.get(2).equals(c) && p.get(3).equals(e2) && p.get(4).equals(b) && p.get(5).equals(e1) && p.get(6).equals(a)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(f) && p.get(1).equals(e5) && p.get(2).equals(a)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 7 && p.get(0).equals(d) && p.get(1).equals(e3) && p.get(2).equals(c) && p.get(3).equals(e2) && p.get(4).equals(b) && p.get(5).equals(e1) && p.get(6).equals(a)));
    }

    @Test
    public void testFriendOfFriendOutHas() {
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

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V().hasLabel("Friend")
                .has("name", P.within("a", "b"))
                .repeat(__.out("of").simplePath())
                .until(__.not(__.out("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        stopWatch.stop();
        Assert.assertEquals(5, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c) && p.get(3).equals(d)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c) && p.get(3).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(b) && p.get(1).equals(c) && p.get(2).equals(d)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(b) && p.get(1).equals(c) && p.get(2).equals(e)));
        LOGGER.info("repeat query time: {}", stopWatch);
    }

    @Test
    public void testFriendOfFriendOutHasWithEdgeInPath() {
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
        Edge e1 = a.addEdge("of", b);
        Edge e2 = b.addEdge("of", c);
        Edge e3 = c.addEdge("of", d);
        Edge e4 = c.addEdge("of", e);
        Edge e5 = a.addEdge("of", f);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V().hasLabel("Friend")
                .has("name", P.within("a", "b"))
                .repeat(__.outE("of").inV().simplePath())
                .until(__.not(__.outE("of").simplePath()))
                .path();
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        stopWatch.stop();
        Assert.assertEquals(5, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 7 && p.get(0).equals(a) && p.get(1).equals(e1) && p.get(2).equals(b) && p.get(3).equals(e2) && p.get(4).equals(c) && p.get(5).equals(e3) && p.get(6).equals(d)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 7 && p.get(0).equals(a) && p.get(1).equals(e1) && p.get(2).equals(b) && p.get(3).equals(e2) && p.get(4).equals(c) && p.get(5).equals(e4) && p.get(6).equals(e)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(e5) && p.get(2).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(b) && p.get(1).equals(e2) && p.get(2).equals(c) && p.get(3).equals(e3) && p.get(4).equals(d)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(b) && p.get(1).equals(e2) && p.get(2).equals(c) && p.get(3).equals(e4) && p.get(4).equals(e)));
        LOGGER.info("repeat query time: {}", stopWatch);
    }

    @Test
    public void testFriendOfFriendOut() {
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

        Vertex first = this.sqlgGraph.traversal().V().hasLabel("Friend").has("name", "a").tryNext().orElseThrow();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(first)
                .repeat(__.out("of").simplePath())
                .until(__.not(__.out("of").simplePath()))
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        stopWatch.stop();
        Assert.assertEquals(3, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 2 && p.get(0).equals(a) && p.get(1).equals(f)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c) && p.get(3).equals(d)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 4 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c) && p.get(3).equals(e)));
        LOGGER.info("repeat query time: {}", stopWatch);

        stopWatch.reset();
        stopWatch.start();
        ListOrderedSet<RepeatRow> result = executeSqlForDirectionOUT(((RecordId) first.id()).sequenceId());
        stopWatch.stop();
        LOGGER.info("sql query time: {}", stopWatch);
        Assert.assertEquals(3, result.size());
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 6L})));
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 2L, 3L, 4L})));
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 2L, 3L, 5L})));

        stopWatch.reset();
        stopWatch.start();
        first = this.sqlgGraph.traversal().V().hasLabel("Friend").has("name", "d").tryNext().orElseThrow();
        paths = this.sqlgGraph.traversal().V(first)
                .repeat(__.in("of").simplePath()).until(__.not(__.in("of").simplePath()))
                .path()
                .toList();
        stopWatch.stop();
        Assert.assertEquals(1, paths.size());
        Assert.assertEquals(4, paths.get(0).size());
        LOGGER.info("repeat query time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();
        result = executeSqlForDirectionIN(((RecordId) first.id()).sequenceId());
        stopWatch.stop();
        LOGGER.info("sql query time: {}", stopWatch);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{4L, 3L, 2L, 1L})));

    }

    @Test
    public void testFriendOfFriendCycles() {
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
        a.addEdge("of", b);
        b.addEdge("of", c);
        c.addEdge("of", a);
        this.sqlgGraph.tx().commit();
        stopWatch.stop();
        LOGGER.info("insert time: {}", stopWatch);
        stopWatch.reset();
        stopWatch.start();

        Vertex first = this.sqlgGraph.traversal().V().hasLabel("Friend").has("name", "a").tryNext().orElseThrow();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(first)
                .repeat(__.out("of").simplePath())
                .until(__.not(__.out("of").simplePath()))
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        stopWatch.stop();
        LOGGER.info("repeat query time: {}", stopWatch);
        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c)));

        stopWatch.reset();
        stopWatch.start();
        ListOrderedSet<RepeatRow> result = executeSqlForDirectionOUT(((RecordId) first.id()).sequenceId());
        stopWatch.stop();
        LOGGER.info("sql query time: {}", stopWatch);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 2L, 3L})));

        stopWatch.reset();
        stopWatch.start();
        first = this.sqlgGraph.traversal().V().hasLabel("Friend").has("name", "a").tryNext().orElseThrow();
        paths = this.sqlgGraph.traversal().V(first)
                .repeat(__.in("of").simplePath()).until(__.not(__.in("of").simplePath()))
                .path()
                .toList();
        stopWatch.stop();
        LOGGER.info("repeat query time: {}", stopWatch);
        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(c) && p.get(2).equals(b)));

        stopWatch.reset();
        stopWatch.start();
        result = executeSqlForDirectionIN(((RecordId) first.id()).sequenceId());
        stopWatch.stop();
        LOGGER.info("sql query time: {}", stopWatch);
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 3L, 2L})));
    }

    @Test
    public void testFriendOfFriendBOTHComplicated() {
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

        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "name", "a");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "name", "b");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "name", "c");
        Vertex aa = sqlgGraph.addVertex(T.label, "Friend", "name", "aa");
        Vertex bb = sqlgGraph.addVertex(T.label, "Friend", "name", "b");

        a.addEdge("of", b);
        b.addEdge("of", c);
        aa.addEdge("of", bb);
        aa.addEdge("of", b);
        bb.addEdge("of", c);
        this.sqlgGraph.tx().commit();

        StopWatch stopWatch = StopWatch.createStarted();
        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(__.both("of").simplePath())
                .until(__.not(__.both("of").simplePath()))
                .path();
        stopWatch.stop();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        LOGGER.info("repeat query time: {}", stopWatch);

        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(c) && p.get(3).equals(bb) && p.get(4).equals(aa)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(a) && p.get(1).equals(b) && p.get(2).equals(aa) && p.get(3).equals(bb) && p.get(4).equals(c)));
        stopWatch.reset();
        stopWatch.start();
        ListOrderedSet<RepeatRow> result = executeSqlForDirectionBOTH(((RecordId) a.id()).sequenceId());
        stopWatch.stop();
        LOGGER.info("sql query time: {}", stopWatch);
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 2L, 3L, 5L, 4L})));
        Assert.assertTrue(result.stream().anyMatch(r -> Arrays.equals(r.path, new Long[]{1L, 2L, 4L, 5L, 3L})));
    }

    private ListOrderedSet<RepeatRow> executeSqlForDirectionOUT(Long startNode) {
        ListOrderedSet<RepeatRow> result = new ListOrderedSet<>();
        Connection connection = this.sqlgGraph.tx().getConnection();
        String sql = """
                --OUT
                WITH a AS (
                WITH RECURSIVE search_tree("ID", "public.Friend__O", "public.Friend__I", depth, is_cycle, previous, path) AS (
                    SELECT e."ID", e."public.Friend__O", e."public.Friend__I", 1, false, ARRAY[e."public.Friend__O"], ARRAY[e."public.Friend__O", e."public.Friend__I"]
                    FROM "E_of" e
                    WHERE "public.Friend__O" = {x}
                    UNION ALL
                    SELECT e."ID", e."public.Friend__O", e."public.Friend__I", st.depth + 1, e."public.Friend__I" = ANY(path), path, path || e."public.Friend__I"
                    FROM "E_of" e, search_tree st
                    WHERE st."public.Friend__I" = e."public.Friend__O" AND NOT is_cycle
                )
                SELECT * FROM search_tree
                WHERE NOT is_cycle
                )
                SELECT a.path from a
                WHERE a.path NOT IN (SELECT previous from a)
                """;
        sql = sql.replace("{x}", startNode.toString());
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                Array path = rs.getArray(1);
                result.add(new RepeatRow(((Long[]) path.getArray())));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private ListOrderedSet<RepeatRow> executeSqlForDirectionIN(Long startNode) {
        ListOrderedSet<RepeatRow> result = new ListOrderedSet<>();
        Connection connection = this.sqlgGraph.tx().getConnection();
        String sql = """
                WITH a AS (
                WITH RECURSIVE search_tree("ID", "public.Friend__I", "public.Friend__O", depth, is_cycle, previous, path) AS (
                    SELECT e."ID", e."public.Friend__I", e."public.Friend__O", 1, false, ARRAY[e."public.Friend__I"], ARRAY[e."public.Friend__I", e."public.Friend__O"]
                    FROM "E_of" e
                    WHERE "public.Friend__I" = {x}
                    UNION ALL
                    SELECT e."ID", e."public.Friend__I", e."public.Friend__O", st.depth + 1, e."public.Friend__O" = ANY(path), path, path || e."public.Friend__O"
                    FROM "E_of" e, search_tree st
                    WHERE st."public.Friend__O" = e."public.Friend__I" AND NOT is_cycle
                )
                SELECT * FROM search_tree\s
                WHERE NOT is_cycle
                )
                SELECT a.path from a
                WHERE a.path NOT IN (SELECT previous from a);
                """;
        sql = sql.replace("{x}", startNode.toString());
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                Array path = rs.getArray(1);
                result.add(new RepeatRow(((Long[]) path.getArray())));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private ListOrderedSet<RepeatRow> executeSqlForDirectionBOTH(Long startNode) {
        ListOrderedSet<RepeatRow> result = new ListOrderedSet<>();
        Connection connection = this.sqlgGraph.tx().getConnection();
        String sql = """
                WITH a as (
                    WITH RECURSIVE search_tree("ID", "public.Friend__O", "public.Friend__I", depth, is_cycle, previous, path, direction) AS (
                        SELECT e."ID", e."public.Friend__O", e."public.Friend__I", 1, false,
                               CASE
                                   WHEN "public.Friend__O" = 1 THEN ARRAY[e."public.Friend__O"]
                                   WHEN "public.Friend__I" = 1 THEN ARRAY[e."public.Friend__I"]
                                   END,
                               CASE
                                   WHEN "public.Friend__O" = 1 THEN ARRAY[e."public.Friend__O", e."public.Friend__I"]
                                   WHEN "public.Friend__I" = 1 THEN ARRAY[e."public.Friend__I", e."public.Friend__O"]
                                   END,
                               CASE
                                   WHEN "public.Friend__O" = 1 THEN 'OUT'
                                   WHEN "public.Friend__I" = 1 THEN 'IN'
                                   END
                        FROM "E_of" e
                        WHERE "public.Friend__O" = 1 or "public.Friend__I" = 1
                        UNION ALL
                        SELECT e."ID", e."public.Friend__O", e."public.Friend__I", st.depth + 1,
                               CASE
                                   WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__O" THEN e."public.Friend__I" = ANY(path)
                                   WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__I" THEN e."public.Friend__O" = ANY(path)
                                   WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__I" THEN e."public.Friend__O" = ANY(path)
                                   WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__O" THEN e."public.Friend__I" = ANY(path)
                                   END,
                               CASE
                                   WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__O" THEN path
                                   WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__I" THEN path
                                   WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__I" THEN path
                                   WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__O" THEN path
                                   END,
                               CASE
                                   WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__O" THEN path || e."public.Friend__I"
                                   WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__I" THEN path || e."public.Friend__O"
                                   WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__I" THEN path || e."public.Friend__O"
                                   WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__O" THEN path || e."public.Friend__I"
                                   END,
                               CASE
                                   WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__O" THEN 'OUT'
                                   WHEN st.direction = 'OUT' AND st."public.Friend__I" = e."public.Friend__I" THEN 'IN'
                                   WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__I" THEN 'IN'
                                   WHEN st.direction = 'IN' AND st."public.Friend__O" = e."public.Friend__O" THEN 'OUT'
                                   END
                        FROM "E_of" e, search_tree st
                        WHERE
                            (
                                (st.direction = 'OUT' AND (st."public.Friend__I" = e."public.Friend__O" OR st."public.Friend__I" = e."public.Friend__I"))
                                    OR
                                (st.direction = 'IN' AND (st."public.Friend__O" = e."public.Friend__I" OR st."public.Friend__O" = e."public.Friend__O"))
                                )
                          AND NOT is_cycle
                    )
                    SELECT * FROM search_tree
                    WHERE NOT is_cycle
                )
                SELECT a.path from a
                WHERE a.path NOT IN (SELECT previous from a);
                """;
        sql = sql.replace("{x}", startNode.toString());
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                Array path = rs.getArray(1);
                result.add(new RepeatRow(((Long[]) path.getArray())));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private record RepeatRow(Long[] path) {

    }
}
