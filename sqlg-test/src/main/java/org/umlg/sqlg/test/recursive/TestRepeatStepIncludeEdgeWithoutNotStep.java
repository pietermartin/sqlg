package org.umlg.sqlg.test.recursive;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
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

public class TestRepeatStepIncludeEdgeWithoutNotStep extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestRepeatStepIncludeEdgeWithoutNotStep.class);

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        Assume.assumeTrue(isPostgres());
    }

    //used in docs
//    @Test
    public void testOutRepeatWithEdgeInPathDoc() {
        VertexLabel friendVertexLabel = this.sqlgGraph.getTopology().getPublicSchema().ensureVertexLabelExist("Friend", new LinkedHashMap<>() {{
            put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
        }});
        friendVertexLabel.ensureEdgeLabelExist(
                "of",
                friendVertexLabel,
                EdgeDefinition.of(
                        Multiplicity.of(0, -1),
                        Multiplicity.of(0, -1)
                ),
                new LinkedHashMap<>() {{
                    put("name", PropertyDefinition.of(PropertyType.STRING, Multiplicity.of(1, 1)));
                }}
        );
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.getTopology().lock();

        Vertex a = sqlgGraph.addVertex(T.label, "Friend", "name", "a");
        Vertex b = sqlgGraph.addVertex(T.label, "Friend", "name", "b");
        Vertex c = sqlgGraph.addVertex(T.label, "Friend", "name", "c");
        Vertex d = sqlgGraph.addVertex(T.label, "Friend", "name", "d");
        Vertex e = sqlgGraph.addVertex(T.label, "Friend", "name", "e");
        Vertex f = sqlgGraph.addVertex(T.label, "Friend", "name", "f");

        a.addEdge("of", b, "name", "ab");
        a.addEdge("of", c, "name", "ac");
        c.addEdge("of", d, "name", "cd");
        c.addEdge("of", e, "name", "ce");
        e.addEdge("of", f, "name", "ef");

        this.sqlgGraph.tx().commit();

        List<Path> paths = this.sqlgGraph.traversal().V(a)
                .repeat(__.outE("of").as("e").inV().as("v").simplePath())
                .until(
                        __.select("e").has("name", "ce")
                )
                .path().by("name")
                .toList();
        for (Path path : paths) {
            LOGGER.debug(path.toString());
        }
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
        Vertex d = sqlgGraph.addVertex(T.label, "Friend", "field1", "d", "field2", "dd");

        Edge e1 = a.addEdge("of", b, "field1", "of1", "field2", "of11");
        Edge e2 = b.addEdge("of", c, "field1", "of2", "field2", "of22");
        Edge e3 = a.addEdge("of", d, "field1", "of3", "field2", "of33");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(__.outE("of").inV().simplePath())
                .until(
                        __.has("field1", "c")
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(a) && p.get(1).equals(e1) && p.get(2).equals(b) && p.get(3).equals(e2) && p.get(4).equals(c)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(__.outE("of").as("e").inV().as("v").simplePath())
                .until(
                        __.select("e").has("field2", "of22")
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(a) && p.get(1).equals(e1) && p.get(2).equals(b) && p.get(3).equals(e2) && p.get(4).equals(c)));
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
        Vertex d = sqlgGraph.addVertex(T.label, "Friend", "field1", "d", "field2", "dd");

        Edge e1 = c.addEdge("of", b, "field1", "of1", "field2", "of11");
        Edge e2 = b.addEdge("of", a, "field1", "of2", "field2", "of22");
        Edge e3 = d.addEdge("of", a, "field1", "of3", "field2", "of33");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(__.inE("of").outV().simplePath())
                .until(
                        __.has("field1", "c")
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(a) && p.get(1).equals(e2) && p.get(2).equals(b) && p.get(3).equals(e1) && p.get(4).equals(c)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(__.inE("of").as("e").outV().as("v").simplePath())
                .until(
                        __.select("e").has("field2", "of22")
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(e2) && p.get(2).equals(b)));
    }

    @Test
    public void testBothRepeatWithEdgeInPath0() {
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
        Vertex d = sqlgGraph.addVertex(T.label, "Friend", "field1", "d", "field2", "dd");

        Edge e1 = c.addEdge("of", b, "field1", "of1", "field2", "of11");
        Edge e2 = b.addEdge("of", a, "field1", "of2", "field2", "of22");
        Edge e3 = d.addEdge("of", a, "field1", "of3", "field2", "of33");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(__.bothE("of").otherV().simplePath())
                .until(
                        __.has("field1", "c")
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(a) && p.get(1).equals(e2) && p.get(2).equals(b) && p.get(3).equals(e1) && p.get(4).equals(c)));

        traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(__.bothE("of").as("e").otherV().as("v").simplePath())
                .until(
                        __.select("e").has("field2", "of22")
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());

        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(e2) && p.get(2).equals(b)));
    }
}
