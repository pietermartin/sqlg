package org.umlg.sqlg.test.recursive;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
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
import org.umlg.sqlg.structure.DefaultSqlgTraversal;
import org.umlg.sqlg.structure.Multiplicity;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeDefinition;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.LinkedHashMap;
import java.util.List;

public class TestRepeatStepIncludeEdgeWithNotStep extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestRepeatStepIncludeEdgeWithNotStep.class);

    @BeforeClass
    public static void beforeClass() {
        BaseTest.beforeClass();
        Assume.assumeTrue(isPostgres());
    }

    @Test
    public void testOutRepeatWithEdgeInUntil() {
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

        Edge e1 = a.addEdge("of", b);
        Edge e2 = b.addEdge("of", c);
        Edge e3 = a.addEdge("of", d);
        this.sqlgGraph.tx().commit();

        DefaultSqlgTraversal<Vertex, Path> traversal = (DefaultSqlgTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(__.outE("of").inV().simplePath())
                .until(
                        __.or(
                                __.not(__.both("of").simplePath()),
                                __.has("name", "c")
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(a) && p.get(1).equals(e1) && p.get(2).equals(b) && p.get(3).equals(e2) && p.get(4).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(e3) && p.get(2).equals(d)));
    }

    @Test
    public void testInRepeatWithEdgeInUntil() {
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

        Edge e1 = a.addEdge("of", b);
        Edge e2 = b.addEdge("of", c);
        Edge e3 = a.addEdge("of", d);
        this.sqlgGraph.tx().commit();

        DefaultSqlgTraversal<Vertex, Path> traversal = (DefaultSqlgTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(c)
                .repeat(__.inE("of").inV().simplePath())
                .until(
                        __.or(
                                __.not(__.both("of").simplePath()),
                                __.has("name", "a")
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        Assert.assertEquals(1, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(c) && p.get(1).equals(e2) && p.get(2).equals(b) && p.get(3).equals(e1) && p.get(4).equals(a)));
    }

    @Test
    public void testBothRepeatWithEdgeInUntil() {
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

        Edge e1 = a.addEdge("of", b);
        Edge e2 = b.addEdge("of", c);
        Edge e3 = a.addEdge("of", d);
        this.sqlgGraph.tx().commit();

        DefaultSqlgTraversal<Vertex, Path> traversal = (DefaultSqlgTraversal<Vertex, Path>) this.sqlgGraph.traversal().V(a)
                .repeat(__.bothE("of").otherV().simplePath())
                .until(
                        __.or(
                                __.not(__.both("of").simplePath()),
                                __.has("name", "a")
                        )
                )
                .path();
        Assert.assertEquals(3, traversal.getSteps().size());
        List<Path> paths = traversal.toList();
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(SqlgGraphStep.class, traversal.getSteps().get(0).getClass());
        Assert.assertEquals(PathStep.class, traversal.getSteps().get(1).getClass());
        Assert.assertEquals(2, paths.size());
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 5 && p.get(0).equals(a) && p.get(1).equals(e1) && p.get(2).equals(b) && p.get(3).equals(e2) && p.get(4).equals(c)));
        Assert.assertTrue(paths.stream().anyMatch(p -> p.size() == 3 && p.get(0).equals(a) && p.get(1).equals(e3) && p.get(2).equals(d)));
    }
}
