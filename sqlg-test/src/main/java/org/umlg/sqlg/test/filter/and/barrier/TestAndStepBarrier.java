package org.umlg.sqlg.test.filter.and.barrier;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.step.barrier.SqlgAndStepBarrier;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.Schema;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2017/10/27
 */
public class TestAndStepBarrier extends BaseTest {

    @Test
    public void shouldFilterEdgeCriterion() {
        loadModern();
        final Traversal<Edge, ?> edgeCriterion = __.or(
                __.has("weight", 1.0d).hasLabel("knows"), // 8
                __.has("weight", 0.4d).hasLabel("created").outV().has("name", "marko"), // 9
                __.has("weight", 1.0d).hasLabel("created") // 10
        );

        GraphTraversalSource g = this.sqlgGraph.traversal();
        final SubgraphStrategy strategy = SubgraphStrategy.build().edges(edgeCriterion).create();
        final GraphTraversalSource sg = g.withStrategies(strategy);

        // all vertices are here
        Assert.assertEquals(6, g.V().count().next().longValue());
        final Traversal t = sg.V();
        t.hasNext();
        printTraversalForm(t);


        Assert.assertEquals(6, sg.V().count().next().longValue());

        // only the given edges are included
        Assert.assertEquals(6, g.E().count().next().longValue());
        Assert.assertEquals(3, sg.E().count().next().longValue());

        Assert.assertEquals(2, g.V(convertToVertexId("marko")).outE("knows").count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("marko")).outE("knows").count().next().longValue());

        // wrapped Traversal<Vertex, Vertex> takes into account the edges it must pass through
        Assert.assertEquals(2, g.V(convertToVertexId("marko")).out("knows").count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("marko")).out("knows").count().next().longValue());
        Assert.assertEquals(2, g.V(convertToVertexId("josh")).out("created").count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).out("created").count().next().longValue());

        // from vertex

        Assert.assertEquals(2, g.V(convertToVertexId("josh")).outE().count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).outE().count().next().longValue());
        Assert.assertEquals(2, g.V(convertToVertexId("josh")).out().count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).out().count().next().longValue());

        Assert.assertEquals(1, g.V(convertToVertexId("josh")).inE().count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).inE().count().next().longValue());
        Assert.assertEquals(1, g.V(convertToVertexId("josh")).in().count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).in().count().next().longValue());

        Assert.assertEquals(3, g.V(convertToVertexId("josh")).bothE().count().next().longValue());
        Assert.assertEquals(2, sg.V(convertToVertexId("josh")).bothE().count().next().longValue());
        Assert.assertEquals(3, g.V(convertToVertexId("josh")).both().count().next().longValue());
        Assert.assertEquals(2, sg.V(convertToVertexId("josh")).both().count().next().longValue());

        // with label

        Assert.assertEquals(2, g.V(convertToVertexId("josh")).outE("created").count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).outE("created").count().next().longValue());
        Assert.assertEquals(2, g.V(convertToVertexId("josh")).out("created").count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).out("created").count().next().longValue());
        Assert.assertEquals(2, g.V(convertToVertexId("josh")).bothE("created").count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).bothE("created").count().next().longValue());
        Assert.assertEquals(2, g.V(convertToVertexId("josh")).both("created").count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).both("created").count().next().longValue());

        Assert.assertEquals(1, g.V(convertToVertexId("josh")).inE("knows").count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).inE("knows").count().next().longValue());
        Assert.assertEquals(1, g.V(convertToVertexId("josh")).in("knows").count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).in("knows").count().next().longValue());
        Assert.assertEquals(1, g.V(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        Assert.assertEquals(1, g.V(convertToVertexId("josh")).both("knows").count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).both("knows").count().next().longValue());

        // with branch factor

        Assert.assertEquals(1, g.V(convertToVertexId("josh")).limit(1).local(__.bothE().limit(1)).count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).limit(1).local(__.bothE().limit(1)).count().next().longValue());
        Assert.assertEquals(1, g.V(convertToVertexId("josh")).limit(1).local(__.bothE().limit(1)).inV().count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).limit(1).local(__.bothE().limit(1)).inV().count().next().longValue());
        Assert.assertEquals(1, g.V(convertToVertexId("josh")).local(__.bothE("knows", "created").limit(1)).count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).local(__.bothE("knows", "created").limit(1)).count().next().longValue());
        Assert.assertEquals(1, g.V(convertToVertexId("josh")).local(__.bothE("knows", "created").limit(1)).inV().count().next().longValue());
        Assert.assertEquals(1, sg.V(convertToVertexId("josh")).local(__.bothE("knows", "created").limit(1)).inV().count().next().longValue());

        // from edge

        Assert.assertEquals(2, g.E(convertToEdgeId(this.sqlgGraph, "marko", "knows", "josh")).bothV().count().next().longValue());
        Assert.assertEquals(2, sg.E(convertToEdgeId(this.sqlgGraph, "marko", "knows", "josh")).bothV().count().next().longValue());

        Assert.assertEquals(3, g.E(convertToEdgeId(this.sqlgGraph, "marko", "knows", "josh")).outV().outE().count().next().longValue());
        Assert.assertEquals(2, sg.E(convertToEdgeId(this.sqlgGraph, "marko", "knows", "josh")).outV().outE().count().next().longValue());
    }

    @Test
    public void g_VX1X_repeatXboth_simplePathX_untilXhasXname_peterX_and_loops_isX3XX_hasXname_peterX_path_byXnameX() {
        loadModern();
        final DefaultGraphTraversal<Vertex, Path> traversal = (DefaultGraphTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V(convertToVertexId("marko"))
                .repeat(
                        __.both().simplePath()
                )
                .until(
                        __.has("name", "peter")
                                .and()
                                .loops().is(3)
                ).has("name", "peter").path().by("name");
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        final Path path = traversal.next();
        Assert.assertEquals(4, path.size());
        Assert.assertEquals("marko", path.get(0));
        Assert.assertEquals("josh", path.get(1));
        Assert.assertEquals("lop", path.get(2));
        Assert.assertEquals("peter", path.get(3));
        Assert.assertFalse(traversal.hasNext());

        List<SqlgAndStepBarrier> sqlgAndStepBarriers = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgAndStepBarrier.class, traversal);
        Assert.assertEquals(1, sqlgAndStepBarriers.size());

    }

    @Test
    public void g_V_asXaX_outXknowsX_and_outXcreatedX_inXcreatedX_asXaX_name() {
        loadModern();
        final DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>)this.sqlgGraph.traversal()
                .V().as("a")
                .out("knows")
                .and()
                .out("created").in("created").as("a")
                .<Vertex>values("name");
        printTraversalForm(traversal);
        checkResults(Arrays.asList(convertToVertex(this.sqlgGraph, "marko")), traversal);
        List<SqlgAndStepBarrier> sqlgAndStepBarriers = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgAndStepBarrier.class, traversal);
        Assert.assertEquals(1, sqlgAndStepBarriers.size());
    }

    @Test
    public void testAndStepBarrier() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        a1.addEdge("abb", b1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        a2.addEdge("abb", b2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B");
        a3.addEdge("abbb", b3);


        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A").and(
                __.out("ab"),
                __.out("abb")
        );
        printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(a1));

        List<SqlgAndStepBarrier> sqlgAndStepBarriers = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgAndStepBarrier.class, traversal);
        Assert.assertEquals(1, sqlgAndStepBarriers.size());
    }

    @Test
    public void testAndStepBarrierWithUserSuppliedID() {
        Schema publicSchema = this.sqlgGraph.getTopology().getPublicSchema();
        VertexLabel aVertexLabel = publicSchema.ensureVertexLabelExist(
                "A",
                new HashMap<>() {{
                    put("uid", PropertyDefinition.of(PropertyType.varChar(100)));
                    put("country", PropertyDefinition.of(PropertyType.varChar(100)));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country")));
        VertexLabel bVertexLabel = publicSchema.ensureVertexLabelExist(
                "B",
                new HashMap<>() {{
                    put("uid", PropertyDefinition.of(PropertyType.varChar(100)));
                    put("country", PropertyDefinition.of(PropertyType.varChar(100)));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country")));
        aVertexLabel.ensureEdgeLabelExist(
                "ab",
                bVertexLabel,
                new HashMap<>() {{
                    put("uid", PropertyDefinition.of(PropertyType.varChar(100)));
                    put("country", PropertyDefinition.of(PropertyType.varChar(100)));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country")));
        aVertexLabel.ensureEdgeLabelExist(
                "abb",
                bVertexLabel,
                new HashMap<>() {{
                    put("uid", PropertyDefinition.of(PropertyType.varChar(100)));
                    put("country", PropertyDefinition.of(PropertyType.varChar(100)));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country")));
        aVertexLabel.ensureEdgeLabelExist(
                "abbb",
                bVertexLabel,
                new HashMap<>() {{
                    put("uid", PropertyDefinition.of(PropertyType.varChar(100)));
                    put("country", PropertyDefinition.of(PropertyType.varChar(100)));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("uid", "country")));
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "country", "SA");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "country", "SA");
        a1.addEdge("ab", b1 , "uid", UUID.randomUUID().toString(), "country", "SA");
        a1.addEdge("abb", b1, "uid", UUID.randomUUID().toString(), "country", "SA");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "country", "SA");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "country", "SA");
        a2.addEdge("abb", b2, "uid", UUID.randomUUID().toString(), "country", "SA");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "country", "SA");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "country", "SA");
        a3.addEdge("abbb", b3, "uid", UUID.randomUUID().toString(), "country", "SA");

        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A").and(
                __.out("ab"),
                __.out("abb")
        );
        printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(a1));

        List<SqlgAndStepBarrier> sqlgAndStepBarriers = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgAndStepBarrier.class, traversal);
        Assert.assertEquals(1, sqlgAndStepBarriers.size());
    }

    @Test
    public void testAndStepBarrierMultiple() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B");
        a1.addEdge("ab", b1);
        a1.addEdge("abb", b1);
        a1.addEdge("abbb", b1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        a2.addEdge("abb", b2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B");
        a3.addEdge("abbb", b3);


        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A").and(
                __.out("ab"),
                __.out("abb"),
                __.out("abbb")
        );
        printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(a1));

        List<SqlgAndStepBarrier> sqlgAndStepBarriers = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgAndStepBarrier.class, traversal);
        Assert.assertEquals(1, sqlgAndStepBarriers.size());
    }

}
