package org.umlg.sqlg.test.columnRefactor;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.step.SqlgVertexStep;
import org.umlg.sqlg.step.barrier.SqlgLocalStepBarrier;
import org.umlg.sqlg.structure.DefaultSqlgTraversal;
import org.umlg.sqlg.structure.PropertyDefinition;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.EdgeLabel;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;
import java.util.function.Predicate;

/**
 * @author <a href="https://github.com/pietermartin">Pieter Martin</a>
 * Date: 2019/01/13
 */
@SuppressWarnings("Duplicates")
public class TestColumnRefactor extends BaseTest {

    @Test
    public void test() {
        this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A").toList();
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testOptionalLabel() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("A").optional(__.out());
        printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b1));
        Assert.assertTrue(vertices.contains(a2));
    }

    @Test
    public void testRepeatLabel() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("A").repeat(__.out()).emit();
        printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(b1));
    }

    @Test
    public void testOptionalOptionalLabel() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("A").optional(__.out().optional(__.out()));
        printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(a2));
        Assert.assertTrue(vertices.contains(b2));
        Assert.assertTrue(vertices.contains(c1));

        Traversal<Vertex, Path> pathTraversal = this.sqlgGraph.traversal().V().hasLabel("A").optional(__.out().optional(__.out())).path();
        printTraversalForm(pathTraversal);
        List<Path> paths = pathTraversal.toList();
        Assert.assertEquals(3, paths.size());
    }

    @Test
    public void testMapUserSuppliedPK() {
        VertexLabel vertexLabel = this.sqlgGraph.getTopology().getPublicSchema()
                .ensureVertexLabelExist(
                        "Person",
                        new HashMap<>() {{
                            put("uid", PropertyDefinition.of(PropertyType.varChar(100)));
                            put("name1", PropertyDefinition.of(PropertyType.STRING));
                            put("name2", PropertyDefinition.of(PropertyType.STRING));
                            put("name3", PropertyDefinition.of(PropertyType.STRING));
                        }},
                        ListOrderedSet.listOrderedSet(Collections.singletonList("uid"))
                );
        Map<String, Object> map = new HashMap<>();
        map.put("uid", UUID.randomUUID().toString());
        map.put("name1", "p1");
        map.put("name2", "p2");
        map.put("name3", "p3");
        Vertex v1 = this.sqlgGraph.addVertex("Person", map);
        this.sqlgGraph.tx().commit();
        Vertex v2 = this.sqlgGraph.traversal().V().has(T.label, "Person").next();
        Assert.assertEquals(v1, v2);
        Assert.assertEquals("p1", v2.property("name1").value());
        Assert.assertEquals("p2", v2.property("name2").value());
        Assert.assertEquals("p3", v2.property("name3").value());
    }

    @Test
    public void testTrivial() {
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a1");
        this.sqlgGraph.tx().commit();
        Vertex v2 = this.sqlgGraph.traversal().V().has(T.label, "Person").next();
    }

    @Test
    public void testLocalOptionalNested() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        a2.addEdge("ab", b2);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        this.sqlgGraph.tx().commit();

        DefaultSqlgTraversal<Vertex, Path> traversal = (DefaultSqlgTraversal<Vertex, Path>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .local(
                        __.optional(
                                __.out().optional(
                                        __.out()
                                )
                        )
                )
                .path();
        List<Path> paths = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgLocalStepBarrier);
        SqlgLocalStepBarrier<?, ?> localStep = (SqlgLocalStepBarrier) traversal.getSteps().get(1);
        List<SqlgVertexStep> sqlgVertexStepCompileds = TraversalHelper.getStepsOfAssignableClassRecursively(SqlgVertexStep.class, localStep.getLocalChildren().get(0));
        Assert.assertEquals(1, sqlgVertexStepCompileds.size());
        SqlgVertexStep sqlgVertexStepCompiled = sqlgVertexStepCompileds.get(0);
        assertStep(sqlgVertexStepCompiled, false, false, true);
        Assert.assertEquals(3, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 3 && p.get(0).equals(a1) && p.get(1).equals(b1) && p.get(2).equals(c1),
                p -> p.size() == 2 && p.get(0).equals(a2) && p.get(1).equals(b2),
                p -> p.size() == 1 && p.get(0).equals(a3)
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            Assert.assertTrue(path.isPresent());
            Assert.assertTrue(paths.remove(path.get()));
        }
        Assert.assertTrue(paths.isEmpty());
    }

    @Test
    public void testDuplicatePath() {
        VertexLabel person = this.sqlgGraph.getTopology().ensureVertexLabelExist(
                "Person",
                new HashMap<>(){{
                    put("name", PropertyDefinition.of(PropertyType.varChar(100)));
                    put("surname", PropertyDefinition.of(PropertyType.varChar(100)));
                }},
                ListOrderedSet.listOrderedSet(Arrays.asList("name", "surname"))
        );
        @SuppressWarnings("unused")
        EdgeLabel livesAt = person.ensureEdgeLabelExist(
                "loves",
                person,
                new HashMap<>() {{
                    put("country", PropertyDefinition.of(PropertyType.STRING));
                }}
        );
        this.sqlgGraph.tx().commit();

        Vertex person1 = this.sqlgGraph.addVertex(T.label, "Person", "name", "John", "surname", "Smith");
        Vertex person2 = this.sqlgGraph.addVertex(T.label, "Person", "name", "Suzi", "surname", "Lovenot");
        person1.addEdge("loves", person2);
        person2.addEdge("loves", person1);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("Person").out().toList();
        Assert.assertEquals(2, vertices.size());
        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").in().toList();
        Assert.assertEquals(2, vertices.size());

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").outE().inV().toList();
        Assert.assertEquals(2, vertices.size());

        vertices = this.sqlgGraph.traversal().V().hasLabel("Person").outE().outV().toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void dropMultiplePathsToVertices() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
        Vertex c5 = this.sqlgGraph.addVertex(T.label, "C", "name", "c5");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        b1.addEdge("bc", c1);
        b1.addEdge("bc", c2);
        b1.addEdge("bc", c3);
        b2.addEdge("bc", c3);
        b2.addEdge("bc", c4);
        b2.addEdge("bc", c5);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().hasLabel("A")
                .out()
                .out()
                .toList();
        Assert.assertEquals(6, vertices.size());
        Assert.assertTrue(vertices.removeAll(Arrays.asList(c1, c2, c3, c4, c5)));
        Assert.assertEquals(0, vertices.size());

        this.sqlgGraph.traversal().V().hasLabel("A")
                .out()
                .out()
                .drop()
                .iterate();
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(0L, this.sqlgGraph.traversal().V().hasLabel("C").count().next(), 0L);
        Assert.assertEquals(0L, this.sqlgGraph.traversal().E().hasLabel("bc").count().next(), 0L);

    }

}
