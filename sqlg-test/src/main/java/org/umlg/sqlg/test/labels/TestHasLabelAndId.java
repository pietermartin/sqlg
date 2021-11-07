package org.umlg.sqlg.test.labels;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.sql.parse.ReplacedStep;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.structure.SqlgGraph;
import org.umlg.sqlg.test.BaseTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Date: 2014/07/29
 * Time: 2:21 PM
 */
@SuppressWarnings({"rawtypes", "DuplicatedCode"})
public class TestHasLabelAndId extends BaseTest {

    @Test
    public void test1() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name1", "a");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name2", "a");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();
        System.out.println(this.sqlgGraph.traversal().V().hasLabel("A").count().next());
    }

    @Test
    public void testNeqWithinManyIDs() {
        Vertex a = sqlgGraph.addVertex("A");
        Vertex b1 = sqlgGraph.addVertex("B");
        Vertex b2 = sqlgGraph.addVertex("B");
        Vertex b3 = sqlgGraph.addVertex("B");
        Vertex b4 = sqlgGraph.addVertex("B");
        Vertex b5 = sqlgGraph.addVertex("B");
        Vertex b6 = sqlgGraph.addVertex("B");
        Vertex c = sqlgGraph.addVertex("C");

        a.addEdge("e_a", b1);
        a.addEdge("e_a", b2);
        a.addEdge("e_a", b3);
        a.addEdge("e_a", b4);
        a.addEdge("e_a", b5);
        a.addEdge("e_a", b6);
        a.addEdge("e_a", c);

        //uses tmp table to join on
        GraphTraversal<Vertex, Vertex> t = sqlgGraph.traversal().V(a).out().has(T.id, P.without(b1.id(), b2.id(), b3.id(), b4.id(), b5.id()));
        List<Vertex> vertices = t.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b6));
        Assert.assertTrue(vertices.contains(c));
    }

    @Test
    public void testNeqWithinID() {
        Vertex a = sqlgGraph.addVertex("A");
        Vertex b1 = sqlgGraph.addVertex("B");
        Vertex b2 = sqlgGraph.addVertex("B");
        Vertex c = sqlgGraph.addVertex("C");

        a.addEdge("e_a", b1);
        a.addEdge("e_a", b2);
        a.addEdge("e_a", c);

        GraphTraversal<Vertex, Vertex> t = sqlgGraph.traversal().V(a).out().has(T.id, P.without(b1.id()));
        List<Vertex> vertices = t.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b2));
        Assert.assertTrue(vertices.contains(c));
    }

    @Test
    public void testNeqWithID() {
        Vertex a = sqlgGraph.addVertex("A");
        Vertex b1 = sqlgGraph.addVertex("B");
        Vertex b2 = sqlgGraph.addVertex("B");
        Vertex c = sqlgGraph.addVertex("C");

        a.addEdge("e_a", b1);
        a.addEdge("e_a", b2);
        a.addEdge("e_a", c);

        GraphTraversal<Vertex, Vertex> t = sqlgGraph.traversal().V(a).out().has(T.id, P.neq(b1.id()));
        List<Vertex> vertices = t.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b2));
        Assert.assertTrue(vertices.contains(c));
    }

    @Test
    public void testHasLabelWithIDs() {
        Vertex a = sqlgGraph.addVertex("A");
        Vertex b = sqlgGraph.addVertex("B");

        GraphTraversal<Vertex, Vertex> t = sqlgGraph.traversal().V(a, b).hasLabel("A");

        Assert.assertTrue(t.hasNext());
        Vertex v = t.next();
        Assert.assertEquals(a, v);
        boolean hasNext = t.hasNext();
        if (hasNext) {
            Assert.fail(t.next().toString());
        }
    }

    @Test
    public void testConsecutiveIdCollectionAfterOut() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        a1.addEdge("ab", b4);
        a1.addEdge("ac", c1);
        a1.addEdge("ac", c2);
        a1.addEdge("ac", c3);
        a1.addEdge("ac", c4);
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V(a1)
                .out()
                .hasId(Arrays.asList(b1, c1, b2, c2, b3, c3, b4, c4))
                .hasId(b2, c2, b3, c3).toList();
        Assert.assertEquals(4, vertices.size());
        Assert.assertTrue(
                vertices.contains(b2) &&
                        vertices.contains(c2) &&
                        vertices.contains(b3) &&
                        vertices.contains(c3)
        );

    }

    @Test
    public void testCollectionIdsComplex() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V(Arrays.asList(a1, b1, c1, a2, b2, c2)).toList();
        Assert.assertEquals(6, vertices.size());
        Assert.assertTrue(
                vertices.contains(a1) &&
                        vertices.contains(b1) &&
                        vertices.contains(c1) &&
                        vertices.contains(a2) &&
                        vertices.contains(b2) &&
                        vertices.contains(c2)
        );
    }

    @Test
    public void testCollectionIdsComplexConsecutive() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V(Arrays.asList(a1, b1, c1, a2, b2, c2))
                .hasId(Arrays.asList(a1, b1, c1, a3, b3, c3))
                .toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(a1) && vertices.contains(b1) && vertices.contains(c1));
    }

    //More than 2 parameters for an in clause goes via tmp tables
    @Test
    public void testCollectionIdsComplexConsecutiveUsingTmpTables() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "name", "b2");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "name", "b3");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "name", "b4");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "c1");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C", "name", "c2");
        Vertex c3 = this.sqlgGraph.addVertex(T.label, "C", "name", "c3");
        Vertex c4 = this.sqlgGraph.addVertex(T.label, "C", "name", "c4");
        this.sqlgGraph.tx().commit();
        List<Vertex> vertices = this.sqlgGraph.traversal()
                .V(Arrays.asList(a1, b1, c1, a2, b2, c2, a3, b3, c3, a4, b4, c4))
                .hasId(Arrays.asList(a1, b1, c1, a3, b3, c3, a4, b4, c4))
                .toList();
        Assert.assertEquals(9, vertices.size());
        Assert.assertTrue(
                vertices.contains(a1) && vertices.contains(b1) && vertices.contains(c1) &&
                        vertices.contains(a3) && vertices.contains(b3) && vertices.contains(c3) &&
                        vertices.contains(a4) && vertices.contains(b4) && vertices.contains(c4)
        );
    }

    @Test
    public void testCollectionIds() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasId(a.id()).hasId(b.id());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(0, vertices.size());

        traversal = this.sqlgGraph.traversal().V(a.id()).hasId(b.id());
        vertices = traversal.toList();
        Assert.assertEquals(0, vertices.size());

        traversal = this.sqlgGraph.traversal().V(a.id()).has(T.id, P.within(b.id(), c.id(), d.id()));
        vertices = traversal.toList();
        Assert.assertEquals(0, vertices.size());

        traversal = this.sqlgGraph.traversal().V(a.id()).has(T.id, P.within(a2.id(), b.id(), c.id(), d.id()));
        vertices = traversal.toList();
        Assert.assertEquals(0, vertices.size());

        traversal = this.sqlgGraph.traversal().V(Arrays.asList(a.id(), a2.id(), b.id(), c.id(), d.id()));
        vertices = traversal.toList();
        Assert.assertEquals(5, vertices.size());
    }

    @Test
    public void testConsecutiveHasId() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasId(a.id()).hasId(b.id());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(0, vertices.size());

        traversal = this.sqlgGraph.traversal().V(a.id()).hasId(b.id());
        vertices = traversal.toList();
        Assert.assertEquals(0, vertices.size());

        traversal = this.sqlgGraph.traversal().V(a.id()).has(T.id, P.within(b.id(), c.id(), d.id()));
        vertices = traversal.toList();
        Assert.assertEquals(0, vertices.size());

        traversal = this.sqlgGraph.traversal().V(a.id()).has(T.id, P.within(a2.id(), b.id(), c.id(), d.id()));
        vertices = traversal.toList();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testConsecutiveEqHasLabels() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A").hasLabel("B");
        Assert.assertEquals(0, traversal.toList().size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testConsecutiveEqNeqHasLabels() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").has(T.label, P.neq("A"));
        Assert.assertEquals(0, traversal.toList().size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testConsecutiveEqNeqHasLabels2() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").has(T.label, P.neq("B"));
        Assert.assertEquals(1, traversal.toList().size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testConsecutiveEqWithinHasLabels2() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").has(T.label, P.within("B"));
        Assert.assertEquals(0, traversal.toList().size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testConsecutiveHasLabelWithout() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").has(T.label, P.without("B", "C"));
        Assert.assertEquals(1, traversal.toList().size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testConsecutiveEqHasIdAndLabels() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a).hasLabel("B");
        Assert.assertEquals(0, traversal.toList().size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testConsecutiveEqHasIdAndLabels2() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a, b, c).hasLabel("B");
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(b, vertices.get(0));
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testConsecutiveEqHasIdAndLabels3() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V(a).hasLabel("A").hasId(b.id());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(0, vertices.size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(3, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testConsecutiveEqHasIdAndLabels3HasIdWithin() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("B").has(T.id, P.without(b.id(), b2.id()));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertEquals(b3, vertices.get(0));
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        //without merges its label hasContainer into the previous labelHasContainer
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testConsecutiveEqHasIdAndLabels4() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A", "B", "C").hasId(b.id());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(b, vertices.get(0));
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testConsecutiveEqHasIdAndLabels5() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A", "B", "C").hasId(b.id()).hasId(a.id());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(0, vertices.size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(3, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testConsecutiveEqHasIdAndLabels6() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A", "B", "C").hasId(b.id()).hasId(b.id());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(3, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testConsecutiveEqHasWithinIdAndLabels() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A", "B", "C").has(T.id, P.within(b.id(), b2.id()));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testConsecutiveEqHasWithoutIdAndLabels() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A", "B", "C", "D").has(T.id, P.without(b.id(), b2.id()));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(a) && vertices.contains(c) && vertices.contains(d));
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        //without's labels are merged into the previous labelHasContainers
        //this is because without has 'or' logic rather than 'and'
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testConsecutiveEqHasNeqIdAndLabels() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A", "B", "C", "D").has(T.id, P.neq(b.id()));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(4, vertices.size());
        Assert.assertTrue(vertices.contains(a) && vertices.contains(b2) && vertices.contains(c) && vertices.contains(d));
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(1, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        //without's labels are merged into the previous labelHasContainers
        //this is because without has 'or' logic rather than 'and'
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testOutWithHasLabel() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        a.addEdge("ab", b);
        a.addEdge("ac", c);
        a.addEdge("ad", d);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").out().hasLabel("B");
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(2, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
        replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(1);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testInWithHasLabel() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        b.addEdge("ab", a);
        c.addEdge("ac", a);
        d.addEdge("ad", a);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").in().hasLabel("B");
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(2, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
        replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(1);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testOutWithHasLabelAndHasId() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        a.addEdge("ab", b);
        a.addEdge("ab", b2);
        a.addEdge("ac", c);
        a.addEdge("ad", d);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").out().hasLabel("B").hasId(b.id());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(2, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
        replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(1);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testInWithHasLabelAndHasId() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        b.addEdge("ab", a);
        b2.addEdge("ab", a);
        c.addEdge("ac", a);
        d.addEdge("ad", a);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").in().hasLabel("B").hasId(b.id());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(b, vertices.get(0));
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(2, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
        replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(1);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testOutWithHasLabelAndHasNeqId() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        a.addEdge("ab", b);
        a.addEdge("ab", b2);
        a.addEdge("ac", c);
        a.addEdge("ad", d);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").out().hasLabel("B").has(T.id, P.neq(b.id()));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(b2, vertices.get(0));
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(2, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
        replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(1);
        //neq merges its label with the previous labelHasContainers labels
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testOutWithHasLabelAndHasWithinId() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        a.addEdge("ab", b);
        a.addEdge("ab", b2);
        a.addEdge("ac", c);
        a.addEdge("ad", d);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").out().hasLabel("B", "C", "D").has(T.id, P.within(b.id(), c.id()));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b) && vertices.contains(c));
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(2, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
        replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(1);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testOutWithHasLabelAndHasWithoutId() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        a.addEdge("ab", b);
        a.addEdge("ab", b2);
        a.addEdge("ac", c);
        a.addEdge("ad", d);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").out().hasLabel("B", "C", "D").has(T.id, P.without(b.id(), c.id()));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b2) && vertices.contains(d));
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(2, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
        replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(1);
        //without gets merged into previous labelHasContainer
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testOutWithHasLabelAndHasWithoutNeqId() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        a.addEdge("ab", b);
        a.addEdge("ab", b2);
        a.addEdge("ac", c);
        a.addEdge("ad", d);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").out().hasLabel("B", "C", "D").has(T.id, P.neq(c.id()));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(b) && vertices.contains(b2) && vertices.contains(d));
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(2, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
        replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(1);
        //without gets merged into previous labelHasContainer
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testInWithHasLabelAndHasWithinId2() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        b.addEdge("ab", a);
        b2.addEdge("ab", a);
        c.addEdge("ac", a);
        d.addEdge("ad", a);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").in().hasLabel("B", "C", "D").has(T.id, P.within(b.id(), c.id()));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b) && vertices.contains(c));
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(2, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
        replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(1);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testInWithHasLabelAndHasWithoutId2() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        b.addEdge("ab", a);
        b2.addEdge("ab", a);
        c.addEdge("ac", a);
        d.addEdge("ad", a);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").in().hasLabel("B", "C", "D").has(T.id, P.without(b.id(), c.id()));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b2) && vertices.contains(d));
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(2, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
        replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(1);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testInWithHasLabelAndHasNeqId() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        b.addEdge("ab", a);
        b2.addEdge("ab", a);
        c.addEdge("ac", a);
        d.addEdge("ad", a);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").in().hasLabel("B", "C", "D").has(T.id, P.neq(b.id()));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(b2) && vertices.contains(c) && vertices.contains(d));
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(2, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
        replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(1);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testHasLabelAndIdOnOutEdge() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        Edge ab = a.addEdge("ab", b);
        Edge ac = a.addEdge("ac", c);
        Edge ad = a.addEdge("ad", d);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Edge> traversal = this.sqlgGraph.traversal().V().hasLabel("A").outE().hasLabel("ab", "ac");
        List<Edge> edges = traversal.toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertTrue(edges.contains(ab) && edges.contains(ac));

        traversal = this.sqlgGraph.traversal().V().hasLabel("A").outE().hasLabel("ab").hasLabel("ac");
        edges = traversal.toList();
        Assert.assertEquals(0, edges.size());

        traversal = this.sqlgGraph.traversal().V().hasLabel("A").outE().hasLabel("ab", "ac").hasLabel("ac");
        edges = traversal.toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(ac, edges.get(0));

        traversal = this.sqlgGraph.traversal().V().hasLabel("A").outE().hasLabel("ab", "ac").hasId(ac.id());
        edges = traversal.toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(ac, edges.get(0));

        traversal = this.sqlgGraph.traversal().V().hasLabel("A").outE().hasLabel("ab", "ac").has(T.id, P.within(ab.id(), ac.id()));
        edges = traversal.toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertTrue(edges.contains(ab) && edges.contains(ac));

        traversal = this.sqlgGraph.traversal().V().hasLabel("A").outE().hasLabel("ab", "ac").has(T.id, P.without(ab.id(), ac.id()));
        edges = traversal.toList();
        Assert.assertEquals(0, edges.size());

        traversal = this.sqlgGraph.traversal().V().hasLabel("A").outE().hasLabel("ab", "ac").has(T.id, P.neq(ab.id()));
        edges = traversal.toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(ac, edges.get(0));
    }

    @Test
    public void testHasLabelAndIdOnInEdge() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        Edge ab = b.addEdge("ab", a);
        Edge ac = c.addEdge("ac", a);
        Edge ad = d.addEdge("ad", a);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Vertex, Edge> traversal = this.sqlgGraph.traversal().V().hasLabel("A").inE().hasLabel("ab", "ac");
        List<Edge> edges = traversal.toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertTrue(edges.contains(ab) && edges.contains(ac));

        traversal = this.sqlgGraph.traversal().V().hasLabel("A").inE().hasLabel("ab").hasLabel("ac");
        edges = traversal.toList();
        Assert.assertEquals(0, edges.size());

        traversal = this.sqlgGraph.traversal().V().hasLabel("A").inE().hasLabel("ab", "ac").hasLabel("ac");
        edges = traversal.toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(ac, edges.get(0));

        traversal = this.sqlgGraph.traversal().V().hasLabel("A").inE().hasLabel("ab", "ac").hasId(ac.id());
        edges = traversal.toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(ac, edges.get(0));

        traversal = this.sqlgGraph.traversal().V().hasLabel("A").inE().hasLabel("ab", "ac").has(T.id, P.within(ab.id(), ac.id()));
        edges = traversal.toList();
        Assert.assertEquals(2, edges.size());
        Assert.assertTrue(edges.contains(ab) && edges.contains(ac));

        traversal = this.sqlgGraph.traversal().V().hasLabel("A").inE().hasLabel("ab", "ac").has(T.id, P.without(ab.id(), ac.id()));
        edges = traversal.toList();
        Assert.assertEquals(0, edges.size());

        traversal = this.sqlgGraph.traversal().V().hasLabel("A").inE().hasLabel("ab", "ac").has(T.id, P.neq(ab.id()));
        edges = traversal.toList();
        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(ac, edges.get(0));
    }

    @Test
    public void testOutEWithHasLabelAndId() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex c2 = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        Vertex d2 = this.sqlgGraph.addVertex(T.label, "D");
        Edge ab = b.addEdge("ab", a);
        Edge ab2 = b2.addEdge("ab", a);
        Edge ac = c.addEdge("ac", a);
        Edge ac2 = c2.addEdge("ac", a);
        Edge ad = d.addEdge("ad", a);
        Edge ad2 = d2.addEdge("ad", a);
        this.sqlgGraph.tx().commit();

        GraphTraversal<Edge, Vertex> traversal = this.sqlgGraph.traversal().E().hasLabel("ab").inV();
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertEquals(a, vertices.get(0));
        Assert.assertEquals(a, vertices.get(1));

        traversal = this.sqlgGraph.traversal().E().hasLabel("ab").inV().hasLabel("B");
        vertices = traversal.toList();
        Assert.assertEquals(0, vertices.size());

        traversal = this.sqlgGraph.traversal().E().hasLabel("ab").inV().hasLabel("A");
        vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertEquals(a, vertices.get(0));
        Assert.assertEquals(a, vertices.get(1));

        traversal = this.sqlgGraph.traversal().E().has(T.label, P.within("ab", "ac", "ad")).inV().hasLabel("A", "B");
        vertices = traversal.toList();
        Assert.assertEquals(6, vertices.size());
        Assert.assertTrue(vertices.stream().allMatch(v -> v.equals(a)));

        traversal = this.sqlgGraph.traversal().E().has(T.label, P.within("ab", "ac", "ad")).outV().hasLabel("C", "B");
        vertices = traversal.toList();
        Assert.assertEquals(4, vertices.size());
        Assert.assertTrue(vertices.contains(b) && vertices.contains(b2) && vertices.contains(c) && vertices.contains(c2));

        traversal = this.sqlgGraph.traversal().E().has(T.label, P.within("ab", "ac", "ad")).outV().hasLabel("C", "B").has(T.id, P.eq(b));
        vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(b, vertices.get(0));

        traversal = this.sqlgGraph.traversal().E().has(T.label, P.within("ab", "ac", "ad")).outV().hasLabel("C", "B").has(T.id, P.neq(b));
        vertices = traversal.toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(b2) && vertices.contains(c) && vertices.contains(c2));

        traversal = this.sqlgGraph.traversal().E().has(T.label, P.within("ab", "ac", "ad")).outV().hasLabel("C", "B").has(T.id, P.within(b.id(), b2.id(), c.id()));
        vertices = traversal.toList();
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.contains(b) && vertices.contains(b2) && vertices.contains(c));

        traversal = this.sqlgGraph.traversal().E().has(T.label, P.within("ab", "ac", "ad")).outV().hasLabel("C", "B").has(T.id, P.without(b.id(), b2.id(), c.id()));
        vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(c2, vertices.get(0));
    }


    @Test
    public void testInWithHasLabelAndHasId2() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        b.addEdge("ab", a);
        b2.addEdge("ab", a);
        c.addEdge("ac", a);
        d.addEdge("ad", a);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").in().hasLabel("B").hasId(b.id());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(2, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
        replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(1);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testInWithHasLabelAndHasWithinId() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        b.addEdge("ab", a);
        b2.addEdge("ab", a);
        c.addEdge("ac", a);
        d.addEdge("ad", a);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").in().hasLabel("B").has(T.id, P.within(b.id(), b2.id(), c.id()));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(2, vertices.size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(2, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
        replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(1);
        Assert.assertEquals(2, replacedStep.getLabelHasContainers().size());
    }

    @Test
    public void testInWithHasLabelAndHasWithoutId() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        b.addEdge("ab", a);
        b2.addEdge("ab", a);
        c.addEdge("ac", a);
        d.addEdge("ad", a);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").in().hasLabel("B").has(T.id, P.without(b.id(), c.id()));
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(b2, vertices.get(0));
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        Assert.assertEquals(2, sqlgGraphStep.getReplacedSteps().size());
        ReplacedStep replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(0);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
        replacedStep = (ReplacedStep) sqlgGraphStep.getReplacedSteps().get(1);
        Assert.assertEquals(1, replacedStep.getLabelHasContainers().size());
    }


    @Test
    public void testGraphStepHasLabelWithinCollection() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A");
        Vertex b = this.sqlgGraph.addVertex(T.label, "B");
        Vertex c = this.sqlgGraph.addVertex(T.label, "C");
        Vertex d = this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("A", "B");
        printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.toList().size());
        traversal = this.sqlgGraph.traversal().V().hasLabel(P.within("A", "B"));
        Assert.assertEquals(2, traversal.toList().size());
        traversal = this.sqlgGraph.traversal().V().hasLabel(P.within(new HashSet<>(Arrays.asList("A", "B"))));
        Assert.assertEquals(2, traversal.toList().size());

        Assert.assertEquals(0, this.sqlgGraph.traversal().V(a, b).hasLabel(P.within("C")).toList().size());
        List<Vertex> vertices = this.sqlgGraph.traversal().V(a, b).hasLabel(P.within(new HashSet<>(Arrays.asList("A", "B", "D")))).toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testGraphStepHasLabelWithin() {
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "B");
        this.sqlgGraph.addVertex(T.label, "C");
        this.sqlgGraph.addVertex(T.label, "D");
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal().V().hasLabel("A", "B");
        printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.toList().size());
    }

    @Test
    public void testVertexStepWithLabelWithinOnOut() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "x");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "x");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "x");
        a1.addEdge("ab", b1);
        a1.addEdge("ac", c1);
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal()
                .V()
                .where(__.has("name", "x"))
                .out().hasLabel("B", "D");
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.toList().size());
    }

    @Test
    public void testVertexStepWithLabelWithinOnIn() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "x");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "x");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "x");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal()
                .V()
                .where(__.has("name", "x"))
                .in().hasLabel("B", "D");
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.toList().size());
    }

    @Test
    public void testVertexStepWithLabelWithinOnEdgeOut() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "x");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "x");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "x");
        a1.addEdge("ab", b1);
        a1.addEdge("ac", c1);
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Edge> traversal = this.sqlgGraph.traversal()
                .V()
                .where(__.has("name", "x"))
                .outE().hasLabel("ab", "ce");
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.toList().size());
    }

    @Test
    public void testVertexStepWithLabelWithinOnEdgeIn() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "x");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "x");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "x");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Edge> traversal = this.sqlgGraph.traversal()
                .V()
                .where(__.has("name", "x"))
                .inE().hasLabel("ab", "ce");
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.toList().size());
    }

    @Test
    public void testVertexStepWithLabelWithinOnOtherVertexStepVertexStepOut() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "x");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "x");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "x");
        a1.addEdge("ab", b1);
        a1.addEdge("ac", c1);
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal()
                .V()
                .where(__.has("name", "x"))
                .outE().otherV().hasLabel("B", "D");
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.toList().size());
    }

    @Test
    public void testVertexStepWithLabelWithinOnOtherVertexStepIn() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "x");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "x");
        Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "name", "x");
        a1.addEdge("ab", b1);
        b1.addEdge("bc", c1);
        this.sqlgGraph.tx().commit();
        GraphTraversal<Vertex, Vertex> traversal = this.sqlgGraph.traversal()
                .V()
                .where(__.has("name", "x"))
                .inE().otherV().hasLabel("B", "D");
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.toList().size());
    }

    @Test
    public void testHasLabelWithin() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1");
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "name", "b1");
        a1.addEdge("ab", b1);
        this.sqlgGraph.tx().commit();

        GraphTraversal traversal = this.sqlgGraph.traversal().V(a1).out().hasLabel("C", "B").in();
        printTraversalForm(traversal);
        Assert.assertEquals(1, traversal.toList().size());

    }

    @Test
    public void testHasLabelWithWithinPredicate() {
        Vertex vEPerson = this.sqlgGraph.addVertex(T.label, "EnterprisePerson", "_uniqueId", "1");
        Vertex vEProvider = this.sqlgGraph.addVertex(T.label, "EnterpriseProvider", "_uniqueId", "2");
        Vertex vSPerson = this.sqlgGraph.addVertex(T.label, "SystemPerson", "_uniqueId", "3");
        Vertex vSProvider = this.sqlgGraph.addVertex(T.label, "SystemProvider", "_uniqueId", "4");
        Edge e1 = vSPerson.addEdge("euid", vEPerson);
        Edge e2 = vSProvider.addEdge("euid", vEProvider);
        Edge e3 = vSProvider.addEdge("primary", vSPerson);
        this.sqlgGraph.tx().commit();

        DefaultGraphTraversal<Vertex, Vertex> traversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V()
                .hasLabel("EnterprisePerson")
                .has("_uniqueId", "1")
                .in("euid")
                .bothE("primary")
                .otherV()
                .hasLabel("SystemPerson", "SystemProvider")
                .out("euid");
        printTraversalForm(traversal);
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(vEProvider, vertices.get(0));
    }

    @Test
    public void testConsecutiveHasLabels() {

        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(4, this.sqlgGraph.traversal().V().hasLabel("A").hasLabel("A").toList().size());

    }

    @Test
    public void testHasCompareEq() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        this.sqlgGraph.addVertex(T.label, "A", "name", "b");
        this.sqlgGraph.tx().commit();
        DefaultGraphTraversal<Vertex, Vertex> graphTraversal = (DefaultGraphTraversal<Vertex, Vertex>) this.sqlgGraph.traversal().V().hasLabel("A").has("name", "a");
        Assert.assertEquals(2, graphTraversal.getSteps().size());
        List<Vertex> vertices = graphTraversal.toList();
        Assert.assertEquals(1, graphTraversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(a1, vertices.get(0));
    }

    @Test
    public void testHasCompareBetween() throws InterruptedException {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", 1);
        this.sqlgGraph.addVertex(T.label, "A", "name", 2);
        this.sqlgGraph.tx().commit();
        testCompareBetween_assert(this.sqlgGraph, a1);
        if (this.sqlgGraph1 != null) {
            Thread.sleep(1000);
            testCompareBetween_assert(this.sqlgGraph1, a1);
        }
    }

    private void testCompareBetween_assert(SqlgGraph sqlgGraph, Vertex a1) {
        DefaultGraphTraversal<Vertex, Vertex> graphTraversal = (DefaultGraphTraversal<Vertex, Vertex>) sqlgGraph.traversal().V().hasLabel("A").has("name", P.between(1, 2));
        Assert.assertEquals(2, graphTraversal.getSteps().size());
        List<Vertex> vertices = graphTraversal.toList();
        Assert.assertEquals(1, graphTraversal.getSteps().size());
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(a1, vertices.get(0));
    }

    @Test
    public void testHasLabel() {
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();

        Assert.assertEquals(8, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
    }

    @Test
    public void testNonExistingLabel() {
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.addVertex(T.label, "Person");
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
        Assert.assertEquals(0, this.sqlgGraph.traversal().V().has(T.label, "Animal").count().next(), 0);
    }

    @Test
    public void testInLabels() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "Person", "name", "a");
        Vertex b = this.sqlgGraph.addVertex(T.label, "Person", "name", "b");
        Vertex c = this.sqlgGraph.addVertex(T.label, "Person", "name", "c");
        Vertex d = this.sqlgGraph.addVertex(T.label, "Person", "name", "d");
        a.addEdge("knows", b);
        a.addEdge("created", b);
        a.addEdge("knows", c);
        a.addEdge("created", d);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(4, this.sqlgGraph.traversal().V().has(T.label, "Person").count().next(), 0);
        Assert.assertEquals(2, this.sqlgGraph.traversal().E().has(T.label, P.within(Collections.singletonList("knows"))).count().next(), 0);
        Assert.assertEquals(2, this.sqlgGraph.traversal().E().has(T.label, P.within(Collections.singletonList("created"))).count().next(), 0);
        Assert.assertEquals(4, this.sqlgGraph.traversal().E().has(T.label, P.within(Arrays.asList("knows", "created"))).count().next(), 0);
    }

    @Test
    public void g_V_outE_hasLabel_inV() {
        loadModern();
        List<Vertex> vertices = gt.V().outE().hasLabel("created").inV().toList();
        Assert.assertEquals(4, vertices.size());
        vertices = gt.V().outE().hasLabel("knows").inV().toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testEdgeHasLabel() {
        Vertex person1 = this.sqlgGraph.addVertex(T.label, "schema1.person");
        Vertex person2 = this.sqlgGraph.addVertex(T.label, "schema1.person");
        Vertex address1 = this.sqlgGraph.addVertex(T.label, "schema2.address");
        Vertex address2 = this.sqlgGraph.addVertex(T.label, "schema2.address");
        person1.addEdge("address", address1);
        person2.addEdge("address", address2);
        this.sqlgGraph.tx().commit();
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().outE().otherV().count().next().intValue());
        Assert.assertEquals(2, this.sqlgGraph.traversal().V().inE().otherV().count().next().intValue());
    }

    @Test
    public void testEdgeNotHasLabel() {
        loadModern();
        List<Edge> edges = gt.V().outE().not(__.hasLabel("created").inV()).toList();
        Assert.assertEquals(2, edges.size());
    }

}
