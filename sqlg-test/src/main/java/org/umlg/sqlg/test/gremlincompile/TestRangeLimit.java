package org.umlg.sqlg.test.gremlincompile;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.step.SqlgElementMapStep;
import org.umlg.sqlg.step.SqlgGraphStep;
import org.umlg.sqlg.step.SqlgPropertiesStep;
import org.umlg.sqlg.structure.DefaultSqlgTraversal;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;


/**
 * Test range and limit, they should be implemented as the SQL level
 *
 * @author jpmoresmau
 */
public class TestRangeLimit extends BaseTest {

    /**
     * ensure once we've built the traversal, it contains a RangeGlobalStep
     *
     * @param g
     */
    private void ensureRangeGlobal(GraphTraversal<?, ?> g) {
        DefaultSqlgTraversal<?, ?> dgt = (DefaultSqlgTraversal<?, ?>) g;
        boolean found = false;
        for (Step<?, ?> s : dgt.getSteps()) {
            found |= (s instanceof RangeGlobalStep<?>);
        }
        Assert.assertTrue(found);
    }


    /**
     * once we've run the traversal, it shouldn't contain the RangeGlobalStep,
     * since it was changed into a Range on the ReplacedStep
     *
     * @param g
     */
    private void ensureNoRangeGlobal(GraphTraversal<?, ?> g) {
        DefaultSqlgTraversal<?, ?> dgt = (DefaultSqlgTraversal<?, ?>) g;
        for (Step<?, ?> s : dgt.getSteps()) {
            Assert.assertFalse(s instanceof RangeGlobalStep<?>);
        }
    }

    @Test
    public void testConsecutiveLimits() {
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.addVertex(T.label, "A");
        this.sqlgGraph.tx().commit();

        List<Vertex> vertices = this.sqlgGraph.traversal().V().limit(1).limit(2).toList();
        Assert.assertEquals(1, vertices.size());

        vertices = this.sqlgGraph.traversal().V().limit(3).limit(2).toList();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testRange() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a");
        for (int i = 0; i < 10; i++) {
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b" + i);
            a.addEdge("ab", b);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .out()
                .range(5, 6);
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        assertStep(sqlgGraphStep, true, false, false, true, true);
        Assert.assertEquals(1, vertices.size());
    }

    @Test
    public void testStepsAfterRangeNotOptimized() {
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "d");
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "c");
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "b");
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a");

        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "surname", "b");
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "surname", "b");
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "surname", "b");
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "surname", "b");
        a1.addEdge("ab", b1);
        a2.addEdge("ab", b2);
        a3.addEdge("ab", b3);
        a4.addEdge("ab", b4);
        this.sqlgGraph.tx().commit();

        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A").order().by("name").limit(2)
                .out("ab");
        Assert.assertEquals(5, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();

        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        assertStep(sqlgGraphStep, true, false, false, false, true);

        Assert.assertEquals(2, vertices.size());
        Assert.assertTrue(vertices.contains(b3));
        Assert.assertTrue(vertices.contains(b4));
    }


    @Test
    public void testLimitAfterNonOptimizedStep() {
        for (int i = 0; i < 100; i++) {
            Vertex column = this.sqlgGraph.addVertex(T.label, "BigData.Column");
            Vertex tag = this.sqlgGraph.addVertex(T.label, "BigData.Tag", "name", "NonAnonymized");
            tag.addEdge("tag", column);
        }
        for (int i = 0; i < 100; i++) {
            Vertex column = this.sqlgGraph.addVertex(T.label, "BigData.Column");
            Vertex tag = this.sqlgGraph.addVertex(T.label, "BigData.Tag", "name", "Anonymized");
            tag.addEdge("tag", column);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("BigData.Column")
                .where(
                        __.in("tag").hasLabel("BigData.Tag").has("name", "Anonymized")
                )
                .limit(3);
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        assertStep(sqlgGraphStep, true, false, false, true, true);

        Assert.assertEquals(3, vertices.size());
    }

    @Test
    public void testSkipAfterNonOptimizedStep() {
        for (int i = 0; i < 100; i++) {
            Vertex column = this.sqlgGraph.addVertex(T.label, "BigData.Column");
            Vertex tag = this.sqlgGraph.addVertex(T.label, "BigData.Tag", "name", "NonAnonymized");
            tag.addEdge("tag", column);
        }
        for (int i = 0; i < 100; i++) {
            Vertex column = this.sqlgGraph.addVertex(T.label, "BigData.Column");
            Vertex tag = this.sqlgGraph.addVertex(T.label, "BigData.Tag", "name", "Anonymized");
            tag.addEdge("tag", column);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("BigData.Column")
                .where(
                        __.in("tag").hasLabel("BigData.Tag").has("name", "Anonymized")
                )
                .skip(2);
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(3, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        assertStep(sqlgGraphStep, true, false, false, true, true);

        Assert.assertEquals(98, vertices.size());
    }

    @Test
    public void testVWithLimit() {
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i);
            this.sqlgGraph.addVertex(T.label, "B", "name", "a" + i);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V()
                .range(1, 2);
        ensureRangeGlobal(traversal);
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertEquals(1, traversal.toList().size());
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        assertStep(sqlgGraphStep, true, false, true, true, false);
    }

    @Test
    public void testRangeOnVertexLabels() {
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Object> g = (DefaultSqlgTraversal<Vertex, Object>)this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .order().by("name")
                .range(1, 4).values("name");
        Assert.assertEquals(5, g.getSteps().size());
        ensureRangeGlobal(g);
        int cnt = 0;
        Set<String> names = new HashSet<>();
        String previous = null;
        if (g.hasNext()) {
            Assert.assertEquals(2, g.getSteps().size());
            Assert.assertTrue(g.getSteps().get(0) instanceof SqlgGraphStep);
            SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) g.getSteps().get(0);
            assertStep(sqlgGraphStep, true, false, false, false, true);
        }
        while (g.hasNext()) {
            String n = (String) g.next();
            names.add(n);
            if (previous != null) {
                Assert.assertTrue(previous.compareTo(n) < 0);
            }
            previous = n;
            cnt++;
        }
        ensureNoRangeGlobal(g);
        Assert.assertEquals(3, cnt);
        Assert.assertEquals(names.toString(), 3, names.size());
        Assert.assertTrue(names.toString(), names.contains("a1"));
        Assert.assertTrue(names.toString(), names.contains("a10"));
        Assert.assertTrue(names.toString(), names.contains("a11"));

    }

    @Test
    public void testRangeOnVertexLabelsCriteria() {
        for (int i = 0; i < 100; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i, "prop0", "value");
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Object> g = (DefaultSqlgTraversal<Vertex, Object>)this.sqlgGraph.traversal()
                .V().hasLabel("A").has("prop0", "value")
                .order().by("name")
                .range(1, 4)
                .values("name");
        Assert.assertEquals(5, g.getSteps().size());
        ensureRangeGlobal(g);
        int cnt = 0;
        Set<String> names = new HashSet<>();
        String previous = null;
        if (g.hasNext()) {
            Assert.assertEquals(2, g.getSteps().size());
            Assert.assertTrue(g.getSteps().get(0) instanceof SqlgGraphStep);
            SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) g.getSteps().get(0);
            assertStep(sqlgGraphStep, true, false, false, false, true);
        }
        while (g.hasNext()) {
            String n = (String) g.next();
            names.add(n);
            if (previous != null) {
                Assert.assertTrue(previous.compareTo(n) < 0);
            }
            previous = n;
            cnt++;
        }
        ensureNoRangeGlobal(g);
        Assert.assertEquals(3, cnt);
        Assert.assertEquals(names.toString(), 3, names.size());
        Assert.assertTrue(names.toString(), names.contains("a1"));
        Assert.assertTrue(names.toString(), names.contains("a10"));
        Assert.assertTrue(names.toString(), names.contains("a11"));

    }

    @Test
    public void testRangeOnVertexLabelsNoOrder() {
        for (int i = 0; i < 20; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Object> g = (DefaultSqlgTraversal<Vertex, Object>)this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .range(1, 4)
                .values("name");
        Assert.assertEquals(4, g.getSteps().size());
        ensureRangeGlobal(g);
        int cnt = 0;
        Set<String> names = new HashSet<>();
        if (g.hasNext()) {
            Assert.assertEquals(2, g.getSteps().size());
            Assert.assertTrue(g.getSteps().get(0) instanceof SqlgGraphStep);
            SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) g.getSteps().get(0);
            assertStep(sqlgGraphStep, true, false, false, true, true);
        }
        while (g.hasNext()) {
            String n = (String) g.next();
            names.add(n);
            cnt++;
        }
        ensureNoRangeGlobal(g);
        Assert.assertEquals(3, cnt);
        Assert.assertEquals(names.toString(), 3, names.size());
    }

    @Test
    public void testLimitOnVertexLabels() {
        for (int i = 0; i < 20; i++) {
            this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Object> g = (DefaultSqlgTraversal<Vertex, Object>)this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .order().by("name")
                .limit(3)
                .values("name");
        Assert.assertEquals(5, g.getSteps().size());
        ensureRangeGlobal(g);
        int cnt = 0;
        Set<String> names = new HashSet<>();
        String previous = null;
        if (g.hasNext()) {
            Assert.assertEquals(2, g.getSteps().size());
            Assert.assertTrue(g.getSteps().get(0) instanceof SqlgGraphStep);
            SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) g.getSteps().get(0);
            assertStep(sqlgGraphStep, true, false, false, false, true);
        }
        while (g.hasNext()) {
            String n = (String) g.next();
            names.add(n);
            if (previous != null) {
                Assert.assertTrue(previous.compareTo(n) < 0);
            }
            previous = n;
            cnt++;
        }
        ensureNoRangeGlobal(g);
        Assert.assertEquals(3, cnt);
        Assert.assertEquals(names.toString(), 3, names.size());
        Assert.assertTrue(names.toString(), names.contains("a1"));
        Assert.assertTrue(names.toString(), names.contains("a10"));
        Assert.assertTrue(names.toString(), names.contains("a0"));
    }

    @Test
    public void testRangeOnEdgeLabels() {
        for (int i = 0; i < 20; i++) {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i);
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b" + i);
            a.addEdge("E", b, "name", "e" + i);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Edge, Object> g = (DefaultSqlgTraversal<Edge, Object>)this.sqlgGraph.traversal()
                .E().hasLabel("E")
                .order().by("name")
                .range(1, 4)
                .values("name");
        Assert.assertEquals(5, g.getSteps().size());
        ensureRangeGlobal(g);
        int cnt = 0;
        Set<String> names = new HashSet<>();
        String previous = null;
        if (g.hasNext()) {
            Assert.assertEquals(2, g.getSteps().size());
            Assert.assertTrue(g.getSteps().get(0) instanceof SqlgGraphStep);
            SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) g.getSteps().get(0);
            assertStep(sqlgGraphStep, true, false, false, false, true);
        }
        while (g.hasNext()) {
            String n = (String) g.next();
            names.add(n);
            if (previous != null) {
                Assert.assertTrue(previous.compareTo(n) < 0);
            }
            previous = n;
            cnt++;
        }
        ensureNoRangeGlobal(g);
        Assert.assertEquals(3, cnt);
        Assert.assertEquals(names.toString(), 3, names.size());
        Assert.assertTrue(names.toString(), names.contains("e1"));
        Assert.assertTrue(names.toString(), names.contains("e10"));
        Assert.assertTrue(names.toString(), names.contains("e11"));
    }

    @Test
    public void testRangeOnMultipleLabelsOrdered() {
        Vertex c = this.sqlgGraph.addVertex(T.label, "C", "name", "c" + 12);
        for (int i = 0; i < 20; i++) {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i);
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b" + i);
            a.addEdge("E", b, "name", "e" + i);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Object> g = (DefaultSqlgTraversal<Vertex, Object>)this.sqlgGraph.traversal()
                .V().hasLabel("A", "B")
                .order().by("name")
                .range(1, 4)
                .values("name");
        Assert.assertEquals(5, g.getSteps().size());
        ensureRangeGlobal(g);
        int cnt = 0;
        Set<String> names = new HashSet<>();
        String previous = null;
        if (g.hasNext()) {
            Assert.assertEquals(2, g.getSteps().size());
            Assert.assertTrue(g.getSteps().get(0) instanceof SqlgGraphStep);
            SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) g.getSteps().get(0);
            assertStep(sqlgGraphStep, true, true, true, true, false);
        }
        while (g.hasNext()) {
            String n = (String) g.next();
            names.add(n);
            if (previous != null) {
                Assert.assertTrue(previous.compareTo(n) < 0);
            }
            previous = n;
            cnt++;
        }
        // order by on multiple labels is not done in SQL, so the range isn't
//        ensureRangeGlobal(g);
        Assert.assertEquals(3, cnt);
        Assert.assertEquals(names.toString(), 3, names.size());
        Assert.assertTrue(names.toString(), names.contains("a1"));
        Assert.assertTrue(names.toString(), names.contains("a10"));
        Assert.assertTrue(names.toString(), names.contains("a11"));

    }

    @Test
    public void testRangeOnMultipleLabelsOrderedWithSkip() {
        Vertex c = this.sqlgGraph.addVertex(T.label, "C", "name", "c" + 12);
        for (int i = 0; i < 20; i++) {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i);
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b" + i);
            a.addEdge("E", b, "name", "e" + i);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Object> g = (DefaultSqlgTraversal<Vertex, Object>)this.sqlgGraph.traversal()
                .V().hasLabel("A", "B")
                .order().by("name")
                .skip(2)
                .values("name");
        Assert.assertEquals(5, g.getSteps().size());
        ensureRangeGlobal(g);
        int cnt = 0;
        Set<String> names = new HashSet<>();
        String previous = null;
        if (g.hasNext()) {
            Assert.assertEquals(2, g.getSteps().size());
            Assert.assertTrue(g.getSteps().get(0) instanceof SqlgGraphStep);
            SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) g.getSteps().get(0);
            assertStep(sqlgGraphStep, true, true, true, true, false);
        }
        while (g.hasNext()) {
            String n = (String) g.next();
            names.add(n);
            if (previous != null) {
                Assert.assertTrue(previous.compareTo(n) < 0);
            }
            previous = n;
            cnt++;
        }
        // order by on multiple labels is not done in SQL, so the range isn't
//        ensureRangeGlobal(g);
        Assert.assertEquals(38, cnt);
        Assert.assertEquals(names.toString(), 38, names.size());
        Assert.assertTrue(names.toString(), !names.contains("a0"));
        Assert.assertTrue(names.toString(), !names.contains("a1"));

    }

    @Test
    public void testRangeOnMultipleLabelsOffset() {
        for (int i = 0; i < 20; i++) {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i);
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b" + i);
            a.addEdge("E", b, "name", "e" + i);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Object> g = (DefaultSqlgTraversal<Vertex, Object>) this.sqlgGraph.traversal()
                .V().hasLabel("A", "B")
                .range(1, 4)
                .values("name");
        Assert.assertEquals(4, g.getSteps().size());
        ensureRangeGlobal(g);
        int cnt = 0;
        Set<String> names = new HashSet<>();
        if (g.hasNext()) {
            Assert.assertEquals(2, g.getSteps().size());
            Assert.assertTrue(g.getSteps().get(0) instanceof SqlgGraphStep);
            SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) g.getSteps().get(0);
            assertStep(sqlgGraphStep, true, false, true, true, false);
        }
        while (g.hasNext()) {
            String n = (String) g.next();
            names.add(n);
            cnt++;
        }
        // cannot have offset on different labels
//        ensureRangeGlobal(g);
        Assert.assertEquals(3, cnt);
        Assert.assertEquals(names.toString(), 3, names.size());
    }

    @Test
    public void testRangeOnMultipleLabels() {
        for (int i = 0; i < 20; i++) {
            Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a" + i);
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b" + i);
            a.addEdge("E", b, "name", "e" + i);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Object> g = (DefaultSqlgTraversal<Vertex, Object>)this.sqlgGraph.traversal()
                .V().hasLabel("A", "B")
                .limit(4)
                .values("name");
        Assert.assertEquals(4, g.getSteps().size());
        ensureRangeGlobal(g);
        int cnt = 0;
        Set<String> names = new HashSet<>();
        if (g.hasNext()) {
            Assert.assertEquals(2, g.getSteps().size());
            Assert.assertTrue(g.getSteps().get(0) instanceof SqlgGraphStep);
            SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) g.getSteps().get(0);
            //TODO this really should execute limit on the db and finally in the step.
            //That way less results are returned from the db
            assertStep(sqlgGraphStep, true, false, true, true, false);
        }
        while (g.hasNext()) {
            String n = (String) g.next();
            names.add(n);
            cnt++;
        }
        // we still have to cut the union result
//        ensureRangeGlobal(g);
        Assert.assertEquals(4, cnt);
        Assert.assertEquals(names.toString(), 4, names.size());

    }

    @Test
    public void testRangeOnEdgesOutput() {
        Vertex a = this.sqlgGraph.addVertex(T.label, "A", "name", "a0");

        for (int i = 0; i < 20; i++) {
            Vertex b = this.sqlgGraph.addVertex(T.label, "B", "name", "b" + i);
            a.addEdge("E", b, "name", "e" + i);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Object> g = (DefaultSqlgTraversal<Vertex, Object>)this.sqlgGraph.traversal()
                .V(a)
                .out("E")
                .order().by("name")
                .range(1, 4)
                .values("name");
        Assert.assertEquals(5, g.getSteps().size());
        ensureRangeGlobal(g);
        int cnt = 0;
        Set<String> names = new HashSet<>();
        String previous = null;
        if (g.hasNext()) {
            Assert.assertEquals(2, g.getSteps().size());
            Assert.assertTrue(g.getSteps().get(0) instanceof SqlgGraphStep);
            SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) g.getSteps().get(0);
            assertStep(sqlgGraphStep, true, false, false, false, true);
        }
        while (g.hasNext()) {
            String n = (String) g.next();
            names.add(n);
            if (previous != null) {
                Assert.assertTrue(previous.compareTo(n) < 0);
            }
            previous = n;
            cnt++;
        }
        ensureNoRangeGlobal(g);
        Assert.assertEquals(3, cnt);
        Assert.assertEquals(names.toString(), 3, names.size());
        Assert.assertTrue(names.toString(), names.contains("b1"));
        Assert.assertTrue(names.toString(), names.contains("b10"));
        Assert.assertTrue(names.toString(), names.contains("b11"));

    }

    @Test
    public void testRangeOut() {
        for (int i = 0; i < 100; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "age", i);
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "age", i);
            Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "age", i);
            a1.addEdge("ab", b1);
            b1.addEdge("bc", c1);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .out()
                .out()
                .order().by("age")
                .range(10, 20);
        Assert.assertEquals(6, traversal.getSteps().size());
        List<Vertex> vertexList = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        assertStep(sqlgGraphStep, true, false, false, false, true);
        Assert.assertEquals(10, vertexList.size());
        for (Vertex v : vertexList) {
            Assert.assertEquals("C", v.label());
            int i = (Integer) v.property("age").value();
            Assert.assertTrue(i >= 10 && i < 20);
        }
    }

    @Test
    public void testRangeRepeatOut() {
        for (int i = 0; i < 100; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "age", i);
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "age", i);
            Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "age", i);
            a1.addEdge("ab", b1);
            b1.addEdge("bc", c1);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Vertex> g = (DefaultSqlgTraversal<Vertex, Vertex>)this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .repeat(
                        __.out()
                ).times(2)
                .order().by("age")
                .range(10, 20);
        Assert.assertEquals(5, g.getSteps().size());
        ensureRangeGlobal(g);
        List<Vertex> vertexList = g.toList();
        Assert.assertEquals(1, g.getSteps().size());
        Assert.assertTrue(g.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) g.getSteps().get(0);
        assertStep(sqlgGraphStep, true, false, false, false, true);
        ensureNoRangeGlobal(g);
        Assert.assertEquals(10, vertexList.size());
        for (Vertex v : vertexList) {
            Assert.assertEquals("C", v.label());
            int i = (Integer) v.property("age").value();
            Assert.assertTrue(i >= 10 && i < 20);
        }
    }

    @Test
    public void testRangeBoth() {
        for (int i = 0; i < 100; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "age", i);
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "age", i);
            Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "age", i);
            a1.addEdge("ab", b1);
            b1.addEdge("bc", c1);
        }
        this.sqlgGraph.tx().commit();
        DefaultSqlgTraversal<Vertex, Vertex> g = (DefaultSqlgTraversal<Vertex, Vertex>)this.sqlgGraph.traversal()
                .V().hasLabel("B")
                .both()
                .order().by("age")
                .range(10, 20);
        Assert.assertEquals(5, g.getSteps().size());
        ensureRangeGlobal(g);
        List<Vertex> vertexList = g.toList();
        Assert.assertEquals(1, g.getSteps().size());
        Assert.assertTrue(g.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) g.getSteps().get(0);
        assertStep(sqlgGraphStep, true, true, true, true, false);

        // cannot be done in SQL
//        ensureRangeGlobal(g);
        Assert.assertEquals(10, vertexList.size());
        for (Vertex v : vertexList) {
            Assert.assertTrue(v.label().equals("A") || v.label().equals("C"));
            int i = (Integer) v.property("age").value();
            Assert.assertTrue(String.valueOf(i), i >= 5 && i < 10);
        }
    }

    @Test
    public void testRangeWithNoOrder() {
        for (int i = 0; i < 100; i++) {
            Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "age", i);
            Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "age", i);
            Vertex c1 = this.sqlgGraph.addVertex(T.label, "C", "age", i);
            a1.addEdge("ab", b1);
            b1.addEdge("bc", c1);
        }
        this.sqlgGraph.tx().commit();

        DefaultSqlgTraversal<Vertex, Vertex> traversal = (DefaultSqlgTraversal<Vertex, Vertex>) this.sqlgGraph.traversal()
                .V().hasLabel("B")
                .both()
                .limit(10);
        Assert.assertEquals(4, traversal.getSteps().size());
        List<Vertex> vertices = traversal.toList();
        Assert.assertEquals(1, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        SqlgGraphStep sqlgGraphStep = (SqlgGraphStep) traversal.getSteps().get(0);
        assertStep(sqlgGraphStep, true, false, true, true, false);
        Assert.assertEquals(10, vertices.size());
    }

    @Test
    public void g_V_hasLabelXpersonX_order_byXageX_skipX1X_valuesXnameX() {
        loadModern();

        final Traversal<Vertex, String> traversal =  this.sqlgGraph.traversal()
                .V().hasLabel("person")
                .order().by("age").skip(1).values("name");
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        Assert.assertEquals(Arrays.asList("marko", "josh", "peter"), traversal.toList());
    }

    @Test
    public void testRangeAfterPropertiesStep() {
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "name", "a", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "b", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "c", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "d", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "e", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "f", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "g", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "h", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "i", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "j", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "k", "name1", "2");
        this.sqlgGraph.tx().commit();

        DefaultSqlgTraversal<Vertex, String> traversal  = (DefaultSqlgTraversal<Vertex, String>)this.sqlgGraph.traversal().V().hasLabel("A")
                .<String>values("name")
                .limit(10);
        printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep<?>);
        List<String> values = traversal.toList();
        Assert.assertEquals(10, values.size());

        traversal  = (DefaultSqlgTraversal<Vertex, String>)this.sqlgGraph.traversal().V().hasLabel("A")
                .order().by("name")
                .<String>values("name")
                .limit(10);
        printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgPropertiesStep<?>);
        values = traversal.toList();
        Assert.assertEquals(10, values.size());
        Assert.assertEquals("j", values.get(9));
    }

    @Test
    public void testRangeAfterElementMapStep() {
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "A", "name", "a", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "b", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "c", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "d", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "e", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "f", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "g", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "h", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "i", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "j", "name1", "2");
        this.sqlgGraph.addVertex(T.label, "A", "name", "k", "name1", "2");
        this.sqlgGraph.tx().commit();

        List<String> columns = List.of("name");
        DefaultSqlgTraversal<Vertex, Map<Object, Object>> traversal = (DefaultSqlgTraversal<Vertex, Map<Object, Object>>)this.sqlgGraph.traversal().V().hasLabel("A")
                .elementMap(columns.toArray(new String[]{}))
                .limit(10);

        printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgElementMapStep<?,?>);
        List<Map<Object, Object>> values = traversal.toList();
        Assert.assertEquals(10, values.size());

        //Test with order by
        traversal = (DefaultSqlgTraversal<Vertex, Map<Object, Object>>)this.sqlgGraph.traversal().V().hasLabel("A")
                .elementMap(columns.toArray(new String[]{}))
                .order().by("name", Order.desc)
                .limit(10);

        printTraversalForm(traversal);
        Assert.assertEquals(2, traversal.getSteps().size());
        Assert.assertTrue(traversal.getSteps().get(0) instanceof SqlgGraphStep);
        Assert.assertTrue(traversal.getSteps().get(1) instanceof SqlgElementMapStep<?,?>);
        values = traversal.toList();
        Assert.assertEquals(10, values.size());
        Assert.assertEquals("b", values.get(9).get("name"));
    }
}
